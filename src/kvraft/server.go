package raftkv

import (
	"bytes"
	"encoding/gob"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientID  int64
	Sequence  int
}

type LatestReply struct {
	Sequence int      // latest request
	Reply    GetReply // latest reply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persist       *raft.Persister
	db            map[string]string
	snapshotIndex int
	notifyChs     map[int]chan struct{} // per log

	// shut down channel
	shoudownCh chan struct{}

	// duplication detection table
	duplicate map[int64]*LatestReply
}

func needSnapshot(kv *KVServer) bool {
	if kv.maxraftstate < 0 {
		return false
	}

	if kv.maxraftstate < kv.persist.RaftStateSize() {
		return true
	}

	// abs < 10% of max
	var abs = kv.maxraftstate - kv.persist.RaftStateSize()
	var threshold = kv.maxraftstate / 10
	if abs < threshold {
		return true
	}
	return false
}

func (kv *KVServer) generateSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index
	e.Encode(kv.db)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.duplicate)

	data := w.Bytes()
	kv.persist.SaveSnapshot(data)
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.db)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.duplicate)

}

// Get RPC
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//not leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}
	DPrintf("[%d]: leader %d receive RPC: Get(%s)\n", kv.me, kv.me, args.Key)

	kv.mu.Lock()
	if dup, ok := kv.duplicate[args.ClientId]; ok {
		if args.Sequence == dup.Sequence {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.Reply.Value
			return
		}
	}

	cmd := Op{
		Key:       args.Key,
		Operation: "Get",
		ClientID:  args.ClientId,
		Sequence:  args.Sequence,
	}
	index, term, _ := kv.rf.Start(cmd)

	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	select {
	case <-kv.shoudownCh:
		return
	case <-ch:
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != currentTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}

		kv.mu.Lock()
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		DPrintf("[%d]: leader %d return RPC: Get(%s)\n", kv.me, kv.me, args.Key)
	}
}

// PutAppend RPC
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// not leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}
	DPrintf("[%d]: leader %d receive RPC: PutAppend(%s => (%s, %s), (%d-%d))\n", kv.me, kv.me, args.Op, args.Key, args.Value, args.ClientId, args.Sequence)

	kv.mu.Lock()
	if dup, ok := kv.duplicate[args.ClientId]; ok {
		if args.Sequence == dup.Sequence {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	cmd := Op{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ClientID:  args.ClientId,
		Sequence:  args.Sequence,
	}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	select {
	case <-kv.shoudownCh:
		return
	case <-ch:
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != currentTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}
		DPrintf("[%d]: leader %d return RPC: PutAppend(%s => (%s, %s), (%d-%d))\n", kv.me, kv.me, args.Op, args.Key, args.Value, args.ClientId, args.Sequence)
	}
}

func (kv *KVServer) applyDaemon() {
	for {
		select {
		case <-kv.shoudownCh:
			DPrintf("[%d]: server %d is shutting down.\n", kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {
				// use snapshot
				if !msg.CommandValid {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					// must be persisted, in case of crashing before generating another snapshot
					kv.generateSnapshot(msg.CommandIndex)
					kv.mu.Unlock()
					continue
				}

				//-------------------------

				if msg.Command != nil && msg.CommandIndex > kv.snapshotIndex {
					cmd := msg.Command.(Op)
					kv.mu.Lock()
					dup, ok := kv.duplicate[cmd.ClientID]
					if !ok || dup.Sequence < cmd.Sequence {
						switch cmd.Operation {
						case "Get":
							kv.duplicate[cmd.ClientID] = &LatestReply{
								Sequence: cmd.Sequence,
								Reply: GetReply{
									Value: kv.db[cmd.Key],
								},
							}
						case "Put":
							kv.db[cmd.Key] = cmd.Value
							kv.duplicate[cmd.ClientID] = &LatestReply{
								Sequence: cmd.Sequence,
							}
						case "Append":
							kv.db[cmd.Key] += cmd.Value
							kv.duplicate[cmd.ClientID] = &LatestReply{
								Sequence: cmd.Sequence,
							}
						default:
							panic("invalid command operation")
						}
					}

					// snapshot detection: up through msg.Index
					if needSnapshot(kv) {
						// save snapshot and notify raft
						DPrintf("[%d]: server %d need generate snapshot @ %d (%d vs %d), client: %d.\n",
							kv.me, kv.me, msg.CommandIndex, kv.maxraftstate, kv.persist.RaftStateSize(), cmd.ClientID)
						kv.generateSnapshot(msg.CommandIndex)
						kv.rf.NewSnapshot(msg.CommandIndex)
						kv.persist.RaftStateSize()
					}

					// notify channel
					if notifyCh, ok := kv.notifyChs[msg.CommandIndex]; ok && notifyCh != nil {
						close(notifyCh)
						delete(kv.notifyChs, msg.CommandIndex)
					}
					kv.mu.Unlock()

				}

			}

		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	close(kv.shoudownCh)
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persist = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)             // store key-value pairs
	kv.notifyChs = make(map[int]chan struct{})  // per log
	kv.duplicate = make(map[int64]*LatestReply) // duplication detection table: ClientId -> (sequence, GetReply)
	kv.readSnapshot(kv.persist.ReadSnapshot())

	// shutdown channel
	kv.shoudownCh = make(chan struct{})

	go kv.applyDaemon()

	return kv
}
