package shardkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Database map[string]string
type Duplicate map[int64]*LatestReply

type LatestReply struct {
	Seq   int
	Reply GetReply
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Get", "Put" or "Append"
	ClientID int64
	SeqNo    int
}

// new configuration to switch
type Cfg struct {
	Config shardmaster.Config
}

type Mig struct {
	Num   int
	Shard int
	Gid   int
	Data  Database
	Dup   Duplicate
}

type CleanUp struct {
	Num   int
	Shard int
	Gid   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shutdownCh    chan struct{}
	db            Database         // database
	duplicate     Duplicate        // duplication detection table
	notifyChs     map[int]chan Err // per raft log index -> notify channel
	persist       *raft.Persister  // store snapshot
	snapshotIndex int
	mck           *shardmaster.Clerk   // talk to master
	configs       []shardmaster.Config //configs[0] is the current config
	workList      map[int]MigrateWork  // config No. -> work to be done
	gcHistory     map[int]int          // shard->config
}

type MigrateWork struct {
	RecFrom []Item
	Last    shardmaster.Config
}
type Item struct {
	Shard, Gid int
}

func printDup(dup Duplicate) {
	for clientId, latestReply := range dup {
		DPrintf("clientID:%d, dup.Seq:%d\n", clientId, latestReply.Seq)
	}
}

// RPC function
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// defer func() {
	// 	DPrintf("[%d-%d]: Get args: %v, reply: %v. (shard: %d)\n", kv.gid, kv.me, args, reply, key2shard(args.Key))
	// }()

	// not leader?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	shard := key2shard(args.Key)
	kv.mu.Lock()
	// whether to responsible for this key?
	if kv.configs[0].Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// whether already receive data from previous owner
	cur := kv.configs[0].Num
	if work, ok := kv.workList[cur]; ok {
		recFrom := work.RecFrom
		for _, item := range recFrom {
			// check whether still in migration,waiting data? postpone client request
			if shard == item.Shard {
				kv.mu.Unlock()
				reply.Err = ErrWrongGroup
				return
			}
		}
	}

	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		if args.SeqNo == dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.Reply.Value
			return
		}
	}

	cmd := Op{
		Key:      args.Key,
		Op:       "Get",
		ClientID: args.ClientID,
		SeqNo:    args.SeqNo,
	}

	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan Err)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = true
	reply.Err = ""

	// wait for raft to complete aggrement
	select {
	case <-kv.shutdownCh:
		return
	case err, ok := <-ch:
		if !ok {
			DPrintf("DEBUG")
			panic("aaa")
			return
		}
		// check for leadership
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
			reply.Err = ""
			DPrintf("DEBUG at GET")
			return
		}

		reply.Err = err
		reply.WrongLeader = false
		if err == OK {
			kv.mu.Lock()
			if value, ok := kv.db[args.Key]; ok {
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	}
}

// RPC function
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		DPrintf("[%d-%d]: PutAppend args: %v, reply: %v. (shard: %d, Num: %d)\n", kv.gid, kv.me, args, reply, key2shard(args.Key), kv.configs[0].Num)
		if dup, ok := kv.duplicate[args.ClientID]; (!ok || dup.Seq < args.SeqNo) && reply.Err == OK {
			DPrintf("FAIL: dup.Seq(%d) > args.SeqNo(%d)", dup.Seq, args.SeqNo)
			panic("dup.Seq > args.SeqNo")
		}
	}()

	// not leader?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	shard := key2shard(args.Key)
	kv.mu.Lock()
	// whether to responsible for this key?
	if kv.configs[0].Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// whether already receive data from previous owner
	cur := kv.configs[0].Num
	if work, ok := kv.workList[cur]; ok {
		recFrom := work.RecFrom
		for _, item := range recFrom {
			// check whether still in migration,waiting data? postpone client request
			if shard == item.Shard {
				kv.mu.Unlock()
				reply.Err = "inMigration"
				return
			}
		}
	}

	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		if args.SeqNo == dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	// new request
	cmd := Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		ClientID: args.ClientID,
		SeqNo:    args.SeqNo,
	}

	DPrintf("[%d-%d]DEBUG: kv.rf.Start(%v) @num(%d)", kv.gid, kv.me, cmd, kv.configs[0].Num)

	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan Err)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = true
	reply.Err = "shutdown"

	// wait for raft to complete aggrement
	select {
	case err := <-ch:
		// check for leadership
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.Err = ""
			DPrintf("DEBUG at PutAppend")
			return
		}
		reply.Err = err
		reply.WrongLeader = false
		// DPrintf("[%d-%d]: PutAppend args: %v, reply: %v. (shard: %d, Num: %d)\n", kv.gid, kv.me, args, reply, key2shard(args.Key), kv.configs[0].Num)
	case <-kv.shutdownCh:
		return
	}
}

// RPC function
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	defer func() {
		DPrintf("[%d-%d]: request Migrate, args: %v, reply: %t, %q, db: %v.\n", kv.gid, kv.me, args,
			reply.WrongLeader, reply.Err, reply.Data)
		printDup(reply.Dup)
	}()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if receive unexpected newer config migrate?
	// postpone until itself detect new config
	if args.Num > kv.configs[0].Num {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	reply.Data, reply.Dup = kv.copyDataDup(args.Shard)
	reply.WrongLeader = false
	reply.Err = OK
	reply.Shard = args.Shard
	reply.Num = args.Num
	reply.Gid = kv.gid
	// DPrintf("[%d-%d]: request Migrate, args: %v, reply: %t, %q, db: %v.\n", kv.gid, kv.me, args,
	// 	reply.WrongLeader, reply.Err, reply.Data)
	return
}

// should be called when holding the lock
func (kv *ShardKV) copyDataDup(shard int) (Database, Duplicate) {
	data := make(Database)
	dup := make(Duplicate)
	for k, v := range kv.db {
		if key2shard(k) == shard {
			data[k] = v
		}
	}
	for k, v := range kv.duplicate {
		latestReply := &LatestReply{v.Seq, v.Reply}
		dup[k] = latestReply
	}
	return data, dup
}

// RPC: called by other group to notify cleaning up unnecessary Shards
func (kv *ShardKV) CleanUp(args *CleanUpArgs, reply *CleanUpReply) {
	defer func() {
		DPrintf("[%d-%d]: request CleanUp, args: %v, reply: %v.\n", kv.gid, kv.me, args, reply)
	}()

	// not leader?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if receive unexpected newer config cleanup? postpone until itself detect new config
	if args.Num > kv.configs[0].Num {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	if kv.gcHistory[args.Shard] < args.Num {
		kv.rf.Start(CleanUp{Num: args.Num, Shard: args.Shard, Gid: args.Gid})
	}

	reply.WrongLeader = false
	reply.Err = OK
}

// garbage collection of state when lost ownership
// should be called when holding the lock
func (kv *ShardKV) shardGC(args CleanUp) {
	for k := range kv.db {
		if key2shard(k) == args.Shard {
			delete(kv.db, k)
		}
	}
	DPrintf("[%d-%d]: server %d has gc shard: %d @ config: %d, from gid: %d\n",
		kv.gid, kv.me, kv.me, args.Shard, args.Num, args.Gid)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdownCh)
}

func (kv *ShardKV) restartMigration() {
	kv.mu.Lock()
	cur := kv.configs[0].Num
	work, ok := kv.workList[cur]
	kv.mu.Unlock()

	if cur == 0 || !ok {
		return
	}

	for {
		select {
		case <-kv.shutdownCh:
			return
		default:
			kv.mu.Lock()
			now := kv.configs[0].Num
			kv.mu.Unlock()

			if cur != now {
				DPrintf("done....\n")
				return
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				go kv.requestShards(&work.Last, now)
			}

			time.Sleep(100 * time.Millisecond)
			// DPrintf("[%d-%d]: restartMigration: config: %d, work: %v\n", kv.gid, kv.me, now, work.RecFrom)
		}
	}

}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	kv.db = make(Database)
	kv.duplicate = make(Duplicate)

	d.Decode(&kv.db)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.duplicate)
	d.Decode(&kv.configs)
	d.Decode(&kv.workList)
	d.Decode(&kv.gcHistory)

	DPrintf("[%d-%d]: server %d read snapshot (configs: %v, worklist: %v).\n",
		kv.gid, kv.me, kv.me, kv.configs, kv.workList)

	// if snapshot occurs in middle of migration
	if kv.configs[0].Num != 0 {
		go kv.restartMigration()
	}
}

func (kv *ShardKV) generateSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.db)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.duplicate)
	e.Encode(kv.configs)
	e.Encode(kv.workList)
	e.Encode(kv.gcHistory)

	data := w.Bytes()
	kv.persist.SaveSnapshot(data)

	DPrintf("[%d-%d]: server %d generate snapshot (configs: %v, worklist: %v).\n",
		kv.gid, kv.me, kv.me, kv.configs, kv.workList)
}

func needSnapshot(kv *ShardKV) bool {
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

// should be called when holding the lock
// all server in a replica group switch to new config at the same point
func (kv *ShardKV) switchConfig(new *shardmaster.Config) {
	var old = &kv.configs[0]
	kv.generateWorkList(old, new)

	DPrintf("[%d-%d]: server %d switch to new config (%d->%d, shards: %v, workList: %v, configs: %v).\n",
		kv.gid, kv.me, kv.me, kv.configs[0].Num, new.Num, new.Shards, kv.workList[new.Num], len(kv.configs))

	if len(kv.configs) > 2 {
		log.Println(new.Num, kv.configs)
		panic("len(kv.configs) > 2")
	}

	// for all servers, switch to new config
	if len(kv.configs) == 1 {
		kv.configs[0] = *new
	} else {
		if kv.configs[1].Num != new.Num {
			log.Println(new.Num, kv.configs)
			panic("kv.configs[1].Num != new.Num")
		}
		kv.configs = kv.configs[1:]
	}

	// for leader
	if _, isLeader := kv.rf.GetState(); isLeader {
		if work, ok := kv.workList[new.Num]; ok && len(work.RecFrom) > 0 {
			go kv.requestShards(old, new.Num)
		}
	}
	// go kv.restartMigration()
}

// should be called when holding the lock
func (kv *ShardKV) generateWorkList(old *shardmaster.Config, new *shardmaster.Config) {
	// 0 is initial state, no need to send any shard
	if old.Num == 0 {
		return
	}

	if new.Num != old.Num+1 {
		panic("new.Num != old.Num + 1")
	}

	var oldShards, newShards = make(map[int]bool), make(map[int]bool)
	for shard, gid := range old.Shards {
		if gid == kv.gid {
			oldShards[shard] = true
		}
	}

	for shard, gid := range new.Shards {
		if gid == kv.gid {
			newShards[shard] = true
		}
	}

	var recFrom []Item
	for shard, _ := range newShards {
		if _, ok := oldShards[shard]; !ok {
			recFrom = append(recFrom, Item{shard, old.Shards[shard]})
		}
	}

	if len(recFrom) != 0 {
		kv.workList[new.Num] = MigrateWork{RecFrom: recFrom, Last: *old}
	} else {
		// DPrintf("[%d-%d] no need mig @%d", kv.gid, kv.me, new.Num)
	}

}

// using new config's Num.
func (kv *ShardKV) requestShards(old *shardmaster.Config, num int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	recFrom := kv.workList[num].RecFrom

	fillMigrateArgs := func(shard int) *MigrateArgs {
		return &MigrateArgs{
			Num:   num,
			Shard: shard,
			Gid:   kv.gid,
		}
	}

	replyHandler := func(reply *MigrateReply) {
		// still leader？
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if reply.Num == kv.configs[0].Num && !kv.isMigrateDone("replyHandler") {
			var mig = Mig{Num: reply.Num, Shard: reply.Shard, Gid: reply.Gid, Data: reply.Data, Dup: reply.Dup}
			kv.rf.Start(mig)
			DPrintf("[%d-%d]: leader %d receive shard: %d, from: %d).\n", kv.gid, kv.me, kv.me,
				reply.Shard, reply.Gid)
		}
	}

	// act as a client
	for _, task := range recFrom {
		go func(shard int, gid int) {
			args := fillMigrateArgs(shard)
			for {
				// shutdown?
				select {
				case <-kv.shutdownCh:
					return
				default:
				}
				// still leader?
				if _, isLeader := kv.rf.GetState(); !isLeader {
					DPrintf("DEBUG at requestShart")
					return
				}

				if servers, ok := old.Groups[gid]; ok {
					// try each server for shard
					for _, server := range servers {
						serv := kv.make_end(server)
						var reply MigrateReply
						ok := serv.Call("ShardKV.Migrate", args, &reply)
						if ok && !reply.WrongLeader && reply.Err == OK {
							replyHandler(&reply)
							return
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(task.Shard, task.Gid)
	}

}

func (kv *ShardKV) isMigrateDone(pos string) bool {
	_, ok := kv.workList[kv.configs[0].Num]
	defer func() {
		DPrintf("[%d-%d]: %s: Is migrate to %d Done? %t\n",
			kv.gid, kv.me, pos, kv.configs[0].Num, !ok)
	}()
	return !ok
}

func (kv *ShardKV) applyMigratedData(mig Mig) {
	// update data
	for k, v := range mig.Data {
		if key2shard(k) == mig.Shard {
			kv.db[k] = v
		}
	}

	for client, dup := range mig.Dup {
		d, ok := kv.duplicate[client]
		if ok {
			if d.Seq < dup.Seq {
				kv.duplicate[client].Seq = dup.Seq
				kv.duplicate[client].Reply = dup.Reply
			}
		} else {
			kv.duplicate[client] = dup
		}
	}

	if work, ok := kv.workList[mig.Num]; ok {
		recFrom := work.RecFrom
		var done = -1
		for i, item := range recFrom {
			if item.Shard == mig.Shard && item.Gid == mig.Gid {
				done = i
				// if leader, it's time to notify original owner to cleanup
				if _, isLeader := kv.rf.GetState(); isLeader {
					go kv.requestCleanUp(item.Shard, item.Gid, &work.Last)
				}
				break
			}
		}
		if done != -1 {
			tmp := recFrom[done+1:]
			recFrom = recFrom[:done]
			recFrom = append(recFrom, tmp...)

			// done
			if len(recFrom) == 0 {
				delete(kv.workList, mig.Num)
				return
			}
			// update
			kv.workList[mig.Num] = MigrateWork{recFrom, kv.workList[mig.Num].Last}
		}
	}
}

// notify cleanup garbage shard
func (kv *ShardKV) requestCleanUp(shard, gid int, config *shardmaster.Config) {
	args := &CleanUpArgs{
		Num:   kv.configs[0].Num,
		Shard: shard,
		Gid:   kv.gid,
	}

	DPrintf("[%d-%d]: leader %d issue cleanup shard: %d, gid: %d).\n", kv.gid, kv.me, kv.me, shard, gid)

	for {
		select {
		case <-kv.shutdownCh:
			return
		default:
		}

		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		if servers, ok := config.Groups[gid]; ok {
			// try every server for the shard
			for _, server := range servers {
				srv := kv.make_end(server)
				var reply CleanUpReply
				ok := srv.Call("ShardKV.CleanUp", args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value state machine
// then notify related client if it's leader
func (kv *ShardKV) applyDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d-%d]: server %d is shutting down.\n", kv.gid, kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {
				// have snapshot to apply
				if !msg.CommandValid {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					kv.generateSnapshot(msg.CommandIndex)
					kv.mu.Unlock()
					continue
				}
				// have client's request? must filter duplicate command
				if msg.Command != nil && msg.CommandIndex > kv.snapshotIndex {
					var err Err = ""
					kv.mu.Lock()
					switch cmd := msg.Command.(type) {
					case Op:
						// swith to new configuration already?
						DPrintf("[%d-%d] receive Op message in applyCh: %v", kv.gid, kv.me, cmd)
						shard := key2shard(cmd.Key)
						if kv.configs[0].Shards[shard] != kv.gid {
							// DPrintf("[%d-%d]: server %d (gid: %d) has switched to new config %d, "+
							// 	"no responsibility for shard %d\n",
							// 	kv.gid, kv.me, kv.me, kv.gid, kv.configs[0].Num, shard)
							err = ErrWrongGroup
							break
						}
						var pos string
						if dup, ok := kv.duplicate[cmd.ClientID]; !ok || cmd.SeqNo > dup.Seq {

							switch cmd.Op {
							case "Get":
								err = OK
								kv.duplicate[cmd.ClientID] = &LatestReply{
									Seq:   cmd.SeqNo,
									Reply: GetReply{Value: kv.db[cmd.Key]},
								}
								pos = "get"
							case "Put":
								err = OK
								kv.db[cmd.Key] = cmd.Value
								kv.duplicate[cmd.ClientID] = &LatestReply{
									Seq: cmd.SeqNo,
								}
								pos = "put"
							case "Append":
								err = OK
								kv.db[cmd.Key] += cmd.Value
								kv.duplicate[cmd.ClientID] = &LatestReply{
									Seq: cmd.SeqNo,
								}
								pos = "append"
								DPrintf("[%d-%d] duplicate table for append cmd updata(%d-%d)", kv.gid, kv.me, cmd.ClientID, cmd.SeqNo)
							default:
								DPrintf("[%d-%d]: server %d receive invalid cmd: %v\n", kv.gid, kv.me, kv.me, cmd)
								panic("invalid command operation")
							}
						}
						pos = pos
					case Cfg:

						if cmd.Config.Num == kv.configs[0].Num+1 {
							kv.switchConfig(&cmd.Config)
						} else if cmd.Config.Num > kv.configs[0].Num+1 {
							fmt.Printf("cmd.Config.Num(%d), kv.configs[0].Num(%d)\n", cmd.Config.Num, kv.configs[0].Num)
							panic("some thing wrong here")
						}
					case Mig:

						// apply data and dup, then start to accept client requests
						if cmd.Num == kv.configs[0].Num && !kv.isMigrateDone("applyMigrateData") {
							kv.applyMigratedData(cmd)
						} else if kv.isMigrateDone("applyMigrateData") {
							// fmt.Printf("kv.dup\n")
							// printDup(kv.duplicate)
							// fmt.Printf("cmd.dup\n")
							// printDup(cmd.Dup)

						}
					case CleanUp:
						if kv.gcHistory[cmd.Shard] < cmd.Num && cmd.Num <= kv.configs[0].Num {
							if kv.configs[0].Shards[cmd.Shard] != kv.gid {
								kv.shardGC(cmd)
								kv.gcHistory[cmd.Shard] = cmd.Num
							} else {
								kv.gcHistory[cmd.Shard] = kv.configs[0].Num
							}
						} else {
							DPrintf("[%d-%d]: server %d, shard: %d, config: %d - %d, gc history: %d\n",
								kv.gid, kv.me, kv.me, cmd.Shard, cmd.Num, kv.configs[0].Num, kv.gcHistory[cmd.Shard])
						}
					default:
						panic("Oops... unknown cmd type from applyCh")
					}

					// snapshot detection: up through msg.Index
					if needSnapshot(kv) {
						kv.generateSnapshot(msg.CommandIndex)
						kv.rf.NewSnapshot(msg.CommandIndex)
					}

					// notify channels
					if notifyCh, ok := kv.notifyChs[msg.CommandIndex]; ok && notifyCh != nil {
						//DPrintf("[%d-%d]DEBUG[%s-%d]: before notify, dup.Seq(%d), cmd.Seq(%d)", kv.gid, kv.me, pos, cmd.ClientID, kv.duplicate[cmd.ClientID].Seq, cmd.SeqNo)
						// pos = pos
						notifyCh <- err
						delete(kv.notifyChs, msg.CommandIndex)

					}

					kv.mu.Unlock()
				}
			}
		}
	}

}

func (kv *ShardKV) getNextConfigDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			return
		default:
			kv.mu.Lock()
			cur := kv.configs[0].Num
			kv.mu.Unlock()

			// try to get next config
			config := kv.mck.Query(cur + 1)

			// get new config
			kv.mu.Lock()
			if config.Num == kv.configs[0].Num+1 {

				if len(kv.configs) == 1 {
					kv.configs = append(kv.configs, config)
				} else if len(kv.configs) != 2 {
					fmt.Printf("len(kv.configs)=%d", len(kv.configs))
					panic("len(kv.configs)!= 1 or 2")
				}

				if _, isLeader := kv.rf.GetState(); isLeader && kv.isMigrateDone("getNextConfig()") {
					kv.rf.Start(Cfg{Config: kv.configs[1]})
					DPrintf("[%d-%d]: leader %d detect new config (%d->%d).\n", kv.gid, kv.me, kv.me,
						kv.configs[0].Num, kv.configs[1].Num)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Cfg{})
	labgob.Register(Mig{})
	labgob.Register(CleanUp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persist = persister
	kv.shutdownCh = make(chan struct{})
	kv.db = make(Database)
	kv.duplicate = make(Duplicate)
	kv.notifyChs = make(map[int]chan Err)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.configs = []shardmaster.Config{{}}
	kv.workList = make(map[int]MigrateWork)
	kv.gcHistory = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// read snapshot when start
	kv.readSnapshot(kv.persist.ReadSnapshot())

	go kv.applyDaemon()
	go kv.getNextConfigDaemon()

	DPrintf("[%d-%d] StartServer\n", kv.gid, kv.me)
	// DPrintf("[%d-%d] StartServer:\ndb: %v\ndup:%v\n", kv.gid, kv.me, kv.db, kv.duplicate)
	return kv
}
