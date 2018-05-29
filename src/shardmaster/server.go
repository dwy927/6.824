package shardmaster

import (
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

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs    []Config               // indexed by config num
	notifyChs  map[int]chan struct{}  // per log entry index in raft log
	duplicate  map[int64]*LatestReply // ClientID -> the latest reply in this client
	shutdownCh chan struct{}
}

// for duplicate
type LatestReply struct {
	Seq int // latest request
}

// for raft.start()
type Op struct {
	// Your data here.
	ClientID  int64 // for duplicate request detection
	SeqNo     int
	Operation string // must be one of "Join", "Leave", "Move" and "Query"

	Servers map[int][]string // args of "Join"
	GIDs    []int            // args of "Leave"
	Shard   int              // args of "Move"
	GID     int              // args of "Move"
	Num     int              // args of "Query" desired config number
}

// common function
func (sm *ShardMaster) requestAgreement(cmd Op, fillReply func(bool)) {
	// check is Leader
	if _, isLeader := sm.rf.GetState(); !isLeader {
		fillReply(false)
		return
	}

	sm.mu.Lock()

	// check duplication
	if dup, ok := sm.duplicate[cmd.ClientID]; ok {
		if cmd.SeqNo == dup.Seq {
			sm.mu.Unlock()
			fillReply(true)
			return
		}
	}

	// notify Raft to aggrement
	index, term, _ := sm.rf.Start(cmd)
	ch := make(chan struct{})
	sm.notifyChs[index] = ch
	sm.mu.Unlock()

	// assume it will success
	fillReply(true)

	// wait for raft to complete aggrement
	select {
	case <-sm.shutdownCh:
	case <-ch:
		// loose leadership?
		currentTerm, isLeader := sm.rf.GetState()
		if !isLeader || term != currentTerm {
			fillReply(false)
			return
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		SeqNo:     args.SeqNo,
		Operation: "Join",
		Servers:   args.Servers,
	}

	sm.requestAgreement(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		SeqNo:     args.SeqNo,
		Operation: "Leave",
		GIDs:      args.GIDs,
	}

	sm.requestAgreement(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		SeqNo:     args.SeqNo,
		Operation: "Move",
		Shard:     args.Shard,
		GID:       args.GID,
	}

	sm.requestAgreement(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""

		}
	})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op{
		ClientID:  args.ClientID,
		SeqNo:     args.SeqNo,
		Operation: "Query",
		Num:       args.Num,
	}

	sm.requestAgreement(cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK

			sm.mu.Lock()
			sm.copyConfig(cmd.Num, &reply.Config)
			sm.mu.Unlock()
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

type Status struct {
	group, count int
}

// should only be called when holding the lock
// target: minimum shard movement
func (sm *ShardMaster) rebalance(config *Config) {
	groupNum := len(config.Groups)
	shardNum := len(config.Shards)

	if groupNum == 0 {
		return
	}

	avgShard := shardNum / groupNum
	leftShard := shardNum - avgShard*groupNum

	// map gid -> shards
	var reverse = make(map[int][]int)
	for shard, gid := range config.Shards {
		reverse[gid] = append(reverse[gid], shard)
	}

	var lacking []Status
	var extra []Status
	lackingNum, extraNum := 0, 0

	// check for Jion gids
	for gid, _ := range config.Groups {
		if _, ok := reverse[gid]; !ok {
			if avgShard > 0 {
				lacking = append(lacking, Status{gid, avgShard})
				lackingNum += avgShard
			}
		}
	}

	// check for Leave gids
	for gid, shards := range reverse {
		if _, ok := config.Groups[gid]; !ok {
			extra = append(extra, Status{gid, len(shards)})
			extraNum += len(shards)
		} else {
			if len(shards) > avgShard {
				if leftShard == 0 {
					extra = append(extra, Status{gid, len(shards) - avgShard})
					extraNum += len(shards) - avgShard
				} else if len(shards) == avgShard+1 {
					leftShard--
				} else if len(shards) > avgShard+1 {
					extra = append(extra, Status{gid, len(shards) - avgShard - 1})
					extraNum += len(shards) - avgShard - 1
					leftShard--
				}
			} else if len(shards) < avgShard {
				lacking = append(lacking, Status{gid, avgShard - len(shards)})
				lackingNum += avgShard - len(shards)
			}
		}
	}

	if leftShard > 0 {
		if len(lacking) < leftShard {
			diff := leftShard - len(lacking)
			for gid, _ := range config.Groups {
				if len(reverse[gid]) == avgShard {
					lacking = append(lacking, Status{gid, 0})
					diff--
					if diff == 0 {
						break
					}
				}
			}
		}
		for i, _ := range lacking {
			lacking[i].count++
			leftShard--
			if leftShard == 0 {
				break
			}
		}
	}
	// modify reverse
	for len(lacking) != 0 && len(extra) != 0 {
		DPrintf("[%d]len(lacking) = %d, len(extra) = %d", sm.me, len(lacking), len(extra))
		l, e := lacking[0], extra[0]
		src, dest := e.group, l.group
		if e.count > l.count {
			// move l.count from e to l
			balance(reverse, src, dest, l.count)
			lacking = lacking[1:]
			extra[0].count -= l.count
		} else if e.count < l.count {
			// move e.count from e to l
			balance(reverse, src, dest, e.count)
			extra = extra[1:]
			lacking[0].count -= e.count
		} else {
			balance(reverse, src, dest, e.count)
			lacking = lacking[1:]
			extra = extra[1:]
		}
	}
	DPrintf("[%d]len(lacking) = %d, len(extra) = %d", sm.me, len(lacking), len(extra))
	if len(lacking) != 0 || len(extra) != 0 {
		// DPrintf("extra - lacking: %v <-> %v, %v\n", extra, lacking, config)
		panic("re-balance function bug")
	}

	for k, v := range reverse {
		for _, s := range v {
			config.Shards[s] = k
		}
	}
	// DPrintf("after rebalance, config.Shards: %v\n", config.Shards)
}

// helper function
func balance(data map[int][]int, src, dst, cnt int) {
	s, d := data[src], data[dst]
	if cnt > len(s) {
		DPrintf("cnd > len(s): %d <-> %d\n", cnt, len(s))
		panic("Oops...")
	}
	e := s[:cnt]
	d = append(d, e...)
	s = s[cnt:]

	data[src] = s
	data[dst] = d
}

// should only be called when holding the lock
func (sm *ShardMaster) copyConfig(index int, config *Config) {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	config.Groups = make(map[int][]string)
	for k, v := range sm.configs[index].Groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}
}

// should only be called when holding the lock
func (sm *ShardMaster) joinConfig(servers map[int][]string) {
	// step 1: struct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	//add or update new GID-Servers
	for k, v := range servers {
		var serversList = make([]string, len(v))
		copy(serversList, v)
		config.Groups[k] = serversList
	}

	// step 2. rebalance
	sm.rebalance(&config)

	// step 3. store the new config
	sm.configs = append(sm.configs, config)
	// fmt.Printf("DEBUG: join config change %v\n", config)

}

// should only be called when holding the lock
func (sm *ShardMaster) leaveConfig(gids []int) {
	// step 1: struct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	//add or update new GID-Servers
	for _, gid := range gids {
		delete(config.Groups, gid)
	}
	// step 2. rebalance
	sm.rebalance(&config)

	// step 3. store the new config
	sm.configs = append(sm.configs, config)
	// fmt.Printf("DEBUG: leave config change %v\n", config)

}

// should only be called when holding the lock
func (sm *ShardMaster) moveConfig(shard int, gid int) {
	// step 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	// step 2. no need to re-balance, just move shards allocation
	config.Shards[shard] = gid

	// step 3. store the new config
	sm.configs = append(sm.configs, config)
	// fmt.Printf("DEBUG: move config change %v\n", config)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdownCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) applyDaemon() {
	for {
		select {
		case <-sm.shutdownCh:
			DPrintf("shutdown")
			return
		case msg, ok := <-sm.applyCh:
			if ok && msg.Command != nil {
				cmd := msg.Command.(Op)
				sm.mu.Lock()
				if dup, ok := sm.duplicate[cmd.ClientID]; !ok || dup.Seq < cmd.SeqNo {
					sm.duplicate[cmd.ClientID] = &LatestReply{cmd.SeqNo}

					switch cmd.Operation {
					case "Join":
						sm.joinConfig(cmd.Servers)
					case "Leave":
						sm.leaveConfig(cmd.GIDs)
					case "Move":
						sm.moveConfig(cmd.Shard, cmd.GID)
					case "Query":
						// no need to modify config
					default:
						panic("invalid command operation")
					}
				}

				if notifyCh, ok := sm.notifyChs[msg.CommandIndex]; ok && notifyCh != nil {
					close(notifyCh)
					delete(sm.notifyChs, msg.CommandIndex)
				}
				sm.mu.Unlock()
			}
		}
	}

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	// -----以后再看~！！！！---------
	labgob.Register(Op{})
	// -----------------------------

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.shutdownCh = make(chan struct{})
	sm.notifyChs = make(map[int]chan struct{})
	sm.duplicate = make(map[int64]*LatestReply)

	go sm.applyDaemon()

	return sm
}
