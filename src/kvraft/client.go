package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

var clients = make(map[int64]bool)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int   // remember the last leader
	sequence int   // RPC sequence number
	id       int64 // the Clerk client id

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func generateID() int64 {
	for {
		x := nrand()
		if clients[x] {
			continue
		}
		clients[x] = true
		return x
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.sequence = 1
	ck.id = generateID()
	DPrintf("Clerk: %d\n", ck.id)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("Clerk: Get %s\n", key)
	// You will have to modify this function.
	cnt := len(ck.servers)
	for {
		args := &GetArgs{
			Key:      key,
			ClientId: ck.id,
			Sequence: ck.sequence,
		}

		reply := new(GetReply)
		ck.leader %= cnt

		done := make(chan bool)
		go func() {
			ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
			done <- ok
		}()

		select {
		case <-time.After(200 * time.Millisecond): // RPC timeout: 200ms
			ck.leader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader {
				ck.sequence++
				if reply.Err == OK {
					return reply.Value
				}
				return ""
			}
			ck.leader++
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("Clerk: PutAppend: %s => (%s,%s) from %d\n", op, key, value, ck.id)
	// You will have to modify this function.
	cnt := len(ck.servers)
	for {
		args := &PutAppendArgs{
			Key:      key,
			Value:    value,
			Op:       op,
			ClientId: ck.id,
			Sequence: ck.sequence,
		}

		reply := new(PutAppendReply)
		ck.leader %= cnt
		done := make(chan bool)
		go func() {
			ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
			done <- ok
		}()
		select {
		case <-time.After(200 * time.Millisecond):
			ck.leader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader && reply.Err == OK {
				ck.sequence++
				return
			}
			ck.leader++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
