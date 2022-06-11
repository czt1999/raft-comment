package shardkv

import (
	"824/labrpc"
	"824/shardctrler"
	"fmt"
	"sync/atomic"
	"time"
)
import "824/raft"
import "sync"
import "824/labgob"

type Op struct {
	Type   string
	Key    string
	Value  string
	ID     int64
	LastID int64

	ConfigNum int
	Shard     int
	State     map[string]string
	DupMap    map[int64]bool
	From      string
	To        []string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	mck        *shardctrler.Clerk
	lastConfig shardctrler.Config

	shards             map[int]bool
	sendFlags          []int32
	waitFlags          []int32
	shardsLock         []int32
	shardsToSend       []int
	shardsWaiting      []int
	lastIndexForShards []int32

	lastApplied int32
	state       map[int]map[string]string // 分片状态 { shard: { key: value } }
	dupMap      map[int]map[int64]bool    // 记录操作的处理状态，防止重复apply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	// 如果对应的Raft实例不是Leader, 返回ErrWrongLeader

	s := args.Shard
	// 检查分片s是否属于该组, 若不是则返回ErrWrongGroup

	// 对于args携带的最近一次请求ID, 由于Clerk已经知道它处理成功, 所以Server不再需要记录其状态

	// 检查该操作是否处理过, 如果是, 获取Value并设置Err为OK, 直接返回

	// 检查分片s是否在等待迁移
	for atomic.LoadInt32(&kv.shardsLock[s]) == 1 {
		time.Sleep(100 * time.Millisecond)
		if kv.killed() {
			return
		}

		// 若发现s已不属于该组, 返回ErrWrongGroup
	}

	// 创建一个Op, 通过rf.Start传递给下层的Raft
	cmd := Op{Type: "Get", Key: args.Key, Shard: s, ID: args.ID, LastID: args.LastID}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待raft达成一致并提交
	// 在此过程中若出现了新的term, 返回失败
	if !kv.waitRaft(index, term) {
		reply.Err = ErrWrongTerm
		return
	}

	// Get请求已被Raft提交, 获取Value并设置Err为OK
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// 与Get逻辑相同
}

// ShardKV 之间通过 RecieveShard 完成分区的迁移
func (kv *ShardKV) RecieveShard(args *RecieveShardArgs, reply *RecieveShardReply) {
	s := args.Shard

	// 幂等性
	if atomic.LoadInt32(&kv.waitFlags[s]) >= int32(args.ConfigNum) {
		reply.Err = OK
		return
	}

	op := Op{Type: "Gain", Shard: s, State: args.State, DupMap: args.OkMap, From: args.From, ConfigNum: args.ConfigNum}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.waitRaft(index, term) {
		reply.Err = ErrWrongTerm
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) getByKey(shard int, key string) string {
	v, ok := kv.state[shard][key]
	if ok {
		return v
	}
	return ""
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		m := <-kv.applyCh
		kv.mu.Lock()
		if m.CommandValid && m.CommandIndex > int(atomic.LoadInt32(&kv.lastApplied)) {
			op := m.Command.(Op)
			id, la, s, k := op.ID, op.LastID, op.Shard, op.Key
			switch op.Type {
			case "Put":
				// 避免重复操作
				if _, dup := kv.dupMap[s][id]; !dup {
					kv.state[s][k] = op.Value
				}
			case "Append":
				// 避免重复操作
				if _, dup := kv.dupMap[s][id]; !dup {
					val, hasVal := kv.state[s][k]
					if hasVal {
						kv.state[s][k] = val + op.Value
					} else {
						kv.state[s][k] = op.Value
					}
				}
			case "Lose":
				if atomic.LoadInt32(&kv.sendFlags[s]) < int32(op.ConfigNum) {
					atomic.StoreInt32(&kv.sendFlags[s], int32(op.ConfigNum))
				}
			case "Gain":
				if atomic.LoadInt32(&kv.waitFlags[s]) < int32(op.ConfigNum) {
					kv.state[s] = make(map[string]string)
					for k, v := range op.State {
						kv.state[s][k] = v
					}
					kv.dupMap[s] = make(map[int64]bool)
					for i := range op.DupMap {
						kv.dupMap[s][i] = true
					}
					atomic.StoreInt32(&kv.waitFlags[s], int32(op.ConfigNum))
				}
			}

			atomic.StoreInt32(&kv.lastApplied, int32(m.CommandIndex))

			// 标记该操作处理成功
			kv.dupMap[s][id] = true

			// 不需要记录已经被Clerk接受的操作
			delete(kv.dupMap[s], la)

			// 检查是否有必要生成快照
			if kv.maxraftstate > 0 && kv.maxraftstate < 7*kv.persister.RaftStateSize() {
				kv.rf.Snapshot(m.CommandIndex, kv.encodeSnapshot())
			}
		}
		kv.mu.Unlock()
	}
}

// fetchConfig 查询新配置并更新内部状态
// 1. 若因宕机/网络故障导致上次迁移过程中断, 会先完成剩下的部分
// 2. 只获取比当前配置版本更新一级的配置, 保证不因版本跳跃导致数据缺失
// 3. 对于迁出的分片, 会通过 sendShards 向目标分组发送分片数据
// 4. 对于迁入的分片, 会通过 waitShards 阻塞等待新的分片数据
func (kv *ShardKV) fetchConfig() {
	for !kv.killed() {

		// 检查是否上次迁移过程没完成
		ch := make(chan bool)
		check := 0

		kv.mu.Lock()
		// 迁出的分片
		if len(kv.shardsToSend) > 0 {
			go kv.sendShards(ch, kv.shardsToSend, kv.lastConfig)
			check += 1
		}
		// 迁入的分片
		if len(kv.shardsWaiting) > 0 {
			go kv.waitShards(ch, kv.shardsWaiting, kv.lastConfig)
			check += 1
		}
		kv.mu.Unlock()
		for i := 0; i < check; i++ {
			<-ch
		}

		kv.mu.Lock()
		kv.shardsToSend = nil
		kv.shardsWaiting = nil
		kv.mu.Unlock()

		// 查询更新一级的配置
		cfg := kv.mck.Query(-1)
		kv.mu.Lock()
		if cfg.Num-kv.lastConfig.Num > 1 {
			cfg = kv.mck.Query(kv.lastConfig.Num + 1)
		}

		if cfg.Num == kv.lastConfig.Num {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		newShards := make(map[int]bool)
		for s, g := range cfg.Shards {
			if g == kv.gid {
				newShards[s] = true
			}
		}

		// 分配的shard减少了
		// 向新的group迁移数据
		var st []int
		for s := range kv.shards {
			if _, ok := newShards[s]; !ok {
				st = append(st, s)
			}
		}
		kv.shardsToSend = st

		// 分配的shard增加了
		// 新增的shard在其他server上有数据，等待迁移
		var sw []int
		for s := range newShards {
			if _, ok := kv.shards[s]; !ok && kv.lastConfig.Shards[s] > 0 {
				sw = append(sw, s)
				atomic.StoreInt32(&kv.shardsLock[s], 1)
			}
		}
		kv.shardsWaiting = sw

		// 更新config
		kv.lastConfig = cfg
		kv.shards = newShards

		if kv.maxraftstate > 0 {
			kv.rf.Snapshot(int(atomic.LoadInt32(&kv.lastApplied)), kv.encodeSnapshot())
		}

		kv.mu.Unlock()

		done := make(chan bool)

		go kv.sendShards(done, st, cfg)
		go kv.waitShards(done, sw, cfg)

		for i := 0; i < 2; i++ {
			<-done
		}

		kv.mu.Lock()
		kv.shardsToSend = nil
		kv.shardsWaiting = nil
		if kv.maxraftstate > 0 {
			kv.rf.Snapshot(int(atomic.LoadInt32(&kv.lastApplied)), kv.encodeSnapshot())
		}
		kv.mu.Unlock()

	}
}

func (kv *ShardKV) sendShards(ch chan bool, shards []int, cfg shardctrler.Config) {
	rpcCh := make(chan bool)
	for _, shard := range shards {
		go func(s int) {
			// 迁移操作提交给raft
			op := Op{Type: "Lose", ConfigNum: cfg.Num, Shard: s, To: cfg.Groups[cfg.Shards[s]]}
			for !kv.killed() && atomic.LoadInt32(&kv.sendFlags[s]) < int32(cfg.Num) {
				index, term, isLeader := kv.rf.Start(op)
				if isLeader && kv.waitRaft(index, term) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			// 达成一致，可以发送RPC
			kv.mu.Lock()
			state := make(map[string]string)
			okMap := make(map[int64]bool)
			for k, v := range kv.state[s] {
				state[k] = v
			}
			for i := range kv.dupMap[s] {
				okMap[i] = true
			}
			kv.mu.Unlock()
			args := RecieveShardArgs{
				ConfigNum: cfg.Num,
				Shard:     s,
				State:     state,
				OkMap:     okMap,
				From:      fmt.Sprintf("%v-%v", kv.gid, kv.me),
			}
			// 轮询
			i := 0
			for !kv.killed() {
				end := kv.make_end(op.To[i])
				var reply RecieveShardReply
				if end.Call("ShardKV.RecieveShard", &args, &reply) && reply.Err == OK {
					break
				}
				i = (i + 1) % len(op.To)
			}
			rpcCh <- true
		}(shard)
	}
	for i := 0; i < len(shards); i++ {
		<-rpcCh
	}
	ch <- true
}

func (kv *ShardKV) waitShards(ch chan bool, shards []int, cfg shardctrler.Config) {
	rpcCh := make(chan bool)
	for _, shard := range shards {
		go func(s int) {
			for atomic.LoadInt32(&kv.waitFlags[s]) < int32(cfg.Num) {
				time.Sleep(100 * time.Millisecond)
			}
			atomic.StoreInt32(&kv.shardsLock[s], 0)
			rpcCh <- true
		}(shard)
	}
	for i := 0; i < len(shards); i++ {
		<-rpcCh
	}
	ch <- true
}

func (kv *ShardKV) waitRaft(index int, term int) bool {
	for !kv.killed() && index > int(atomic.LoadInt32(&kv.lastApplied)) {
		time.Sleep(10 * time.Millisecond)
		// 出现新的term，返回失败
		if checkTerm, _ := kv.rf.GetState(); checkTerm != term {
			return false
		}
	}
	return true
}

func (kv *ShardKV) encodeSnapshot() []byte {
	// ...
	return nil
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	// ...
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	//fmt.Printf("[%v-%v] killed \n", kv.gid, kv.me)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	return 1 == atomic.LoadInt32(&kv.dead)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	kv.state = make(map[int]map[string]string)
	kv.dupMap = make(map[int]map[int64]bool)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.state[i] = make(map[string]string)
	}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.dupMap[i] = make(map[int64]bool)
	}

	kv.shards = make(map[int]bool)
	kv.sendFlags = make([]int32, shardctrler.NShards)
	kv.waitFlags = make([]int32, shardctrler.NShards)
	kv.shardsLock = make([]int32, shardctrler.NShards)

	kv.decodeSnapshot(persister.ReadSnapshot())

	go kv.apply()
	go kv.fetchConfig()

	kv.rf.Start(Op{})

	return kv
}
