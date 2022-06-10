package kvraft

import (
	"824/labgob"
	"824/labrpc"
	"824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// 操作类型, Key, Value
	// 本次操作的ID, 最近一次操作的ID
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Raft的lastApplied
	// 最新的键值状态
	// 为了保证幂等性, 需要记录处理过的操作ID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	// 对于args携带的最近一次请求ID, 由于Clerk已经知道它处理成功, 所以Server不再需要记录其状态

	// 检查该操作是否处理过, 如果是, 获取Value并设置Err为OK, 直接返回

	// 创建一个Op, 通过rf.Start传递给下层的Raft
	// 如果返回的第三个值是false, 设置Err为ErrWrongLeader, 返回

	// 等待Raft达成一致并提交
	// 在此过程中若出现了新的term, 返回失败

	// Get请求已被Raft提交, 获取Value并设置Err为OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// 与Get逻辑相同
}

// 提取出 apply 操作, 在 KVServer 初始化时开启一条goroutine
// 接收来自 applyCh 的消息并更新键值状态
func (kv *KVServer) apply() {
	for !kv.killed() {
		m := <-kv.applyCh
		// 区分Command和Snapshot两种情况

		// Command:
		// 检查Op的ID, 避免重复操作
		// 更新键值状态和lastApplied
		// 检查是否有必要生成快照, 若有则调用rf.Snapshot

		// Snapshot:
		// 调用rf.CondInstallSnapshot检查是否要decode
	}
}

// 生成快照
func (kv *KVServer) encodeSnapshot() []byte {
	return nil
}

// 根据快照更新状态
func (kv *KVServer) decodeSnapshot(data []byte) {

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.decodeSnapshot(persister.ReadSnapshot())

	go kv.apply()

	return kv
}
