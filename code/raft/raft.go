package raft

import (
	"824/labgob"
	"bytes"
	"errors"
	"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"824/labgob"
	"824/labrpc"
)

type ApplyMsg struct {
	// omitted
}

type Entry struct {
	// omitted
}

type Raft struct {
	mu        sync.Mutex          
	peers     []*labrpc.ClientEnd 
	persister *Persister          
	me        int                 
	dead      int32               

	applyCh chan ApplyMsg

	// 需要持久化的字段, 参考论文 Figure 2, 另外还要考虑 Lab 3 的 snapshot

	// 可在 crash 后零初始化的字段, 参考论文 Figure 2, 另外还要考虑 leader 和心跳机制
}

func (rf *Raft) GetState() (int, bool) {

	// 返回 curTerm 和 isLeader, 注意线程安全

}

func (rf *Raft) encodeState() []byte {

	// 抽取 encode 逻辑

}

func (rf *Raft) persist() {

	// 使用 rf.persister.SaveStateAndSnapshot 保存状态和快照信息

}

func (rf *Raft) readPersist(state []byte, snapshot []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	var (
		i1 int
		i2 int
		i3 int
		i4 int
		e  []Entry
	)

	// read and decode ...

}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// 按上方注解要求, 当 lastApplied 和第 2 个参数相等时返回 true

}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

	// 开启一个 goroutine, 生成快照, 否则会跟 cfg 里的 apply 循环形成死锁

}

func (rf *Raft) makeSnapshot(index int, snapshot []byte) {

	// 裁剪日志, 生成快照, 更新状态, 发送消息到 applyCh

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// 设置一个持久化标志, 并用 defer 检查

	// 对于任何情况, 都先让 reply.Term = curTerm


	// 1) 比当前 term 大
	// 2) 与当前 term 相等且这个时候还没有 leader 并且没有给其他节点投票

	var cond bool
	if cond {
		lastTerm := rf.logAt(rf.lastIndex).Term

		// 对方的状态至少要跟自己一样新才能投票

		if cond {

			// 更新心跳标志, 等待 leader 的产生

		}
		
		// 更新 curTerm, leader, 持久化标志

	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 设置一个持久化标志, 并用 defer 检查

	// 对于任何情况, 都先让 reply.Term = curTerm

	// term 比自己小, 拒绝

	// term 相同
	if rf.curTerm == args.Term {
		
		// 当前无 leader, 或给它投了票, 接受这个新的 leader, 重置 voteFor 

		// 当前有 leader 但不是发送者, 说明算法实现有问题, 可以考虑 log.Fatalf 打印异常状态
	}

	// term 比自己大
	if rf.curTerm < args.Term {

		// 更新 leader 和 term, 重置 voteFor 

	}

	// 更新心跳接收状态

	// 检查 PreLogIndex 和对应的 term 是否与自己一致
	// 若不一致, 将 PrevLogIndex - 1 项对应的 term 中最小的 index 赋给 reply.NextIndex

	// 删除冲突的项, 如果有剩下的, 添加到日志, 更新状态

	if args.LeaderCommit > rf.commitIndex {

		// 更新 commitIndex

	}

	// 设置 Success
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	// 更新 curTerm, leader, 心跳状态

	// 仅当 args.LastIncludedIndex 大于当前快照的 index 时才执行修改操作

	var cond bool
	if cond {

		// 先执行对日志和 commitIndex, lastApplied 的修改
		// 后执行对 snapshot 状态的修改

		// 异步发送消息到 applyCh
	}

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// index :=
	// term :=
	// isLeader :=

	// 如果是 leader, 生成新的 Entry 并追加到日志, 持久化后广播给其他节点

	return 0, 0, false
}

func (rf *Raft) applyTicker() {

	// 本方法用于异步地定时检查日志, 并进行 apply
	// 注意: 
	// 1) 传递消息到 applyCh 时要释放锁
	// 2) 按照 Figure 2 的 5.3 和 5.4 更新 commitIndex
	
	for !rf.killed() {

		// 间隔 10ms 是可行的
	
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	// 根据当前节点的编号进行 rand.Seed()

	for !rf.killed() {

		// 设置心跳标志为等待

		// 根据 HeartbeatInterval 生成一个随机的 timeout

		// 定时检查心跳标志

		// 若超时后依旧在等待状态, 启动选举

	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化状态, 在开启快照的情况下进行必要的调整

	go rf.ticker()

	// 开启一个 goroutine 用于异步 apply

	return rf
}

// 下面这些方法用于在开启快照的情况下对 index 进行从逻辑到物理的映射

func (rf *Raft) logAt(index int) Entry {
	
	// 如果参数 index 小于生成快照的 index, 返回快照中的最后一个 Entry

	return nil
}

func (rf *Raft) logFrom(from int) []Entry {
	return nil
}

func (rf *Raft) logTo(to int) []Entry {
	return nil
}

func (rf *Raft) logBetween(from int, to int) []Entry {
	return nil
}

func (rf *Raft) logLen() int {
	return 0
}
