package raft

import (
    "sync/atomic"
    "time"
)

// 定义一个心跳间隔, 按照 Lab3/4 的要求设为 100ms

func (rf *Raft) startElection() {
    
    // 已经是 leader / 在这个 term 内给其他节点投过票 / 收到了其他节点的 leader 声明, 返回

    // 更新 leader, curTerm, voteFor, 在发送 RPC 之前需要持久化

    // 通用的 args
    args := RequestVoteArgs{}

    var count int32 = 1
    var maxTerm int32

    // 向其他节点发送 RequestVote RPC
    for i := range rf.peers {
        if i != rf.me {
            go func(server int) {
                reply := RequestVoteReply{}
                if rf.sendRequestVote(server, &args, &reply) {
                
                    // 若本次投票有效, count 累加
                    // 记录 maxTerm
                
                }
            }(i)
        }
    }

    // 设置选举 timeout
    timeout := HeartbeatInterval * 10
    for ; timeout > 0; timeout -= 10 {

        // 检查 killed        

        // 检查心跳标志, 也许出现了新的 leader

        // 获得大多数投票, 成为 leader, 退出循环 

    }

    // 发现有其他节点的 term 比自己大，更新

    // 如果没有发生新的选举，voteFor 重置为空

}

func (rf *Raft) becomeLeader(term int) {

    // 如果 term < rf.curTerm, 返回

    // 更新 leader, voteFor, nextIndex, matchIndex
    
    // 开启一个 goroutine 用于发送心跳

}

func (rf *Raft) sendHeartbeat(thisTerm int) {
    for i := range rf.peers {
        if i != rf.me {
            go func(server int) {
                for {

                    // 如果 leader, curTerm 改变, 或 killed, 停止发送心跳
                    
                    // 检查是否有必要设置快照
                    
                    // 准备心跳参数
                    // 注意, 由于切片会共享底层数组, 需要拷贝[]Entry
                    args := AppendEntriesArgs{}

                    reply := AppendEntriesReply{}

                    go func(args AppendEntriesArgs, reply AppendEntriesReply) {
                        
                        // 发送 RPC

                        // 如果回复成功, 更新 matchIndex
                        // 如果回复失败, 检查 term, 决定是要转换为 follower 还是调整 nextIndex

                    }

                    // 间隔
                    time.Sleep(HeartbeatInterval * time.Millisecond)
                }
            }(i)
        }
    }
}

func (rf *Raft) broadcastAppendEntries(last int, term int) {

    var count int32 = 1
    var wait int32 = 1
    for i := range rf.peers {
        if i != rf.me {
            go func(server int) {
                retry := true
                for retry {

                    // 下述情形无需再发送这个AppendEntries
                    // 1) rf.leader != rf.m
                    // 2) curTerm > term
                    // 3) prevLogIndex >= last
                    // 4) rf.lastIndex > last
                    // 5) killed

                    // 检查是否有必要设置快照, 如果是, 等待一段时间让 InstallSnapshot 完成

                    // 准备参数, 发送 RPC

                    // 如果成功, 用原子指令累加 count, 更新对应的 nextIndex, matchIndex, 令 retry = false

                    // 如果失败, 发现比自己大的 term, 如果还没出现新的 leader, 将 leader 重置为空, 结束广播过程
                    // 否则, 调整 NextIndex, 重试

                }
            }(i)
        }
    }

    for !rf.killed() && atomic.LoadInt32(&wait) == 1 {

        // 超半数达成一致, 可更新 commitIndex
        // 注意, 只提交在当前 term 备份的日志项, 见论文 5.4.2

        time.Sleep(5 * time.Millisecond)
    }
}

func (rf *Raft) checkAndSendInstallSnapshot(server int, term int, prevLogIndex int) bool {
    
    // 用于检查是否要对 server 发起 InstallSnapshot
    
    // 如果当前节点的快照 index 大于 prevLogIndex, 有必要告知对方生成快照

    // 发送 RPC, 若成功, 更新 nextIndex

    return false
}
