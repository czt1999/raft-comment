package shardctrler

import (
	"824/raft"
	"sync/atomic"
)
import "824/labrpc"
import "sync"
import "824/labgob"

// The shardctrler manages a sequence of numbered configurations.
// Each configuration describes a set of replica groups and
// an assignment of shards to replica groups.

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs      []Config // indexed by config num
	lastCfgIndex int
	lastIndex    int32

	// 分区状态
	shardsByGroup map[int][]int

	// 为了保证幂等性, 需要记录处理过的操作ID
	okMap map[int64]bool

	lastApplied int32
}

type Op struct {
	ID     int64
	LastID int64
	Type   string
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

	// 检查okMap, 保证幂等性

	// 创建Op, 通用rf.Start传递给下层的Raft
	// 如果返回的第三个值是false, 设置Err为ErrWrongLeader, 返回

	// 等待Raft达成一致并提交

	for index > int(atomic.LoadInt32(&sc.lastIndex)) {

		// 在此过程中若出现了新的term, 返回失败
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {

	// 与Join逻辑相同
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {

	// ...
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

	// ...
	// 根据args.Num返回合适的Config
}

// 封装 Move 操作, 更新 shardsByGroup
func (sc *ShardCtrler) moveShard(shard int, from int, to int) {
	if from != to {
		shardFrom, shardTo := sc.shardsByGroup[from], sc.shardsByGroup[to]
		shardFromNew := make([]int, 0, 1)
		for _, s := range shardFrom {
			if s != shard {
				shardFromNew = append(shardFromNew, s)
			}
		}
		sc.shardsByGroup[from] = shardFromNew
		sc.shardsByGroup[to] = append(shardTo, shard)
	}
}

// 调整 shardsByGroup, 让任意两个分片的长度之差不大于2
func (sc *ShardCtrler) balanceShards(cfg *Config) {
	var (
		max    int
		maxLen int
		min    int
		minLen int
	)
	for {
		// 循环直到平衡为止
		maxLen = 0
		minLen = NShards
		for gid := range sc.shardsByGroup {
			shardsLen := len(sc.shardsByGroup[gid])
			// 如果有相同的maxLen/minLen，取id较大的
			// 避免multi config不一致的问题
			if maxLen < shardsLen {
				max, maxLen = gid, shardsLen
			} else if maxLen == shardsLen && max < gid {
				max = gid
			}
			if minLen > shardsLen {
				min, minLen = gid, shardsLen
			} else if minLen == shardsLen && min < gid {
				min = gid
			}
		}
		// 无需平衡
		if maxLen-minLen < 2 {
			break
		}
		// 从max移动一个server到min
		sc.moveShard(sc.shardsByGroup[max][maxLen-1], max, min)
	}
	for gid, shards := range sc.shardsByGroup {
		for _, s := range shards {
			cfg.Shards[s] = gid
		}
	}
}

// 提取出 apply 主体操作, 在 ShardCtrler 初始化时开启一条goroutine
// 接收来自 applyCh 的消息并更新分片状态
func (sc *ShardCtrler) apply() {
	for m := range sc.applyCh {
		atomic.StoreInt32(&sc.lastIndex, int32(m.CommandIndex))
		if m.CommandValid && m.CommandIndex > int(atomic.LoadInt32(&sc.lastApplied)) {
			op := m.Command.(Op)
			sc.mu.Lock()
			if _, ok := sc.okMap[op.ID]; !ok {
				if op.Type != "Query" {
					// 更新配置, 创建新的Config

					// 根据操作的类型调用相应的applyXXX

					// 平衡分片数量

					// 追加新配置, 更新 lastCfgIndex, lastApplied

					// 标记该操作处理成功
				}
			}
			sc.mu.Unlock()
		}
	}
}

// 根据已提交的 Join 操作更新分片状态
func (sc *ShardCtrler) applyJoin(cfg *Config, op *Op) {
	if len(sc.shardsByGroup) == 0 {
		// 加入的是唯一的组
		var max = 0
		for gid, srv := range op.Servers {
			cfg.Groups[gid] = srv
			sc.shardsByGroup[gid] = make([]int, 0, 1)
			if max < gid {
				max = gid
			}
		}
		// 先给id最大的组分配所有分片
		sc.shardsByGroup[max] = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		for i := 0; i < NShards; i += 1 {
			cfg.Shards[i] = max
		}
	} else {
		// 在之前的配置基础上更新
		oldCfg := sc.configs[sc.lastCfgIndex]
		for gid, srv := range oldCfg.Groups {
			cfg.Groups[gid] = srv
		}
		for gid, srv := range op.Servers {
			cfg.Groups[gid] = srv
			sc.shardsByGroup[gid] = make([]int, 0, 1)
		}
	}
}

// 根据已提交的 Leave 操作更新分片状态
func (sc *ShardCtrler) applyLeave(cfg *Config, op *Op) {
	// 在之前的配置基础上更新
	oldCfg := sc.configs[sc.lastCfgIndex]
	for gid, srv := range oldCfg.Groups {
		cfg.Groups[gid] = srv
	}
	leaveShards := make([]int, 0, 1)
	for _, gid := range op.GIDs {
		leaveShards = append(leaveShards, sc.shardsByGroup[gid]...)
		delete(cfg.Groups, gid)
		delete(sc.shardsByGroup, gid)
	}
	// 先把剩下的分片分配给id最大的组
	var max = 0
	for gid := range sc.shardsByGroup {
		if max < gid {
			max = gid
		}
	}
	if max > 0 {
		sc.shardsByGroup[max] = append(sc.shardsByGroup[max], leaveShards...)
	}
}

// 根据已提交的 Move 操作更新分片状态
func (sc *ShardCtrler) applyMove(cfg *Config, op *Op) {
	// 在之前的配置基础上更新
	oldCfg := sc.configs[sc.lastCfgIndex]
	for gid, srv := range oldCfg.Groups {
		cfg.Groups[gid] = srv
	}
	sc.moveShard(op.Shard, oldCfg.Shards[op.Shard], op.GID)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.lastCfgIndex = 0
	sc.lastIndex = 0
	sc.shardsByGroup = make(map[int][]int)
	sc.okMap = make(map[int64]bool)

	go sc.apply()

	return sc
}
