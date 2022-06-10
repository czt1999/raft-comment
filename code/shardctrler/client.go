package shardctrler

//
// Shardctrler clerk.
//

import "824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	// 为了保证幂等性, 需要记录最近一次请求的ID
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// ...
	return ck
}

func (ck *Clerk) Query(num int) Config {

	// 创建QueryArgs, 随机生成ID, 携带最近一次请求的ID
	args := &QueryArgs{}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				// 成功, 更新最近一次请求的ID
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// 与Query逻辑相同
}

func (ck *Clerk) Leave(gids []int) {
	// ...
}

func (ck *Clerk) Move(shard int, gid int) {
	// ...
}
