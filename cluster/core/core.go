package core

import (
	"sync"

	"redis/myredis/cluster/raft"
	dbimpl "redis/myredis/database"
	"redis/myredis/datastruct/set"
	"redis/myredis/interface/database"
	"redis/myredis/interface/redis"
	"redis/myredis/lib/utils"
	"redis/myredis/redis/protocol"
	rdbcore "github.com/hdt3213/rdb/core"
)

type Cluster struct {
	raftNode    *raft.Node
	db          database.DBEngine
	connections ConnectionFactory
	config      *Config

	slotsManager    *slotsManager
	rebalanceManger *rebalanceManager
}

type Config struct {
	raft.RaftConfig
	StartAsSeed    bool
	JoinAddress    string
	connectionStub ConnectionFactory // for test
}

func (c *Cluster) SelfID() string {
	return c.raftNode.Cfg.ID()
}

// slotsManager 负责管理当前 node 上的 slot
type slotsManager struct {
	mu            *sync.RWMutex
	slots         map[uint32]*slotStatus // 记录当前node上的 slot
	importingTask *raft.MigratingTask
}

const (
	slotStateHosting = iota
	slotStateImporting
	slotStateExporting
)

type slotStatus struct {
	mu    *sync.RWMutex
	state int
	keys  *set.Set // 记录当前 slot 上的 key

	exportSnapshot *set.Set // 开始传输时拷贝 slot 中的 key, 避免并发并发
	dirtyKeys      *set.Set // 传输开始后被修改的key, 在传输结束阶段需要重传一遍
}

func newSlotsManager() *slotsManager {
	return &slotsManager{
		mu:    &sync.RWMutex{},
		slots: map[uint32]*slotStatus{},
	}
}

func (ssm *slotsManager) getSlot(index uint32) *slotStatus {
	ssm.mu.RLock()
	slot := ssm.slots[index]
	ssm.mu.RUnlock()
	if slot != nil {
		return slot
	}
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	// check-lock-check
	slot = ssm.slots[index]
	if slot != nil {
		return slot
	}
	slot = &slotStatus{
		state: slotStateHosting,
		keys:  set.Make(),
		mu:    &sync.RWMutex{},
	}
	ssm.slots[index] = slot
	return slot
}

func NewCluster(cfg *Config) (*Cluster, error) {
	var connections ConnectionFactory
	if cfg.connectionStub != nil {
		connections = cfg.connectionStub
	} else {
		connections = newDefaultClientFactory()
	}
	db := dbimpl.NewStandaloneServer()
	raftNode, err := raft.StartNode(&cfg.RaftConfig)
	if err != nil {
		return nil, err
	}
	hasState, err := raftNode.HasExistingState()
	if err != nil {
		return nil, err
	}
	if !hasState {
		if cfg.StartAsSeed {
			err = raftNode.BootstrapCluster(SlotCount)
			if err != nil {
				return nil, err
			}
		} else {
			// join cluster
			conn, err := connections.BorrowPeerClient(cfg.JoinAddress)
			if err != nil {
				return nil, err
			}
			result := conn.Send(utils.ToCmdLine(joinClusterCommand, cfg.RedisAdvertiseAddr, cfg.RaftAdvertiseAddr))
			if err := protocol.Try2ErrorReply(result); err != nil {
				return nil, err
			}
		}
	}
	cluster := &Cluster{
		raftNode:        raftNode,
		db:              db,
		connections:     connections,
		config:          cfg,
		rebalanceManger: newRebalanceManager(),
		slotsManager:    newSlotsManager(),
	}
	cluster.injectInsertCallback()
	cluster.injectDeleteCallback()
	return cluster, nil
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {

}

func (cluster *Cluster) Close() {
	cluster.db.Close()
	err := cluster.raftNode.Close()
	if err != nil {
		panic(err)
	}
}

// LoadRDB real implementation of loading rdb file
func (cluster *Cluster) LoadRDB(dec *rdbcore.Decoder) error {
	return cluster.db.LoadRDB(dec)
}
