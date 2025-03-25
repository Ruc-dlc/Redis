package core

import (
	"hash/crc32"
	"strings"

	"redis/myredis/interface/redis"
	"redis/myredis/redis/protocol"
)

const SlotCount int = 1024

const getCommittedIndexCommand = "raft.committedindex"

func init() {
	RegisterCmd(getCommittedIndexCommand, execRaftCommittedIndex)
}

// relay function relays command to peer or calls cluster.Exec
func (cluster *Cluster) Relay(peerId string, c redis.Connection, cmdLine [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peerId == cluster.SelfID() {
		// to self db
		return cluster.Exec(c, cmdLine)
	}
	// peerId is peer.Addr
	cli, err := cluster.connections.BorrowPeerClient(peerId)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.connections.ReturnPeerClient(cli)
	}()
	return cli.Send(cmdLine)
}

// GetPartitionKey extract hashtag
func GetPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

func GetSlot(key string) uint32 {
	partitionKey := GetPartitionKey(key)
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(SlotCount)
}

// pickNode returns the node id hosting the given slot.
// If the slot is migrating, return the node which is exporting the slot
func (cluster *Cluster) PickNode(slotID uint32) string {
	return cluster.raftNode.FSM.PickNode(slotID)
}

// format: raft.committedindex
func execRaftCommittedIndex(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	index, err := cluster.raftNode.CommittedIndex()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeIntReply(int64(index))
}