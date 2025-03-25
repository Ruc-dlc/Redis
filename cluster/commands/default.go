package commands

import "redis/myredis/cluster/core"

func RegisterCommands() {
	defaultCmds := []string{
		"expire",
		"expireAt",
		"pExpire",
		"pExpireAt",
		"ttl",
		"PTtl",
		"persist",
		"exists",
		"type",
		"set",
		"setNx",
		"setEx",
		"pSetEx",
		"get",
		"getEx",
		"getSet",
		"getDel",
		"incr",
		"incrBy",
		"incrByFloat",
		"decr",
		"decrBy",
		"lPush",
		"lPushX",
		"rPush",
		"rPushX",
		"LPop",
		"RPop",
		"LRem",
		"LLen",
		"LIndex",
		"LSet",
		"LRange",
		"HSet",
		"HSetNx",
		"HGet",
		"HExists",
		"HDel",
		"HLen",
		"HStrLen",
		"HMGet",
		"HMSet",
		"HKeys",
		"HVals",
		"HGetAll",
		"HIncrBy",
		"HIncrByFloat",
		"HRandField",
		"SAdd",
		"SIsMember",
		"SRem",
		"SPop",
		"SCard",
		"SMembers",
		"SInter",
		"SInterStore",
		"SUnion",
		"SUnionStore",
		"SDiff",
		"SDiffStore",
		"SRandMember",
		"ZAdd",
		"ZScore",
		"ZIncrBy",
		"ZRank",
		"ZCount",
		"ZRevRank",
		"ZCard",
		"ZRange",
		"ZRevRange",
		"ZRangeByScore",
		"ZRevRangeByScore",
		"ZRem",
		"ZRemRangeByScore",
		"ZRemRangeByRank",
		"GeoAdd",
		"GeoPos",
		"GeoDist",
		"GeoHash",
		"GeoRadius",
		"GeoRadiusByMember",
		"GetVer",
		"DumpKey",
	}
	for _, name := range defaultCmds {
		core.RegisterDefaultCmd(name)
	}

}
