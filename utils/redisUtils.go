package utils

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

var cmdReturnTypeMapping map[string]interface{}

type RedisWrapper struct {
	RedisConn    *redis.Client
	Ctx          context.Context
	RequestQueue string
	ParserQueues []string
}

func InitializeCentralRedis() *RedisWrapper {
	rdb := new(RedisWrapper)
	rdb.RequestQueue = EmptyString
	rdb.ParserQueues = []string{EmptyString}
	rdb.Ctx = ctx
	rdb.RedisConn = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	_, err := rdb.RedisConn.Ping(rdb.Ctx).Result()
	if err != nil {
		panic(fmt.Sprintln("Error while connecting to redis db - ", err))
	}
	return rdb
}

func (redisWrapper *RedisWrapper) ClearAllKeysByTag(tag string) string {
	pattern := strings.Join([]string{"*", tag, "*"}, "")
	var args []string
	args = append(args, "DEL")
	rdb := redisWrapper.RedisConn
	keys, _, err := rdb.Scan(redisWrapper.Ctx, 0, pattern, 100).Result()
	if err != nil {
		panic(err)
	}
	for _, key := range keys {
		args = append(args, key)
	}
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, args...)
	return rettext[0]
}

func (redisWrapper *RedisWrapper) SetRequestQueue(requestQueueName string) {
	if requestQueueName != EmptyString {
		redisWrapper.RequestQueue = requestQueueName
	}
}

func (redisWrapper *RedisWrapper) SetParserQueues(parserQueueName string, numParsers int) {
	if parserQueueName != EmptyString {
		for i := 0; i < numParsers; i++ {
			redisWrapper.ParserQueues = append(redisWrapper.ParserQueues, parserQueueName+"_"+strconv.Itoa(i))
		}
	}

}

func (redisWrapper *RedisWrapper) GetParserIndexFromSet(set string) int {
	return int(binary.BigEndian.Uint16([]byte(set))) % 5
}

func (redisWrapper *RedisWrapper) SetHarvesterPolling() {
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "SET", "arxiv:string:poll:harvester", "1")

}

func (redisWrapper *RedisWrapper) StopHarvesterPolling() {
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "SET", "arxiv:string:poll:harvester", "0")

}

func (redisWrapper *RedisWrapper) ShouldPollHarvester() string {
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "GET", "arxiv:string:poll:harvester")

	return rettext[0]
}

func (redisWrapper *RedisWrapper) PushToRequest(payload string) {
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPUSH", redisWrapper.RequestQueue, payload)

}

func (redisWrapper *RedisWrapper) PopFromRequest() string {
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPOP", redisWrapper.RequestQueue)

	return rettext[0]
}

func (redisWrapper *RedisWrapper) HasRequestsToHarvest() bool {
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LLEN", redisWrapper.RequestQueue)

	return rettext[0] != "0"
}

func (redisWrapper *RedisWrapper) PopFromParser(parserIndex int) string {
	queueName := redisWrapper.GetParserQueueNameFromIndex(parserIndex)
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPOP", queueName)

	return rettext[0]
}

func (redisWrapper *RedisWrapper) PushToParser(payload string, set string) {
	parserIndex := 0
	if len(set) > 0 {
		parserIndex = redisWrapper.GetParserIndexFromSet(set)
	}
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPUSH", redisWrapper.ParserQueues[parserIndex], payload)

}

func (redisWrapper *RedisWrapper) GetParserQueueNameFromIndex(index int) string {
	return redisWrapper.ParserQueues[index]
}

func (redisWrapper *RedisWrapper) HasRequestsToParse(parserIndex int) bool {
	queueName := redisWrapper.GetParserQueueNameFromIndex(parserIndex)
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LLEN", queueName)

	return rettext[0] != "0"
}

func (redisWrapper *RedisWrapper) UpdateSetInfo(setInfo map[string]string) {
	var args []string
	args = append(args, "HMSET", "arxiv:hash:sets")
	for k, v := range setInfo {
		args = append(args, k)
		args = append(args, v)
	}
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, args...)
}

func (redisWrapper *RedisWrapper) GetAllSetValues() []string {
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "HVALS", "arxiv:hash:sets")
	return rettext
}

func (redisWrapper *RedisWrapper) GetAllSets() []string {
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "HKEYS", "arxiv:hash:sets")
	return rettext
}

func runCommand(rdb *redis.Client, ctx *context.Context, args ...string) []string {
	varargs := make([]interface{}, len(args))
	for i, v := range args {
		varargs[i] = v
	}
	fmt.Sprintln("Executing cmd ", strings.Join(args, " "))
	rettext, err := execute(rdb, *ctx, varargs)
	if err != nil {
		panic(fmt.Sprintln("Error while executing cmd ", strings.Join(args, " "), ": ", err))
	}

	return rettext
}

func execute(rdb *redis.Client, ctx context.Context, args []interface{}) ([]string, error) {
	var rettext interface{}
	var retval []string
	var err error
	cmd := args[0].(string)
	switch strings.ToLower(cmd) {
	case "get":
		cmdString := redis.NewStringCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "set":
		cmdString := redis.NewStringCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "hmget":
		cmdString := redis.NewStringSliceCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "hmset":
		cmdString := redis.NewStringCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "hkeys":
		cmdString := redis.NewStringSliceCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "hvals":
		cmdString := redis.NewStringSliceCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "lpush":
		cmdString := redis.NewStringCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "lpop":
		cmdString := redis.NewStringCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "llen":
		cmdString := redis.NewStringCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()
		break
	case "del":
		cmdString := redis.NewStringCmd(ctx, args...)
		rdb.Process(ctx, cmdString)
		rettext, err = cmdString.Result()

	default:
		panic(fmt.Sprintln("Please add the cmd ", cmd))
	}
	if _, ok := rettext.(string); ok {
		retval = []string{rettext.(string)}
	} else {
		retval = rettext.([]string)
	}
	return retval, err
}
