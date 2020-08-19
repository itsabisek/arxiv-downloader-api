package utils

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type RedisWrapper struct {
	RedisConn    *redis.Client
	Ctx          context.Context
	RequestQueue string
	ParserQueues []string
}

func InitializeCentralRedis() *RedisWrapper {
	Rdb := new(RedisWrapper)
	Rdb.RequestQueue = EmptyString
	Rdb.ParserQueues = []string{EmptyString}
	Rdb.Ctx = ctx
	Rdb.RedisConn = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	_, err := Rdb.RedisConn.Ping(Rdb.Ctx).Result()
	if err != nil {
		panic(fmt.Sprintln("Error while connecting to redis db - ", err))
	}

	return Rdb
}

func (redisWrapper *RedisWrapper) SetRequestQueue(requestQueueName string) {
	if requestQueueName != EmptyString {
		redisWrapper.RequestQueue = requestQueueName
	}
}

func (redisWrapper *RedisWrapper) SetParserQueues(parserQueueName string, numParsers int) {
	if parserQueueName != EmptyString {
		for i := 0; i < numParsers; i++ {
			fmt.Println(string(i))
			redisWrapper.ParserQueues = append(redisWrapper.ParserQueues, parserQueueName+"_"+strconv.Itoa(i))
		}
	}

}

func (redisWrapper *RedisWrapper) GetParserIndexFromSet(set string) int {
	return int(binary.BigEndian.Uint16([]byte(set))) % 5
}

func (redisWrapper *RedisWrapper) SetHarvesterPolling() {
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "SET", "string:poll:harvester", "1")

}

func (redisWrapper *RedisWrapper) StopHarvesterPolling() {
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "SET", "string:poll:harvester", "0")

}

func (redisWrapper *RedisWrapper) ShouldPollHarvester() string {
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "GET", "string:poll:harvester")

	return rettext
}

func (redisWrapper *RedisWrapper) PushToRequest(payload string) {
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPUSH", redisWrapper.RequestQueue, payload)

}

func (redisWrapper *RedisWrapper) PopFromRequest() string {
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPOP", redisWrapper.RequestQueue)

	return rettext
}

func (redisWrapper *RedisWrapper) HasRequestsToHarvest() bool {
	fmt.Println(redisWrapper.RequestQueue)
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LLEN", redisWrapper.RequestQueue)

	return rettext != "0"
}

func (redisWrapper *RedisWrapper) PopFromParser(parserIndex int) string {
	queueName := redisWrapper.GetParserQueueNameFromIndex(parserIndex)
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPOP", queueName)

	return rettext
}

func (redisWrapper *RedisWrapper) PushToParser(payload string, set string) {
	parserIndex := redisWrapper.GetParserIndexFromSet(set)
	runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LPUSH", redisWrapper.ParserQueues[parserIndex], payload)

}

func (redisWrapper *RedisWrapper) GetParserQueueNameFromIndex(index int) string {
	return redisWrapper.ParserQueues[index]
}

func (redisWrapper *RedisWrapper) HasRequestsToParse(parserIndex int) bool {
	queueName := redisWrapper.GetParserQueueNameFromIndex(parserIndex)
	rettext := runCommand(redisWrapper.RedisConn, &redisWrapper.Ctx, "LLEN", queueName)

	return rettext != "0"
}

func runCommand(rdb *redis.Client, ctx *context.Context, args ...string) string {
	var cmd, key, val string = EmptyString, EmptyString, EmptyString
	for k, v := range args {
		switch k {
		case 0:
			cmd = v
			break
		case 1:
			key = v
			break
		case 2:
			val = v
			break
		}
	}
	cmdString := redis.NewStringCmd(*ctx, cmd, key)
	if val != EmptyString {
		cmdString = redis.NewStringCmd(*ctx, cmd, key, val)
	}

	rdb.Process(*ctx, cmdString)
	rettext, err := cmdString.Result()
	if err != nil {
		fmt.Println(cmdString)
		panic(err)
	}
	return rettext
}
