// This is the main package that starts the harvesting using the harvester
package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/itsabisek/arxiv/arxivharvester"
)

var ctx = context.Background()

func main() {
	redisDB := initializeCentralRedisConnection()
	defer redisDB.Close()
	var harvester *arxivharvester.Harvester = arxivharvester.InitializeHarvester(redisDB)
	harvester.SetVerb(arxivharvester.VerbFor["LIST_RECORDS"])
	harvester.SetSet("cs")
	harvester.StartHarvesting()

}

func initializeCentralRedisConnection() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintln("Error while connecting to redis db - ", err))
	}
	return rdb

}
