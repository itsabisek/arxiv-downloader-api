package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/itsabisek/arxiv/arxivharvester"
	"github.com/itsabisek/arxiv/utils"
)

var ctx = context.Background()

func main() {
	fmt.Println("Initializing Redis Server...")
	redisWrapper := utils.InitializeCentralRedis()
	fmt.Println("Clearing all keys by tag arxiv")
	keysDeleted := redisWrapper.ClearAllKeysByTag("arxiv")
	fmt.Println("Deleted ", keysDeleted, " key(s) from central redis")
	redisWrapper.SetRequestQueue("arxiv:queue:oai_requests")
	redisWrapper.SetParserQueues("arxiv:queue:oai_xml_parser", 5)
	fmt.Println("Stopping harvester polling until bootstraping complete!!")
	redisWrapper.StopHarvesterPolling()
	wgHarvester := &sync.WaitGroup{}
	wgParser := &sync.WaitGroup{}
	var harvester *arxivharvester.Harvester = arxivharvester.InitializeHarvester(redisWrapper)
	var parsers []*arxivharvester.Parser = createParsers(redisWrapper, 5)
	fmt.Println("Creating new parser and request queues...")
	for _, parser := range parsers {
		wgParser.Add(1)
		go initializeParserPolling(redisWrapper, parser, wgParser)
		time.Sleep(time.Duration(5) * time.Second)
	}
	wgHarvester.Add(1)
	go intializeHarvesterPolling(redisWrapper, harvester, wgParser)
	bootstrapHarvesting(harvester, redisWrapper)
	wgHarvester.Wait()
	wgParser.Wait()
}

func intializeHarvesterPolling(redisWrapper *utils.RedisWrapper, harvester *arxivharvester.Harvester, wgHarvester *sync.WaitGroup) {
	for {
		if allSetsDone(redisWrapper) {
			fmt.Println("All sets harvested. Stopped harvester polling")
			break
		}
		// fmt.Println("Polling request queue")
		if redisWrapper.ShouldPollHarvester() == "1" && redisWrapper.HasRequestsToHarvest() {
			fmt.Println("Found request to harvest in ", redisWrapper.RequestQueue)
			harvester.SetHarvestParametersFromRedis()
			harvester.HarvestOnce()
		}
		time.Sleep(time.Duration(15) * time.Second)

	}
	wgHarvester.Done()
}

func initializeParserPolling(redisWrapper *utils.RedisWrapper, parser *arxivharvester.Parser, wgParser *sync.WaitGroup) {
	var sleepTime int
	for {
		sleepTime = 20
		if allSetsDone(redisWrapper) {
			fmt.Println("All sets harvested. Stopped parser polling")
			break
		}
		// fmt.Println("Polling parser queue no ", parser.ParserIndex)
		if redisWrapper.HasRequestsToParse(parser.ParserIndex) {
			fmt.Println("Found responses to parse in queue no. ", parser.ParserIndex)
			parser.ParseOneFromRedis()
			sleepTime = 5
		}
		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
	wgParser.Done()
}

func createParsers(redisWrapper *utils.RedisWrapper, numParsers int) []*arxivharvester.Parser {
	var parsers []*arxivharvester.Parser
	for i := 0; i < numParsers; i++ {
		parsers = append(parsers, arxivharvester.InitializeParser(i, redisWrapper))
	}

	return parsers
}

func allSetsDone(redisWrapper *utils.RedisWrapper) bool {
	allSetValues := redisWrapper.GetAllSetValues()
	if !(len(allSetValues) > 0) {
		return false
	}
	for _, v := range allSetValues {
		if v == "0" {
			return false
		}
	}
	return true
}

func bootstrapHarvesting(harvester *arxivharvester.Harvester, redisWrapper *utils.RedisWrapper) {
	var sets []string
	harvester.SetVerb(utils.VerbFor["LIST_SETS"])
	harvester.SetMetadataPrefix("")
	harvester.HarvestOnce()
	harvester.SetVerb(utils.VerbFor["LIST_RECORDS"])
	harvester.SetMetadataPrefix(utils.MetaFormatFor["ARXIV_RAW"])
	fmt.Println("Waiting for Set Info")
	for {
		sets = redisWrapper.GetAllSets()
		if len(sets) > 0 {
			break
		}
		time.Sleep(time.Duration(5) * time.Second)
	}

	fmt.Println("\nBootstrapping harvesting for ", len(sets), " sets")
	for _, setSpec := range sets {
		harvester.SetSet(setSpec)
		harvester.HarvestOnce()
		time.Sleep(time.Duration(15) * time.Second)
	}
	redisWrapper.SetHarvesterPolling()
}
