// This is the main package that starts the harvesting using the harvester
// Step 1 - Queue up requests for multiple sets into the key - queue:oai_requests
// Step 2 - Start Consuming those requests and put the xml in the key - queue:oai_xml_parser_<parser_no>
// Step 3 - Put back the request obj into - queue:oai_requests
// Step 4 - Start 5 parser goroutines which constantly poll the respective parser queues

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
	redisWrapper := utils.InitializeCentralRedis()
	redisWrapper.SetRequestQueue("queue:oai_requests")
	redisWrapper.SetParserQueues("queue:oai_xml_parser", 5)
	redisWrapper.StopHarvesterPolling()
	wgHarvester := &sync.WaitGroup{}
	wgParser := &sync.WaitGroup{}
	wgHarvester.Add(1)
	wgParser.Add(5)
	var harvester *arxivharvester.Harvester = arxivharvester.InitializeHarvester(redisWrapper)
	var parsers []*arxivharvester.Parser = createParsers(redisWrapper, 5)
	for _, parser := range parsers {
		go initializeParserPolling(redisWrapper, parser, wgParser)
	}
	go intializeHarvesterPolling(redisWrapper, harvester, wgParser)
	sets := []string{"cs"}
	verb := utils.VerbFor["LIST_RECORDS"]
	bootstrapHarvesting(harvester, redisWrapper, sets, verb)
	wgHarvester.Wait()
	wgParser.Wait()
}

func intializeHarvesterPolling(redisWrapper *utils.RedisWrapper, harvester *arxivharvester.Harvester, wgHarvester *sync.WaitGroup) {
	for {
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

func createParsers(redisWrapper *utils.RedisWrapper, numParsers int) []*arxivharvester.Parser {
	var parsers []*arxivharvester.Parser
	for i := 0; i < numParsers; i++ {
		parsers = append(parsers, arxivharvester.InitializeParser(i, redisWrapper))
	}

	return parsers
}

func initializeParserPolling(redisWrapper *utils.RedisWrapper, parser *arxivharvester.Parser, wgParser *sync.WaitGroup) {
	for {
		// fmt.Println("Polling parser queue no ", parser.ParserIndex)
		if redisWrapper.HasRequestsToParse(parser.ParserIndex) {
			fmt.Println("Found responses to parse in queue no. ", parser.ParserIndex)
			parser.ParseOneFromRedis()
		}
		time.Sleep(time.Duration(30) * time.Second)
	}
	wgParser.Done()
}

func bootstrapHarvesting(harvester *arxivharvester.Harvester, redisWrapper *utils.RedisWrapper, set []string, verb string) {
	harvester.SetVerb(verb)
	fmt.Println("\nBootstrapping harvesting for ", len(set), " sets")
	for _, setSpec := range set {
		harvester.SetSet(setSpec)
		harvester.HarvestOnce()
	}
	redisWrapper.SetHarvesterPolling()
}
