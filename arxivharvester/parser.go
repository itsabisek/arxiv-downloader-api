package arxivharvester

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strconv"

	xmlschemas "github.com/itsabisek/arxiv/XMLSchemas"
	"github.com/itsabisek/arxiv/utils"
)

type Parser struct {
	ParserIndex  int
	redisWrapper *utils.RedisWrapper
}

func InitializeParser(parserIndex int, redisWrapper *utils.RedisWrapper) *Parser {
	parser := new(Parser)
	parser.redisWrapper = nil
	parser.ParserIndex = parserIndex
	parser.redisWrapper = redisWrapper

	return parser
}

func (parser *Parser) ParseOneFromRedis() {
	var payload map[string]interface{}
	var rootTag xmlschemas.RootTag
	var queueData string = parser.redisWrapper.PopFromParser(parser.ParserIndex)
	json.Unmarshal([]byte(queueData), &payload)
	referrer := payload["referrer"].(string)
	respBody := payload["response"].(string)
	xml.Unmarshal([]byte(respBody), &rootTag)

	updatedReferrer := parser.ExtractData(rootTag, referrer)

	parser.UpdateHarvestParametersInRedis(updatedReferrer)

}

func (parser *Parser) UpdateHarvestParametersInRedis(updatedReferrer string) {
	var tempPayload = make(map[string]interface{})
	tempPayload["referrer"] = updatedReferrer
	payload, err := json.Marshal(tempPayload)
	if err != nil {
		panic(err)
	}
	parser.redisWrapper.PushToRequest(string(payload))
}

func (parser *Parser) ExtractData(rootTag xmlschemas.RootTag, referrerPayload string) string {
	var parameters, referrer map[string]string
	json.Unmarshal([]byte(referrerPayload), &referrer)
	json.Unmarshal([]byte(referrer["parameters"]), &parameters)
	records := rootTag.ListRecords.Records
	resumptionToken := &rootTag.ListRecords.ResumptionToken
	completeSize, _ := strconv.Atoi(referrer["complete_size"])
	currentSize, _ := strconv.Atoi(referrer["current_size"])

	fmt.Printf("\nParser Stats- Set - %s  :: Cursor - %d :: CompleteListSize - %d :: CurrentListSize - %s :: ResumptionToken - %s\n\n",
		parameters["set"], resumptionToken.Cursor, resumptionToken.CompleteSize, referrer["current_size"], resumptionToken.Value)

	if resumptionToken.CompleteSize != 0 {
		if completeSize != -1 && completeSize != resumptionToken.CompleteSize {
			panic(fmt.Sprintln("Something is wrong. Complete List size don't match!!! | ", completeSize, " != ", resumptionToken.CompleteSize))
		} else {
			completeSize = resumptionToken.CompleteSize
			currentSize += len(records)
			referrer["complete_size"] = strconv.Itoa(completeSize)
			referrer["current_size"] = strconv.Itoa(currentSize)
		}
		if len(resumptionToken.Value) > 0 {
			referrer["resumptionToken"] = resumptionToken.String()
		} else if !(len(resumptionToken.Value) > 0) && currentSize == completeSize {
			fmt.Println("All records harvested and parsed for set ", parameters["set"])
		} else {
			fmt.Println("No Resumption Token found. API rate limited. Requeing for set ", parameters["set"])
		}
	} else {
		fmt.Println("No Resumption Token found. API rate limited. Requeing for set ", parameters["set"])
	}
	updatedReferrer, err := json.Marshal(referrer)
	if err != nil {
		panic(err)
	}

	return string(updatedReferrer)

}
