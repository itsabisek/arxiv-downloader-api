package arxivharvester

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"time"

	xmlschemas "github.com/itsabisek/arxiv/XMLSchemas"
	"github.com/itsabisek/arxiv/utils"
)

const (
	numRetries   = 5
	statusCodeOK = 200
)

type Harvester struct {
	parameters       *Parameters
	baseURL          string
	requestType      string
	resumptionToken  *xmlschemas.ResumptionToken
	client           http.Client
	requestObject    *http.Request
	completeListSize int
	currentSize      int
	redisWrapper     *utils.RedisWrapper
}

func InitializeHarvester(redisWrapper *utils.RedisWrapper) *Harvester {
	var harvester *Harvester = new(Harvester)
	harvester.requestObject = nil
	harvester.redisWrapper = nil
	harvester.resumptionToken = new(xmlschemas.ResumptionToken)
	harvester.baseURL = utils.ArxivOaiBaseURL
	harvester.requestType = utils.GetRequest
	harvester.completeListSize = -1
	harvester.currentSize = 0
	harvester.client = http.Client{Timeout: time.Duration(20 * time.Second)}
	harvester.redisWrapper = redisWrapper
	harvester.parameters = InitializeParameters()
	return harvester
}

func (harvester *Harvester) SetMetadataPrefix(metadataPrefix string) {
	paramNames := []string{"metadataPrefix"}
	paramValues := []string{metadataPrefix}
	harvester.parameters.SetParams(paramNames, paramValues)
}

func (harvester *Harvester) SetVerb(verb string) {
	paramNames := []string{"verb"}
	paramValues := []string{verb}
	harvester.parameters.SetParams(paramNames, paramValues)
}

func (harvester *Harvester) SetSet(set string) {
	paramNames := []string{"set"}
	paramValues := []string{set}
	harvester.parameters.SetParams(paramNames, paramValues)
}

func (harvester *Harvester) SetRange(from string, until string) {
	paramNames := []string{"from", "until"}
	paramValues := []string{from, until}
	harvester.parameters.SetParams(paramNames, paramValues)
}

func (harvester *Harvester) SetID(identifier string) {
	paramNames := []string{"identifier"}
	paramValues := []string{identifier}
	harvester.parameters.SetParams(paramNames, paramValues)
}

func (harvester *Harvester) SetReqType(reqType string) {
	harvester.requestType = reqType
}

func (harvester *Harvester) updateResumptionTokenParam() {
	paramNames := []string{"resumptionToken"}
	paramValues := []string{string(harvester.resumptionToken.Value)}
	harvester.parameters.SetParams(paramNames, paramValues)
}

func (harvester *Harvester) isResumptionTokenPresent() bool {
	if len(*harvester.parameters.ResumptionToken) > 0 {
		return true
	}
	return false
}

func (harvester *Harvester) String() string {
	var tempMap = make(map[string]string)
	tempMap["parameters"] = harvester.parameters.String()
	tempMap["resumptionToken"] = harvester.resumptionToken.String()
	tempMap["base_url"] = harvester.baseURL
	tempMap["request_type"] = harvester.requestType
	tempMap["current_size"] = strconv.Itoa(harvester.currentSize)
	tempMap["complete_size"] = strconv.Itoa(harvester.completeListSize)

	harvesterString, err := json.Marshal(tempMap)
	if err != nil {
		panic(err)
	}
	return string(harvesterString)
}

func (harvester *Harvester) LoadFromMap(referrer map[string]string) {
	harvester.baseURL = referrer["base_url"]
	harvester.requestType = referrer["request_type"]
	harvester.currentSize, _ = strconv.Atoi(referrer["current_size"])
	harvester.completeListSize, _ = strconv.Atoi(referrer["complete_size"])
}

func (harvester *Harvester) SetHarvestParametersFromRedis() {
	var payload, referrer map[string]string
	tempPayload := harvester.redisWrapper.PopFromRequest()

	json.Unmarshal([]byte(tempPayload), &payload)
	json.Unmarshal([]byte(payload["referrer"]), &referrer)
	fmt.Println("Harvester Payload ", payload)
	harvester.LoadFromMap(referrer)
	harvester.parameters.ParseString(referrer["parameters"])
	if len(referrer["resumptionToken"]) > 0 {
		harvester.resumptionToken.ParseString(referrer["resumptionToken"])
	}
	harvester.updateResumptionTokenParam()
}

func (harvester *Harvester) pushToParserWrapper(respBody []byte) {
	var tempPayload = make(map[string]string)
	tempPayload["referrer"] = harvester.String()
	tempPayload["response"] = string(respBody)

	payload, err := json.Marshal(tempPayload)
	if err != nil {
		panic(err)
	}

	harvester.redisWrapper.PushToParser(string(payload), *harvester.parameters.Set)

}

func (harvester *Harvester) buildRequestObj() {
	if harvester.requestObject == nil {
		fmt.Println("Request Obj not cached. Creating a new one")
		req, err := http.NewRequest(harvester.requestType, harvester.baseURL, nil)
		if err != nil {
			panic(fmt.Sprintf("Error creating request Object %T", err))
		}
		req.Header.Set("Content-type", "application/xml")
		harvester.requestObject = req
	}

	q := harvester.requestObject.URL.Query()
	paramsReflect := reflect.ValueOf(harvester.parameters).Elem()
	resumptionTokenPresent := harvester.isResumptionTokenPresent()
	for i := 0; i < paramsReflect.NumField(); i++ {
		name := string(paramsReflect.Type().Field(i).Tag.Get("param"))
		value := paramsReflect.Field(i).Interface().(*string)
		if resumptionTokenPresent && (name != "verb" && name != "resumptionToken") {
			q.Del(name)
			continue
		}
		if len(*value) != 0 {
			q.Set(name, *value)
		}
	}
	harvester.requestObject.URL.RawQuery = q.Encode()
}

func (harvester *Harvester) makeHTTPRequest() (*http.Response, error) {
	var err error = nil
	var resp *http.Response = nil

	harvester.buildRequestObj()
	for i := 1; i <= numRetries; i++ {
		fmt.Println("Retry #", i, " - Making request to ", harvester.requestObject.URL.String())
		resp, err = harvester.client.Do(harvester.requestObject)
		if err != nil {
			fmt.Println("Error while making the request. Trying Again - ", err)
			continue
		}
		if resp.StatusCode != statusCodeOK {
			fmt.Println("Status Code ", resp.StatusCode, " is not 200. Waiting for ", 5*i, " seconds before retrying.")
			time.Sleep(time.Second * time.Duration(5*i))
			continue
		}
		break
	}
	return resp, err
}

func (harvester *Harvester) HarvestOnce() {
	fmt.Println("Requesting Metadata for ", *harvester.parameters.Set)
	startTime := time.Now()
	resp, err := harvester.makeHTTPRequest()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)
	harvester.pushToParserWrapper(respBody)
	fmt.Println("Request completed successfully for ", *harvester.parameters.Set, " | Total Time taken - ", time.Now().Sub(startTime))
}
