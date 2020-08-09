package arxivharvester

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	numRetries   = 3
	statusCodeOK = 200
)

type Harvester struct {
	parameters       *Parameters
	baseURL          string
	requestType      string
	resumptionToken  *ResumptionToken
	client           http.Client
	requestObject    *http.Request
	completeListSize int
	currentSize      int
	redisDB          *redis.Client
}

func InitializeHarvester(rdb *redis.Client) *Harvester {
	var harvester *Harvester = new(Harvester)
	harvester.requestObject = nil
	harvester.redisDB = nil
	harvester.baseURL = ArxivOaiBaseURL
	harvester.requestType = GetRequest
	harvester.completeListSize = -1
	harvester.currentSize = 0
	harvester.client = http.Client{Timeout: time.Duration(20 * time.Second)}
	harvester.redisDB = rdb
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
	if err != nil {
		panic("Error creating req object. Exiting...")
	}
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

func (harvester *Harvester) updateHarvesterParams(respBody []byte) {
	xmlTree := RootTag{}
	xml.Unmarshal(respBody, &xmlTree)
	harvester.resumptionToken = &xmlTree.ListRecords.ResumptionToken
	if len(harvester.resumptionToken.Value) > 0 {
		harvester.updateResumptionTokenParam()
		harvester.currentSize += len(xmlTree.ListRecords.Records)
		if harvester.completeListSize == 0 {
			harvester.completeListSize = harvester.resumptionToken.CompleteSize
		}
	}

}

func (harvester *Harvester) StartHarvesting() {
	fmt.Println("Harvesting Metadata...")
	startTime := time.Now()
	for harvester.currentSize != harvester.completeListSize {
		resp, err := harvester.makeHTTPRequest()
		if err != nil {
			panic(err)
		}

		respBody, _ := ioutil.ReadAll(resp.Body)
		harvester.uploadXMLDataToRedis(respBody)

		resp.Body.Close()

		harvester.updateHarvesterParams(respBody)
		fmt.Printf("\nHarvester Stats- Set - %s  :: Cursor - %d :: CompleteListSize - %d :: CurrnetListSize - %d :: ResumptionToken - %s\n\n",
			*harvester.parameters.Set, harvester.resumptionToken.Cursor, harvester.resumptionToken.CompleteSize, harvester.currentSize, harvester.resumptionToken.Value)
		// TODO: Push the XML body to a central redis queue over here
		if !(len(harvester.resumptionToken.Value) > 0) {
			fmt.Println("No resumption token found")
			if harvester.currentSize == harvester.completeListSize {
				fmt.Println("All Entries Harvested. Exiting...")
			} else {
				fmt.Println("The API imposed Rate Limitations. Sleeping for 30 secs before trying again")
				time.Sleep(time.Duration(30) * time.Second)
			}
		}

	}
	fmt.Println("Metadata Harvesting complete for", *harvester.parameters.Set, "Total Time taken - ", time.Now().Sub(startTime))

}

func (harvester *Harvester) uploadXMLDataToRedis(respBody []byte) {
	ctx := harvester.redisDB.Context()
	err := harvester.redisDB.Set(ctx, "Body", string(respBody[:]), 0).Err()
	if err != nil {
		panic(fmt.Sprintln("Error while uploading data to redis - ", err))
	}
}
