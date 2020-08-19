package arxivharvester

import (
	"encoding/json"
	"errors"

	"github.com/itsabisek/arxiv/utils"
)

// Parameters : Stores the list of parameters to be sent to the urls
type Parameters struct {
	MetadataPrefix  *string `param:"metadataPrefix"`
	Verb            *string `param:"verb"`
	Set             *string `param:"set"`
	From            *string `param:"from"`
	Until           *string `param:"until"`
	Identifier      *string `param:"indentifier"`
	ResumptionToken *string `param:"resumptionToken"`
}

func (params *Parameters) initialize() {
	params.MetadataPrefix = new(string)
	params.Verb = new(string)
	params.Set = new(string)
	params.From = new(string)
	params.Until = new(string)
	params.Identifier = new(string)
	params.ResumptionToken = new(string)
}

func InitializeParameters() *Parameters {
	var params *Parameters = new(Parameters)
	params.initialize()
	*params.MetadataPrefix = utils.MetaFormatFor["ARXIV_RAW"]
	*params.Verb = utils.VerbFor["IDENTIFY"]
	*params.Set = utils.EmptyString
	*params.From = utils.EmptyString
	*params.Until = utils.EmptyString
	*params.Identifier = utils.EmptyString
	*params.ResumptionToken = utils.EmptyString

	return params
}

func (params *Parameters) SetParams(paramNames []string, paramValues []string) error {
	var err error = nil
	for i, value := range paramNames {
		paramValue := paramValues[i]
		paramName := value
		switch paramName {
		case "metadataPrefix":
			*params.MetadataPrefix = paramValue
			break
		case "verb":
			*params.Verb = paramValue
			break
		case "set":
			*params.Set = paramValue
			break
		case "from":
			*params.From = paramValue
			break
		case "until":
			*params.Until = paramValue
			break
		case "identifier":
			*params.Identifier = paramValue
			break
		case "resumptionToken":
			*params.ResumptionToken = paramValue
		default:
			err = errors.New("Wrong paramName specified")
		}
		if err != nil {
			break
		}

	}
	return err
}

func (params *Parameters) String() string {
	var tempMap = make(map[string]interface{})
	tempMap["metadataPrefix"] = *params.MetadataPrefix
	tempMap["verb"] = *params.Verb
	tempMap["set"] = *params.Set
	tempMap["from"] = *params.From
	tempMap["until"] = *params.Until
	tempMap["identifier"] = *params.Identifier
	tempMap["resumptionToken"] = *params.ResumptionToken

	payload, err := json.Marshal(tempMap)
	if err != nil {
		panic(err)
	}

	return string(payload)
}

func (params *Parameters) ParseString(paramsPayload string) {
	var parameters map[string]string
	json.Unmarshal([]byte(paramsPayload), &parameters)
	var paramNames, paramValues []string
	for k, v := range parameters {
		paramNames = append(paramNames, k)
		paramValues = append(paramValues, v)
	}
	err := params.SetParams(paramNames, paramValues)
	if err != nil {
		panic(err)
	}
}
