package arxivharvester

import (
	"errors"
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
	*params.MetadataPrefix = MetaFormatFor["ARXIV_RAW"]
	*params.Verb = VerbFor["IDENTIFY"]
	*params.Set = EmptyString
	*params.From = EmptyString
	*params.Until = EmptyString
	*params.Identifier = EmptyString
	*params.ResumptionToken = EmptyString

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
