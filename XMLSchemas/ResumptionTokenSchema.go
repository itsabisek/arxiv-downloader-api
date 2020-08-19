package xmlschemas

import (
	"encoding/json"
	"encoding/xml"
	"strconv"
)

type ResumptionToken struct {
	XMLName      xml.Name `xml:"resumptionToken"`
	Value        string   `xml:",chardata"`
	Cursor       int      `xml:"cursor,attr"`
	CompleteSize int      `xml:"completeListSize,attr"`
}

func (resumptionToken *ResumptionToken) String() string {
	var tempMap = make(map[string]interface{})
	tempMap["value"] = resumptionToken.Value
	tempMap["cursor"] = strconv.Itoa(resumptionToken.Cursor)
	tempMap["complete_size"] = strconv.Itoa(resumptionToken.CompleteSize)

	payload, err := json.Marshal(tempMap)
	if err != nil {
		panic(err)
	}
	return string(payload)
}

func (resumptionToken *ResumptionToken) ParseString(tokenPayload string) {
	var token map[string]string
	json.Unmarshal([]byte(tokenPayload), &token)
	resumptionToken.Value = token["value"]
	resumptionToken.Cursor, _ = strconv.Atoi(token["cursor"])
	resumptionToken.CompleteSize, _ = strconv.Atoi(token["complete_size"])
}
