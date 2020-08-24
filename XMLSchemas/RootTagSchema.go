package xmlschemas

import "encoding/xml"

type RootTag struct {
	XMLName     xml.Name    `xml:"OAI-PMH"`
	ListRecords ListRecords `xml:"ListRecords"`
}

type SetRoot struct {
	XMLName  xml.Name `xml:"OAI-PMH"`
	ListSets ListSets `xml:"ListSets"`
}

