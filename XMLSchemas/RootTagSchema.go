package xmlschemas

import "encoding/xml"

type RootTag struct {
	XMLName     xml.Name    `xml:"OAI-PMH"`
	ListRecords ListRecords `xml:"ListRecords"`
}
