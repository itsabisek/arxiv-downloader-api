package arxivharvester

import "encoding/xml"

type RootTag struct {
	XMLName     xml.Name    `xml:"OAI-PMH"`
	ListRecords ListRecords `xml:"ListRecords"`
}

type ListRecords struct {
	XMLName         xml.Name        `xml:"ListRecords"`
	Records         []Record        `xml:"record"`
	ResumptionToken ResumptionToken `xml:"resumptionToken"`
}

type Record struct {
	XMLName  xml.Name `xml:"record"`
	Header   Header   `xml:"header"`
	Metadata Metadata `xml:"metadata"`
}

type Header struct {
	XMLName    xml.Name `xml:"header"`
	Identifier string   `xml:"identifier"`
	Published  string   `xml:"datestamp"`
	SetSpec    string   `xml:"setSpec"`
}

type Metadata struct {
	XMLName  xml.Name `xml:"metadata"`
	Metaroot Metaroot `xml:"arXivRaw"`
}

type Metaroot struct {
	XMLName    xml.Name `xml:"arXivRaw"`
	Version    Version  `xml:"version"`
	Authors    string   `xml:"authors"`
	Title      string   `xml:"title"`
	Categories string   `xml:"categories"`
	Abstract   string   `xml:"abstract"`
}

type Version struct {
	XMLName xml.Name `xml:"version"`
	Vno     string   `xml:"version,attr"`
	Date    string   `xml:"date"`
}

type ResumptionToken struct {
	XMLName      xml.Name `xml:"resumptionToken"`
	Value        string   `xml:",chardata"`
	Cursor       int      `xml:"cursor,attr"`
	CompleteSize int      `xml:"completeListSize,attr"`
}
