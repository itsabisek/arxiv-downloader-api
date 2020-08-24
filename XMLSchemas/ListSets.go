package xmlschemas

import "encoding/xml"

type ListSets struct {
	XMLName xml.Name `xml:"ListSets"`
	Sets    []Set    `xml:"set"`
}

type Set struct {
	XMLName xml.Name `xml:"set"`
	SetSpec string   `xml:"setSpec"`
	SetName string   `xml:"setName"`
}
