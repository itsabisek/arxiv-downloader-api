package arxivharvester

import (
	"encoding/xml"
	"reflect"
)

// VerbFor : Verb Mapping
var VerbFor = map[string]string{"GET_RECORD": "GetRecord", "IDENTIFY": "Identify", "LIST_IDENTIFIERS": "ListIdentifiers",
	"LIST_METADATA_FORMATS": "ListMetadataFormats", "LIST_RECORDS": "ListRecords", "LIST_SETS": "ListSets"}

// MetaFormatFor : MetadataPrefix formats
var MetaFormatFor = map[string]string{"OAI": "oai_dc", "ARXIV": "arXiv", "ARXIV_OLD": "arXivOld", "ARXIV_RAW": "arXivRaw"}

const (
	// ArxivOaiBaseURL : Base URL for harvestig OAI data
	ArxivOaiBaseURL = "http://export.arxiv.org/oai2"
	GetRequest      = "GET"
	PostRequest     = "POST"
	EmptyString     = ""
)

var TypeRegistry = map[string]interface{}{"string": reflect.TypeOf((*string)(nil)).Elem(), "int32": reflect.TypeOf((*string)(nil)).Elem(),
	"xml.Name": reflect.TypeOf(xml.Name{})}
