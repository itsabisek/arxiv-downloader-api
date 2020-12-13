package xmlschemas

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Paper struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	PaperID     string             `bson:"paper_id"`
	Title       string             `bson:"title"`
	PublishDate string             `bson:"publish_date"`
	Authors     []string           `bson:"authors"`
	Categories  []string           `bson:"categories"`
	Set         string             `bson:"set"`
	Abstract    string             `bson:"abstract_text"`
	Versions    []PaperVersion     `bson:"versions"`
}

type PaperVersion struct {
	VersionNum     int    `bson:"version_num"`
	SubmissionDate string `bson:"submission_date"`
}
