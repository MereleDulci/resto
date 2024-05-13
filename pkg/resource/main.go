package resource

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type Resourcer interface {
	GetID() string
	InitID()
}

type Filter map[string]string

type Query struct {
	Filter  Filter
	Fields  []string
	Include []string
	Sort    []string
	Limit   int64
	Skip    int64
}

func NewQuery() *Query {
	return &Query{
		Filter:  Filter{},
		Fields:  []string{},
		Include: []string{},
		Sort:    []string{},
	}
}

func GetOIDFromResource(resource Resourcer) primitive.ObjectID {
	var oid primitive.ObjectID
	if resource.GetID() == "" {
		return primitive.NilObjectID
	}

	oid, err := primitive.ObjectIDFromHex(resource.GetID())
	if err != nil {
		panic(err)
	}
	return oid
}

func ParseTimestamp(stamp string) time.Time {
	t, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		t = time.Time{}
	}
	return t
}

func ExtractOIDsFromReferenceSlice[T Resourcer](refs []T) []primitive.ObjectID {
	out := make([]primitive.ObjectID, len(refs))
	for i, r := range refs {
		out[i] = GetOIDFromResource(r)
	}
	return out
}
