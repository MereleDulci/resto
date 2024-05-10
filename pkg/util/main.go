package util

import (
	"github.com/MereleDulci/resto/pkg/typecast"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type HasID interface {
	GetID() string
}

func GetOIDFromResource(resource HasID) primitive.ObjectID {
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

func ExtractOIDsFromReferenceSlice[T typecast.Resource](refs []T) []primitive.ObjectID {
	out := make([]primitive.ObjectID, len(refs))
	for i, r := range refs {
		out[i] = GetOIDFromResource(r)
	}
	return out
}
