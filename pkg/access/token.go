package access

import (
	"github.com/MereleDulci/resto/pkg/req"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type TokenExtension struct {
	Key   string
	Value interface{}
}

type Token struct {
	ID         string           `jsonapi:"primary,authentication-tokens"`
	Token      string           `jsonapi:"attr,token"`
	CreatedAt  time.Time        `jsonapi:"attr,createdAt,iso8601"`
	ExpiresAt  time.Time        `jsonapi:"attr,expiresAt,iso8601"`
	extensions []TokenExtension `jsonapi:"attr,extensions"`
}

func (at *Token) GetID() string {
	if at == nil {
		return ""
	}
	return at.ID
}

func (at *Token) InitID() {
	at.ID = primitive.NewObjectID().Hex()
}

func (at *Token) Clone() req.Cloner {
	next := *at
	return &next
}

func (at *Token) GetAttr(key string) TokenExtension {
	for _, ext := range at.extensions {
		if ext.Key == key {
			return ext
		}
	}

	return TokenExtension{}
}

func (at *Token) SetAttr(key string, val interface{}) {
	for i, ext := range at.extensions {
		if ext.Key == key {
			at.extensions[i].Value = val
			return
		}
	}

	at.extensions = append(at.extensions, TokenExtension{Key: key, Value: val})
}
