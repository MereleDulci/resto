package resource

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type Resourcer interface {
	GetID() string
	InitID()
}

type CollectionMeta struct {
	Count  int64 `json:"count,omitempty"`
	Offset int64 `json:"offset,omitempty"`
}

func (cm CollectionMeta) Map() map[string]interface{} {
	return map[string]interface{}{
		"count":  cm.Count,
		"offset": cm.Offset,
	}
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

func NewQuery() Query {
	return Query{
		Filter:  Filter{},
		Fields:  []string{},
		Include: []string{},
		Sort:    []string{},
	}
}

type TokenExtension struct {
	Key   string
	Value interface{}
}

type Token struct {
	ID         string
	Token      string
	CreatedAt  time.Time
	ExpiresAt  time.Time
	extensions []TokenExtension
}

func (at *Token) GetID() string {
	return at.ID
}

func (at *Token) InitID() {
	at.ID = primitive.NewObjectID().Hex()
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

func NewReq() Req {
	return Req{
		query: NewQuery(),
	}
}

type Req struct {
	id      string
	method  string
	query   Query
	token   *Token
	payload interface{}
}

func (c Req) Id() string {
	return c.id
}

func (c Req) WithId(id string) Req {
	c.id = id
	return c
}

func (c Req) Query() Query {
	return c.query
}

func (c Req) WithQuery(query Query) Req {
	c.query = query
	return c
}

func (c Req) Payload() interface{} {
	return c.payload
}

func (c Req) WithPayload(p interface{}) Req {
	c.payload = p
	return c
}

func (c Req) Method() string {
	return c.method
}

func (c Req) WithMethod(method string) Req {
	c.method = method
	return c
}

func (c Req) Token() *Token {
	if c.token == nil {
		return &Token{}
	}
	return c.token
}

func (c Req) WithToken(t *Token) Req {
	c.token = t
	return c
}
