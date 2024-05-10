package req

import (
	"context"
	"github.com/MereleDulci/resto/pkg/constants"
	"github.com/MereleDulci/resto/pkg/typecast"
)

type Clonable interface {
	Clone() Clonable
}

func Clone[T Clonable](t T) T {
	return t.Clone().(T)
}

type Locals struct {
	values map[string]interface{}
}

const (
	MethodPost   = "POST"
	MethodGet    = "GET"
	MethodPatch  = "PATCH"
	MethodDelete = "DELETE"
)

func MakeNewCtx() *Ctx {
	return &Ctx{
		locals: &Locals{values: map[string]interface{}{}},
		query:  typecast.MakeResourceQuery(),
	}
}

type Ctx struct {
	id                  string
	method              string
	query               *typecast.ResourceQuery
	locals              *Locals
	payload             interface{}
	authenticationToken Clonable
	userContext         context.Context
}

func (c *Ctx) Id() string {
	return c.id
}

func (c *Ctx) SetId(id string) *Ctx {
	c.id = id
	return c
}

func (c *Ctx) Query() *typecast.ResourceQuery {
	return c.query
}

func (c *Ctx) SetQuery(query *typecast.ResourceQuery) *Ctx {
	c.query = query
	return c
}

func (c *Ctx) Authentication() Clonable {
	return c.authenticationToken
}

func (c *Ctx) SetAuthentication(token Clonable) *Ctx {
	c.authenticationToken = token
	return c
}

func (c *Ctx) Payload() interface{} {
	return c.payload
}

func (c *Ctx) SetPayload(p interface{}) *Ctx {
	c.payload = p
	return c
}

func (c *Ctx) Derive() *Ctx {
	next := MakeNewCtx().
		SetUserContext(context.Background()).
		SetAuthentication(Clone(c.Authentication()))
	next.Locals(constants.LocalsRequestID, c.Locals(constants.LocalsRequestID))
	return next
}

func (c *Ctx) Method() string {
	return c.method
}

func (c *Ctx) SetMethod(method string) *Ctx {
	c.method = method
	return c
}

func (c *Ctx) UserContext() context.Context {
	if c.userContext == nil {
		c.userContext = context.Background()
	}

	return c.userContext
}

func (c *Ctx) SetUserContext(ctx context.Context) *Ctx {
	c.userContext = ctx
	return c
}

func (c *Ctx) Locals(key string, values ...interface{}) interface{} {
	if len(values) == 0 {
		return c.locals.UserValue(key)
	}
	c.locals.SetUserValue(key, values[0])
	return values[0]
}

func (l *Locals) UserValue(key string) interface{} {
	v, ok := l.values[key]
	if !ok {
		return nil
	}
	return v
}

func (l *Locals) SetUserValue(key string, values interface{}) {
	l.values[key] = values
}
