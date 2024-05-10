package resto

import (
	"fmt"
	"github.com/MereleDulci/resto/pkg/access"
	"github.com/MereleDulci/resto/pkg/req"
	"github.com/MereleDulci/resto/pkg/typecast"
	"github.com/rs/zerolog/log"
	"time"
)

func MakeSystemClient() RequestProducer {
	ctx := req.MakeNewCtx()
	ctx.SetAuthentication(&access.AuthenticationToken{
		ID:        "system",
		CreatedAt: time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt: time.Unix(1<<63-62135596801, 999999999),
	})

	return &ResourceClient{referenceContext: ctx, handlers: map[string]*ResourceHandle{}}
}

type RequestProducer interface {
	Resource(resourceName string) Requestor
	RegisterHandlers(handlers map[string]*ResourceHandle)
	ScopeToToken(authentication *access.AuthenticationToken) RequestProducer
}

type ResourceClient struct {
	referenceContext *req.Ctx
	handlers         map[string]*ResourceHandle
}

// ScopeToToken returns a new ResourceClient that is scoped to the provided authentication token.
func (client *ResourceClient) ScopeToToken(authentication *access.AuthenticationToken) RequestProducer {
	defer func() {
		if r := recover(); r != nil {
			log.Error().Msgf("panic: %v", r)
		}
	}()
	nextCtx := client.referenceContext.Derive()
	nextCtx.SetAuthentication(authentication)

	return &ResourceClient{referenceContext: nextCtx, handlers: client.handlers}
}

func (client *ResourceClient) RegisterHandler(resourceName string, handler *ResourceHandle) {
	client.handlers[resourceName] = handler
}

func (client *ResourceClient) RegisterHandlers(handlers map[string]*ResourceHandle) {
	for resourceName, handler := range handlers {
		client.RegisterHandler(resourceName, handler)
	}
}

func (client *ResourceClient) Resource(resource string) Requestor {
	if _, ok := client.handlers[resource]; !ok {
		panic(fmt.Sprintf("resource %s not registered on this instance of client", resource))
	}
	return MakeClientRequest(client, resource, client.referenceContext.Derive())
}

type Requestor interface {
	WithQuery(query *typecast.ResourceQuery) Requestor
	Read(query *typecast.ResourceQuery) ([]typecast.Resource, error)
	ReadOne(id string, query *typecast.ResourceQuery) (typecast.Resource, error)
	Create(payload []typecast.Resource) ([]typecast.Resource, error)
	Update(id string, payload []typecast.PatchOperation) (typecast.Resource, error)
}

type Request struct {
	query    *typecast.ResourceQuery
	client   *ResourceClient
	resource string
	context  *req.Ctx
}

func MakeClientRequest(client *ResourceClient, resource string, context *req.Ctx) *Request {
	return &Request{
		client:   client,
		resource: resource,
		context:  context,
	}
}

func (r *Request) WithContext(context *req.Ctx) *Request {
	return &Request{
		client:   r.client,
		resource: r.resource,
		context:  context,
	}
}

func (r *Request) GetContext() *req.Ctx {
	return r.context
}

func (r *Request) WithQuery(query *typecast.ResourceQuery) Requestor {

	return &Request{
		query:    query,
		client:   r.client,
		resource: r.resource,
		context:  r.context,
	}
}

func (r *Request) Read(query *typecast.ResourceQuery) ([]typecast.Resource, error) {
	return r.client.handlers[r.resource].Find(r.context, query)
}

func (r *Request) ReadOne(id string, query *typecast.ResourceQuery) (typecast.Resource, error) {
	if query.Filter == nil {
		query.Filter = map[string]string{}
	}
	query.Filter["id"] = id
	out, err := r.client.handlers[r.resource].Find(r.context, query)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out[0], nil
}

func (r *Request) Create(payload []typecast.Resource) ([]typecast.Resource, error) {
	return r.client.handlers[r.resource].Create(r.context, payload)
}

func (r *Request) Update(id string, payload []typecast.PatchOperation) (typecast.Resource, error) {
	return r.client.handlers[r.resource].Update(r.context, id, payload, r.query)
}

/*
// Simplifies tests by providing a mock implementation
type MockSystemClient struct {
	mock.Mock
}

func (msc *MockSystemClient) ScopeToToken(token *access.AuthenticationToken) RequestProducer {
	msc.Called(token)
	return msc
}

func (msc *MockSystemClient) Resource(name string) Requestor {
	args := msc.Called(name)
	return args.Get(0).(*MockRequest)
}

func (msc *MockSystemClient) RegisterHandlers(handlers map[string]*ResourceHandle) {
	msc.Called(handlers)
}

type MockRequest struct {
	mock.Mock
}

func (mr *MockRequest) WithQuery(query *typecast.ResourceQuery) Requestor {
	mr.Called(query)
	return mr
}

func (mr *MockRequest) Read(query *typecast.ResourceQuery) ([]typecast.Resource, error) {
	args := mr.Called(query)
	face := args.Get(0)
	if face != nil {
		return face.([]typecast.Resource), args.Error(1)
	}
	return nil, args.Error(1)
}

func (mr *MockRequest) ReadOne(id string, query *typecast.ResourceQuery) (typecast.Resource, error) {
	args := mr.Called(id, query)
	face := args.Get(0)
	if face != nil {
		return face.(typecast.Resource), args.Error(1)
	}
	return nil, args.Error(1)
}

func (mr *MockRequest) Create(payload []typecast.Resource) ([]typecast.Resource, error) {
	args := mr.Called(payload)
	face := args.Get(0)
	if face != nil {
		return face.([]typecast.Resource), args.Error(1)
	}
	return nil, args.Error(1)
}

func (mr *MockRequest) Update(id string, payload []typecast.PatchOperation) (typecast.Resource, error) {
	args := mr.Called(id, payload)
	face := args.Get(0)
	if face != nil {
		return face.(typecast.Resource), args.Error(1)
	}
	return nil, args.Error(1)
}
*/
