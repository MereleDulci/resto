package resto

import (
	"context"
	"fmt"
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/MereleDulci/resto/pkg/typecast"
	"time"
)

const SystemAccessTokenID = "system"

func MakeSystemClient() Accessor {
	token := &resource.Token{
		ID:        SystemAccessTokenID,
		CreatedAt: time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt: time.Unix(1<<63-62135596801, 999999999),
	}

	return &ResourceClient{referenceToken: token, handlers: map[string]*ResourceHandle{}}
}

type Accessor interface {
	Resource(resourceName string) Requester
	RegisterHandlers(handlers map[string]*ResourceHandle)
	ScopeToToken(authentication *resource.Token) Accessor
}

type ResourceClient struct {
	referenceToken *resource.Token
	handlers       map[string]*ResourceHandle
}

// ScopeToToken returns a new ResourceClient that is scoped to the provided authentication token.
func (client *ResourceClient) ScopeToToken(authentication *resource.Token) Accessor {
	return &ResourceClient{referenceToken: authentication, handlers: client.handlers}
}

func (client *ResourceClient) RegisterHandler(resourceName string, handler *ResourceHandle) {
	client.handlers[resourceName] = handler
}

func (client *ResourceClient) RegisterHandlers(handlers map[string]*ResourceHandle) {
	for resourceName, handler := range handlers {
		client.RegisterHandler(resourceName, handler)
	}
}

func (client *ResourceClient) Resource(resource string) Requester {
	if _, ok := client.handlers[resource]; !ok {
		panic(fmt.Sprintf("resource %s not registered on this instance of client", resource))
	}
	return MakeClientRequest(client, resource)
}

type Requester interface {
	Read(ctx context.Context, query resource.Query) ([]resource.Resourcer, error)
	ReadOne(ctx context.Context, id string, query resource.Query) (resource.Resourcer, error)
	Create(ctx context.Context, payload []resource.Resourcer) ([]resource.Resourcer, error)
	Update(ctx context.Context, id string, query resource.Query, payload []typecast.PatchOperation) (resource.Resourcer, error)
}

type Request struct {
	query    resource.Query
	client   *ResourceClient
	resource string
}

func MakeClientRequest(client *ResourceClient, resource string) Request {
	return Request{
		client:   client,
		resource: resource,
	}
}

func (r Request) Read(ctx context.Context, query resource.Query) ([]resource.Resourcer, error) {
	rq := resource.NewReq().WithToken(r.client.referenceToken).WithQuery(query)
	return r.client.handlers[r.resource].Find(ctx, rq)
}

func (r Request) ReadOne(ctx context.Context, id string, query resource.Query) (resource.Resourcer, error) {
	if query.Filter == nil {
		query.Filter = map[string]string{}
	}
	query.Filter["id"] = id

	rq := resource.NewReq().WithToken(r.client.referenceToken).WithId(id).WithQuery(query)
	out, err := r.client.handlers[r.resource].Find(ctx, rq)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out[0], nil
}

func (r Request) Create(ctx context.Context, payload []resource.Resourcer) ([]resource.Resourcer, error) {
	rq := resource.NewReq().WithToken(r.client.referenceToken).WithPayload(payload)
	return r.client.handlers[r.resource].Create(ctx, rq)
}

func (r Request) Update(ctx context.Context, id string, query resource.Query, payload []typecast.PatchOperation) (resource.Resourcer, error) {
	rq := resource.NewReq().WithToken(r.client.referenceToken).WithId(id).WithQuery(query).WithPayload(payload)
	return r.client.handlers[r.resource].Update(ctx, rq)
}
