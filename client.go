package resto

import (
	"fmt"
	"github.com/MereleDulci/resto/pkg/access"
	"github.com/MereleDulci/resto/pkg/collection"
	"github.com/MereleDulci/resto/pkg/req"
	"github.com/MereleDulci/resto/pkg/typecast"
	"github.com/rs/zerolog/log"
	"time"
)

func MakeSystemClient() Accessor {
	ctx := req.NewCtx()
	ctx.SetAuthentication(&access.Token{
		ID:        "system",
		CreatedAt: time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt: time.Unix(1<<63-62135596801, 999999999),
	})

	return &ResourceClient{referenceContext: ctx, handlers: map[string]*ResourceHandle{}}
}

type Accessor interface {
	Resource(resourceName string) Requester
	RegisterHandlers(handlers map[string]*ResourceHandle)
	ScopeToToken(authentication *access.Token) Accessor
}

type ResourceClient struct {
	referenceContext *req.Ctx
	handlers         map[string]*ResourceHandle
}

// ScopeToToken returns a new ResourceClient that is scoped to the provided authentication token.
func (client *ResourceClient) ScopeToToken(authentication *access.Token) Accessor {
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

func (client *ResourceClient) Resource(resource string) Requester {
	if _, ok := client.handlers[resource]; !ok {
		panic(fmt.Sprintf("resource %s not registered on this instance of client", resource))
	}
	return MakeClientRequest(client, resource, client.referenceContext.Derive())
}

type Requester interface {
	WithQuery(query *collection.Query) Requester
	Read(query *collection.Query) ([]collection.Resourcer, error)
	ReadOne(id string, query *collection.Query) (collection.Resourcer, error)
	Create(payload []collection.Resourcer) ([]collection.Resourcer, error)
	Update(id string, payload []typecast.PatchOperation) (collection.Resourcer, error)
}

type Request struct {
	query    *collection.Query
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

func (r *Request) WithQuery(query *collection.Query) Requester {

	return &Request{
		query:    query,
		client:   r.client,
		resource: r.resource,
		context:  r.context,
	}
}

func (r *Request) Read(query *collection.Query) ([]collection.Resourcer, error) {
	return r.client.handlers[r.resource].Find(r.context, query)
}

func (r *Request) ReadOne(id string, query *collection.Query) (collection.Resourcer, error) {
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

func (r *Request) Create(payload []collection.Resourcer) ([]collection.Resourcer, error) {
	return r.client.handlers[r.resource].Create(r.context, payload)
}

func (r *Request) Update(id string, payload []typecast.PatchOperation) (collection.Resourcer, error) {
	return r.client.handlers[r.resource].Update(r.context, id, payload, r.query)
}
