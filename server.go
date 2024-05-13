package resto

import (
	"context"
	"errors"
	"fmt"
	"github.com/MereleDulci/resto/pkg/access"
	"github.com/MereleDulci/resto/pkg/hook"
	"github.com/MereleDulci/resto/pkg/relationships"
	"github.com/MereleDulci/resto/pkg/req"
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/MereleDulci/resto/pkg/typecast"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	MethodPost   = "POST"
	MethodGet    = "GET"
	MethodPatch  = "PATCH"
	MethodDelete = "DELETE"
)

type ResourceTimeoutSettings struct {
	WritesTimeout time.Duration
	ReadsTimeout  time.Duration
}

type Decoder interface {
	Decode(interface{}) error
}

type ResourceEncoder interface {
	// ResourceTypeCast Casts user requested query to matching underlying datatypes
	TypeCast() typecast.ResourceTypeCast
	//GetAccessPolicies returns list of access policies for the resource
	Policies() []access.AccessPolicy
	ReadableMapping() map[access.PolicyName][]string
	WritableMappingPatch() map[access.PolicyName][]string
	WritableMappingPost() map[access.PolicyName][]string
	// CursorEncoder Encodes the provided resource to DB representation of self
	CursorEncoder(resource resource.Resourcer) (interface{}, error)
	WithCursorEncoder(newEncoder CursorEncoder) *Encoder
	// CursorDecoder Decodes the provided DB cursor to the public representation of self
	CursorDecoder(decoder Decoder) (resource.Resourcer, error)
	WithCursorDecoder(newDecoder CursorDecoder) *Encoder
	References() []relationships.IncludePath
}

type ResourceHandler interface {
	GetResourceName() string
	GetResourceReflectType() reflect.Type
	GetHooks() *hook.Registry
	Find(*req.Ctx, *resource.Query) ([]resource.Resourcer, error)
	Create(*req.Ctx, []resource.Resourcer) ([]resource.Resourcer, error)
	Update(*req.Ctx, string, []typecast.PatchOperation, *resource.Query) (resource.Resourcer, error)
	Delete(*req.Ctx, string) error
	Include(*req.Ctx, []resource.Resourcer, *resource.Query) ([]resource.Resourcer, error)
}

type Encoder struct {
	referencePaths       []relationships.IncludePath
	policies             []access.AccessPolicy
	queryTypeCast        typecast.ResourceTypeCast
	readableMapping      map[access.PolicyName][]string
	writableMappingPatch map[access.PolicyName][]string
	writableMappingPost  map[access.PolicyName][]string
	cursorEncoder        CursorEncoder
	cursorDecoder        CursorDecoder
}

type ResourceHandle struct {
	ResourceType     reflect.Type
	Hooks            hook.Registry
	resourceTypeCast typecast.ResourceTypeCast
	encoder          ResourceEncoder
	collection       *mongo.Collection
	systemClient     Accessor
	timeouts         ResourceTimeoutSettings
	log              zerolog.Logger
}

type SingleHookResult struct {
	resource   resource.Resourcer
	err        error
	orderIndex int
}

type ReadResult struct {
	result []resource.Resourcer
	err    error
}

type CreateResult struct {
	result []resource.Resourcer
	err    error
}

type UpdateResult struct {
	result resource.Resourcer
	err    error
}

type DeleteResult struct {
	err error
}

type CursorEncoder func(resource resource.Resourcer) (interface{}, error)
type CursorDecoder func(decoder Decoder) (resource.Resourcer, error)

func makeResourceEncoder(resourceType reflect.Type, accessPolicies []access.AccessPolicy) ResourceEncoder {
	return &Encoder{
		referencePaths:       relationships.GetReferencesMapping(resourceType),
		policies:             accessPolicies,
		queryTypeCast:        typecast.MakeTypeCastFromResource(resourceType),
		readableMapping:      access.GetReadableMapping(resourceType, accessPolicies),
		writableMappingPatch: access.GetWritableMappingPatch(resourceType, accessPolicies),
		writableMappingPost:  access.GetWritableMappingPost(resourceType, accessPolicies),
		cursorEncoder: func(resource resource.Resourcer) (interface{}, error) {
			return resource, nil
		},
		cursorDecoder: func(decoder Decoder) (resource.Resourcer, error) {
			instance := reflect.New(resourceType)
			iface := instance.Interface()
			err := decoder.Decode(iface)
			return iface.(resource.Resourcer), err
		},
	}
}

func MakeResourceHandler(t reflect.Type, collection *mongo.Collection, accessPolicies []access.AccessPolicy, systemClient Accessor) *ResourceHandle {

	encoder := makeResourceEncoder(t.Elem(), accessPolicies)

	h := &ResourceHandle{
		ResourceType:     t,
		Hooks:            hook.NewRegistry(),
		resourceTypeCast: typecast.MakeTypeCastFromResource(t.Elem()),
		encoder:          encoder,
		collection:       collection,
		systemClient:     systemClient,
		timeouts: ResourceTimeoutSettings{
			WritesTimeout: 5 * time.Second,
			ReadsTimeout:  5 * time.Second,
		},
		log: log.Logger,
	}

	return h.WithLogger(zerolog.New(io.Discard))
}

func (enc *Encoder) TypeCast() typecast.ResourceTypeCast {
	return enc.queryTypeCast
}
func (enc *Encoder) Policies() []access.AccessPolicy {
	return enc.policies
}
func (enc *Encoder) ReadableMapping() map[access.PolicyName][]string {
	return enc.readableMapping
}
func (enc *Encoder) WritableMappingPatch() map[access.PolicyName][]string {
	return enc.writableMappingPatch
}
func (enc *Encoder) WritableMappingPost() map[access.PolicyName][]string {
	return enc.writableMappingPost
}
func (enc *Encoder) WithCursorEncoder(newEncoder CursorEncoder) *Encoder {
	enc.cursorEncoder = newEncoder
	return enc
}
func (enc *Encoder) CursorEncoder(resource resource.Resourcer) (interface{}, error) {
	return enc.cursorEncoder(resource)
}
func (enc *Encoder) WithCursorDecoder(newDecoder CursorDecoder) *Encoder {
	enc.cursorDecoder = newDecoder
	return enc
}
func (enc *Encoder) CursorDecoder(decoder Decoder) (resource.Resourcer, error) {
	return enc.cursorDecoder(decoder)
}
func (enc *Encoder) References() []relationships.IncludePath {
	return enc.referencePaths
}

func (rh *ResourceHandle) WithLogger(logger zerolog.Logger) *ResourceHandle {
	rh.log = logger.With().Str("resource", rh.ResourceType.String()).Logger()
	return rh
}

func (rh *ResourceHandle) GetResourceName() string {
	return rh.ResourceType.String()
}

func (rh *ResourceHandle) GetHooks() *hook.Registry {
	return &rh.Hooks
}

func (rh *ResourceHandle) GetResourceReflectType() reflect.Type {
	return rh.ResourceType
}

func (rh *ResourceHandle) Find(resourceCtx *req.Ctx, query *resource.Query) ([]resource.Resourcer, error) {
	defer func() {
		if r := recover(); r != nil {
			rh.log.Error().Interface("panic", r).Msg("panic in resource handler")
			panic(r)
		}
	}()
	ctx, cancel := context.WithTimeout(resourceCtx.UserContext(), rh.timeouts.ReadsTimeout)
	defer cancel()

	reschan := make(chan ReadResult)
	defer close(reschan)

	go func() {
		resourceCtx.SetMethod(MethodGet)
		//Validate read access on the resource by the requestor in principle
		applicablePolicies, err := access.ReadPolicyFilter(rh.encoder.Policies(), resourceCtx, rh.collection)
		if err != nil {
			reschan <- ReadResult{nil, err}
			return
		}
		policyNames := lo.Map(applicablePolicies, func(p access.AccessPolicy, i int) string {
			return string(p.Name)
		})
		rh.log.Trace().Str("operation", "read").Str("resource", rh.ResourceType.String()).Strs("policies", policyNames).Msg("read access policies acquired")

		if len(applicablePolicies) == 0 {
			reschan <- ReadResult{nil, errors.New("no applicable policies found for read")}
			return
		}

		rh.log.Trace().Str("operation", "read").Str("resource", rh.ResourceType.String()).Msg("running before read hooks")
		moddedQuery, err := rh.Hooks.RunBeforeReads(resourceCtx, query)
		if err != nil {
			reschan <- ReadResult{nil, err}
			return
		}
		rh.log.Trace().
			Str("operation", "read").
			Str("resource", rh.ResourceType.String()).
			Interface("moddedQuery", moddedQuery).
			Msg("before read hooks finished")

		projection, err := access.MappingToReadProjection(rh.encoder.ReadableMapping(), applicablePolicies, rh.resourceTypeCast.RenameFields(moddedQuery.Fields))
		if err != nil {
			reschan <- ReadResult{nil, err}
			return
		}

		opts := &options.FindOptions{
			Projection: projection,
			Limit:      &query.Limit,
			Skip:       &query.Skip,
		}
		restrictionQuery, err := access.AccessQueryByPolicy(applicablePolicies, resourceCtx)
		if err != nil {
			reschan <- ReadResult{nil, err}
			return
		}

		rh.log.Trace().
			Str("operation", "read").
			Str("resource", rh.ResourceType.String()).
			Interface("restrictionQuery", restrictionQuery).
			Msg("restriction query acquired")

		typeCastedQuery, err := rh.resourceTypeCast.Query(moddedQuery.Filter)
		if err != nil {
			reschan <- ReadResult{nil, err}
			return
		}

		rh.log.Trace().
			Str("operation", "read").
			Str("resource", rh.ResourceType.String()).
			Interface("typeCastedQuery", typeCastedQuery).
			Msg("type casted query acquired")

		fullFilter := bson.D{
			{Key: "$and", Value: bson.A{
				typeCastedQuery,
				restrictionQuery,
			}},
		}

		if len(query.Sort) > 0 {
			sort := bson.D{}
			for _, sortKey := range query.Sort {
				cleanKey := rh.resourceTypeCast.RenameFields([]string{strings.Replace(sortKey, "-", "", 1)})[0]

				sort = append(sort, bson.E{
					Key:   cleanKey,
					Value: lo.Ternary(strings.HasPrefix(sortKey, "-"), -1, 1),
				})
			}
			opts.SetSort(sort)
		}

		cursor, err := rh.collection.Find(ctx, fullFilter, opts)
		if err != nil {
			reschan <- ReadResult{nil, err}
			return
		}

		afterWg := sync.WaitGroup{}
		afterTransformChan := make(chan SingleHookResult)
		orderIndex := 0
		for cursor.Next(ctx) {
			afterWg.Add(1)

			result, err := rh.encoder.CursorDecoder(cursor)
			if err != nil {
				afterTransformChan <- SingleHookResult{nil, err, 0}
				return
			}

			rh.log.Trace().
				Str("operation", "read").
				Str("resource", rh.ResourceType.String()).
				Str("id", result.GetID()).
				Msg("starting after transform for individual resource")
			go func(index int) {
				if afterTransformed, err := rh.Hooks.RunAfterReads(resourceCtx, result); err != nil {
					afterTransformChan <- SingleHookResult{nil, err, 0}
				} else {
					afterTransformChan <- SingleHookResult{afterTransformed, nil, index}
				}
				afterWg.Done()

				rh.log.Trace().
					Str("operation", "read").
					Str("resource", rh.ResourceType.String()).
					Str("id", result.GetID()).
					Msg("finished after transform for individual resource")
			}(orderIndex)

			orderIndex += 1
		}

		go func() {
			afterWg.Wait()
			close(afterTransformChan)
			rh.log.Trace().
				Str("operation", "read").
				Str("resource", rh.ResourceType.String()).
				Msg("after transform channel closed")
		}()

		if err := cursor.Err(); err != nil {
			rh.log.Error().Stack().Err(err).
				Str("operation", "read").
				Str("resource", rh.ResourceType.String()).
				Msg("cursor error")
			reschan <- ReadResult{nil, err}
			return
		}

		results := make([]resource.Resourcer, orderIndex)
		for afterResult := range afterTransformChan {
			if afterResult.err != nil {
				rh.log.Error().Stack().Err(afterResult.err).
					Str("operation", "read").
					Str("resource", rh.ResourceType.String()).
					Msg("after transform aggregation error")
				reschan <- ReadResult{nil, afterResult.err}
				return
			}
			results[afterResult.orderIndex] = afterResult.resource
		}
		rh.log.Trace().
			Str("operation", "read").
			Str("resource", rh.ResourceType.String()).
			Int("resultsCount", len(results)).
			Msg("after transform aggregation finished")

		afterAllTransformed, err := rh.Hooks.RunAfterReadAll(resourceCtx, results)
		if err != nil {
			reschan <- ReadResult{nil, err}
		}

		reschan <- ReadResult{afterAllTransformed, nil}

	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-reschan:
			if res.err != nil {
				return res.result, res.err
			}
			return rh.Include(resourceCtx, res.result, query)
			//return res.result, res.err
		}
	}
}

func (rh *ResourceHandle) Create(resourceCtx *req.Ctx, resources []resource.Resourcer) ([]resource.Resourcer, error) {

	ctx, cancel := context.WithTimeout(resourceCtx.UserContext(), rh.timeouts.WritesTimeout)
	defer cancel()

	var reschan = make(chan CreateResult)
	defer close(reschan)

	go func() {

		resourceCtx.SetMethod(MethodPost)
		resourceCtx.SetPayload(resources)
		applicablePolicies, err := access.CreatePolicyFilter(rh.encoder.Policies(), resourceCtx, rh.collection)
		if err != nil {
			reschan <- CreateResult{nil, err}
			return
		}
		if len(applicablePolicies) == 0 {
			reschan <- CreateResult{nil, errors.New("no applicable policies to create")}
			return
		}

		beforeTransformChan := make(chan SingleHookResult)
		beforeWg := sync.WaitGroup{}
		for _, r := range resources {
			beforeWg.Add(1)
			r := r
			go func() {
				if err := access.ValidateCreateWritableWhitelist(rh.encoder.WritableMappingPost(), applicablePolicies, r); err != nil {
					rh.log.Error().Stack().Err(err).Msg("requested create paths are invalid")
					reschan <- CreateResult{err: err}
					return
				}

				r.InitID()

				beforeTransformed, err := rh.Hooks.RunBeforeCreates(resourceCtx, r)
				if err != nil {
					beforeTransformChan <- SingleHookResult{nil, err, 0}
				} else {
					beforeTransformChan <- SingleHookResult{beforeTransformed, nil, 0}
				}
				beforeWg.Done()
			}()
		}

		go func() {
			beforeWg.Wait()
			close(beforeTransformChan)
		}()

		beforeTransformed := make([]interface{}, 0)
		for res := range beforeTransformChan {
			if res.err != nil {
				reschan <- CreateResult{[]resource.Resourcer{}, res.err}
				return
			} else {
				dbView, err := rh.encoder.CursorEncoder(res.resource)
				if err != nil {
					reschan <- CreateResult{[]resource.Resourcer{}, err}
					return
				}
				beforeTransformed = append(beforeTransformed, dbView)
			}
		}

		resourceCtx.SetPayload(beforeTransformed)
		if _, err := rh.collection.InsertMany(ctx, beforeTransformed); err != nil {
			reschan <- CreateResult{[]resource.Resourcer{}, err}
			return
		}

		afterWg := sync.WaitGroup{}
		afterTransformChan := make(chan SingleHookResult)
		for _, r := range resources {
			afterWg.Add(1)
			r := r
			go func() {
				if afterTransformed, err := rh.Hooks.RunAfterCreates(resourceCtx, r); err != nil {
					afterTransformChan <- SingleHookResult{nil, err, 0}
				} else {
					afterTransformChan <- SingleHookResult{afterTransformed, nil, 0}
				}

				afterWg.Done()
			}()
		}

		go func() {
			afterWg.Wait()
			close(afterTransformChan)
		}()
		afterTransformed := make([]resource.Resourcer, 0)
		for res := range afterTransformChan {
			if res.err != nil {
				reschan <- CreateResult{[]resource.Resourcer{}, res.err}
				return
			} else {
				afterTransformed = append(afterTransformed, res.resource)
			}
		}

		reschan <- CreateResult{result: afterTransformed, err: nil}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-reschan:
			if res.err != nil {
				return res.result, res.err
			}
			return rh.Include(resourceCtx, res.result, resourceCtx.Query())
		}
	}
}

func (rh *ResourceHandle) Update(resourceCtx *req.Ctx, id string, operations []typecast.PatchOperation, query *resource.Query) (resource.Resourcer, error) {
	ctx, cancel := context.WithTimeout(resourceCtx.UserContext(), rh.timeouts.WritesTimeout)
	defer cancel()

	reschan := make(chan UpdateResult)
	defer close(reschan)

	go func() {
		resourceCtx.SetMethod(MethodPatch)
		applicablePolicies, err := access.UpdatePolicyFilter(rh.encoder.Policies(), resourceCtx, rh.collection)
		if err != nil {
			reschan <- UpdateResult{err: err}
			return
		}
		if len(applicablePolicies) == 0 {
			reschan <- UpdateResult{err: errors.New("no applicable policies to update")}
			return
		}

		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("update resource")
		oid, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			reschan <- UpdateResult{err: errors.New("invalid id")}
			return
		}

		//The request can read the requested resource at this point, and it's verified that requested id exists
		//Now we need to check if the requested updates are allowed

		if err := access.ValidateUpdateWritableWhitelist(rh.encoder.WritableMappingPatch(), applicablePolicies, operations); err != nil {
			rh.log.Error().Stack().Err(err).Msg("requested update paths are invalid")
			reschan <- UpdateResult{err: err}
			return
		}
		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("requested update paths are valid")

		updateRestrictionQuery, err := access.AccessQueryByPolicy(applicablePolicies, resourceCtx)
		if err != nil {
			rh.log.Error().Stack().Err(err).Msg("error while generating update restriction query")
			reschan <- UpdateResult{err: err}
			return
		}
		if updateRestrictionQuery == nil {
			reschan <- UpdateResult{err: errors.New("update restriction resolved to nil")}
			return
		}
		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Interface("restriction", updateRestrictionQuery).
			Msg("generated update restriction query")

		//Apply before update hooks to allow internal modifications of the updates
		transformedOperations, err := rh.Hooks.RunBeforeUpdates(resourceCtx, operations)
		if err != nil {
			rh.log.Error().Stack().Err(err).Msg("error while applying before update hooks")
			reschan <- UpdateResult{err: err}
			return
		}
		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("before update hooks applied")

		typeCastedQuery := bson.D{}
		if query != nil {
			typeCastedQuery, err = rh.resourceTypeCast.Query(query.Filter)
			if err != nil {
				reschan <- UpdateResult{nil, err}
				return
			}
		}

		resourceCtx.SetPayload(transformedOperations)
		dbOperations, err := rh.resourceTypeCast.PatchToDBOps(transformedOperations)
		if err != nil {
			reschan <- UpdateResult{err: err}
			return
		}
		testQuery, err := rh.resourceTypeCast.PatchTestToQuery(transformedOperations)
		if err != nil {
			reschan <- UpdateResult{err: err}
			return
		}

		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Interface("ops", dbOperations).
			Msg("patch transformed to db operations")

		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Interface("testQuery", testQuery).
			Interface("typeCastedQuery", typeCastedQuery).
			Interface("updateRestrictionQuery", updateRestrictionQuery).
			Msg("applicable queries")

		singleResult := rh.collection.FindOneAndUpdate(ctx,
			bson.D{
				{"$and", bson.A{
					bson.D{{"_id", oid}},
					typeCastedQuery,
					testQuery,
					updateRestrictionQuery,
				}},
			},
			dbOperations,
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		)
		if singleResult.Err() != nil {
			if singleResult.Err() == mongo.ErrNoDocuments {
				reschan <- UpdateResult{err: errors.New("not found")}
				return
			}
			rh.log.Error().Stack().Err(err).Msg("error updating resource")
			reschan <- UpdateResult{err: singleResult.Err()}
			return
		}

		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("update write successful")

		updatedResource, err := rh.encoder.CursorDecoder(singleResult)
		if err != nil {
			rh.log.Error().Stack().Err(err).Msg("error while decoding updated resource")
			reschan <- UpdateResult{err: err}
			return
		}

		transformedResource, err := rh.Hooks.RunAfterUpdates(resourceCtx, updatedResource)
		if err != nil {
			rh.log.Error().Stack().Err(err).Msg("error while applying after update hooks")
			reschan <- UpdateResult{err: err}
			return
		}

		rh.log.Trace().
			Str("operation", "update").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("after update hooks applied")

		reschan <- UpdateResult{result: transformedResource, err: nil}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-reschan:
			return res.result, res.err
		}
	}
}

func (rh *ResourceHandle) Delete(resourceCtx *req.Ctx, id string) error {
	ctx, cancel := context.WithTimeout(resourceCtx.UserContext(), rh.timeouts.WritesTimeout)
	defer cancel()

	reschan := make(chan DeleteResult)
	defer close(reschan)

	go func() {
		resourceCtx.SetMethod(MethodDelete)
		applicablePolicies, err := access.DeletePolicyFilter(rh.encoder.Policies(), resourceCtx, rh.collection)
		if err != nil {
			reschan <- DeleteResult{err: err}
			return
		}

		if len(applicablePolicies) == 0 {
			reschan <- DeleteResult{err: errors.New("no applicable policies to delete")}
			return
		}

		rh.log.Trace().
			Str("operation", "delete").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("delete resource")

		oid, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			reschan <- DeleteResult{err: errors.New("invalid id")}
			return
		}

		deleteRestrictionQuery, err := access.AccessQueryByPolicy(applicablePolicies, resourceCtx)
		if err != nil {
			reschan <- DeleteResult{err: err}
			return
		}
		if deleteRestrictionQuery == nil {
			reschan <- DeleteResult{err: errors.New("delete restriction resolved to nil")}
			return
		}

		accessCheck := rh.collection.FindOne(ctx, deleteRestrictionQuery)
		if accessCheck.Err() != nil {
			if accessCheck.Err() == mongo.ErrNoDocuments {
				reschan <- DeleteResult{err: errors.New("no access to delete")}
				return
			}
			reschan <- DeleteResult{err: accessCheck.Err()}
			return
		}

		//Access is validated - resolve full document to apply before transform hooks
		//TODO: It might potentially have a mismatch on the restriction queries if the policy granting delete access does not provide read access
		resources, err := rh.Find(resourceCtx.Derive(), &resource.Query{Filter: map[string]string{"id": id}})
		if err != nil {
			reschan <- DeleteResult{err: err}
			return
		}
		if len(resources) == 0 {
			reschan <- DeleteResult{err: errors.New("not found")}
			return
		}

		err = rh.Hooks.RunBeforeDeletes(resourceCtx, resources[0])
		if err != nil {
			reschan <- DeleteResult{err: err}
			return
		}

		rh.log.Trace().
			Str("operation", "delete").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("before delete hooks applied")

		_, err = rh.collection.DeleteOne(ctx, bson.D{{"_id", oid}})
		if err != nil {
			reschan <- DeleteResult{err: err}
			return
		}

		rh.log.Trace().
			Str("operation", "delete").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("delete write successful")

		err = rh.Hooks.RunAfterDeletes(resourceCtx, resources[0])
		if err != nil {
			reschan <- DeleteResult{err: err}
			return
		}
		rh.log.Trace().
			Str("operation", "delete").
			Str("resource", rh.ResourceType.String()).
			Str("id", id).
			Msg("after delete hooks applied")

		reschan <- DeleteResult{err: nil}

	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-reschan:
			return res.err
		}
	}
}

/*
JSONAPI implementation picks included resources from the main resource. So include should be looking to extend

	the data of primary resource directly and return the input array back. Input []Resource is a slice of interfaces

with pointers under the hood so can be modified in principle. However concurrency might become an issue and needs
to be treated.
In order to preserve all functionality related to the access rules "fetch" stage of the include should be going
through the rigth .Find handler of the target resource.
There's no direct access to the other resource handles at the moment. However scoped client can be used to access it
*/
func (rh *ResourceHandle) Include(resourceCtx *req.Ctx, primary []resource.Resourcer, masterQuery *resource.Query) ([]resource.Resourcer, error) {

	//ctx, cancel := context.WithTimeout(resourceCtx.UserContext(), rh.timeouts.ReadsTimeout)
	//defer cancel()

	//Identify if include is requested, pass otherwise
	if masterQuery == nil || len(masterQuery.Include) == 0 {
		rh.log.Trace().Msg("No include requested")
		return primary, nil
	}

	refPaths := rh.encoder.References()
	//Build sub-context for additional finds
	scopedClient := rh.systemClient.ScopeToToken(resourceCtx.Authentication().(*access.Token))

	//Confirm the request is legit and can be processed
	for _, requestedInclude := range relationships.GetTopLevelIncludeKeys(masterQuery.Include) {
		referenceConfig, ok := lo.Find(refPaths, func(path relationships.IncludePath) bool {
			return path.Path == requestedInclude
		})
		if !ok {
			return nil, errors.New("invalid include path" + requestedInclude)
		}

		referencedIds := relationships.GetReferencedIdsFromPrimary(primary, referenceConfig)
		if len(referencedIds) == 0 {
			continue
		}

		secondary, err := scopedClient.Resource(referenceConfig.Resource).Read(&resource.Query{
			Filter: map[string]string{
				fmt.Sprintf("%s[$in]", referenceConfig.RemoteField): strings.Join(referencedIds, ","),
			},
			Include: relationships.GetSubIncludeKeysForPrefix(requestedInclude, masterQuery.Include),
		})
		if err != nil {
			return nil, err
		}

		mergeErr := relationships.MergeWithIncluded(primary, secondary, referenceConfig)
		if mergeErr != nil {
			return nil, mergeErr
		}

	}
	//Distribute find commands
	//Aggregate data back
	//Return result

	return primary, nil
}
