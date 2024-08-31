package resto

import (
	"context"
	"errors"
	"fmt"
	"github.com/MereleDulci/resto/pkg/access"
	"github.com/MereleDulci/resto/pkg/hook"
	"github.com/MereleDulci/resto/pkg/relationships"
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
	Find(context.Context, resource.Req) ([]resource.Resourcer, error)
	Create(context.Context, resource.Req) ([]resource.Resourcer, error)
	Update(context.Context, resource.Req) (resource.Resourcer, error)
	Delete(context.Context, resource.Req) error
	Include(context.Context, []resource.Resourcer, resource.Req) ([]resource.Resourcer, error)
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

func (rh *ResourceHandle) Find(ctx context.Context, r resource.Req) ([]resource.Resourcer, error) {
	ctx, cancel := context.WithTimeout(ctx, rh.timeouts.ReadsTimeout)
	defer cancel()

	r = r.WithMethod(MethodGet)

	reschan := make(chan ReadResult)

	go func() {
		defer close(reschan)

		//Validate read access on the resource by the requestor in principle
		applicablePolicies, err := access.ReadPolicyFilter(ctx, rh.encoder.Policies(), r)
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
		r, err := rh.Hooks.RunBeforeReads(ctx, r)
		if err != nil {
			reschan <- ReadResult{nil, err}
			return
		}
		moddedQuery := r.Query()
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

		query := r.Query()
		opts := &options.FindOptions{
			Projection: projection,
			Limit:      &query.Limit,
			Skip:       &query.Skip,
		}
		restrictionQuery, err := access.AccessQueryByPolicy(ctx, applicablePolicies, r)
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

		if len(r.Query().Sort) > 0 {
			sort := bson.D{}
			for _, sortKey := range r.Query().Sort {
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

			afterWg.Add(1)
			go func(index int) {
				if afterTransformed, err := rh.Hooks.RunAfterReads(ctx, r, result); err != nil {
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

		afterAllTransformed, err := rh.Hooks.RunAfterReadAll(ctx, r, results)
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
			return rh.Include(ctx, res.result, r)
			//return res.result, res.err
		}
	}
}

func (rh *ResourceHandle) Create(ctx context.Context, r resource.Req) ([]resource.Resourcer, error) {

	ctx, cancel := context.WithTimeout(ctx, rh.timeouts.WritesTimeout)
	defer cancel()

	r = r.WithMethod(MethodPost)
	resources, ok := r.Payload().([]resource.Resourcer)
	if !ok {
		return nil, errors.New("invalid payload")
	}

	var reschan = make(chan CreateResult)

	go func() {
		defer close(reschan)

		applicablePolicies, err := access.CreatePolicyFilter(ctx, rh.encoder.Policies(), r)
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
		for _, record := range resources {
			beforeWg.Add(1)
			go func() {
				if err := access.ValidateCreateWritableWhitelist(rh.encoder.WritableMappingPost(), applicablePolicies, record); err != nil {
					rh.log.Error().Stack().Err(err).Msg("requested create paths are invalid")
					reschan <- CreateResult{err: err}
					return
				}

				record.InitID()

				r, record, err = rh.Hooks.RunBeforeCreates(ctx, r, record)
				if err != nil {
					beforeTransformChan <- SingleHookResult{nil, err, 0}
				} else {
					beforeTransformChan <- SingleHookResult{record, nil, 0}
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

		r = r.WithPayload(beforeTransformed)
		if _, err := rh.collection.InsertMany(ctx, beforeTransformed); err != nil {
			reschan <- CreateResult{[]resource.Resourcer{}, err}
			return
		}

		afterWg := sync.WaitGroup{}
		afterTransformChan := make(chan SingleHookResult)
		for _, record := range resources {
			afterWg.Add(1)
			go func() {
				if afterTransformed, err := rh.Hooks.RunAfterCreates(ctx, r, record); err != nil {
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
			return rh.Include(ctx, res.result, r)
		}
	}
}

func (rh *ResourceHandle) Update(ctx context.Context, r resource.Req) (resource.Resourcer, error) {
	ctx, cancel := context.WithTimeout(ctx, rh.timeouts.WritesTimeout)
	defer cancel()

	r = r.WithMethod(MethodPatch)
	id := r.Id()
	operations, ok := r.Payload().([]typecast.PatchOperation)
	if !ok {
		return nil, errors.New("invalid payload")
	}

	reschan := make(chan UpdateResult)

	go func() {
		defer close(reschan)

		applicablePolicies, err := access.UpdatePolicyFilter(ctx, rh.encoder.Policies(), r)
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

		updateRestrictionQuery, err := access.AccessQueryByPolicy(ctx, applicablePolicies, r)
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
		r, transformedOperations, err := rh.Hooks.RunBeforeUpdates(ctx, r, operations)
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

		typeCastedQuery, err := rh.resourceTypeCast.Query(r.Query().Filter)
		if err != nil {
			reschan <- UpdateResult{nil, err}
			return
		}

		r = r.WithPayload(transformedOperations)
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

		transformedResource, err := rh.Hooks.RunAfterUpdates(ctx, r, updatedResource)
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

func (rh *ResourceHandle) Delete(ctx context.Context, r resource.Req) error {
	ctx, cancel := context.WithTimeout(ctx, rh.timeouts.WritesTimeout)
	defer cancel()

	r = r.WithMethod(MethodDelete)
	id := r.Id()

	reschan := make(chan DeleteResult)

	go func() {
		defer close(reschan)
		applicablePolicies, err := access.DeletePolicyFilter(ctx, rh.encoder.Policies(), r)
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

		deleteRestrictionQuery, err := access.AccessQueryByPolicy(ctx, applicablePolicies, r)
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

		//Access is validated - resolve full document to apply before transform hooks. No access to read mean you cannot delete either
		resources, err := rh.Find(ctx, r.WithQuery(resource.Query{Filter: map[string]string{"id": id}}))
		if err != nil {
			reschan <- DeleteResult{err: err}
			return
		}
		if len(resources) == 0 {
			reschan <- DeleteResult{err: errors.New("not found")}
			return
		}

		r, err = rh.Hooks.RunBeforeDeletes(ctx, r, resources[0])
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

		err = rh.Hooks.RunAfterDeletes(ctx, r, resources[0])
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
func (rh *ResourceHandle) Include(ctx context.Context, primary []resource.Resourcer, masterReq resource.Req) ([]resource.Resourcer, error) {
	//Identify if include is requested, pass otherwise
	if len(masterReq.Query().Include) == 0 {
		rh.log.Trace().Msg("No include requested")
		return primary, nil
	}

	refPaths := rh.encoder.References()
	//Build sub-context for additional finds
	scopedClient := rh.systemClient.ScopeToToken(masterReq.Token())

	//Confirm the request is legit and can be processed
	//TODO: distribute to goroutines
	for _, requestedInclude := range relationships.GetTopLevelIncludeKeys(masterReq.Query().Include) {
		referenceConfig, ok := lo.Find(refPaths, func(path relationships.IncludePath) bool {
			return path.Path == requestedInclude
		})
		if !ok {
			return nil, errors.New("invalid include path" + requestedInclude)
		}

		referencedIds := relationships.GetReferencedIdsFromPrimary(primary, referenceConfig)
		if len(referencedIds) == 0 {
			rh.log.Trace().Str("include_path", referenceConfig.LocalField).Msg("No referenced ids found")
			continue
		}

		rh.log.Trace().Str("include_path", referenceConfig.LocalField).Interface("ids", referencedIds).Msg("Referenced ids")
		secondary, err := scopedClient.Resource(referenceConfig.Resource).Read(ctx, resource.Query{
			Filter: map[string]string{
				fmt.Sprintf("%s[$in]", referenceConfig.RemoteField): strings.Join(referencedIds, ","),
			},
			Include: relationships.GetSubIncludeKeysForPrefix(requestedInclude, masterReq.Query().Include),
		})
		if err != nil {
			return nil, err
		}

		rh.log.Trace().Str("include_path", referenceConfig.LocalField).Int("count", len(secondary)).Msg("Secondary resources")
		mergeErr := relationships.MergeWithIncluded(primary, secondary, referenceConfig)
		if mergeErr != nil {
			return nil, mergeErr
		}

	}
	return primary, nil
}

func OIDFromReference(resource resource.Resourcer) primitive.ObjectID {
	var oid primitive.ObjectID
	if resource == nil || resource.GetID() == "" {
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

func OIDsFromReferenceSlice[T resource.Resourcer](refs []T) []primitive.ObjectID {
	out := make([]primitive.ObjectID, len(refs))
	for i, r := range refs {
		out[i] = OIDFromReference(r)
	}
	return out
}
