package access

import (
	"errors"
	"fmt"
	"github.com/MereleDulci/resto/pkg/req"
	"github.com/MereleDulci/resto/pkg/typecast"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
	"slices"
	"strings"
)

type PolicyName string
type PolicyPredicate func(p AccessPolicy, ctx *req.Ctx) bool
type ResolveAccessQuery func(p AccessPolicy, ctx *req.Ctx) (bson.D, error)
type ExtendAuthenticationContext func(ctx *req.Ctx, collection *mongo.Collection) error

type AccessPolicy struct {
	Name                        PolicyName
	CanRead                     PolicyPredicate
	CanCreate                   PolicyPredicate
	CanUpdate                   PolicyPredicate
	CanDelete                   PolicyPredicate
	ExtendAuthenticationContext ExtendAuthenticationContext
	IsApplicable                PolicyPredicate
	ResolveAccessQuery          ResolveAccessQuery
}

func (p AccessPolicy) OverrideResolveAccessQuery(resolver ResolveAccessQuery) AccessPolicy {
	p.ResolveAccessQuery = resolver
	return p
}

func (p AccessPolicy) OverrideIsApplicable(resolver PolicyPredicate) AccessPolicy {
	p.IsApplicable = resolver
	return p
}

func (p AccessPolicy) OverrideCanCreate(predicate PolicyPredicate) AccessPolicy {
	p.CanCreate = predicate
	return p
}

func (p AccessPolicy) OverrideCanRead(predicate PolicyPredicate) AccessPolicy {
	p.CanRead = predicate
	return p
}

func (p AccessPolicy) OverrideCanUpdate(predicate PolicyPredicate) AccessPolicy {
	p.CanUpdate = predicate
	return p
}

func (p AccessPolicy) OverrideCanDelete(predicate PolicyPredicate) AccessPolicy {
	p.CanDelete = predicate
	return p
}

func IdentityPredicate(val bool) PolicyPredicate {
	return func(p AccessPolicy, ctx *req.Ctx) bool {
		return val
	}
}

func IdentityQueryResolver(q bson.D) ResolveAccessQuery {
	return func(p AccessPolicy, ctx *req.Ctx) (bson.D, error) {
		return q, nil
	}
}

func ComposeAccessQueryAnd(resolvers ...ResolveAccessQuery) ResolveAccessQuery {
	return func(p AccessPolicy, ctx *req.Ctx) (bson.D, error) {
		restriction := bson.A{}

		for _, resolver := range resolvers {
			q, err := resolver(p, ctx)
			if err != nil {
				return nil, err
			}
			if q != nil {
				restriction = append(restriction, q)
			}
		}

		if len(restriction) == 0 {
			return nil, errors.New("composed restriction query is empty")
		}

		return bson.D{
			{"$and", restriction},
		}, nil
	}
}

func AccessByConstantMatch(literal bson.D) ResolveAccessQuery {
	return func(p AccessPolicy, ctx *req.Ctx) (bson.D, error) {
		return literal, nil
	}
}

func AccessByExactContextValueMatch(resourceField string, contextField string) ResolveAccessQuery {
	return func(p AccessPolicy, ctx *req.Ctx) (bson.D, error) {
		value := ctx.Locals(contextField)
		if value == nil {
			return nil, fmt.Errorf("context value %s not found", contextField)
		}

		return bson.D{{resourceField, value}}, nil
	}
}

func AccessByInclusiveContextSliceMatch(resourceField string, contextField string) ResolveAccessQuery {
	return func(p AccessPolicy, ctx *req.Ctx) (bson.D, error) {
		value := ctx.Locals(contextField)
		if value == nil {
			return nil, fmt.Errorf("context value %s not found", contextField)
		}

		return bson.D{{
			resourceField, bson.D{{
				"$in", value,
			}},
		}}, nil
	}
}

func AccessQueryByPolicy(policyList []AccessPolicy, ctx *req.Ctx) (bson.D, error) {
	restriction := bson.A{}

	for _, policy := range policyList {
		if policy.IsApplicable(policy, ctx) {
			q, err := policy.ResolveAccessQuery(policy, ctx)
			if err != nil {
				return nil, err
			}
			if q != nil {
				restriction = append(restriction, q)
			}
		}
	}

	if len(restriction) == 0 {
		return nil, nil
	}

	return bson.D{
		{"$or", restriction},
	}, nil
}

func MakeFilterApplicablePolicies(filterPredicate func(p AccessPolicy, ctx *req.Ctx) bool) func([]AccessPolicy, *req.Ctx, *mongo.Collection) ([]AccessPolicy, error) {
	return func(allPolicies []AccessPolicy, ctx *req.Ctx, collection *mongo.Collection) ([]AccessPolicy, error) {
		for _, p := range allPolicies {
			if p.ExtendAuthenticationContext != nil {
				err := p.ExtendAuthenticationContext(ctx, collection)
				if err != nil {
					return nil, err
				}
			}
		}

		return lo.Filter(allPolicies, func(policy AccessPolicy, i int) bool {
			return policy.IsApplicable(policy, ctx) && filterPredicate(policy, ctx)
		}), nil
	}
}

var ReadPolicyFilter = MakeFilterApplicablePolicies(func(p AccessPolicy, ctx *req.Ctx) bool {
	return p.CanRead != nil && p.CanRead(p, ctx)
})
var CreatePolicyFilter = MakeFilterApplicablePolicies(func(p AccessPolicy, ctx *req.Ctx) bool {
	return p.CanCreate != nil && p.CanCreate(p, ctx)
})
var UpdatePolicyFilter = MakeFilterApplicablePolicies(func(p AccessPolicy, ctx *req.Ctx) bool {
	return p.CanUpdate != nil && p.CanUpdate(p, ctx)
})
var DeletePolicyFilter = MakeFilterApplicablePolicies(func(p AccessPolicy, ctx *req.Ctx) bool {
	return p.CanDelete != nil && p.CanDelete(p, ctx)
})

func GetWhitelistFromMapping(mapping map[PolicyName][]string, applicablePolicies []AccessPolicy) []string {
	full := []string{}
	for _, policy := range applicablePolicies {
		full = append(full, mapping[policy.Name]...)
	}

	return full
}

func GetFieldNamesFromMapping(mapping map[PolicyName][]string) []string {
	setmap := map[string]interface{}{}

	for _, fields := range mapping {
		for _, field := range fields {
			setmap[field] = struct{}{}
		}
	}

	full := make([]string, len(setmap))
	i := 0
	for k := range setmap {
		full[i] = k
		i++
	}

	return full
}

func GetReadableMapping(t reflect.Type, declaredPolicies []AccessPolicy) map[PolicyName][]string {

	var readPolicyMapping = make(map[PolicyName][]string)
	declaredPolicyNames := lo.Map(declaredPolicies, func(policy AccessPolicy, i int) PolicyName {
		return policy.Name
	})

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		bsonTag := field.Tag.Get("bson")
		requestedReadPolicy := PolicyName(field.Tag.Get("read"))
		if requestedReadPolicy == "" {
			requestedReadPolicy = declaredPolicyNames[0]
		}
		requestedWritePolicy := PolicyName(field.Tag.Get("write"))

		if requestedReadPolicy != "-" && !slices.Contains(declaredPolicyNames, requestedReadPolicy) {
			panic(fmt.Errorf("invalid policy %s provided on resource %s", requestedReadPolicy, t.Name()))
		}
		if bsonTag == "" && requestedWritePolicy != "-" {
			panic(fmt.Errorf("missing bson tag on field %s, resource %s", field.Name, t.Name()))
		}

		fieldName := strings.Split(bsonTag, ",")[0]

		if requestedReadPolicy != "" && requestedReadPolicy != "-" {
			readPolicyMapping[requestedReadPolicy] = append(readPolicyMapping[requestedReadPolicy], fieldName)
		}
	}

	//Concat lists following policy priority order
	trail := []string{}
	for _, name := range lo.Reverse(declaredPolicyNames) {
		trail = append(trail, readPolicyMapping[name]...)
		readPolicyMapping[name] = trail
	}

	return readPolicyMapping
}

func getPatchFieldName(field reflect.StructField) (string, string) {
	bsontag := field.Tag.Get("bson")
	if bsontag == "" && field.Tag.Get("write") != "-" {
		panic(fmt.Errorf("missing bson tag on field %s", field.Name))
	}

	baseFieldName := strings.Split(field.Tag.Get("bson"), ",")[0]
	return baseFieldName, `/` + baseFieldName
}

func GetWritableMappingPatch(t reflect.Type, declaredPolicies []AccessPolicy) map[PolicyName][]string {
	return GetWritableMapping(getPatchFieldName, t, declaredPolicies)
}

func getPostFieldName(field reflect.StructField) (string, string) {
	return field.Name, field.Name
}

func GetWritableMappingPost(t reflect.Type, declaredPolicies []AccessPolicy) map[PolicyName][]string {
	return GetWritableMapping(getPostFieldName, t, declaredPolicies)
}

func GetWritableMapping(resolver func(reflect.StructField) (string, string), t reflect.Type, declaredPolicies []AccessPolicy) map[PolicyName][]string {
	var policyMapping = make(map[PolicyName][]string)
	declaredPolicyNames := lo.Map(declaredPolicies, func(policy AccessPolicy, i int) PolicyName {
		return policy.Name
	})

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		requestedWritePolicy := PolicyName(field.Tag.Get("write"))
		tag, fieldName := resolver(field)

		if requestedWritePolicy == "" {
			requestedWritePolicy = declaredPolicyNames[0]
		}

		if requestedWritePolicy != "-" && !slices.Contains(declaredPolicyNames, requestedWritePolicy) {
			panic(fmt.Errorf("invalid policy %s provided on resource %s", requestedWritePolicy, t.Name()))
		}

		if tag == "" && requestedWritePolicy != "-" {
			panic(fmt.Errorf("missing bson tag on field %s", field.Name))
		}

		if requestedWritePolicy != "" && requestedWritePolicy != "-" {
			policyMapping[requestedWritePolicy] = append(
				policyMapping[requestedWritePolicy],
				fieldName,
			)
		}
	}

	trail := []string{}
	for _, name := range lo.Reverse(declaredPolicyNames) {
		trail = append(trail, policyMapping[name]...)
		policyMapping[name] = trail
	}

	return policyMapping
}

func MappingToReadProjection(mapping map[PolicyName][]string, appliedPolicies []AccessPolicy, requested []string) (bson.D, error) {
	whitelist := GetWhitelistFromMapping(mapping, appliedPolicies)

	projection := bson.D{}
	if len(requested) == 0 {
		for _, field := range whitelist {
			projection = append(projection, bson.E{field, 1})
		}
	} else {
		for _, field := range requested {
			if !slices.Contains(whitelist, field) {
				return nil, errors.New("not authorized to read field " + field)
			}
			projection = append(projection, bson.E{field, 1})
		}
	}

	return lo.Uniq(projection), nil
}

func ValidateUpdateWritableWhitelist(mapping map[PolicyName][]string, resourcePolicies []AccessPolicy, operations []typecast.PatchOperation) error {
	whitelist := GetWhitelistFromMapping(mapping, resourcePolicies)

	for _, op := range operations {
		if !slices.Contains(whitelist, op.Path) {
			return errors.New("update is forbidden on path " + op.Path)
		}
	}

	return nil
}

func ValidateCreateWritableWhitelist(mapping map[PolicyName][]string, resourcePolicies []AccessPolicy, record interface{}) error {
	whitelist := GetWhitelistFromMapping(mapping, resourcePolicies)
	fields := GetFieldNamesFromMapping(mapping)

	for _, field := range fields {
		if !isFieldValueNull(record, field) && !slices.Contains(whitelist, field) {
			return errors.New("create is forbidden on field " + field)
		}
	}

	return nil
}

func isFieldValueNull(record interface{}, field string) bool {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	fieldval := v.FieldByName(field)
	return !fieldval.IsValid() || fieldval.IsZero()
}
