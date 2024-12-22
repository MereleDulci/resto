package access

import (
	"context"
	"errors"
	"fmt"
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/MereleDulci/resto/pkg/typecast"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
	"slices"
	"strings"
)

type PolicyName string
type PolicyPredicate func(context.Context, resource.Req, AccessPolicy) bool
type ResolveAccessQuery func(context.Context, resource.Req, AccessPolicy) (bson.D, error)
type ExtendAuthenticationContext func(context.Context, resource.Req) error
type PolicyResourceMatch func(context.Context, resource.Req, resource.Resourcer) (bool, error)

type AccessPolicy struct {
	Name                        PolicyName
	CanRead                     PolicyPredicate
	CanCreate                   PolicyPredicate
	CanUpdate                   PolicyPredicate
	CanDelete                   PolicyPredicate
	CanCall                     PolicyPredicate
	ExtendAuthenticationContext ExtendAuthenticationContext
	IsApplicable                PolicyPredicate
	ResolveAccessQuery          ResolveAccessQuery
	ResourceMatch               PolicyResourceMatch
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

func (p AccessPolicy) OverrideCanCall(predicate PolicyPredicate) AccessPolicy {
	p.CanCall = predicate
	return p
}

func (p AccessPolicy) OverrideResourceMatch(match PolicyResourceMatch) AccessPolicy {
	p.ResourceMatch = match
	return p
}

func IdentityPredicate(val bool) PolicyPredicate {
	return func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
		return val
	}
}

func IdentityQueryResolver(q bson.D) ResolveAccessQuery {
	return func(ctx context.Context, r resource.Req, p AccessPolicy) (bson.D, error) {
		return q, nil
	}
}

func IdentityResourceMatch(val bool) PolicyResourceMatch {
	return func(ctx context.Context, r resource.Req, res resource.Resourcer) (bool, error) {
		return val, nil
	}
}

func ComposePredicateAnd(predicates ...PolicyPredicate) PolicyPredicate {
	return func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
		for _, predicate := range predicates {
			if !predicate(ctx, r, p) {
				return false
			}
		}
		return true
	}
}

func ComposePredicateOr(predicates ...PolicyPredicate) PolicyPredicate {
	return func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
		for _, predicate := range predicates {
			if predicate(ctx, r, p) {
				return true
			}
		}
		return false
	}
}

func ComposeAccessQueryAnd(resolvers ...ResolveAccessQuery) ResolveAccessQuery {
	return func(ctx context.Context, r resource.Req, p AccessPolicy) (bson.D, error) {
		restriction := bson.A{}

		for _, resolver := range resolvers {
			q, err := resolver(ctx, r, p)
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
	return func(ctx context.Context, r resource.Req, p AccessPolicy) (bson.D, error) {
		return literal, nil
	}
}

func AccessByExactContextValueMatch(resourceField string, contextField string) ResolveAccessQuery {
	return func(ctx context.Context, r resource.Req, p AccessPolicy) (bson.D, error) {
		value := ctx.Value(contextField)
		if value == nil {
			return nil, fmt.Errorf("context value %s not found", contextField)
		}

		return bson.D{{resourceField, value}}, nil
	}
}

func AccessByInclusiveContextSliceMatch(resourceField string, contextField string) ResolveAccessQuery {
	return func(ctx context.Context, r resource.Req, p AccessPolicy) (bson.D, error) {
		value := ctx.Value(contextField)
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

func AccessQueryByPolicy(ctx context.Context, policyList []AccessPolicy, r resource.Req) (bson.D, error) {
	restriction := bson.A{}

	for _, policy := range policyList {
		if policy.IsApplicable(ctx, r, policy) {
			q, err := policy.ResolveAccessQuery(ctx, r, policy)
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

func MakeFilterApplicablePolicies(filterPredicate func(ctx context.Context, r resource.Req, p AccessPolicy) bool) func(context.Context, []AccessPolicy, resource.Req) ([]AccessPolicy, error) {
	return func(ctx context.Context, allPolicies []AccessPolicy, r resource.Req) ([]AccessPolicy, error) {
		for _, p := range allPolicies {
			if p.ExtendAuthenticationContext != nil {
				err := p.ExtendAuthenticationContext(ctx, r)
				if err != nil {
					return nil, err
				}
			}
		}

		return lo.Filter(allPolicies, func(policy AccessPolicy, i int) bool {
			return policy.IsApplicable(ctx, r, policy) && filterPredicate(ctx, r, policy)
		}), nil
	}
}

var ReadPolicyFilter = MakeFilterApplicablePolicies(func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
	return p.CanRead != nil && p.CanRead(ctx, r, p)
})
var CreatePolicyFilter = MakeFilterApplicablePolicies(func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
	return p.CanCreate != nil && p.CanCreate(ctx, r, p)
})
var UpdatePolicyFilter = MakeFilterApplicablePolicies(func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
	return p.CanUpdate != nil && p.CanUpdate(ctx, r, p)
})
var DeletePolicyFilter = MakeFilterApplicablePolicies(func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
	return p.CanDelete != nil && p.CanDelete(ctx, r, p)
})
var CallPolicyFilter = MakeFilterApplicablePolicies(func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
	return p.CanCall != nil && p.CanCall(ctx, r, p)
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

func GetReadableMappingBSON(t reflect.Type, declaredPolicies []AccessPolicy) map[PolicyName][]string {
	return getReadableMapping(getReadBsonTag, t, declaredPolicies)
}

func GetReadableMappingStruct(t reflect.Type, declaredPolicies []AccessPolicy) map[PolicyName][]string {
	return getReadableMapping(getReadStructFieldName, t, declaredPolicies)
}

func getReadableMapping(resolver func(reflect.StructField) string, t reflect.Type, declaredPolicies []AccessPolicy) map[PolicyName][]string {

	var readPolicyMapping = make(map[PolicyName][]string)
	declaredPolicyNames := lo.Map(declaredPolicies, func(policy AccessPolicy, i int) PolicyName {
		return policy.Name
	})

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		//BSON or struct field depending on mapping needs
		fieldNameSource := resolver(field)

		requestedReadPolicy := PolicyName(field.Tag.Get("read"))
		if requestedReadPolicy == "" {
			requestedReadPolicy = declaredPolicyNames[0]
		}
		requestedWritePolicy := PolicyName(field.Tag.Get("write"))

		if requestedReadPolicy != "-" && !slices.Contains(declaredPolicyNames, requestedReadPolicy) {
			panic(fmt.Errorf("invalid policy %s provided on resource %s", requestedReadPolicy, t.Name()))
		}
		if fieldNameSource == "" && requestedWritePolicy != "-" {
			panic(fmt.Errorf("missing bson tag on field %s, resource %s", field.Name, t.Name()))
		}

		fieldName := strings.Split(fieldNameSource, ",")[0]

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

func getReadBsonTag(field reflect.StructField) string {
	return field.Tag.Get("bson")
}

func getReadStructFieldName(field reflect.StructField) string {
	return field.Name
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
		valid := false
		for _, whitelistEntry := range whitelist {
			//Update path is targeting exact whitelisted entry, or a full prefix of the update is whitelisted
			if strings.HasPrefix(op.Path, whitelistEntry) {
				valid = true
				continue
			}
		}

		if !valid {
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

func PostProcessResourceFields(ctx context.Context, req resource.Req, mapping map[PolicyName][]string, resourcePolicies []AccessPolicy, record resource.Resourcer) (resource.Resourcer, error) {

	perRecordPolicies := make([]AccessPolicy, 0)
	for _, policy := range resourcePolicies {
		if policy.ResourceMatch != nil {
			matched, err := policy.ResourceMatch(ctx, req, record)
			if err != nil {
				return nil, err
			}

			if matched {
				perRecordPolicies = append(perRecordPolicies, policy)
			}
		}
	}

	whitelist := GetWhitelistFromMapping(mapping, perRecordPolicies)

	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := range v.Type().NumField() {
		field := v.Type().Field(i).Name
		if !slices.Contains(whitelist, field) {
			v.FieldByName(field).Set(reflect.Zero(v.FieldByName(field).Type()))
		}
	}

	return record, nil
}

func isFieldValueNull(record interface{}, field string) bool {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	fieldval := v.FieldByName(field)
	return !fieldval.IsValid() || fieldval.IsZero()
}
