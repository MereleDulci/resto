package typecast

import (
	"errors"
	"fmt"
	"github.com/MereleDulci/jsonapi"
	"github.com/MereleDulci/resto/pkg/constants"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/exp/slices"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type FieldCastRule struct {
	Key    string
	Kind   reflect.Kind
	To     string
	Rename string
}

type ResourceTypeCast struct {
	Rules map[string]FieldCastRule
}

var filterModifierRegex = regexp.MustCompile("(.+)(\\[(\\$[[:alpha:]]+)])")
var filterGroupRegex = regexp.MustCompile("(\\[(\\$[[:alpha:]]+)]\\[(\\d+)])(.+)")

func MakeTypeCastFromResource(t reflect.Type) ResourceTypeCast {
	rules := map[string]FieldCastRule{}

	validCastTargets := []string{
		constants.CastTypeObjectID,
		constants.CastTypeBoolean,
		constants.CastTypeTime,
		constants.CastTypeInt,
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		bsonTag := field.Tag.Get("bson")
		jsonapiTag := field.Tag.Get("jsonapi")
		jsonTag := field.Tag.Get("json")
		castTag := field.Tag.Get("cast")

		if jsonapiTag == "" && jsonTag == "" {
			continue
		}
		if castTag != "" && !slices.Contains(validCastTargets, castTag) {
			panic(fmt.Errorf("invalid cast target %s, valid options: %s", castTag, strings.Join(validCastTargets, ", ")))
		}

		jsonParts := strings.Split(jsonTag, ",")
		jsonapiParts := strings.Split(jsonapiTag, ",")
		jsonapiFieldName := ""

		if len(jsonParts) > 0 {
			jsonapiFieldName = jsonParts[0]
		}

		if len(jsonapiParts) > 1 {
			jsonapiFieldName = jsonapiParts[1]
		}

		renameTo := ""
		if bsonTag != jsonapiFieldName {
			renameTo = bsonTag
		}

		if jsonapiParts[0] == "primary" {
			rules["id"] = FieldCastRule{
				Key:    "id",
				Kind:   field.Type.Kind(),
				To:     castTag,
				Rename: bsonTag,
			}
			rules[field.Name] = FieldCastRule{
				Key:    field.Name,
				Kind:   field.Type.Kind(),
				To:     castTag,
				Rename: bsonTag,
			}
		} else if jsonapiParts[0] == "relation" {
			rule := FieldCastRule{
				Key:    jsonapiFieldName,
				Kind:   field.Type.Kind(),
				To:     castTag,
				Rename: renameTo,
			}

			rules[jsonapiFieldName] = rule
		} else {
			if field.Type.Kind() == reflect.Struct || field.Type.Kind() == reflect.Ptr {
				plainT := field.Type
				if field.Type.Kind() == reflect.Ptr {
					plainT = field.Type.Elem()
				}
				innerRules := MakeTypeCastFromResource(plainT)

				for key, rule := range innerRules.Rules {
					rules[fmt.Sprintf("%s.%s", jsonapiFieldName, key)] = rule
				}
			}

			rule := FieldCastRule{
				Key:    jsonapiFieldName,
				Kind:   field.Type.Kind(),
				To:     castTag,
				Rename: renameTo,
			}

			rules[jsonapiFieldName] = rule
		}
	}

	return ResourceTypeCast{
		Rules: rules,
	}
}

func extractModifier(input string) (string, string) {
	match := filterModifierRegex.FindStringSubmatch(input)
	if match == nil {
		return input, ""
	}

	return match[1], match[3]
}

type FilterGroup struct {
	Operator string
	Filters  []map[string]string
}

func groupFilters(input map[string]string) (FilterGroup, error) {

	noneGroup := FilterGroup{
		Operator: "",
		Filters:  []map[string]string{{}},
	}

	andGroup := FilterGroup{
		Operator: "$and",
		Filters:  []map[string]string{},
	}

	orGroup := FilterGroup{
		Operator: "$or",
		Filters:  []map[string]string{},
	}

	for key, value := range input {
		match := filterGroupRegex.FindStringSubmatch(key)
		if match == nil {
			noneGroup.Filters[0][key] = value
			continue
		}
		if len(match) != 5 {
			return FilterGroup{}, errors.New("invalid filter syntax")
		}

		i, err := strconv.Atoi(match[3])
		if err != nil {
			return FilterGroup{}, errors.New("invalid filter syntax")
		}

		switch match[2] {
		case "$and":
			andGroup = extendFilterGroup(andGroup, i, match[4], value)
		case "$or":
			orGroup = extendFilterGroup(orGroup, i, match[4], value)
		}
	}

	if (len(andGroup.Filters) != 0 && len(orGroup.Filters) != 0) ||
		(len(andGroup.Filters) != 0 && len(noneGroup.Filters[0]) != 0) ||
		(len(orGroup.Filters) != 0 && len(noneGroup.Filters[0]) != 0) {
		return FilterGroup{}, errors.New("cannot use different query groups on a single query level")
	}
	if len(andGroup.Filters) != 0 {
		if validateNoEmptyFilter(andGroup) {
			return FilterGroup{}, errors.New("cannot have empty filter in a group")
		}
		return andGroup, nil
	}
	if len(orGroup.Filters) != 0 {
		if validateNoEmptyFilter(orGroup) {
			return FilterGroup{}, errors.New("cannot have empty filter in a group")
		}
		return orGroup, nil
	}

	return noneGroup, nil
}

func extendFilterGroup(group FilterGroup, index int, key string, value string) FilterGroup {
	if len(group.Filters) < index+1 {
		for j := len(group.Filters); j < index+1; j++ {
			group.Filters = append(group.Filters, map[string]string{})
		}
	}

	filterMap := group.Filters[index]

	v, ok := filterMap[key]
	filterMap[key] = lo.Ternary(ok, strings.Join([]string{v, value}, ","), value)

	return group
}

func validateNoEmptyFilter(group FilterGroup) bool {
	_, hasEmpty := lo.Find(group.Filters, func(elt map[string]string) bool {
		return len(elt) == 0
	})
	return hasEmpty
}

func (caster ResourceTypeCast) RenameFields(input []string) []string {
	out := make([]string, len(input))
	for i, field := range input {
		conf, ok := caster.Rules[field]

		if !ok {
			out[i] = field
			continue
		}

		if conf.Rename != "" {
			out[i] = conf.Rename
			continue
		}

		out[i] = conf.Key
	}
	return out
}

func isListLikeString(input interface{}) bool {
	strval, ok := input.(string)
	if !ok {
		return false
	}

	return strings.Contains(strval, ",")
}

func castSingleObjectId(input interface{}) (primitive.ObjectID, error) {
	if _, ok := input.(primitive.ObjectID); ok {
		return input.(primitive.ObjectID), nil
	}

	stringValue, ok := input.(string)
	if !ok {
		return primitive.NilObjectID, errors.New("invalid object id hex value - expecting a string")
	}
	return primitive.ObjectIDFromHex(stringValue)
}

func castSingleInt(value interface{}) (int, error) {
	if _, ok := value.(int); ok {
		return value.(int), nil
	}
	stringValue, ok := value.(string)
	if !ok {
		return 0, errors.New("invalid string representation of integer value")
	}
	return strconv.Atoi(stringValue)
}

func getCasterValueForKey(rules map[string]FieldCastRule, key string, value interface{}) (interface{}, error) {
	rule, ok := rules[key]
	if !ok {
		return value, nil
	}

	var castedValue interface{}
	var err error
	switch rule.To {
	case constants.CastTypeObjectID:
		if isListLikeString(value) {
			parts := strings.Split(value.(string), ",")
			tmp := bson.A{}
			for _, part := range parts {
				tmpVal, err := castSingleObjectId(part)
				if err != nil {
					return nil, err
				}
				tmp = append(tmp, tmpVal)
			}
			castedValue = tmp
		} else {
			if strval, ok := value.(string); ok && strval == "null" {
				castedValue = nil
			} else {
				if castedValue, err = castSingleObjectId(value); err != nil {
					return nil, err
				}
			}
		}
	case constants.CastTypeTime:
		if _, ok := value.(time.Time); ok {
			return value, nil
		}
		stringValue, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid RFC3339 time value")
		}
		if stringValue == "null" {
			castedValue = nil
		} else {
			if castedValue, err = time.Parse(time.RFC3339, stringValue); err != nil {
				return nil, err
			}
		}
	case constants.CastTypeBoolean:
		if _, ok := value.(bool); ok {
			return value, nil
		}
		switch {
		case value == "null":
			castedValue = nil
		case value == "true":
			castedValue = true
		case value == "false":
			castedValue = false
		default:
			return nil, errors.New("invalid boolean value")
		}
	case constants.CastTypeInt:

		if isListLikeString(value) {
			parts := strings.Split(value.(string), ",")
			tmp := bson.A{}
			for _, part := range parts {
				tmpVal, err := castSingleInt(part)
				if err != nil {
					return nil, err
				}
				tmp = append(tmp, tmpVal)
			}
			castedValue = tmp

		} else {
			if strval, ok := value.(string); ok && strval == "null" {
				castedValue = nil
			} else {
				if castedValue, err = castSingleInt(value); err != nil {
					return nil, err
				}
			}
		}
	default:
		//Should also check if the field is nullable (Ptr)
		str, ok := value.(string)
		if ok && str == "null" {
			castedValue = nil
		} else {
			castedValue = value
		}
	}

	return castedValue, nil
}

func (caster ResourceTypeCast) Query(query map[string]string) (bson.D, error) {

	group, err := groupFilters(query)
	if err != nil {
		return nil, err
	}

	out := bson.D{}

	if group.Operator != "" {
		full := bson.A{}
		for _, filter := range group.Filters {
			internal, err := caster.Query(filter)
			if err != nil {
				return nil, err
			}
			full = append(full, internal)
		}

		return bson.D{{group.Operator, full}}, nil
	}

	for _, filter := range group.Filters {
		for key, value := range filter {

			field, modifier := extractModifier(key)

			rule, ok := caster.Rules[field]
			if !ok && modifier == "" {
				out = append(out, bson.E{Key: field, Value: value})
				continue
			}

			if modifier == "$size" {
				castedValue, err := strconv.Atoi(value)
				if err != nil {
					return nil, err
				}

				out, err = extendQuery(out, field, modifier, castedValue)
				if err != nil {
					return nil, err
				}
				continue
			}

			castedValue, err := getCasterValueForKey(caster.Rules, field, value)
			if err != nil {
				return nil, err
			}

			//Handle special case for $in to ensure the value is always a bson.A
			if modifier == "$in" {

				kind := reflect.TypeOf(castedValue).Kind()
				if kind != reflect.Slice {
					if kind == reflect.String {
						castedValue = strings.Split(castedValue.(string), ",")
					} else {
						castedValue = bson.A{castedValue}
					}
				}

			}

			if modifier == "$regex" {
				//Casting value to string, it's going to be appended later with $regex modifier
				castedValue = castedValue.(string)
				//Appending case-insensitive option only in place
				out, err = extendQuery(out, field, "$options", "i")
				if err != nil {
					return nil, err
				}
			}

			if modifier == "$exists" {
				castedValue = castedValue.(string)

				if castedValue == "true" {
					castedValue = true
				} else {
					castedValue = false
				}
			}

			targetField := lo.Ternary(rule.Rename != "", rule.Rename, field)
			out, err = extendQuery(out, targetField, modifier, castedValue)
			if err != nil {
				return nil, err
			}

		}
	}
	return out, nil
}

func extendQuery(query bson.D, targetKey string, modifier string, value interface{}) (bson.D, error) {
	existing, found := lo.Find(query, func(elt bson.E) bool {
		return elt.Key == targetKey
	})
	if !found {
		appendValue := value
		if modifier != "" {
			appendValue = bson.D{
				{Key: modifier, Value: value},
			}
		}

		return append(query, bson.E{
			Key:   targetKey,
			Value: appendValue,
		}), nil
	}

	dValue, ok := existing.Value.(bson.D)
	if modifier == "" || !ok {
		return nil, errors.New("cannot query same field with and without modifiers")
	}

	appendValue := bson.E{
		Key:   modifier,
		Value: value,
	}

	for i, v := range query {
		if v.Key == existing.Key {
			query[i].Value = append(dValue, appendValue)
		}
	}

	return query, nil
}

func interfaceSlice(slice interface{}) []interface{} {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		panic("InterfaceSlice() given a non-slice type")
	}

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret
}

func (caster ResourceTypeCast) transformPathValue(documentPath string, inputValue interface{}) (interface{}, error) {
	if inputValue == nil {
		return nil, nil
	}

	if reflect.TypeOf(inputValue).Kind() == reflect.Slice {
		var typeCastedValue []interface{}
		for _, value := range interfaceSlice(inputValue) {
			v, err := getCasterValueForKey(caster.Rules, documentPath, value)
			if err != nil {
				return nil, err
			}
			typeCastedValue = append(typeCastedValue, v)
		}
		return typeCastedValue, nil
	} else {
		return getCasterValueForKey(caster.Rules, documentPath, inputValue)
	}
}

func (caster ResourceTypeCast) PatchToDBOps(patchOps []jsonapi.PatchOp) (bson.D, error) {
	set := bson.D{}
	push := bson.D{}
	inc := bson.D{}
	pull := bson.D{}

	for _, op := range patchOps {
		documentPath := transformPath(op.Path)

		typeCastedValue, err := caster.transformPathValue(documentPath, op.Value)
		if err != nil {
			return nil, err
		}

		switch op.Op {
		case constants.PatchOpReplace:
			set = append(set, bson.E{
				Key:   documentPath,
				Value: typeCastedValue,
			})
		case constants.PatchOpAdd:
			push = append(push, bson.E{
				Key:   documentPath,
				Value: typeCastedValue,
			})
		case constants.PatchOpRemove:
			pull = append(pull, bson.E{
				Key:   documentPath,
				Value: typeCastedValue,
			})
		case constants.PatchOpInc:
			inc = append(inc, bson.E{
				Key:   documentPath,
				Value: typeCastedValue,
			})
		}
	}

	final := bson.D{}
	if len(set) > 0 {
		final = append(final, bson.E{
			Key:   "$set",
			Value: set,
		})
	}
	if len(push) > 0 {
		final = append(final, bson.E{
			Key:   "$push",
			Value: push,
		})
	}
	if len(pull) > 0 {
		final = append(final, bson.E{
			Key:   "$pull",
			Value: pull,
		})
	}
	if len(inc) > 0 {
		final = append(final, bson.E{
			Key:   "$inc",
			Value: inc,
		})
	}

	return final, nil
}

func (caster ResourceTypeCast) PatchTestToQuery(patchOps []jsonapi.PatchOp) (bson.D, error) {
	query := bson.D{}
	for _, op := range patchOps {
		if op.Op == constants.PatchOpTest {
			documentPath := transformPath(op.Path)
			typeCastedValue, err := caster.transformPathValue(documentPath, op.Value)
			if err != nil {
				return nil, err
			}
			query = append(query, bson.E{
				Key:   documentPath,
				Value: typeCastedValue,
			})
		}
	}
	return query, nil
}

func transformPath(path string) string {
	return strings.Join(strings.Split(path, "/")[1:], ".")
}
