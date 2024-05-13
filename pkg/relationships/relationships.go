package relationships

import (
	"github.com/MereleDulci/resto/pkg/collection"
	"github.com/samber/lo"
	"reflect"
	"slices"
	"strings"
)

type IncludePath struct {
	LocalField  string
	RemoteField string
	Path        string
	Resource    string
}

func GetReferencesMapping(t reflect.Type) []IncludePath {
	references := make([]IncludePath, 0)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		refElement := field.Type
		fieldKind := refElement.Kind()

		if slices.Contains([]reflect.Kind{reflect.Struct, reflect.Ptr, reflect.Slice}, fieldKind) {
			jsonapiTag := field.Tag.Get("jsonapi")
			if jsonapiTag == "" {
				continue
			}
			tagParts := strings.Split(jsonapiTag, ",")
			if len(tagParts) < 2 {
				continue
			}
			if tagParts[0] != "relation" {
				continue
			}

			if fieldKind == reflect.Ptr {
				refElement = field.Type.Elem()
			}
			refPrimary := getPrimaryField(refElement)

			primaryTag := refPrimary.Tag.Get("jsonapi")
			if primaryTag == "" {
				continue
			}
			primaryTagParts := strings.Split(primaryTag, ",")

			references = append(references, IncludePath{
				LocalField:  field.Name,
				RemoteField: refPrimary.Name,
				Path:        tagParts[1],
				Resource:    primaryTagParts[1],
			})
		}
	}
	return references
}

func GetTopLevelIncludeKeys(input []string) []string {
	out := []string{}
	for _, key := range input {
		if !strings.Contains(key, ".") {
			out = append(out, key)
		} else {
			sub := strings.Split(key, ".")[0]
			if !lo.Contains(out, sub) {
				out = append(out, sub)
			}
		}
	}

	return out
}

func GetSubIncludeKeysForPrefix(prefix string, input []string) []string {
	out := []string{}
	for _, key := range input {
		if strings.HasPrefix(key, prefix+".") {
			_, after, isFound := strings.Cut(key, ".")
			if isFound {
				out = append(out, after)
			}
		}
	}

	return out
}

func getPrimaryField(t reflect.Type) reflect.StructField {
	if t.Kind() == reflect.Slice {
		t = reflect.New(t.Elem()).Elem().Type()
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonapiTag := field.Tag.Get("jsonapi")
		if jsonapiTag == "" {
			continue
		}
		tagParts := strings.Split(jsonapiTag, ",")
		if len(tagParts) < 2 {
			continue
		}
		if tagParts[0] != "primary" {
			continue
		}
		return field
	}

	return reflect.StructField{}
}

func GetReferencedIdsFromPrimary(primary []collection.Resourcer, includeConfig IncludePath) []string {
	ids := make([]string, 0)
	for _, p := range primary {
		refField := reflect.ValueOf(p).Elem().FieldByName(includeConfig.LocalField)

		if refField.Kind() == reflect.Ptr {
			if refField.IsNil() {
				continue
			}
			refField = refField.Elem().FieldByName(includeConfig.RemoteField)
			strVal := refField.String()
			if strVal != "" {
				ids = append(ids, strVal)
			}
		}

		if refField.Kind() == reflect.Slice {
			for i := 0; i < refField.Len(); i++ {
				refElement := refField.Index(i)
				if refElement.Kind() == reflect.Ptr {
					if refElement.IsNil() {
						continue
					}
					refElement = refElement.Elem().FieldByName(includeConfig.RemoteField)
					strVal := refElement.String()
					if strVal != "" {
						ids = append(ids, strVal)
					}
				}
			}

		}

	}
	return ids
}

func MergeWithIncluded(primary []collection.Resourcer, secondary []collection.Resourcer, includeConfig IncludePath) error {

	for _, p := range primary {
		localField := reflect.ValueOf(p).Elem().FieldByName(includeConfig.LocalField)

		if localField.Kind() == reflect.Ptr {
			if localField.IsNil() {
				continue
			}

			strVal := localField.Elem().FieldByName(includeConfig.RemoteField).String()

			replaceWith, ok := lo.Find(secondary, func(s collection.Resourcer) bool {
				return s.GetID() == strVal
			})

			//Make typesafe
			if ok && localField.CanSet() {
				localField.Set(reflect.ValueOf(replaceWith))
			}

		}

		if localField.Kind() == reflect.Slice {
			//pick each slice element, find matching secondary for it. set at current index if found
			for i := 0; i < localField.Len(); i++ {
				elt := localField.Index(i).Elem()

				strId := elt.FieldByName(includeConfig.RemoteField).String()

				replaceWith, ok := lo.Find(secondary, func(s collection.Resourcer) bool {
					return s.GetID() == strId
				})

				if ok {
					if elt.CanSet() {
						elt.Set(reflect.ValueOf(replaceWith).Elem())
					}
				}

			}
		}

	}

	return nil
}
