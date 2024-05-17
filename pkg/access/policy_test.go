package access

import (
	"context"
	"errors"
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/MereleDulci/resto/pkg/typecast"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/exp/slices"
	"reflect"
	"testing"
)

func TestGetWhitelistFromMapping(t *testing.T) {

	t.Run("should return a union list of fields matching all applicable policies", func(t *testing.T) {

		fields := GetWhitelistFromMapping(map[PolicyName][]string{
			"applicable": {"field"},
			"additional": {"additional-field"},
		}, []AccessPolicy{
			{
				Name: "applicable",
				IsApplicable: func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
					return true
				},
			},
			{
				Name: "additional",
				IsApplicable: func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
					return true
				},
			},
		})

		assert.Equal(t, []string{"field", "additional-field"}, fields)
	})

	t.Run("should return empty list for no matching policies", func(t *testing.T) {

		fields := GetWhitelistFromMapping(map[PolicyName][]string{}, []AccessPolicy{
			{
				Name: "not-applicable",
				IsApplicable: func(ctx context.Context, r resource.Req, p AccessPolicy) bool {
					return false
				},
			},
		})
		assert.Equal(t, 0, len(fields))
	})

}

func TestGetFieldNamesFromMapping(t *testing.T) {

	t.Run("should return a full list of fields available on the policy mapping", func(t *testing.T) {
		fields := GetFieldNamesFromMapping(map[PolicyName][]string{
			"a": {"fieldA"},
			"b": {"fieldB", "fieldC"},
			"c": {},
		})

		assert.Equal(t, 3, len(fields))
		assert.True(t, slices.Contains(fields, "fieldA"))
		assert.True(t, slices.Contains(fields, "fieldB"))
		assert.True(t, slices.Contains(fields, "fieldC"))
	})
}

func TestGetReadableMapping(t *testing.T) {

	t.Run("should panic if struct references unknown policy ", func(t *testing.T) {
		type X struct {
			Unknown string `read:"unknown" bson:"unknown"`
		}
		assert.PanicsWithError(t, "invalid policy unknown provided on resource X", func() {
			GetReadableMapping(reflect.TypeOf(X{}), []AccessPolicy{})
		})
	})

	t.Run("should distribute struct fields to read access whitelists following policy priority", func(t *testing.T) {

		mapping := GetReadableMapping(reflect.TypeOf(struct {
			Account    string `read:"account" bson:"account"`
			AccountAlt string `read:"account" bson:"account_alt"`
			System     string `read:"system" bson:"system"`
		}{}), []AccessPolicy{
			{Name: "system"},
			{Name: "account"},
		})

		assert.Equal(t, map[PolicyName][]string{
			"account": {"account", "account_alt"},
			"system":  {"account", "account_alt", "system"},
		}, mapping)
	})

	t.Run("should default missing read policy to the least privileged", func(t *testing.T) {

		mapping := GetReadableMapping(reflect.TypeOf(struct {
			Account string `read:"account" bson:"account"`
			System  string `bson:"system"`
		}{}), []AccessPolicy{
			{Name: "system"},
			{Name: "account"},
		})

		assert.Equal(t, map[PolicyName][]string{
			"account": {"account"},
			"system":  {"account", "system"},
		}, mapping)
	})

	t.Run("should correctly handle additional attributes on bson tag", func(t *testing.T) {
		mapping := GetReadableMapping(reflect.TypeOf(struct {
			Account string `read:"account" bson:"account,omitempty"`
		}{}), []AccessPolicy{
			{Name: "account"},
		})
		assert.Equal(t, map[PolicyName][]string{
			"account": {"account"},
		}, mapping)
	})

}

func TestGetWritableMapping(t *testing.T) {

	t.Run("should panic if struct references unknown policy", func(t *testing.T) {

		type X struct {
			Unknown string `write:"unknown" bson:"unknown"`
		}
		assert.PanicsWithError(t, "invalid policy unknown provided on resource X", func() {
			GetWritableMappingPatch(reflect.TypeOf(X{}), []AccessPolicy{})
		})
	})

	t.Run("should distribute struct fields to write access whitelists following policy priority", func(t *testing.T) {

		mapping := GetWritableMappingPatch(reflect.TypeOf(struct {
			Account    string `write:"account" bson:"account"`
			AccountAlt string `write:"account" bson:"account_alt"`
			System     string `write:"system" bson:"system"`
		}{}), []AccessPolicy{
			{Name: "system"},
			{Name: "account"},
		})

		assert.Equal(t, map[PolicyName][]string{
			"account": {"/account", "/account_alt"},
			"system":  {"/account", "/account_alt", "/system"},
		}, mapping)
	})

	t.Run("should default missing write policy to the least privileged", func(t *testing.T) {

		mapping := GetWritableMappingPatch(reflect.TypeOf(struct {
			Account string `write:"account" bson:"account"`
			System  string `bson:"system"`
		}{}), []AccessPolicy{
			{Name: "system"},
			{Name: "account"},
		})

		assert.Equal(t, map[PolicyName][]string{
			"account": {"/account"},
			"system":  {"/account", "/system"},
		}, mapping)
	})

	t.Run("should correctly handle additional attributes on bson tag", func(t *testing.T) {
		mapping := GetWritableMappingPatch(reflect.TypeOf(struct {
			Account string `write:"account" bson:"account,omitempty"`
		}{}), []AccessPolicy{
			{Name: "account"},
		})
		assert.Equal(t, map[PolicyName][]string{
			"account": {"/account"},
		}, mapping)
	})

}

func TestMappingToReadProjection(t *testing.T) {

	t.Run("should return full flat projection for the provided context and mapping if nothing requested", func(t *testing.T) {

		projection, err := MappingToReadProjection(map[PolicyName][]string{
			"account": {"account", "account_alt"},
		}, []AccessPolicy{
			{Name: "account"},
		}, []string{})

		assert.Nil(t, err)
		assert.Equal(t, bson.D{
			{"account", 1},
			{"account_alt", 1},
		}, projection)
	})

	t.Run("should concatenate all allowed fields from multiple applicable policies", func(t *testing.T) {
		projection, err := MappingToReadProjection(map[PolicyName][]string{
			"account": {"account", "account_alt"},
			"system":  {"system"},
		}, []AccessPolicy{
			{Name: "account"},
			{Name: "system"},
		}, []string{})

		assert.Nil(t, err)
		assert.Equal(t, bson.D{
			{"account", 1},
			{"account_alt", 1},
			{"system", 1},
		}, projection)
	})

	t.Run("should compact allowed fields leaving unique values only across all policies", func(t *testing.T) {
		projection, err := MappingToReadProjection(map[PolicyName][]string{
			"account": {"account", "account_alt"},
			"system":  {"account", "account_alt", "system"},
		}, []AccessPolicy{
			{Name: "account"},
			{Name: "system"},
		}, []string{})

		assert.Nil(t, err)
		assert.Equal(t, bson.D{
			{"account", 1},
			{"account_alt", 1},
			{"system", 1},
		}, projection)
	})

	t.Run("should format requested fields to projection", func(t *testing.T) {
		projection, err := MappingToReadProjection(map[PolicyName][]string{
			"account": {"account", "account_alt"},
		}, []AccessPolicy{
			{Name: "account"},
		}, []string{"account"})

		assert.Nil(t, err)
		assert.Equal(t, bson.D{
			{"account", 1},
		}, projection)
	})

	t.Run("should error if one of the requested fields is not readable to the current realm", func(t *testing.T) {

		projection, err := MappingToReadProjection(map[PolicyName][]string{
			"account": {"account", "account_alt"},
		}, []AccessPolicy{
			{Name: "public"},
		}, []string{"account"})

		ref := errors.New("not authorized to read field account")
		assert.Equal(t, ref, err)
		assert.Nil(t, projection)
	})
}

func TestValidateUpdateWritableWhitelist(t *testing.T) {

	t.Run("should return nil if all operation paths found in whitelist", func(t *testing.T) {
		err := ValidateUpdateWritableWhitelist(map[PolicyName][]string{
			"account": {"/account", "/account_alt"},
		}, []AccessPolicy{
			{Name: "account", IsApplicable: IdentityPredicate(true)},
		}, []typecast.PatchOperation{
			{Op: "replace", Path: "/account"},
		})

		assert.Nil(t, err)
	})

	t.Run("should return error if operation path not found in whitelist", func(t *testing.T) {
		err := ValidateUpdateWritableWhitelist(map[PolicyName][]string{
			"account": {"/account", "/account_alt"},
		}, []AccessPolicy{
			{Name: "public", IsApplicable: IdentityPredicate(true)},
		}, []typecast.PatchOperation{
			{Op: "replace", Path: "/account"},
		})

		assert.Equal(t, errors.New("update is forbidden on path /account"), err)
	})

}

func TestValidateCreateWriteableWhitelist(t *testing.T) {

	t.Run("should return nil if all fields are nil", func(t *testing.T) {
		err := ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"public": {"Account", "AccountAlt"},
		}, []AccessPolicy{
			{Name: "public", IsApplicable: IdentityPredicate(true)},
		}, struct{}{})

		assert.Nil(t, err)
	})

	t.Run("should return nil if all fields are zero value", func(t *testing.T) {
		err := ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"account": {"A", "B", "C"},
		}, []AccessPolicy{
			{Name: "public", IsApplicable: IdentityPredicate(true)},
		}, struct {
			A string
			B int
			C bool
		}{
			A: "",
			B: 0,
			C: false,
		})

		assert.Nil(t, err)
	})

	t.Run("should error if write attempted at non-writable field", func(t *testing.T) {
		err := ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"account": {"A", "B", "C"},
		}, []AccessPolicy{
			{Name: "public", IsApplicable: IdentityPredicate(true)},
		}, struct {
			A string
			B int
			C bool
		}{
			A: "A",
			B: 2,
			C: true,
		})

		assert.Errorf(t, err, "create is forbidden on field")
	})

	t.Run("should pass if all fields are writable", func(t *testing.T) {
		err := ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"account": {"A", "B", "C"},
		}, []AccessPolicy{
			{Name: "account", IsApplicable: IdentityPredicate(true)},
		}, struct {
			A string
			B int
			C bool
		}{
			A: "A",
			B: 2,
			C: true,
		})

		assert.Nil(t, err)
	})

	t.Run("should correctly validate nil pointers", func(t *testing.T) {
		type Pet struct{}

		err := ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"account": {"A", "B"},
		}, []AccessPolicy{
			{Name: "account", IsApplicable: IdentityPredicate(true)},
		}, struct {
			A *Pet
			B []*Pet
		}{})

		assert.Nil(t, err)

		err = ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"account": {"A", "B"},
		}, []AccessPolicy{
			{Name: "account", IsApplicable: IdentityPredicate(true)},
		}, &struct {
			A *Pet
			B []*Pet
		}{
			A: nil,
			B: nil,
		})

		assert.Nil(t, err)
	})

	t.Run("should correctly validate nil pointers", func(t *testing.T) {
		type Pet struct{ ID string }

		err := ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"account": {"A", "B"},
		}, []AccessPolicy{
			{Name: "public", IsApplicable: IdentityPredicate(true)},
		}, struct {
			A *Pet
			B []*Pet
		}{
			A: &Pet{ID: "1"},
			B: []*Pet{{ID: "2"}},
		})

		assert.Errorf(t, err, "create is forbidden on field")
	})

	t.Run("should correctly validate nil pointers", func(t *testing.T) {
		type Pet struct{ ID string }
		err := ValidateCreateWritableWhitelist(map[PolicyName][]string{
			"account": {"A", "B"},
		}, []AccessPolicy{
			{Name: "account", IsApplicable: IdentityPredicate(true)},
		}, struct {
			A *Pet
			B []*Pet
		}{
			A: &Pet{ID: "1"},
			B: []*Pet{{ID: "2"}},
		})

		assert.Nil(t, err)
	})
}

func TestAccessByExactContextValueMatch(t *testing.T) {

	t.Run("it should return a query exactly matching the field provided to context value provided", func(t *testing.T) {
		matcher := AccessByExactContextValueMatch("chatThread", "ownThreads")
		ctx := context.WithValue(context.Background(), "ownThreads", "123")
		query, err := matcher(ctx, resource.NewReq(), AccessPolicy{})
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{"chatThread", "123"}}, query)
	})

	t.Run("It should fail if required context value is not found", func(t *testing.T) {

		matcher := AccessByExactContextValueMatch("chatThread", "ownThreads")
		ctx := context.Background()
		query, err := matcher(ctx, resource.NewReq(), AccessPolicy{})
		assert.Equal(t, 0, len(query))
		assert.Error(t, err, "context value ownThreads not found")
	})
}

func TestAccessByInclusiveContextSliceMatch(t *testing.T) {

	t.Run("it should return a query matching context slice to resource field using $in operator", func(t *testing.T) {
		matcher := AccessByInclusiveContextSliceMatch("chatThread", "knownThreads")
		ctx := context.WithValue(context.Background(), "knownThreads", []string{"123", "456"})
		query, err := matcher(ctx, resource.NewReq(), AccessPolicy{})
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{"chatThread", bson.D{{"$in", []string{"123", "456"}}}}}, query)
	})

	t.Run("it should fail if required context value is not found", func(t *testing.T) {
		matcher := AccessByInclusiveContextSliceMatch("chatThread", "knownThreads")
		ctx := context.Background()
		query, err := matcher(ctx, resource.NewReq(), AccessPolicy{})
		assert.Nil(t, query)
		assert.Error(t, err, "context value knownThreads not found")
	})
}
