package typecast

import (
	"github.com/MereleDulci/resto/pkg/constants"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"testing"
	"time"
)

func TestResourceTypeCast_CastQuery(t *testing.T) {
	t.Parallel()

	t.Run("should cast configured keys to object ids", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"_id":      {"_id", reflect.String, constants.CastTypeObjectID, ""},
				"nullable": {"nullable", reflect.String, constants.CastTypeObjectID, ""},
			},
		}
		oid := primitive.NewObjectID()
		query := map[string]string{
			"_id":      oid.Hex(),
			"nullable": "null",
		}

		out, err := tc.Query(query)
		if err != nil {
			t.Error(err)
		}
		filled := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "_id"
		})
		nullable := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "nullable"
		})
		assert.Equal(t, bson.E{"_id", oid}, filled[0])
		assert.Equal(t, bson.E{"nullable", nil}, nullable[0])
	})

	t.Run("should rename configured keys", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"id": FieldCastRule{"id", reflect.String, constants.CastTypeObjectID, "_id"},
			},
		}

		oid := primitive.NewObjectID()
		query := map[string]string{
			"id": oid.Hex(),
		}

		out, err := tc.Query(query)
		if err != nil {
			t.Error(err)
		}
		if out[0].Key != "_id" {
			t.Errorf("expected renamed key to be _id, got %s", out[0].Key)
		}
	})

	t.Run("should not cast keys that are not configured", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"id": {"id", reflect.String, constants.CastTypeObjectID, "_id"},
			},
		}

		query := map[string]string{
			"foo": "bar",
		}

		out, err := tc.Query(query)
		if err != nil {
			t.Error(err)
		}
		if out[0].Key != "foo" {
			t.Errorf("expected key to be foo, got %s", out[0].Key)
		}
		if out[0].Value != "bar" {
			t.Errorf("expected value to be bar, got %s", out[0].Value)
		}
	})

	t.Run("should correctly wrap query modifiers", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"date": {Key: "date", To: constants.CastTypeTime},
			},
		}

		query := map[string]string{
			"date[$gte]": "2020-01-01T00:00:00Z",
			"date[$lte]": "2020-01-02T00:00:00Z",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)

		assert.Equal(t, 1, len(out))

		first, _ := lo.Find(out[0].Value.(bson.D), func(elt bson.E) bool {
			return elt.Key == "$gte"
		})
		expectedtime, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
		assert.Nil(t, err)
		assert.Equal(t, expectedtime, first.Value)

		second, _ := lo.Find(out[0].Value.(bson.D), func(elt bson.E) bool {
			return elt.Key == "$lte"
		})
		expectedtime, err = time.Parse(time.RFC3339, "2020-01-02T00:00:00Z")
		assert.Nil(t, err)
		assert.Equal(t, expectedtime, second.Value)

	})

	t.Run("should correctly apply multiple modifiers provided to the same field", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"list": {Key: "list", To: constants.CastTypeObjectID},
			},
		}

		oid := primitive.NewObjectID()
		query := map[string]string{
			"list[$in]":   oid.Hex(),
			"list[$size]": "2",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)

		internal := out[0].Value.(bson.D)
		assert.NotEmpty(t, internal)

		in, _ := lo.Find(internal, func(elt bson.E) bool {
			return elt.Key == "$in"
		})
		assert.Equal(t, bson.E{
			Key:   "$in",
			Value: bson.A{oid},
		}, in)

		size, _ := lo.Find(internal, func(elt bson.E) bool {
			return elt.Key == "$size"
		})
		assert.Equal(t, bson.E{
			Key:   "$size",
			Value: 2,
		}, size)
	})
	t.Run("should correctly typecast time", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"createdAt": {Key: "createdAt", To: constants.CastTypeTime},
			},
		}
		query := map[string]string{
			"createdAt": "2020-01-01T00:00:00Z",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		expectedtime, err := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
		assert.Nil(t, err)
		assert.Equal(t, expectedtime, out[0].Value)
	})

	t.Run("should correctly typecast nil time", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"createdAt": {Key: "createdAt", To: constants.CastTypeTime},
			},
		}

		query := map[string]string{
			"createdAt": "null",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{"createdAt", nil}}, out)
	})

	t.Run("should correctly typecast booleans", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"active":   {Key: "active", To: constants.CastTypeBoolean},
				"inactive": {Key: "inactive", To: constants.CastTypeBoolean},
				"nullable": {Key: "nullable", To: constants.CastTypeBoolean},
			},
		}
		query := map[string]string{
			"active":   "true",
			"inactive": "false",
			"nullable": "null",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)

		active := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "active"
		})
		assert.Equal(t, "active", active[0].Key)
		assert.Equal(t, true, active[0].Value)
		inactive := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "inactive"
		})
		assert.Equal(t, "inactive", inactive[0].Key)
		assert.Equal(t, false, inactive[0].Value)
		nullable := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "nullable"
		})
		assert.Equal(t, "nullable", nullable[0].Key)
		assert.Equal(t, nil, nullable[0].Value)
	})

	t.Run("should correctly type cast integers", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"age":      {Key: "age", To: constants.CastTypeInt},
				"nullable": {Key: "nullable", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"age":      "10",
			"nullable": "null",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)

		age := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "age"
		})
		assert.Equal(t, 10, age[0].Value)

		nullable := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "nullable"
		})
		assert.Equal(t, nil, nullable[0].Value)
	})

	t.Run("should correctly type cast 'null' as $eq nil", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"test": {Key: "test"},
			},
		}

		query := map[string]string{
			"test": "null",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{"test", nil}}, out)
	})

	t.Run("should correctly handle list queries for primitives", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"strings": {Key: "strings"},
				"ints":    {Key: "ints", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"strings[$in]": "foo,bar",
			"ints[$in]":    "1,2",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		strings := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "strings"
		})
		ints := lo.Filter(out, func(e bson.E, i int) bool {
			return e.Key == "ints"
		})
		assert.Equal(t, bson.E{
			"strings", bson.D{{"$in", []string{"foo", "bar"}}},
		}, strings[0])
		assert.Equal(t, bson.E{
			"ints", bson.D{{"$in", bson.A{1, 2}}},
		}, ints[0])
	})

	t.Run("should correctly handle list queries for object ids", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"id": {Key: "id", To: constants.CastTypeObjectID, Rename: "_id"},
			},
		}

		oid1, _ := primitive.ObjectIDFromHex("5f0b3b9b9d9b3e0001a3e0a0")
		oid2, _ := primitive.ObjectIDFromHex("5f0b3b9b9d9b3e0001a3e0a1")

		query := map[string]string{
			"id[$in]": "5f0b3b9b9d9b3e0001a3e0a0,5f0b3b9b9d9b3e0001a3e0a1",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"_id", bson.D{{
				"$in", bson.A{oid1, oid2},
			}},
		}}, out)
	})

	t.Run("should correctly handle modified queries for object ids", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"id":  {Key: "id", To: constants.CastTypeObjectID, Rename: "_id"},
				"ref": {Key: "ref", To: constants.CastTypeObjectID, Rename: "_ref"},
			},
		}
		oid1, _ := primitive.ObjectIDFromHex("5f0b3b9b9d9b3e0001a3e0a0")
		oid2, _ := primitive.ObjectIDFromHex("5f0b3b9b9d9b3e0001a3e0a1")

		query := map[string]string{
			"id[$gt]":  oid1.Hex(),
			"ref[$lt]": oid2.Hex(),
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)

		_id, _ := lo.Find(out, func(elt bson.E) bool {
			return elt.Key == "_id"
		})
		assert.Equal(t, bson.E{"_id", bson.D{{"$gt", oid1}}}, _id)

		_ref, _ := lo.Find(out, func(elt bson.E) bool {
			return elt.Key == "_ref"
		})
		assert.Equal(t, bson.E{"_ref", bson.D{{"$lt", oid2}}}, _ref)
	})

	t.Run("should correctly handle list queries for integers", func(t *testing.T) {

		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"age": {Key: "age", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"age[$in]": "10,20",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"age", bson.D{{
				"$in", bson.A{10, 20},
			}},
		}}, out)
	})

	t.Run("should correctly handle $size modifiers", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"list": {Key: "list", To: constants.CastTypeObjectID},
			},
		}

		query := map[string]string{
			"list[$size]": "2",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"list", bson.D{{"$size", 2}},
		}}, out)
	})

	t.Run("should correctly handle $regex modifiers", func(t *testing.T) {

		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"text": {Key: "text"},
			},
		}

		query := map[string]string{
			"text[$regex]": "val",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"text", bson.D{
				{"$options", "i"},
				{"$regex", "val"},
			},
		}}, out)

	})

	t.Run("should correctly handle $exists modifiers", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"text": {Key: "text"},
			},
		}

		truthy, err := tc.Query(map[string]string{
			"text[$exists]": "true",
		})
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"text", bson.D{{"$exists", true}},
		}}, truthy)

		falsy, err := tc.Query(map[string]string{
			"text[$exists]": "other",
		})
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"text", bson.D{{"$exists", false}},
		}}, falsy)
	})

	t.Run("should apply modifiers to paths without typecast configuration ", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"someOtherList": {Key: "someOtherList", To: constants.CastTypeObjectID},
			},
		}

		one, err := tc.Query(map[string]string{
			"list[$size]": "2",
		})
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"list", bson.D{{"$size", 2}},
		}}, one)

		oid := primitive.NewObjectID()
		two, err := tc.Query(map[string]string{
			"list[$in]": oid.Hex(),
		})

		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"list", bson.D{{"$in", []string{oid.Hex()}}},
		}}, two)
	})

	t.Run("should error if same key is queried with and without modifiers", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"val": {Key: "val", To: constants.CastTypeInt},
			},
		}
		query := map[string]string{
			"val":      "10",
			"val[$gt]": "5",
		}

		out, err := tc.Query(query)
		assert.Nil(t, out)
		assert.ErrorContainsf(t, err, "cannot query same field with and without modifiers", "expected error to be returned")
	})

	t.Run("should correctly handle $and / $or modifiers", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"a": {Key: "a", To: constants.CastTypeInt},
				"b": {Key: "b", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"[$and][0]a": "10",
			"[$and][1]b": "20",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{
			"$and", bson.A{
				bson.D{{"a", 10}},
				bson.D{{"b", 20}},
			},
		}}, out)
	})

	t.Run("should correctly handle queries to inner structs", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"inner.date": {Key: "date", To: constants.CastTypeTime, Rename: "asdate"},
				"inner.int":  {Key: "int", To: constants.CastTypeInt},
				"inner":      {Key: "inner"},
			},
		}

		query := map[string]string{
			"inner.int[$gt]": "10",
			"inner.date":     "2020-01-01T00:00:00Z",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)

		for _, elt := range out {
			if elt.Key == "inner.int" {
				assert.Equal(t, bson.E{"inner.int", bson.D{{"$gt", 10}}}, elt)
			}
			if elt.Key == "inner.date" {
				assert.Equal(t, bson.E{"inner.asdate", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}, elt)
			}
		}
	})
}

func TestResourceTypeCast_Query_GroupModifiers(t *testing.T) {
	t.Parallel()

	t.Run("should default to $and query type if no modifiers provided", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"val": {Key: "val", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"val": "10",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, bson.D{{"val", 10}}, out)
	})

	t.Run("should require all keys to have the same top level modifier if specified", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"val": {Key: "val", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"[$and][0]va": "10",
			"[$or][0]val": "20",
		}

		out, err := tc.Query(query)
		assert.Nil(t, out)
		assert.ErrorContainsf(t, err, "cannot use different query groups on a single query level", "expected error to be returned")

		alt, err := tc.Query(map[string]string{
			"[$and][0]val": "10",
			"val":          "10",
		})
		assert.Nil(t, alt)
		assert.ErrorContainsf(t, err, "cannot use different query groups on a single query level", "expected error to be returned")

		alt, err = tc.Query(map[string]string{
			"[$or][0]val": "10",
			"val":         "10",
		})
		assert.Nil(t, alt)
		assert.ErrorContainsf(t, err, "cannot use different query groups on a single query level", "expected error to be returned")
	})

	t.Run("should correctly transform single-level $and query to bson.D", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"val": {Key: "val", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"[$and][0]val":   "10",
			"[$and][0]field": "test",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, "$and", out[0].Key)

		queries := out[0].Value.(bson.A)[0]
		val, _ := lo.Find(queries.(bson.D), func(elt bson.E) bool {
			return elt.Key == "val"
		})
		assert.Equal(t, bson.E{"val", 10}, val)

		field, _ := lo.Find(queries.(bson.D), func(elt bson.E) bool {
			return elt.Key == "field"
		})
		assert.Equal(t, bson.E{"field", "test"}, field)
	})

	t.Run("should correctly transform single-level $or query to bson.D", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"val": {Key: "val", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"[$or][0]val":   "10",
			"[$or][0]field": "test",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, "$or", out[0].Key)

		queries := out[0].Value.(bson.A)[0]
		val, _ := lo.Find(queries.(bson.D), func(elt bson.E) bool {
			return elt.Key == "val"
		})
		assert.Equal(t, bson.E{"val", 10}, val)

		field, _ := lo.Find(queries.(bson.D), func(elt bson.E) bool {
			return elt.Key == "field"
		})
		assert.Equal(t, bson.E{"field", "test"}, field)
	})

	t.Run("should correctly transform multi-level $and / $or query to bson.D", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"val": {Key: "val", To: constants.CastTypeInt},
			},
		}

		query := map[string]string{
			"[$and][0][$or][0]val":   "10",
			"[$and][0][$or][0]field": "test",
		}

		out, err := tc.Query(query)
		assert.Nil(t, err)
		assert.Equal(t, "$and", out[0].Key)

		topQueries := out[0].Value.(bson.A)[0]
		or, ok := lo.Find(topQueries.(bson.D), func(elt bson.E) bool {
			return elt.Key == "$or"
		})
		assert.Equal(t, true, ok)

		orQueries := or.Value.(bson.A)[0]
		val, _ := lo.Find(orQueries.(bson.D), func(elt bson.E) bool {
			return elt.Key == "val"
		})
		assert.Equal(t, bson.E{"val", 10}, val)
		field, _ := lo.Find(orQueries.(bson.D), func(elt bson.E) bool {
			return elt.Key == "field"
		})
		assert.Equal(t, bson.E{"field", "test"}, field)
	})
}

func TestResourceTypeCast_CastFields(t *testing.T) {
	t.Parallel()

	t.Run("should correctly transform provided list of fields", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"direct":  {Key: "direct", To: constants.CastTypeBoolean},
				"renamed": {Key: "renamed", To: constants.CastTypeTime, Rename: "x_renamed"},
			},
		}
		fields := []string{"nocast", "direct", "renamed"}
		out := tc.RenameFields(fields)
		assert.Equal(t, 3, len(out))
		assert.Equal(t, "nocast", out[0])
		assert.Equal(t, "direct", out[1])
		assert.Equal(t, "x_renamed", out[2])
	})

}

func TestResourceTypeCast_RenameFields(t *testing.T) {

	t.Run("should return field without configured rule as is", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"id": {Key: "id", To: constants.CastTypeObjectID, Rename: "_id"},
			},
		}

		out := tc.RenameFields([]string{"foo"})
		assert.Equal(t, []string{"foo"}, out)
	})

	t.Run("should rename configured fields to their db values", func(t *testing.T) {
		tc := ResourceTypeCast{
			Rules: map[string]FieldCastRule{
				"id": {Key: "id", To: constants.CastTypeObjectID, Rename: "_id"},
			},
		}

		out := tc.RenameFields([]string{"id"})
		assert.Equal(t, []string{"_id"}, out)
	})
}

func TestResourceTypeCast_PatchToDBOperations(t *testing.T) {
	t.Parallel()

	t.Run("should convert replace patch to correct $set", func(t *testing.T) {
		tc := ResourceTypeCast{}
		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    "replace",
				Path:  "/a/b/c",
				Value: "string value",
			},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, update, "expected update to not be empty")
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$set", update[0].Key, "expected update to have $set key")

		internal, ok := update[0].Value.(bson.D)
		assert.True(t, ok, "expected update to have bson.D value")
		assert.Equal(t, 1, len(internal), "expected update to have 1 element")
		assert.Equal(t, "a.b.c", internal[0].Key, "expected update to have a.b.c key")
		assert.Equal(t, "string value", internal[0].Value, "expected update to have string value")
	})

	t.Run("should convert inc patch to correct $inc operation at provided path", func(t *testing.T) {
		tc := ResourceTypeCast{}
		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    "inc",
				Path:  "/a/b/c",
				Value: 1,
			},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, update, "expected update to not be empty")
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$inc", update[0].Key, "expected update to have $inc key")

		internal, ok := update[0].Value.(bson.D)
		assert.True(t, ok, "expected update to have bson.D value")
		assert.Equal(t, 1, len(internal), "expected update to have 1 element")
		assert.Equal(t, "a.b.c", internal[0].Key, "expected update to have a.b.c key")
		assert.Equal(t, 1, internal[0].Value, "expected update to have 1 value")
	})

	t.Run("should convert add patch to correct $push for slice targets", func(t *testing.T) {
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Slice []string `jsonapi:"attr,slice" bson:"slice"`
		}{}))

		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    constants.PatchOpAdd,
				Path:  "/slice",
				Value: "string value",
			},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, update, "expected update to not be empty")
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$push", update[0].Key, "expected update to have $push key")

		internal, ok := update[0].Value.(bson.D)
		assert.True(t, ok, "expected update to have bson.D value")
		assert.Equal(t, 1, len(internal), "expected update to have 1 element")
		assert.Equal(t, "slice", internal[0].Key, "expected update to have slice key")
		assert.Equal(t, "string value", internal[0].Value, "expected update to have string value")
	})

	t.Run("should convert remove patch to correct $pull for slice targets", func(t *testing.T) {
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Slice []string `jsonapi:"attr,slice" bson:"slice"`
		}{}))

		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    constants.PatchOpRemove,
				Path:  "/slice",
				Value: "string value",
			},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, update, "expected update to not be empty")
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$pull", update[0].Key, "expected update to have $pull key")

		internal, ok := update[0].Value.(bson.D)
		assert.True(t, ok, "expected update to have bson.D value")
		assert.Equal(t, 1, len(internal), "expected update to have 1 element")
		assert.Equal(t, "slice", internal[0].Key, "expected update to have slice key")
		assert.Equal(t, "string value", internal[0].Value, "expected update to have string value")
	})

	t.Run("should correctly combine operations of different types", func(t *testing.T) {
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Slice []string `jsonapi:"attr,slice" bson:"slice"`
			Plain string   `jsonapi:"attr,plain" bson:"plain"`
		}{}))

		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    constants.PatchOpAdd,
				Path:  "/slice",
				Value: "string value",
			},
			{
				Op:    constants.PatchOpReplace,
				Path:  "/plain",
				Value: "string value",
			},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, update, "expected update to not be empty")
		assert.Equal(t, 2, len(update), "expected update to have 2 elements")
		assert.Equal(t, "$set", update[0].Key, "expected update to have $set key")
		assert.Equal(t, "$push", update[1].Key, "expected update to have $push key")
	})

	t.Run("should correctly cast object id targets on single value operations", func(t *testing.T) {
		type Rel struct {
			ID primitive.ObjectID `jsonapi:"attr,id" bson:"_id"`
		}
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Ref      *Rel   `jsonapi:"attr,rel" cast:"ObjectID" bson:"rel"`
			MultiRef []*Rel `jsonapi:"attr,multirel" cast:"ObjectID" bson:"multirel"`
		}{}))

		oid := primitive.NewObjectID()

		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    constants.PatchOpReplace,
				Path:  "/rel",
				Value: oid.Hex(),
			},
			{
				Op:    constants.PatchOpAdd,
				Path:  "/multirel",
				Value: oid.Hex(),
			},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, update, "expected update to not be empty")
		assert.Equal(t, 2, len(update), "expected update to have 1 element")
		assert.Equal(t, "$set", update[0].Key, "expected update to have $set key")
		assert.Equal(t, "$push", update[1].Key, "expected update to have $push key")

		set := update[0].Value.(bson.D)
		assert.Equal(t, 1, len(set), "expected $set update to have 1 element")

		assert.Equal(t, "rel", set[0].Key, "expected update to have rel key")
		assert.Equal(t, oid, set[0].Value, "expected update to have object id value")

		push := update[1].Value.(bson.D)
		assert.Equal(t, 1, len(push), "expected $push update to have 1 element")
		assert.Equal(t, "multirel", push[0].Key, "expected update to have multirel key")
		assert.Equal(t, oid, push[0].Value, "expected update to have object id value")
	})
	t.Run("should correctly handle slice update values when type cast is necessary", func(t *testing.T) {
		type Rel struct {
			ID primitive.ObjectID `jsonapi:"attr,id" bson:"_id"`
		}
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Ref      *Rel   `jsonapi:"attr,rel" cast:"ObjectID" bson:"rel"`
			MultiRef []*Rel `jsonapi:"attr,multirel" cast:"ObjectID" bson:"multirel"`
		}{}))

		oid := primitive.NewObjectID()
		hex := oid.Hex()

		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    constants.PatchOpReplace,
				Path:  "/multirel",
				Value: []string{hex},
			},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, update, "expected update to not be empty")
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$set", update[0].Key, "expected update to have $set key")
		set := update[0].Value.(bson.D)
		assert.Equal(t, 1, len(set), "expected $set update to have 1 element")
		assert.Equal(t, "multirel", set[0].Key, "expected update to have multirel key")
		assert.Equal(t, []interface{}{oid}, set[0].Value, "expected update to have object id value")
	})

	t.Run("should correctly handle correctly typed values provided in patch", func(t *testing.T) {
		type Rel struct {
			ID primitive.ObjectID `jsonapi:"attr,id" bson:"_id"`
		}
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Ref   *Rel      `jsonapi:"attr,rel" cast:"ObjectID" bson:"rel"`
			Stamp time.Time `jsonapi:"attr,stamp" cast:"Time" bson:"stamp"`
			Legit bool      `jsonapi:"attr,legit" cast:"Boolean" bson:"legit"`
			Count int       `jsonapi:"attr,count" cast:"Int" bson:"count"`
		}{}))

		oid := primitive.NewObjectID()
		timestamp := time.Now()

		update, err := tc.PatchToDBOps([]PatchOperation{
			{
				Op:    constants.PatchOpReplace,
				Path:  "/rel",
				Value: oid,
			},
			{
				Op:    constants.PatchOpReplace,
				Path:  "/stamp",
				Value: timestamp,
			},
			{
				Op:    constants.PatchOpReplace,
				Path:  "/legit",
				Value: true,
			},
			{
				Op:    constants.PatchOpReplace,
				Path:  "/count",
				Value: 1,
			},
		})

		assert.Nil(t, err)
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$set", update[0].Key, "expected update to have $set key")
		set := update[0].Value.(bson.D)
		assert.Equal(t, 4, len(set), "expected $set update to have 1 element")
		assert.Equal(t, "rel", set[0].Key, "expected update to have rel key")
		assert.Equal(t, oid, set[0].Value, "expected update to have object id value")
		assert.Equal(t, "stamp", set[1].Key, "expected update to have stamp key")
		assert.Equal(t, timestamp, set[1].Value, "expected update to have timestamp value")
		assert.Equal(t, "legit", set[2].Key, "expected update to have legit key")
		assert.Equal(t, true, set[2].Value, "expected update to have legit value")
		assert.Equal(t, "count", set[3].Key, "expected update to have count key")
		assert.Equal(t, 1, set[3].Value, "expected update to have count value")

	})

	t.Run("should correctly patch to nil values", func(t *testing.T) {
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Stamp time.Time `jsonapi:"attr,stamp" cast:"Time" bson:"stamp"`
		}{}))

		update, err := tc.PatchToDBOps([]PatchOperation{
			{Op: constants.PatchOpReplace, Path: "/stamp", Value: nil},
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$set", update[0].Key)
		assert.Equal(t, bson.D{{"stamp", nil}}, update[0].Value)
	})

	t.Run("should correctly map updates targeting maps", func(t *testing.T) {
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Map map[string]string `jsonapi:"attr,map" bson:"map"`
		}{}))

		update, err := tc.PatchToDBOps([]PatchOperation{
			{Op: constants.PatchOpReplace, Path: "/map", Value: map[string]string{"key": "new"}},
		})

		assert.Nil(t, err)
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$set", update[0].Key)
		v := update[0].Value.(bson.D)
		assert.Equal(t, "map", v[0].Key)
		assert.Equal(t, map[string]string{"key": "new"}, v[0].Value)
	})

	t.Run("should correctly map updates targeting keys of nested maps", func(t *testing.T) {
		tc := MakeTypeCastFromResource(reflect.TypeOf(struct {
			Map map[string]string `jsonapi:"attr,map" bson:"map"`
		}{}))

		update, err := tc.PatchToDBOps([]PatchOperation{
			{Op: constants.PatchOpReplace, Path: "/map/key", Value: "new"},
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(update), "expected update to have 1 element")
		assert.Equal(t, "$set", update[0].Key)
		v := update[0].Value.(bson.D)
		assert.Equal(t, "map.key", v[0].Key)
		assert.Equal(t, "new", v[0].Value)
	})
}

func TestResourceTypeCast_PatchTestToQuery(t *testing.T) {
	t.Parallel()

	t.Run("should transform provided test operations to dict", func(t *testing.T) {
		tc := ResourceTypeCast{}
		query, err := tc.PatchTestToQuery([]PatchOperation{
			{Op: "test", Path: "/a/b/c", Value: "string value"},
		})

		assert.Nil(t, err)

		assert.NotEmpty(t, query, "expected query to not be empty")
		assert.Equal(t, 1, len(query), "expected query to have 1 element")
		assert.Equal(t, "a.b.c", query[0].Key, "expected query to have a.b.c key")
		assert.Equal(t, "string value", query[0].Value, "expected query to have string value")
	})
}

func TestMakeTypeCastFromResource(t *testing.T) {
	t.Parallel()
	t.Run("should error if configured with not supported type cast rule", func(t *testing.T) {

		type testStruct struct {
			ID string `jsonapi:"attr,oid" cast:"NotSupported"`
		}

		assert.Panics(t, func() {
			MakeTypeCastFromResource(reflect.TypeOf(testStruct{}))
		})
	})

	t.Run("should call primary jsonapi paths id and create alias for struct field name", func(t *testing.T) {

		type testStruct struct {
			ID string `jsonapi:"primary,testStructs" cast:"ObjectID"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(testStruct{}))
		assert.Equal(t, 2, len(tc.Rules))
		oidRule, ok := tc.Rules["id"]
		assert.True(t, ok)
		assert.Equal(t, "id", oidRule.Key)
		assert.Equal(t, constants.CastTypeObjectID, oidRule.To)

		structFieldRule, structOk := tc.Rules["ID"]
		assert.True(t, structOk)
		assert.Equal(t, "ID", structFieldRule.Key)
		assert.Equal(t, constants.CastTypeObjectID, structFieldRule.To)
	})

	t.Run("should make ObjectID typecast rules for OID paths", func(t *testing.T) {

		type testStruct struct {
			ID string `jsonapi:"attr,oid" cast:"ObjectID"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(testStruct{}))

		assert.Equal(t, 1, len(tc.Rules))
		oidRule, ok := tc.Rules["oid"]
		assert.True(t, ok)
		assert.Equal(t, "oid", oidRule.Key)
		assert.Equal(t, constants.CastTypeObjectID, oidRule.To)
		assert.Equal(t, "", oidRule.Rename)
	})

	t.Run("should configure rename rule if bson path differs to jsonapi name", func(t *testing.T) {

		type testStruct struct {
			ID string `jsonapi:"attr,oid" cast:"ObjectID" bson:"_id"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(testStruct{}))
		assert.Equal(t, 1, len(tc.Rules))
		oidRule, ok := tc.Rules["oid"]
		assert.True(t, ok)
		assert.Equal(t, "oid", oidRule.Key)
		assert.Equal(t, constants.CastTypeObjectID, oidRule.To)
		assert.Equal(t, "_id", oidRule.Rename)
	})

	t.Run("should not populate rename rule if bson path is the same as jsonapi name", func(t *testing.T) {
		type testStruct struct {
			ID string `jsonapi:"attr,oid" cast:"ObjectID" bson:"oid"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(testStruct{}))
		assert.Equal(t, 1, len(tc.Rules))
		oidRule, ok := tc.Rules["oid"]
		assert.True(t, ok)
		assert.Equal(t, "oid", oidRule.Key)
		assert.Equal(t, "", oidRule.Rename)
	})

	t.Run("should correctly capture slice types", func(t *testing.T) {
		type testStruct struct {
			Slice []string  `jsonapi:"attr,slice" bson:"slice"`
			Sptr  []*string `jsonapi:"attr,sptr" bson:"sptr"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(testStruct{}))
		assert.Equal(t, 2, len(tc.Rules))
		sliceRule, ok := tc.Rules["slice"]
		assert.True(t, ok)
		assert.Equal(t, "slice", sliceRule.Key)
		assert.Equal(t, reflect.Slice, sliceRule.Kind)

		sptrRule, ok := tc.Rules["sptr"]
		assert.True(t, ok)
		assert.Equal(t, "sptr", sptrRule.Key)
		assert.Equal(t, reflect.Slice, sptrRule.Kind)
	})

	t.Run("should extend nested struct by value", func(t *testing.T) {
		type Inner struct {
			Date time.Time `json:"date" bson:"date" cast:"Time"`
		}
		type Outer struct {
			Inner Inner `jsonapi:"attr,inner" bson:"inner"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(Outer{}))
		assert.Equal(t, 2, len(tc.Rules))

		outer, ok := tc.Rules["inner"]
		assert.True(t, ok)
		assert.Equal(t, "inner", outer.Key)

		inner, ok := tc.Rules["inner.date"]
		assert.True(t, ok)
		assert.Equal(t, "date", inner.Key)
		assert.Equal(t, "Time", inner.To)
	})

	t.Run("should extend nested struct by pointer", func(t *testing.T) {
		type Inner struct {
			Date time.Time `json:"date" bson:"asdate" cast:"Time"`
		}
		type Outer struct {
			Inner *Inner `jsonapi:"attr,inner" bson:"inner"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(Outer{}))
		assert.Equal(t, 2, len(tc.Rules))

		outer, ok := tc.Rules["inner"]
		assert.True(t, ok)
		assert.Equal(t, "inner", outer.Key)

		inner, ok := tc.Rules["inner.date"]
		assert.True(t, ok)
		assert.Equal(t, "date", inner.Key)
		assert.Equal(t, "Time", inner.To)
		assert.Equal(t, "asdate", inner.Rename)
	})

	t.Run("should correctly interpret relations", func(t *testing.T) {

		type testStructA struct {
			ID string `jsonapi:"primary,resourcename" cast:"ObjectID" bson:"_id"`
		}

		type testStructB struct {
			A *testStructA `jsonapi:"relation,a" cast:"ObjectID" bson:"a"`
		}

		tc := MakeTypeCastFromResource(reflect.TypeOf(testStructB{}))
		assert.Equal(t, 1, len(tc.Rules))
		aRule, ok := tc.Rules["a"]
		assert.True(t, ok)
		assert.Equal(t, "a", aRule.Key)
		assert.Equal(t, constants.CastTypeObjectID, aRule.To)
	})

}

func TestGroupFilters(t *testing.T) {
	t.Run("should join together $and filters correctly", func(t *testing.T) {
		q := map[string]string{
			"[$and][0]participants": "65148c0590d4a9bc055564dc",
			"[$and][1]participants": "65147ff890d4a9bc055564b4",
			"[$and][1]admin":        "65147ff890d4a9bc055564b5",
		}
		g, err := groupFilters(q)
		assert.Nil(t, err)
		assert.Equal(t, FilterGroup{
			Operator: "$and",
			Filters: []map[string]string{
				{
					"participants": "65148c0590d4a9bc055564dc",
				},
				{
					"participants": "65147ff890d4a9bc055564b4",
					"admin":        "65147ff890d4a9bc055564b5",
				},
			},
		}, g)
	})
	t.Run("should join together $or filters correctly", func(t *testing.T) {
		q := map[string]string{
			"[$or][0]participants": "65148c0590d4a9bc055564dc",
			"[$or][1]participants": "65147ff890d4a9bc055564b4",
			"[$or][2]admin":        "65147ff890d4a9bc055564b5",
		}
		g, err := groupFilters(q)
		assert.Nil(t, err)
		assert.Equal(t, FilterGroup{
			Operator: "$or",
			Filters: []map[string]string{
				{
					"participants": "65148c0590d4a9bc055564dc",
				},
				{
					"participants": "65147ff890d4a9bc055564b4",
				},
				{
					"admin": "65147ff890d4a9bc055564b5",
				},
			},
		}, g)
	})

	t.Run("should correctly group multifold filters", func(t *testing.T) {

		q := map[string]string{
			"[$and][0][$or][0]a[$gt]": "1",
			"[$and][0][$or][1]a[$lt]": "2",
			"[$and][1][$or][0]b[$gt]": "1",
			"[$and][1][$or][1]b[$lt]": "2",
		}

		g, err := groupFilters(q)
		assert.Nil(t, err)
		assert.Equal(t, FilterGroup{
			Operator: "$and",
			Filters: []map[string]string{
				{"[$or][0]a[$gt]": "1", "[$or][1]a[$lt]": "2"},
				{"[$or][0]b[$gt]": "1", "[$or][1]b[$lt]": "2"},
			},
		}, g)
	})

	t.Run("should join together plain filters correctly", func(t *testing.T) {
		q := map[string]string{
			"participant": "65148c0590d4a9bc055564dc",
			"admin":       "65147ff890d4a9bc055564b4",
		}
		g, err := groupFilters(q)
		assert.Nil(t, err)
		assert.Equal(t, FilterGroup{
			Operator: "",
			Filters: []map[string]string{
				{
					"participant": "65148c0590d4a9bc055564dc",
					"admin":       "65147ff890d4a9bc055564b4",
				},
			},
		}, g)
	})
	t.Run("should throw if multiple operators are provided on a single level", func(t *testing.T) {
		q := map[string]string{
			"[$or][0]participants":  "65148c0590d4a9bc055564dc",
			"[$and][1]participants": "65147ff890d4a9bc055564b4",
		}
		g, err := groupFilters(q)
		assert.Error(t, err)
		assert.Empty(t, g)
	})
	t.Run("should throw if $and or $or parts are indexed non-sequentially", func(t *testing.T) {
		g, err := groupFilters(map[string]string{
			"[$and][0]a": "a",
			"[$and][2]b": "b",
		})
		assert.Error(t, err)
		assert.Empty(t, g)

		g, err = groupFilters(map[string]string{
			"[$or][0]a": "a",
			"[$or][2]b": "b",
		})
		assert.Error(t, err)
		assert.Empty(t, g)
	})

}
