package relationships

import (
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type ref struct {
	ID  string `jsonapi:"primary,ref"`
	Url string
}

func (r *ref) GetID() string {
	return r.ID
}
func (r *ref) InitID() {
	r.ID = ""
}

type main struct {
	ID        string `jsonapi:"primary,main"`
	Images    []*ref `jsonapi:"relation,images"`
	Avatar    *ref   `jsonapi:"relation,avatar"`
	ValImages []ref  `jsonapi:"relation,valImages"`
	ValAvatar ref    `jsonapi:"relation,valAvatar"`
}

func (m *main) GetID() string {
	return ""
}
func (m *main) InitID() {}

func TestGetReferencesMapping(t *testing.T) {
	t.Parallel()

	t.Run("it should extract list of value reference fields", func(t *testing.T) {
		refs := GetReferencesMapping(reflect.TypeOf(new(struct {
			Avatar *ref   `jsonapi:"relation,avatar"`
			Images []*ref `jsonapi:"relation,images"`
		})))

		assert.Equal(t, []IncludePath{
			{LocalField: "Avatar", RemoteField: "ID", Path: "avatar", Resource: "ref"},
			{LocalField: "Images", RemoteField: "ID", Path: "images", Resource: "ref"},
		}, refs)
	})

	t.Run("it should not include pointer fields with no relationship tag", func(t *testing.T) {
		refs := GetReferencesMapping(reflect.TypeOf(new(struct {
			NonReferencePrimitive *string    `jsonapi:"attr,nonReferencePrimitive"`
			NonReferenceStr       *time.Time `jsonapi:"attr,nonReferenceStr"`
		})))

		assert.Equal(t, []IncludePath{}, refs)
	})

	t.Run("it should correctly handle non-pointer references", func(t *testing.T) {
		refs := GetReferencesMapping(reflect.TypeOf(new(struct {
			Plain  ref   `jsonapi:"relation,plain"`
			Sliced []ref `jsonapi:"relation,sliced"`
		})))

		assert.Equal(t, 2, len(refs))

		assert.Equal(t, []IncludePath{
			{LocalField: "Plain", RemoteField: "ID", Path: "plain", Resource: "ref"},
			{LocalField: "Sliced", RemoteField: "ID", Path: "sliced", Resource: "ref"},
		}, refs)
	})
}

func TestGetTopLevelIncludeKeys(t *testing.T) {
	t.Run("should return a list of keys that have no dot in the path", func(t *testing.T) {
		out := GetTopLevelIncludeKeys([]string{"a", "a.b", "c", "c.d"})
		assert.Equal(t, []string{"a", "c"}, out)
	})

	t.Run("should extract the first part of the dotted path and return unique keys ", func(t *testing.T) {
		out := GetTopLevelIncludeKeys([]string{"a", "a.b", "c.d", "d.e"})
		assert.Equal(t, []string{"a", "c", "d"}, out)
	})
}

func TestGetSubIncludeKeysForPrefix(t *testing.T) {

	t.Run("should filter out irrelevant keys with a different prefix", func(t *testing.T) {
		out := GetSubIncludeKeysForPrefix("a", []string{"a.b", "b.c", "d"})
		assert.Equal(t, 1, len(out))
	})
	t.Run("should transform returned keys to omit the requested top level key", func(t *testing.T) {
		out := GetSubIncludeKeysForPrefix("a", []string{"a.b", "a.c", "a.d"})
		assert.Equal(t, []string{"b", "c", "d"}, out)
	})
	t.Run("should keep full tail of the intermediate paths", func(t *testing.T) {
		out := GetSubIncludeKeysForPrefix("a", []string{"a.b.c", "a.b.d"})
		assert.Equal(t, []string{"b.c", "b.d"}, out)
	})
	t.Run("should return an empty list if no keys match the prefix", func(t *testing.T) {
		out := GetSubIncludeKeysForPrefix("a", []string{"b.c", "b.d"})
		assert.Equal(t, []string{}, out)
	})
}

func TestGetReferencedIdsFromPrimary(t *testing.T) {
	t.Parallel()

	refs := GetReferencesMapping(reflect.TypeOf(new(main)))
	avatarRef, _ := lo.Find(refs, func(includePath IncludePath) bool {
		return includePath.Path == "avatar"
	})
	imageRef, _ := lo.Find(refs, func(includePath IncludePath) bool {
		return includePath.Path == "images"
	})

	t.Run("it should return a list of ids from one-to-one reference on primaries", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{Avatar: &ref{ID: "1"}},
			&main{Avatar: &ref{ID: "2"}},
		}

		ids := GetReferencedIdsFromPrimary(primary, avatarRef)
		assert.Equal(t, []string{"1", "2"}, ids)
	})

	t.Run("it should not crash for empty one-to-one references", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{},
		}

		ids := GetReferencedIdsFromPrimary(primary, avatarRef)
		assert.Equal(t, []string{}, ids)
	})
	t.Run("it should not include zero values for one-to-one references", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{Avatar: &ref{ID: ""}},
			&main{Avatar: &ref{ID: "1"}},
		}

		ids := GetReferencedIdsFromPrimary(primary, avatarRef)
		assert.Equal(t, []string{"1"}, ids)
	})

	t.Run("it should return a list of ids from one-to-many reference on primary", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{Images: []*ref{{ID: "1"}, {ID: "2"}}},
			&main{Images: []*ref{{ID: "3"}, {ID: "4"}}},
		}

		ids := GetReferencedIdsFromPrimary(primary, imageRef)
		assert.Equal(t, []string{"1", "2", "3", "4"}, ids)
	})

	t.Run("it should not include zero values for one-to-many references", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{Images: []*ref{{ID: ""}}},
			&main{Images: []*ref{{ID: "1"}, {ID: ""}}},
		}
		ids := GetReferencedIdsFromPrimary(primary, imageRef)
		assert.Equal(t, []string{"1"}, ids)
	})

	t.Run("It should not crash on empty one-to-many references", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{},
			&main{Images: []*ref{}},
		}
		ids := GetReferencedIdsFromPrimary(primary, imageRef)
		assert.Equal(t, []string{}, ids)
	})
}

func TestMergeWithIncluded(t *testing.T) {
	refs := GetReferencesMapping(reflect.TypeOf(new(main)))
	avatarRef, _ := lo.Find(refs, func(includePath IncludePath) bool {
		return includePath.Path == "avatar"
	})
	imagesRef, _ := lo.Find(refs, func(includePath IncludePath) bool {
		return includePath.Path == "images"
	})
	avatarValRef, _ := lo.Find(refs, func(includePath IncludePath) bool {
		return includePath.Path == "valAvatar"
	})
	imagesValRef, _ := lo.Find(refs, func(includePath IncludePath) bool {
		return includePath.Path == "valImages"
	})

	t.Run("should correctly merge one-to-one pointer relationships", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{Avatar: &ref{}},
			&main{Avatar: &ref{ID: "1"}},
			&main{Avatar: &ref{ID: "2"}},
		}
		secondary := []resource.Resourcer{
			&ref{ID: "1", Url: "https://example.com/1"},
			&ref{ID: "3", Url: "https://example.com/3"},
		}

		mergeErr := MergeWithIncluded(primary, secondary, avatarRef)
		assert.Nil(t, mergeErr)

		assert.Equal(t, "", primary[0].(*main).Avatar.ID)
		assert.Equal(t, "https://example.com/1", primary[1].(*main).Avatar.Url)
		assert.Equal(t, "2", primary[2].(*main).Avatar.ID)
	})

	t.Run("should correctly merge one-to-one value relationships", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{ValAvatar: ref{}},
			&main{ValAvatar: ref{ID: "1"}},
			&main{ValAvatar: ref{ID: "2"}},
		}
		secondary := []resource.Resourcer{
			&ref{ID: "1", Url: "https://example.com/1"},
			&ref{ID: "3", Url: "https://example.com/3"},
		}

		mergeErr := MergeWithIncluded(primary, secondary, avatarValRef)
		assert.Nil(t, mergeErr)

		assert.Equal(t, "", primary[0].(*main).ValAvatar.ID)
		assert.Equal(t, "https://example.com/1", primary[1].(*main).ValAvatar.Url)
		assert.Equal(t, "2", primary[2].(*main).ValAvatar.ID)
	})

	t.Run("should correctly merge one-to-many relationships", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{Images: []*ref{}},
			&main{Images: []*ref{{ID: "1"}}},
			&main{Images: []*ref{{ID: "2"}, {ID: "3"}}},
		}
		secondary := []resource.Resourcer{
			&ref{ID: "1", Url: "https://example.com/1"},
			&ref{ID: "3", Url: "https://example.com/3"},
		}

		mergeErr := MergeWithIncluded(primary, secondary, imagesRef)
		assert.Nil(t, mergeErr)

		assert.Equal(t, 0, len(primary[0].(*main).Images))
		assert.Equal(t, 1, len(primary[1].(*main).Images))
		assert.Equal(t, 2, len(primary[2].(*main).Images))

		assert.Equal(t, "https://example.com/1", primary[1].(*main).Images[0].Url)

		assert.Equal(t, "2", primary[2].(*main).Images[0].ID)
		assert.Equal(t, "", primary[2].(*main).Images[0].Url)
		assert.Equal(t, "https://example.com/3", primary[2].(*main).Images[1].Url)

	})

	t.Run("should correctly merge one-to-many value relationships", func(t *testing.T) {
		primary := []resource.Resourcer{
			&main{ValImages: []ref{}},
			&main{ValImages: []ref{{ID: "1"}}},
			&main{ValImages: []ref{{ID: "2"}, {ID: "3"}}},
		}
		secondary := []resource.Resourcer{
			&ref{ID: "1", Url: "https://example.com/1"},
			&ref{ID: "3", Url: "https://example.com/3"},
		}

		mergeErr := MergeWithIncluded(primary, secondary, imagesValRef)
		assert.Nil(t, mergeErr)

		assert.Equal(t, 0, len(primary[0].(*main).ValImages))
		assert.Equal(t, 1, len(primary[1].(*main).ValImages))
		assert.Equal(t, 2, len(primary[2].(*main).ValImages))

		assert.Equal(t, "https://example.com/1", primary[1].(*main).ValImages[0].Url)

		assert.Equal(t, "2", primary[2].(*main).ValImages[0].ID)
		assert.Equal(t, "", primary[2].(*main).ValImages[0].Url)
		assert.Equal(t, "https://example.com/3", primary[2].(*main).ValImages[1].Url)
	})
}
