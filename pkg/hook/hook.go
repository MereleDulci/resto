package hook

import (
	"github.com/MereleDulci/resto/pkg/collection"
	"github.com/MereleDulci/resto/pkg/req"
	"github.com/MereleDulci/resto/pkg/typecast"
)

type BeforeRead func(*req.Ctx, *collection.Query) (*collection.Query, error)
type BeforeCreate func(*req.Ctx, collection.Resourcer) (collection.Resourcer, error)
type BeforeUpdate func(*req.Ctx, []typecast.PatchOperation) ([]typecast.PatchOperation, error)
type BeforeDelete func(*req.Ctx, collection.Resourcer) error

type After func(*req.Ctx, collection.Resourcer) (collection.Resourcer, error)
type AfterDelete func(*req.Ctx, collection.Resourcer) error
type AfterAll func(*req.Ctx, []collection.Resourcer) ([]collection.Resourcer, error)

func NewRegistry() Registry {
	return Registry{}
}

type Registry struct {
	beforeReads   []BeforeRead
	beforeDeletes []BeforeDelete

	beforeCreates []BeforeCreate

	beforeUpdates []BeforeUpdate

	afterReads    []After
	afterReadAlls []AfterAll
	afterCreates  []After
	afterUpdates  []After
	afterDeletes  []AfterDelete
}

func (hr *Registry) RegisterBeforeCreate(hook BeforeCreate) {
	hr.beforeCreates = append(hr.beforeCreates, hook)
}

func (hr *Registry) RegisterBeforeRead(hook BeforeRead) {
	hr.beforeReads = append(hr.beforeReads, hook)
}

func (hr *Registry) RegisterBeforeUpdate(hook BeforeUpdate) {
	hr.beforeUpdates = append(hr.beforeUpdates, hook)
}

func (hr *Registry) RegisterAfterCreate(hook After) {
	hr.afterCreates = append(hr.afterCreates, hook)
}

func (hr *Registry) RegisterAfterRead(hook After) {
	hr.afterReads = append(hr.afterReads, hook)
}

func (hr *Registry) RegisterAfterReadAll(hook AfterAll) {
	hr.afterReadAlls = append(hr.afterReadAlls, hook)
}

func (hr *Registry) RegisterAfterUpdate(hook After) {
	hr.afterUpdates = append(hr.afterUpdates, hook)
}

func (hr *Registry) RegisterBeforeDelete(hook BeforeDelete) {
	hr.beforeDeletes = append(hr.beforeDeletes, hook)
}

func (hr *Registry) RunBeforeCreates(c *req.Ctx, r collection.Resourcer) (collection.Resourcer, error) {
	nextResource := r
	var err error
	for _, hook := range hr.beforeCreates {
		nextResource, err = hook(c, nextResource)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (hr *Registry) RunAfterCreates(c *req.Ctx, r collection.Resourcer) (collection.Resourcer, error) {
	nextResource := r
	var err error
	for _, hook := range hr.afterCreates {
		nextResource, err = hook(c, nextResource)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (hr *Registry) RunBeforeReads(c *req.Ctx, query *collection.Query) (*collection.Query, error) {
	var err error
	nextQuery := query
	for _, hook := range hr.beforeReads {
		nextQuery, err = hook(c, nextQuery)
		if err != nil {
			return nil, err
		}
	}
	return nextQuery, nil
}

func (hr *Registry) RunAfterReads(c *req.Ctx, r collection.Resourcer) (collection.Resourcer, error) {
	nextResource := r
	var err error
	for _, hook := range hr.afterReads {
		nextResource, err = hook(c, nextResource)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (hr *Registry) RunAfterReadAll(c *req.Ctx, rs []collection.Resourcer) ([]collection.Resourcer, error) {
	nextResources := rs
	var err error
	for _, hook := range hr.afterReadAlls {
		nextResources, err = hook(c, nextResources)
		if err != nil {
			return nil, err
		}
	}
	return rs, nil
}

func (hr *Registry) RunBeforeUpdates(c *req.Ctx, ops []typecast.PatchOperation) ([]typecast.PatchOperation, error) {
	var err error
	nextOps := ops
	for _, hook := range hr.beforeUpdates {
		nextOps, err = hook(c, nextOps)
		if err != nil {
			return nil, err
		}
	}
	return nextOps, nil
}

func (hr *Registry) RunAfterUpdates(c *req.Ctx, r collection.Resourcer) (collection.Resourcer, error) {
	nextResource := r
	var err error
	for _, hook := range hr.afterUpdates {
		nextResource, err = hook(c, nextResource)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (hr *Registry) RunBeforeDeletes(c *req.Ctx, r collection.Resourcer) error {
	var err error
	for _, hook := range hr.beforeDeletes {
		err = hook(c, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (hr *Registry) RunAfterDeletes(c *req.Ctx, r collection.Resourcer) error {
	var err error
	for _, hook := range hr.afterDeletes {
		err = hook(c, r)
		if err != nil {
			return err
		}
	}
	return nil
}
