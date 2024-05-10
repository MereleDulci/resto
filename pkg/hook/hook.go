package hook

import (
	"github.com/MereleDulci/resto/pkg/req"
	"github.com/MereleDulci/resto/pkg/typecast"
)

type BeforeReadHook func(*req.Ctx, *typecast.ResourceQuery) (*typecast.ResourceQuery, error)
type BeforeCreateHook func(*req.Ctx, typecast.Resource) (typecast.Resource, error)
type BeforeUpdateHook func(*req.Ctx, []typecast.PatchOperation) ([]typecast.PatchOperation, error)
type BeforeDeleteHook func(*req.Ctx, typecast.Resource) error

type AfterHook func(*req.Ctx, typecast.Resource) (typecast.Resource, error)
type AfterDeleteHook func(*req.Ctx, typecast.Resource) error
type AfterAllHook func(*req.Ctx, []typecast.Resource) ([]typecast.Resource, error)

func MakeResourceHookRegistry() ResourceHookRegistry {
	return ResourceHookRegistry{}
}

type ResourceHookRegistry struct {
	beforeReads   []BeforeReadHook
	beforeDeletes []BeforeDeleteHook

	beforeCreates []BeforeCreateHook

	beforeUpdates []BeforeUpdateHook

	afterReads    []AfterHook
	afterReadAlls []AfterAllHook
	afterCreates  []AfterHook
	afterUpdates  []AfterHook
	afterDeletes  []AfterDeleteHook
}

func (hr *ResourceHookRegistry) RegisterBeforeCreate(hook BeforeCreateHook) {
	hr.beforeCreates = append(hr.beforeCreates, hook)
}

func (hr *ResourceHookRegistry) RegisterBeforeRead(hook BeforeReadHook) {
	hr.beforeReads = append(hr.beforeReads, hook)
}

func (hr *ResourceHookRegistry) RegisterBeforeUpdate(hook BeforeUpdateHook) {
	hr.beforeUpdates = append(hr.beforeUpdates, hook)
}

func (hr *ResourceHookRegistry) RegisterAfterCreate(hook AfterHook) {
	hr.afterCreates = append(hr.afterCreates, hook)
}

func (hr *ResourceHookRegistry) RegisterAfterRead(hook AfterHook) {
	hr.afterReads = append(hr.afterReads, hook)
}

func (hr *ResourceHookRegistry) RegisterAfterReadAll(hook AfterAllHook) {
	hr.afterReadAlls = append(hr.afterReadAlls, hook)
}

func (hr *ResourceHookRegistry) RegisterAfterUpdate(hook AfterHook) {
	hr.afterUpdates = append(hr.afterUpdates, hook)
}

func (hr *ResourceHookRegistry) RegisterBeforeDelete(hook BeforeDeleteHook) {
	hr.beforeDeletes = append(hr.beforeDeletes, hook)
}

func (hr *ResourceHookRegistry) RunBeforeCreates(c *req.Ctx, r typecast.Resource) (typecast.Resource, error) {
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

func (hr *ResourceHookRegistry) RunAfterCreates(c *req.Ctx, r typecast.Resource) (typecast.Resource, error) {
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

func (hr *ResourceHookRegistry) RunBeforeReads(c *req.Ctx, query *typecast.ResourceQuery) (*typecast.ResourceQuery, error) {
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

func (hr *ResourceHookRegistry) RunAfterReads(c *req.Ctx, r typecast.Resource) (typecast.Resource, error) {
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

func (hr *ResourceHookRegistry) RunAfterReadAll(c *req.Ctx, rs []typecast.Resource) ([]typecast.Resource, error) {
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

func (hr *ResourceHookRegistry) RunBeforeUpdates(c *req.Ctx, ops []typecast.PatchOperation) ([]typecast.PatchOperation, error) {
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

func (hr *ResourceHookRegistry) RunAfterUpdates(c *req.Ctx, r typecast.Resource) (typecast.Resource, error) {
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

func (hr *ResourceHookRegistry) RunBeforeDeletes(c *req.Ctx, r typecast.Resource) error {
	var err error
	for _, hook := range hr.beforeDeletes {
		err = hook(c, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (hr *ResourceHookRegistry) RunAfterDeletes(c *req.Ctx, r typecast.Resource) error {
	var err error
	for _, hook := range hr.afterDeletes {
		err = hook(c, r)
		if err != nil {
			return err
		}
	}
	return nil
}
