package hook

import (
	"context"
	"github.com/MereleDulci/resto/pkg/resource"
	"github.com/MereleDulci/resto/pkg/typecast"
)

type BeforeRead func(context.Context, resource.Req) (resource.Req, error)
type BeforeCreate func(context.Context, resource.Req, resource.Resourcer) (resource.Req, resource.Resourcer, error)
type BeforeUpdate func(context.Context, resource.Req, []typecast.PatchOperation) (resource.Req, []typecast.PatchOperation, error)
type BeforeDelete func(context.Context, resource.Req, resource.Resourcer) (resource.Req, error)

type After func(context.Context, resource.Req, resource.Resourcer) (resource.Resourcer, error)
type AfterDelete func(context.Context, resource.Req, resource.Resourcer) error
type AfterAll func(context.Context, resource.Req, []resource.Resourcer) ([]resource.Resourcer, error)

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

func (hr *Registry) RegisterAfterDelete(hook AfterDelete) {
	hr.afterDeletes = append(hr.afterDeletes, hook)
}

func (hr *Registry) RunBeforeCreates(ctx context.Context, r resource.Req, record resource.Resourcer) (resource.Req, resource.Resourcer, error) {
	nextRecord := record
	var err error
	for _, hook := range hr.beforeCreates {
		r, nextRecord, err = hook(ctx, r, nextRecord)
		if err != nil {
			return r, nil, err
		}
	}
	return r, nextRecord, nil
}

func (hr *Registry) RunAfterCreates(ctx context.Context, r resource.Req, record resource.Resourcer) (resource.Resourcer, error) {
	nextRecord := record
	var err error
	for _, hook := range hr.afterCreates {
		nextRecord, err = hook(ctx, r, nextRecord)
		if err != nil {
			return nil, err
		}
	}
	return nextRecord, nil
}

func (hr *Registry) RunBeforeReads(ctx context.Context, r resource.Req) (resource.Req, error) {
	var err error
	for _, hook := range hr.beforeReads {
		r, err = hook(ctx, r)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}

func (hr *Registry) RunAfterReads(ctx context.Context, r resource.Req, record resource.Resourcer) (resource.Resourcer, error) {
	nextRecord := record
	var err error
	for _, hook := range hr.afterReads {
		nextRecord, err = hook(ctx, r, nextRecord)
		if err != nil {
			return nil, err
		}
	}
	return nextRecord, nil
}

func (hr *Registry) RunAfterReadAll(ctx context.Context, r resource.Req, records []resource.Resourcer) ([]resource.Resourcer, error) {
	nextRecords := records
	var err error
	for _, hook := range hr.afterReadAlls {
		nextRecords, err = hook(ctx, r, nextRecords)
		if err != nil {
			return nil, err
		}
	}
	return nextRecords, nil
}

func (hr *Registry) RunBeforeUpdates(ctx context.Context, r resource.Req, ops []typecast.PatchOperation) (resource.Req, []typecast.PatchOperation, error) {
	var err error
	nextOps := ops
	for _, hook := range hr.beforeUpdates {
		r, nextOps, err = hook(ctx, r, nextOps)
		if err != nil {
			return r, nil, err
		}
	}
	return r, nextOps, nil
}

func (hr *Registry) RunAfterUpdates(ctx context.Context, r resource.Req, record resource.Resourcer) (resource.Resourcer, error) {
	nextRecord := record
	var err error
	for _, hook := range hr.afterUpdates {
		nextRecord, err = hook(ctx, r, nextRecord)
		if err != nil {
			return nil, err
		}
	}
	return nextRecord, nil
}

func (hr *Registry) RunBeforeDeletes(ctx context.Context, r resource.Req, record resource.Resourcer) (resource.Req, error) {
	var err error
	for _, hook := range hr.beforeDeletes {
		r, err = hook(ctx, r, record)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}

func (hr *Registry) RunAfterDeletes(ctx context.Context, r resource.Req, record resource.Resourcer) error {
	var err error
	for _, hook := range hr.afterDeletes {
		err = hook(ctx, r, record)
		if err != nil {
			return err
		}
	}
	return nil
}
