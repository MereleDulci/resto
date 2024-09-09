package action

import (
	"context"
	"github.com/MereleDulci/resto/pkg/access"
	"github.com/MereleDulci/resto/pkg/resource"
	"reflect"
)

type CollectionAction func(context.Context, resource.Req) ([]resource.Resourcer, error)
type SingleAction func(context.Context, resource.Req, []resource.Resourcer) ([]resource.Resourcer, error)

func NewRegistry() Registry {
	return Registry{
		CollectionActions: make(map[string]CollectionRegistryEntry),
		SingleActions:     make(map[string]SingleRegistryEntry),
	}
}

func NewActionConfig(name string, method string, payload reflect.Type) EntryConfig {
	return EntryConfig{
		Name:        name,
		Method:      method,
		PayloadType: payload,
		Policies:    make([]access.AccessPolicy, 0),
	}
}

type EntryConfig struct {
	Name        string
	Method      string
	PayloadType reflect.Type
	Policies    []access.AccessPolicy
}

func (re EntryConfig) WithPolicies(policies []access.AccessPolicy) EntryConfig {
	re.Policies = append(re.Policies, policies...)
	return re
}

type CollectionRegistryEntry struct {
	Config EntryConfig
	Action CollectionAction
}

type SingleRegistryEntry struct {
	Config EntryConfig
	Action SingleAction
}

type Registry struct {
	CollectionActions map[string]CollectionRegistryEntry
	SingleActions     map[string]SingleRegistryEntry
}

func (ar Registry) AddCollection(config EntryConfig, action CollectionAction) {
	entry := CollectionRegistryEntry{
		Config: config,
		Action: action,
	}

	ar.CollectionActions[config.Name] = entry
}

func (ar Registry) AddSingle(config EntryConfig, action SingleAction) {
	entry := SingleRegistryEntry{
		Config: config,
		Action: action,
	}

	ar.SingleActions[config.Name] = entry
}

func (ar Registry) HasCollection(name string) bool {
	_, ok := ar.CollectionActions[name]
	return ok
}

func (ar Registry) HasSingle(name string) bool {
	_, ok := ar.SingleActions[name]
	return ok
}
