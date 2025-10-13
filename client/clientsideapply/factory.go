package clientsideapply

import (
	"fmt"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/structured-merge-diff/v4/fieldpath"
)

// FieldManagerFactoryOption is an option interface for FieldManagerFactory
type FieldManagerFactoryOption interface {
	apply(*FieldManagerFactory)
}

type factoryOption struct {
	fn func(*FieldManagerFactory)
}

func (f *factoryOption) apply(fm *FieldManagerFactory) {
	f.fn(fm)
}

func newFactoryOptionFunc(f func(*FieldManagerFactory)) FieldManagerFactoryOption {
	return &factoryOption{fn: f}
}

// WithScheme sets the scheme for the FieldManagerFactory
func WithScheme(scheme *runtime.Scheme) FieldManagerFactoryOption {
	return newFactoryOptionFunc(func(f *FieldManagerFactory) {
		f.scheme = scheme
	})
}

// WithTypeConverter sets the TypeConverter for the FieldManagerFactory
func WithTypeConverter(typeConverter fieldmanager.TypeConverter) FieldManagerFactoryOption {
	return newFactoryOptionFunc(func(f *FieldManagerFactory) {
		f.typeConverter = typeConverter
	})
}

// WithAdoptNonApplied sets the adoptNonApplied flag for the FieldManagerFactory.
// The FieldManagerFactory will use AdoptNonApplied field manager to take over all fields
// before first apply.
func WithAdoptNonApplied() FieldManagerFactoryOption {
	return newFactoryOptionFunc(func(f *FieldManagerFactory) {
		f.adoptNonApplied = true
	})
}

type FieldManagerFactory struct {
	cache sync.Map

	// options
	scheme          *runtime.Scheme
	typeConverter   fieldmanager.TypeConverter
	adoptNonApplied bool
}

func NewFieldManagerFactory(options ...FieldManagerFactoryOption) *FieldManagerFactory {
	fmf := &FieldManagerFactory{
		cache: sync.Map{},
	}

	for _, o := range options {
		o.apply(fmf)
	}

	fmf.setDefaults()

	return fmf
}

func (f *FieldManagerFactory) setDefaults() {
	if f.scheme == nil {
		f.scheme = clientgoscheme.Scheme
	}
	if f.typeConverter == nil {
		f.typeConverter = fieldmanager.DeducedTypeConverter{}
	}
}

func (f *FieldManagerFactory) NewFieldManager(gvk schema.GroupVersionKind, subresource string) (FieldManager, error) {
	key := f.getCacheKey(gvk, subresource)
	v, ok := f.cache.Load(key)
	if ok {
		return v.(FieldManager), nil
	}

	resetFiles, err := f.getResetFields(gvk, subresource)
	if err != nil {
		return nil, err
	}

	var creator runtime.ObjectCreater
	var convertor runtime.ObjectConvertor
	var defaultor runtime.ObjectDefaulter

	if f.scheme.Recognizes(gvk) {
		creator = f.scheme
		convertor = f.scheme
		defaultor = f.scheme
	} else {
		creator = &unstructuredCreater{}
		convertor = &unstructuredConvertor{}
		defaultor = f.scheme
	}

	fm, err := f.new(
		f.typeConverter,
		convertor,
		defaultor,
		creator,
		gvk,
		gvk.GroupVersion(),
		subresource,
		resetFiles,
	)
	if err != nil {
		return nil, err
	}

	manager := &clientApplyFieldManager{
		fieldmanager: fm,
		gvk:          gvk,
	}

	f.cache.LoadOrStore(key, manager)

	return manager, nil
}

func (f *FieldManagerFactory) getCacheKey(gvk schema.GroupVersionKind, subresource string) string {
	return fmt.Sprintf("%s, Subresource=%s", gvk.String(), subresource)
}

func (f *FieldManagerFactory) getResetFields(gvk schema.GroupVersionKind, subresource string) (map[fieldpath.APIVersion]*fieldpath.Set, error) {
	apiVersion := fieldpath.APIVersion(gvk.GroupVersion().String())
	result := map[fieldpath.APIVersion]*fieldpath.Set{}
	obj, err := f.scheme.New(gvk)
	if err != nil {
		// unkown gvk
		if len(subresource) > 0 {
			result = map[fieldpath.APIVersion]*fieldpath.Set{
				apiVersion: fieldpath.NewSet(
					fieldpath.MakePathOrDie("spec"),
				),
			}
		}
		result = map[fieldpath.APIVersion]*fieldpath.Set{
			apiVersion: fieldpath.NewSet(
				fieldpath.MakePathOrDie("status"),
			),
		}
		return result, nil
	}

	t := reflect.TypeOf(obj)
	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("object is not a pointer")
	}
	t = t.Elem()
	_, hasSpec := t.FieldByName("Spec")
	_, hasStatus := t.FieldByName("Status")

	if len(subresource) > 0 && hasSpec {
		result = map[fieldpath.APIVersion]*fieldpath.Set{
			apiVersion: fieldpath.NewSet(
				fieldpath.MakePathOrDie("spec"),
			),
		}
	}
	if len(subresource) == 0 && hasStatus {
		result = map[fieldpath.APIVersion]*fieldpath.Set{
			apiVersion: fieldpath.NewSet(
				fieldpath.MakePathOrDie("status"),
			),
		}
	}
	return result, nil
}

func (f *FieldManagerFactory) new(
	typeConverter fieldmanager.TypeConverter,
	objectConverter runtime.ObjectConvertor,
	objectDefaulter runtime.ObjectDefaulter,
	objectCreater runtime.ObjectCreater,
	kind schema.GroupVersionKind,
	hub schema.GroupVersion,
	subresource string,
	resetFields map[fieldpath.APIVersion]*fieldpath.Set,
) (*fieldmanager.FieldManager, error) {
	fm, err := fieldmanager.NewStructuredMergeManager(typeConverter, objectConverter, objectDefaulter, kind.GroupVersion(), hub, resetFields)
	if err != nil {
		return nil, fmt.Errorf("failed to create field manager: %v", err)
	}

	fm = fieldmanager.NewCapManagersManager(
		fieldmanager.NewBuildManagerInfoManager(
			fieldmanager.NewManagedFieldsUpdater(
				fieldmanager.NewStripMetaManager(fm),
			), kind.GroupVersion(), subresource,
		), fieldmanager.DefaultMaxUpdateManagers,
	)

	if f.adoptNonApplied {
		fm = NewAdoptNonAppliedManager(fm, objectCreater, kind)
	} else {
		fm = fieldmanager.NewSkipNonAppliedManager(fm, objectCreater, kind)
	}

	return fieldmanager.NewFieldManager(fm, subresource), nil
}
