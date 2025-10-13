package clientsideapply

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ runtime.ObjectCreater = &unstructuredCreater{}

type unstructuredCreater struct{}

func (c *unstructuredCreater) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	ret := &unstructured.Unstructured{}
	ret.SetGroupVersionKind(kind)
	return ret, nil
}

var _ runtime.ObjectConvertor = &unstructuredConvertor{}

type unstructuredConvertor struct{}

// Convert implements runtime.ObjectConvertor.
func (c *unstructuredConvertor) Convert(in any, out any, context any) error {
	unstructIn, ok := in.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("input type %T in not valid for unstructured conversion to %T", in, out)
	}

	unstructOut, ok := out.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("output type %T in not valid for unstructured conversion from %T", out, in)
	}

	outGVK := unstructOut.GroupVersionKind()
	converted, err := c.ConvertToVersion(unstructIn, outGVK.GroupVersion())
	if err != nil {
		return err
	}
	unstructuredConverted, ok := converted.(runtime.Unstructured)
	if !ok {
		// this should not happened
		return fmt.Errorf("unstructured conversion failed")
	}
	unstructOut.SetUnstructuredContent(unstructuredConverted.UnstructuredContent())
	return nil
}

// ConvertFieldLabel implements runtime.ObjectConvertor.
func (c *unstructuredConvertor) ConvertFieldLabel(gvk schema.GroupVersionKind, label string, value string) (string, string, error) {
	return runtime.DefaultMetaV1FieldSelectorConversion(label, value)
}

// ConvertToVersion implements runtime.ObjectConvertor.
func (c *unstructuredConvertor) ConvertToVersion(in runtime.Object, target runtime.GroupVersioner) (out runtime.Object, err error) {
	fromGVK := in.GetObjectKind().GroupVersionKind()
	toGVK, ok := target.KindForGroupVersionKinds([]schema.GroupVersionKind{fromGVK})
	if !ok {
		return nil, fmt.Errorf("failed to convert gvk from %q to %q", fromGVK.String(), target)
	}
	return c.convertToVersion(in, toGVK.GroupVersion())
}

func (c *unstructuredConvertor) convertToVersion(in runtime.Object, targetGV schema.GroupVersion) (runtime.Object, error) {
	// Run the converter on the list items instead of list itself
	if list, ok := in.(*unstructured.UnstructuredList); ok {
		for i := range list.Items {
			list.Items[i].SetGroupVersionKind(targetGV.WithKind(list.Items[i].GroupVersionKind().Kind))
		}
	}
	in.GetObjectKind().SetGroupVersionKind(targetGV.WithKind(in.GetObjectKind().GroupVersionKind().Kind))
	return in, nil
}
