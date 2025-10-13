package clientsideapply

import (
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/client-go/discovery"
	"k8s.io/kube-openapi/pkg/util/proto"
)

func GetOpenAPIModels(discoveryClient discovery.OpenAPISchemaInterface) (proto.Models, error) {
	doc, err := discoveryClient.OpenAPISchema()
	if err != nil {
		return nil, err
	}
	return proto.NewOpenAPIData(doc)
}

func NewTypeConverterByDiscovery(discoveryClient discovery.DiscoveryInterface) (fieldmanager.TypeConverter, error) {
	models, err := GetOpenAPIModels(discoveryClient)
	if err != nil {
		return nil, err
	}
	return fieldmanager.NewTypeConverter(models, true)
}
