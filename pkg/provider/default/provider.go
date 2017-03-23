/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provider

import (
	"time"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/metrics/pkg/apis/custom_metrics"
	"k8s.io/client-go/pkg/api"
	_ "k8s.io/client-go/pkg/api/install"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/scheme"

	"k8s.io/custom-metrics-boilerplate/pkg/provider"
)

type DefaultMetricsProvider struct {
	client coreclient.CoreV1Interface
	impl   provider.CustomMetricsImplementation
}

func NewDefaultProvider(client coreclient.CoreV1Interface, impl provider.CustomMetricsImplementation) provider.CustomMetricsProvider {
	return &DefaultMetricsProvider{
		client: client,
		impl: impl,
	}
}

func (p *DefaultMetricsProvider) metricFor(groupResource schema.GroupResource, namespace string, name string, metricName string) (*custom_metrics.MetricValue, error) {
	group, err := api.Registry.Group(groupResource.Group)
	if err != nil {
		return nil, err
	}
	kind, err := api.Registry.RESTMapper().KindFor(groupResource.WithVersion(group.GroupVersion.Version))
	if err != nil {
		return nil, err
	}

	value, window := p.impl.ValueForMetric(name, groupResource, namespace, metricName)

	return &custom_metrics.MetricValue{
		DescribedObject: api.ObjectReference{
			APIVersion: groupResource.Group+"/"+runtime.APIVersionInternal,
			Kind: kind.Kind,
			Name: name,
			Namespace: namespace,
		},
		MetricName: metricName,
		Timestamp: metav1.Time{time.Now()},
		Value: *resource.NewMilliQuantity(value, resource.DecimalSI),
		WindowSeconds: &window,
	}, nil
}

func (p *DefaultMetricsProvider) metricsFor(groupResource schema.GroupResource, metricName string, list runtime.Object) (*custom_metrics.MetricValueList, error) {
	if !apimeta.IsListType(list) {
		return nil, fmt.Errorf("returned object was not a list")
	}

	res := make([]custom_metrics.MetricValue, 0)

	err := apimeta.EachListItem(list, func(item runtime.Object) error {
		objMeta := item.(metav1.ObjectMetaAccessor).GetObjectMeta()
		metric, err := p.metricFor(groupResource, objMeta.GetNamespace(), objMeta.GetName(), metricName)
		if err != nil {
			return err
		}
		res = append(res, *metric)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &custom_metrics.MetricValueList{
		Items: res,
	}, nil
}

func (p *DefaultMetricsProvider) GetRootScopedMetricByName(groupResource schema.GroupResource, name string, metricName string) (*custom_metrics.MetricValueList, error) {
	metric, err := p.metricFor(groupResource, "", name, metricName)
	if err != nil {
		return nil, err
	}
	return &custom_metrics.MetricValueList{
		Items: []custom_metrics.MetricValue{*metric},
	}, nil
}

func (p *DefaultMetricsProvider) GetNamespacedMetricByName(groupResource schema.GroupResource, namespace string, name string, metricName string) (*custom_metrics.MetricValueList, error) {
	metric, err := p.metricFor(groupResource, namespace, name, metricName)
	if err != nil {
		return nil, err
	}
	return &custom_metrics.MetricValueList{
		Items: []custom_metrics.MetricValue{*metric},
	}, nil
}

func (p *DefaultMetricsProvider) GetRootScopedMetricBySelector(groupResource schema.GroupResource, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// TODO: work for objects not in core v1
	matchingObjectsRaw, err := p.client.RESTClient().Get().
		Resource(groupResource.Resource).
		VersionedParams(&metav1.ListOptions{LabelSelector: selector.String()}, scheme.ParameterCodec).
		Do().
		Get()
	if err != nil {
		return nil, err
	}
	return p.metricsFor(groupResource, metricName, matchingObjectsRaw)
}

func (p *DefaultMetricsProvider) GetNamespacedMetricBySelector(groupResource schema.GroupResource, namespace string, selector labels.Selector, metricName string) (*custom_metrics.MetricValueList, error) {
	// TODO: work for objects not in core v1
	matchingObjectsRaw, err := p.client.RESTClient().Get().
		Namespace(namespace).
		Resource(groupResource.Resource).
		VersionedParams(&metav1.ListOptions{LabelSelector: selector.String()}, scheme.ParameterCodec).
		Do().
		Get()
	if err != nil {
		return nil, err
	}
	return p.metricsFor(groupResource, metricName, matchingObjectsRaw)
}

func (p *DefaultMetricsProvider) ListAllMetrics() []provider.MetricInfo {
	return p.impl.ListAllMetrics()
}
