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

package prometheus

import (
	"time"
	"fmt"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/custom-metrics-boilerplate/pkg/provider"
	clientapi "k8s.io/custom-metrics-boilerplate/pkg/sample-cmd/provider/prometheus/client"

	"github.com/prometheus/common/model"

	"golang.org/x/net/context"
)

func NewPrometheusImplementation(endpoint string, resolutionSeconds int64) *promCustomMetricsImplementation {
	return &promCustomMetricsImplementation{
		endpoint: endpoint,
		resolutionSeconds: resolutionSeconds,
	}
}

type promCustomMetricsImplementation struct {
	endpoint string
	resolutionSeconds int64
}

func (p *promCustomMetricsImplementation) ValueForMetric(name string, groupResource schema.GroupResource, namespace string, metricName string) (int64, int64) {
	if !validateMetricName(metricName) {
		return 0, p.resolutionSeconds
	}
	if len(namespace) == 0 {
		return 0, p.resolutionSeconds
	}

	query := buildQuery(name, groupResource.Resource, metricName, namespace, p.resolutionSeconds)
	fmt.Println(query)
	client, err := clientapi.New(clientapi.Config{Address: p.endpoint})
	if err != nil {
		fmt.Printf("%v\n", err)
		return 0, p.resolutionSeconds
	}

	queryAPI := clientapi.NewQueryAPI(client)
	vectorObj, err := queryAPI.Query(context.TODO(), query, time.Now())
	if err != nil {
		fmt.Printf("%v\n", err)
		return 0, p.resolutionSeconds
	}
	vector := vectorObj.(model.Vector)
	if vector == nil {
		fmt.Println("vector is nil")
		return 0, p.resolutionSeconds
	}
	if len(vector) == 0 {
		fmt.Println("vector doesn't contain any elements")
		return 0, p.resolutionSeconds
	}

	fmt.Printf("%v\n", vector)

	floatVal := vector[0].Value
	fmt.Println("floatVal", floatVal)

	intVal := int64(floatVal * 1000)
	return intVal, p.resolutionSeconds
}

func (p *promCustomMetricsImplementation) ListAllMetrics() []provider.MetricInfo {
	metricInfo := []provider.MetricInfo{}

	labelsEndpoint := fmt.Sprintf("%s/api/v1/label/__name__/values", p.endpoint)
	resp, err := http.Get(labelsEndpoint)
	if err != nil {
		return metricInfo
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return metricInfo
	}
	labelvals := &promLabelValues{}
	err = json.Unmarshal(body, labelvals)
	if err != nil {
		return metricInfo
	}
	if labelvals.status != "success" {
		return metricInfo
	}
	
	for _, label := range labelvals.data {
		if validateMetricName(label) {
			metricInfo = append(metricInfo, provider.MetricInfo{
				GroupResource: schema.GroupResource{Group: "", Resource: "pods"},
				Metric: label,
				Namespaced: true,
			}, provider.MetricInfo{
				GroupResource: schema.GroupResource{Group: "", Resource: "services"},
				Metric: label,
				Namespaced: true,
			})
		}
	}
	return metricInfo
}

func validateMetricName(metricName string) bool {
	// TODO: This should be so much more sophisticated
	return strings.HasSuffix(metricName, "_total")
}

func buildQuery(name, resource, metricName, namespace string, resolutionSeconds int64) string {
	selectors := []string{fmt.Sprintf("namespace=%q", namespace)}
	if strings.HasPrefix(strings.ToLower(resource), "service") {
		selectors = append(selectors, fmt.Sprintf("svc_name=%q", name))
	} else if strings.HasPrefix(strings.ToLower(resource), "pod") {
		selectors = append(selectors, fmt.Sprintf("pod_name=%q", name))
	}
	return fmt.Sprintf("sum(rate(%s{%s}[%ds]))", metricName, strings.Join(selectors, ","), resolutionSeconds)
}

type promLabelValues struct {
	status string
	data []string
}
