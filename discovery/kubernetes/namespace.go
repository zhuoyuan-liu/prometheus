// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubernetes

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
)

var (
	namespaceAddCount    = eventCount.WithLabelValues("namespace", "add")
	namespaceUpdateCount = eventCount.WithLabelValues("namespace", "update")
	namespaceDeleteCount = eventCount.WithLabelValues("namespace", "delete")
)

// Namespace discovers Kubernetes namespaces.
type Namespace struct {
	logger   log.Logger
	informer cache.SharedInformer
	store    cache.Store
	queue    *workqueue.Type
}

// NewNamespace returns a new namespace discovery.
func NewNamespace(l log.Logger, inf cache.SharedInformer) *Namespace {
	if l == nil {
		l = log.NewNopLogger()
	}
	n := &Namespace{logger: l, informer: inf, store: inf.GetStore(), queue: workqueue.NewNamed("namespace")}
	_, err := n.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o interface{}) {
			namespaceAddCount.Inc()
			n.enqueue(o)
		},
		DeleteFunc: func(o interface{}) {
			namespaceDeleteCount.Inc()
			n.enqueue(o)
		},
		UpdateFunc: func(_, o interface{}) {
			namespaceUpdateCount.Inc()
			n.enqueue(o)
		},
	})
	if err != nil {
		level.Error(l).Log("msg", "Error adding namespaces event handler.", "err", err)
	}
	return n
}

func (n *Namespace) enqueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}

	n.queue.Add(key)
}

// Run implements the Discoverer interface.
func (n *Namespace) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	defer n.queue.ShutDown()

	if !cache.WaitForCacheSync(ctx.Done(), n.informer.HasSynced) {
		if !errors.Is(ctx.Err(), context.Canceled) {
			level.Error(n.logger).Log("msg", "namespace informer unable to sync cache")
		}
		return
	}

	go func() {
		for n.process(ctx, ch) { // nolint:revive
		}
	}()

	// Block until the target provider is explicitly canceled.
	<-ctx.Done()
}

func (n *Namespace) process(ctx context.Context, ch chan<- []*targetgroup.Group) bool {
	keyObj, quit := n.queue.Get()
	if quit {
		return false
	}
	defer n.queue.Done(keyObj)
	key := keyObj.(string)

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return true
	}

	o, exists, err := n.store.GetByKey(key)
	if err != nil {
		return true
	}
	if !exists {
		send(ctx, ch, &targetgroup.Group{Source: namespaceSourceFromName(name)})
		return true
	}
	namespace, err := convertToNamespace(o)
	if err != nil {
		level.Error(n.logger).Log("msg", "converting to Namespace object failed", "err", err)
		return true
	}
	send(ctx, ch, n.buildNamespace(namespace))
	return true
}

func convertToNamespace(o interface{}) (*apiv1.Namespace, error) {
	namespace, ok := o.(*apiv1.Namespace)
	if ok {
		return namespace, nil
	}

	return nil, fmt.Errorf("received unexpected object: %v", o)
}

func namespaceSource(n *apiv1.Namespace) string {
	return namespaceSourceFromName(n.Name)
}

func namespaceSourceFromName(name string) string {
	return "namespace/" + name
}

const (
	namespaceNameLabel               = metaLabelPrefix + "namespace_name"
	namespaceLabelPrefix             = metaLabelPrefix + "namespace_label_"
	namespaceLabelPresentPrefix      = metaLabelPrefix + "namespace_labelpresent_"
	namespaceAnnotationPrefix        = metaLabelPrefix + "namespace_annotation_"
	namespaceAnnotationPresentPrefix = metaLabelPrefix + "namespace_annotationpresent_"
)

func namespaceLabels(n *apiv1.Namespace) model.LabelSet {
	// Each label and annotation will create two key-value pairs in the map.
	ls := make(model.LabelSet, 2*(len(n.Labels)+len(n.Annotations))+1)

	ls[namespaceNameLabel] = lv(n.Name)

	for k, v := range n.Labels {
		ln := strutil.SanitizeLabelName(k)
		ls[model.LabelName(namespaceLabelPrefix+ln)] = lv(v)
		ls[model.LabelName(namespaceLabelPresentPrefix+ln)] = presentValue
	}

	for k, v := range n.Annotations {
		ln := strutil.SanitizeLabelName(k)
		ls[model.LabelName(namespaceAnnotationPrefix+ln)] = lv(v)
		ls[model.LabelName(namespaceAnnotationPresentPrefix+ln)] = presentValue
	}
	return ls
}

func (n *Namespace) buildNamespace(namespace *apiv1.Namespace) *targetgroup.Group {
	tg := &targetgroup.Group{
		Source: namespaceSource(namespace),
	}
	tg.Labels = namespaceLabels(namespace)

	t := model.LabelSet{
		model.InstanceLabel: lv(namespace.Name),
	}
	tg.Targets = append(tg.Targets, t)

	return tg
}
