package driver

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/informers/internalinterfaces"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

const (
	templateDevicesKey        = "devices"
	templateSharedCountersKey = "sharedCounters"
)

type resourcesController struct {
	wg              sync.WaitGroup
	informer        cache.SharedIndexInformer
	configMapLister listersv1.ConfigMapLister
	helper          *kubeletplugin.Helper
	nodeName        string
}

func startResourcesController(ctx context.Context, client kubernetes.Interface, helper *kubeletplugin.Helper, driverName, nodeName string) (*resourcesController, error) {
	logger := klog.FromContext(ctx)

	informer := informersv1.NewConfigMapInformerWithOptions(client, "", internalinterfaces.InformerOptions{
		TweakListOptions: func(opts *metav1.ListOptions) {
			opts.LabelSelector = driverName + "/node-local-devices"
		},
	})

	c := &resourcesController{
		informer:        informer,
		configMapLister: listersv1.NewConfigMapLister(informer.GetIndexer()),
		helper:          helper,
		nodeName:        nodeName,
	}

	c.wg.Go(func() {
		logger.Info("Starting informer")
		defer logger.Info("Stopped informer")
		c.informer.RunWithContext(ctx)
	})

	handler, err := c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			configMap, ok := obj.(*corev1.ConfigMap)
			if !ok {
				return
			}
			logger.V(5).Info("ConfigMap add", "configMap", klog.KObj(configMap))
			if err := c.syncResources(ctx); err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "error syncing driver resources")
			}
		},
		UpdateFunc: func(oldObj any, newObj any) {
			configMap, ok := newObj.(*corev1.ConfigMap)
			if !ok {
				return
			}
			logger.V(5).Info("ConfigMap update", "configMap", klog.KObj(configMap))
			if err := c.syncResources(ctx); err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "error syncing driver resources")
			}
		},
		DeleteFunc: func(obj any) {
			tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
			if ok {
				obj = tombstone.Obj
			}
			configMap, ok := obj.(*corev1.ConfigMap)
			if !ok {
				return
			}
			logger.V(5).Info("ConfigMap delete", "configMap", klog.KObj(configMap))
			if err := c.syncResources(ctx); err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "error syncing driver resources")
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("add event handler: %w", err)
	}
	if !cache.WaitForNamedCacheSyncWithContext(ctx, handler.HasSynced) {
		return nil, fmt.Errorf("wait for informer event handler to sync failed")
	}

	return c, nil
}

func (c *resourcesController) run(_ context.Context) {
	// TODO: workqueue
	c.wg.Wait()
}

func (c *resourcesController) syncResources(ctx context.Context) error {
	configMaps, err := c.configMapLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list ConfigMaps: %w", err)
	}
	slices.SortFunc(configMaps, func(a, b *corev1.ConfigMap) int {
		if a.Namespace != b.Namespace {
			return cmp.Compare(a.Namespace, b.Namespace)
		}
		return cmp.Compare(a.Name, b.Name)
	})

	var resourceSlices []resourceslice.Slice
	for _, configMap := range configMaps {
		var devices []resourcev1.Device
		if err := unmarshalConfigMapKey(configMap, templateDevicesKey, &devices); err != nil {
			return err
		}

		var sharedCounters []resourcev1.CounterSet
		if err := unmarshalConfigMapKey(configMap, templateSharedCountersKey, &sharedCounters); err != nil {
			return err
		}

		resourceSlices = append(resourceSlices, resourceslice.Slice{
			Devices:        devices,
			SharedCounters: sharedCounters,
		})
	}

	resources := resourceslice.DriverResources{
		Pools: map[string]resourceslice.Pool{
			c.nodeName: {
				Slices: resourceSlices,
			},
		},
	}

	return c.helper.PublishResources(ctx, resources)
}

func unmarshalConfigMapKey(configMap *corev1.ConfigMap, key string, to any) error {
	data, ok := configMap.Data[key]
	if !ok {
		return nil
	}
	err := yaml.Unmarshal([]byte(data), to)
	if err != nil {
		return fmt.Errorf("unmarshal ConfigMap %s/%s data.%s: %w", configMap.Namespace, configMap.Name, key, err)
	}
	return nil
}
