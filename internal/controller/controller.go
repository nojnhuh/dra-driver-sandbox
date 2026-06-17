package controller

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/informers/internalinterfaces"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"

	"github.com/nojnhuh/dra-driver-sandbox/internal/driver"
)

type controller struct {
	queue    workqueue.TypedRateLimitingInterface[string]
	informer cache.SharedIndexInformer

	controller *resourceslice.Controller
	resources  *resourceslice.DriverResources
}

func Run(ctx context.Context, clientset kubernetes.Interface) error {
	logger := klog.FromContext(ctx)

	c := &controller{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{},
		),
		informer: informersv1.NewConfigMapInformerWithOptions(
			clientset,
			metav1.NamespaceAll,
			internalinterfaces.InformerOptions{
				Indexers: cache.Indexers{},
				TweakListOptions: func(opts *metav1.ListOptions) {
					opts.LabelSelector = driver.DriverName + "/cluster-devices"
				},
			},
		),
		resources: &resourceslice.DriverResources{Pools: map[string]resourceslice.Pool{}},
	}

	handler, err := c.informer.AddEventHandlerWithOptions(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				configMap, ok := obj.(*corev1.ConfigMap)
				if !ok {
					return
				}
				pool, ok := configMap.Data["pool"]
				if ok {
					c.queue.Add(pool)
				}
			},
			UpdateFunc: func(oldObj, newObj any) {
				configMap, ok := newObj.(*corev1.ConfigMap)
				if !ok {
					return
				}
				pool, ok := configMap.Data["pool"]
				if ok {
					c.queue.Add(pool)
				}
			},
			DeleteFunc: func(obj any) {
				if deleted, ok := obj.(cache.DeletedFinalStateUnknown); ok {
					obj = deleted.Obj
				}
				configMap, ok := obj.(*corev1.ConfigMap)
				if !ok {
					return
				}
				pool, ok := configMap.Data["pool"]
				if ok {
					c.queue.Add(pool)
				}
			},
		},
		cache.HandlerOptions{
			Logger: &logger,
		},
	)
	if err != nil {
		return fmt.Errorf("add event handler: %w", err)
	}
	err = c.informer.AddIndexers(cache.Indexers{
		"resourcePool": func(obj any) ([]string, error) {
			configMap, ok := obj.(*corev1.ConfigMap)
			if !ok {
				return nil, fmt.Errorf("indexer for ConfigMaps got a %T", obj)
			}
			pool, ok := configMap.Data["pool"]
			if !ok {
				return nil, nil
			}
			return []string{pool}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("add pool indexer: %w", err)
	}

	logger.Info("Starting controller")
	defer logger.Info("Stopped controller")
	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Go(func() {
		logger.Info("Starting informer")
		defer logger.Info("Stopped informer")
		c.informer.RunWithContext(ctx)
	})
	if !cache.WaitFor(ctx, "", handler.HasSyncedChecker()) {
		return fmt.Errorf("wait for informer sync: %w", ctx.Err())
	}

	c.controller, err = resourceslice.StartController(ctx, resourceslice.Options{
		DriverName: "sandbox.example.com",
		KubeClient: clientset,
	})
	if err != nil {
		return fmt.Errorf("start ResourceSlice controller: %w", err)
	}

	defer runtime.HandleCrashWithContext(ctx)

	defer func() {
		logger.Info("Shutting down queue")
		defer logger.Info("Shut down queue")
		c.queue.ShutDown()
	}()
	wg.Go(func() {
		wait.UntilWithContext(ctx, c.run, 1*time.Second)
		c.run(ctx)
	})

	<-ctx.Done()
	defer logger.Info("Stopping controller")
	return nil
}

func (c *controller) run(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncPool(ctx, key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	runtime.HandleErrorWithContext(ctx, err, "Work item failed", "item", key)
	c.queue.AddRateLimited(key)

	return true
}

func (c *controller) syncPool(ctx context.Context, poolName string) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Syncing pool", "pool", poolName)

	objs, err := c.informer.GetIndexer().ByIndex("resourcePool", poolName)
	if err != nil {
		return fmt.Errorf("list ConfigMaps for pool %s: %w", poolName, err)
	}
	configMaps := make([]*corev1.ConfigMap, len(objs))
	for i, obj := range objs {
		var ok bool
		configMaps[i], ok = obj.(*corev1.ConfigMap)
		if !ok {
			return fmt.Errorf("indexer for ConfigMaps got a %T", obj)
		}
	}
	slices.SortFunc(configMaps, func(a, b *corev1.ConfigMap) int {
		if a.Namespace != b.Namespace {
			return cmp.Compare(a.Namespace, b.Namespace)
		}
		return cmp.Compare(a.Name, b.Name)
	})

	resourceSlices := make([]resourceslice.Slice, 0, len(configMaps))
	var allNodes bool
	var nodeSelector *corev1.NodeSelector
	for _, configMap := range configMaps {
		var devices []resourcev1.Device
		if err := driver.UnmarshalConfigMapKey(configMap, driver.TemplateDevicesKey, &devices); err != nil {
			return err
		}

		var sharedCounters []resourcev1.CounterSet
		if err := driver.UnmarshalConfigMapKey(configMap, driver.TemplateSharedCountersKey, &sharedCounters); err != nil {
			return err
		}

		resourceSlices = append(resourceSlices, resourceslice.Slice{
			Devices:        devices,
			SharedCounters: sharedCounters,
		})

		if _, ok := configMap.Data["nodeSelector"]; ok {
			var sel corev1.NodeSelector
			if err := driver.UnmarshalConfigMapKey(configMap, "nodeSelector", &sel); err != nil {
				return err
			}
			nodeSelector = new(sel)
		}
	}

	c.resources.Pools[poolName] = resourceslice.Pool{
		Slices:       resourceSlices,
		AllNodes:     allNodes,
		NodeSelector: nodeSelector,
	}

	c.controller.Update(c.resources)
	return nil
}
