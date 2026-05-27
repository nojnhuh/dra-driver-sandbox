package driver

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/pflag"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
	"tags.cncf.io/container-device-interface/pkg/cdi"
	"tags.cncf.io/container-device-interface/pkg/parser"
	cdispecs "tags.cncf.io/container-device-interface/specs-go"
)

const driverName = "sandbox.example.com"

type Options struct {
	KubeletPluginDataDirectoryPath string
	KubeletRegistrarDirectoryPath  string
	NodeName                       string
	Namespace                      string
	PodUID                         string
}

func (o *Options) AddFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.KubeletPluginDataDirectoryPath, "kubelet-plugin-data-directory-path", kubeletplugin.KubeletPluginsDir, "Path to the kubelet's plugins directory")
	flags.StringVar(&o.KubeletRegistrarDirectoryPath, "kubelet-registrar-directory-path", kubeletplugin.KubeletRegistryDir, "Path to the kubelet's plugins registry directory")
	flags.StringVar(&o.NodeName, "node-name", os.Getenv("NODE_NAME"), "Name of the Node where the driver is running")
	flags.StringVar(&o.Namespace, "namespace", os.Getenv("NAMESPACE"), "Namespace where the driver is running")
	flags.StringVar(&o.PodUID, "pod-uid", os.Getenv("POD_UID"), "UID of the Pod in which the driver is running")
}

type driver struct {
	helper    *kubeletplugin.Helper
	cancel    func(error)
	client    kubernetes.Interface
	nodeName  string
	namespace string

	checkpointPath    string
	checkpointDecoder runtime.Decoder
	checkpointEncoder runtime.Encoder
}

// fatalError represents an error that causes the driver to exit
// unsuccessfully.
type fatalError struct {
	error
}

// Run starts the kubelet plugin, blocking until the context is canceled. If
// initialization fails for a fatal background error occurs, then that error is
// returned. Other causes for canceling the context cause Run to return no
// error.
func Run(ctx context.Context, clientset kubernetes.Interface, opts Options) error {
	logger := klog.FromContext(ctx)

	checkpointDecoder, checkpointEncoder, err := checkpointSerializer()
	if err != nil {
		return fmt.Errorf("initialize checkpoint scheme: %w", err)
	}

	d := &driver{
		client:            clientset,
		nodeName:          opts.NodeName,
		namespace:         opts.Namespace,
		checkpointPath:    filepath.Join(opts.KubeletPluginDataDirectoryPath, driverName, "checkpoint.json"),
		checkpointDecoder: checkpointDecoder,
		checkpointEncoder: checkpointEncoder,
	}

	err = os.MkdirAll(filepath.Join(opts.KubeletPluginDataDirectoryPath, driverName), 0750)
	if err != nil {
		return fmt.Errorf("create kubelet plugin data directory path: %w", err)
	}

	ctx, d.cancel = context.WithCancelCause(ctx)
	d.helper, err = kubeletplugin.Start(ctx, d,
		kubeletplugin.KubeClient(clientset),
		kubeletplugin.DriverName(driverName),
		kubeletplugin.PluginDataDirectoryPath(opts.KubeletPluginDataDirectoryPath),
		kubeletplugin.RegistrarDirectoryPath(opts.KubeletRegistrarDirectoryPath),
		kubeletplugin.NodeName(opts.NodeName),
		kubeletplugin.RollingUpdate(types.UID(opts.PodUID)),
	)
	if err != nil {
		return fmt.Errorf("start kubelet plugin: %w", err)
	}
	defer d.helper.Stop()

	controller, err := startResourcesController(ctx, clientset, d.helper, d.nodeName)
	if err != nil {
		return fmt.Errorf("start driver resources controller: %w", err)
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		defer logger.Info("Controller stopped")
		controller.run(ctx)
	})

	<-ctx.Done()

	err = context.Cause(ctx)
	if _, fatal := errors.AsType[fatalError](err); fatal {
		return err
	}
	return nil
}

// PrepareResourceClaims implements [kubeletplugin.DRAPlugin].
func (d *driver) PrepareResourceClaims(ctx context.Context, claims []*resourcev1.ResourceClaim) (results map[types.UID]kubeletplugin.PrepareResult, err error) {
	results = make(map[types.UID]kubeletplugin.PrepareResult)
	for _, claim := range claims {
		result, err := d.prepareClaim(ctx, claim)
		if err != nil {
			return nil, fmt.Errorf("prepare ResourceClaim %s/%s, UID=%s: %w", claim.Namespace, claim.Name, claim.UID, err)
		}
		results[claim.UID] = result
	}
	return
}

// UnprepareResourceClaims implements [kubeletplugin.DRAPlugin].
func (d *driver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (result map[types.UID]error, err error) {
	result = make(map[types.UID]error)
	for _, claim := range claims {
		claimErr, unprepareErr := d.unprepareClaim(ctx, claim)
		if unprepareErr != nil {
			return nil, fmt.Errorf("unprepare ResourceClaim %s/%s, UID=%s: %w", claim.Namespace, claim.Name, claim.UID, unprepareErr)
		}
		result[claim.UID] = claimErr
	}
	return
}

// HandleError implements [kubeletplugin.DRAPlugin].
func (d *driver) HandleError(ctx context.Context, err error, msg string) {
	utilruntime.HandleErrorWithContext(ctx, err, msg)
	if !errors.Is(err, kubeletplugin.ErrRecoverable) {
		d.cancel(fatalError{err})
	}
}

// prepareClaim prepares a single ResourceClaim.
func (d *driver) prepareClaim(ctx context.Context, claim *resourcev1.ResourceClaim) (result kubeletplugin.PrepareResult, retErr error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Preparing claim", "claim", klog.KObj(claim), "uid", claim.UID)

	checkpoint, err := readCheckpoint(d.checkpointPath, d.checkpointDecoder)
	if err != nil {
		return kubeletplugin.PrepareResult{}, fmt.Errorf("read checkpoint: %w", err)
	}
	defer func() {
		if err := writeCheckpoint(d.checkpointPath, d.checkpointEncoder, checkpoint); err != nil {
			// A failure to write the checkpoint doesn't affect only this claim.
			// None of the claims being prepared now should be considered valid.
			retErr = multierror.Append(retErr, fmt.Errorf("write checkpoint: %w", err))
		}
	}()

	// TODO: avoid repeating any non-idempotent steps if this isn't the first
	// time this claim is being prepared. If work needs to be done, do it here
	// and then update the checkpoint with enough data to enable
	// [driver.unprepareClaim] to fully reset.

	return result, nil

	mergedSpec := new(cdi.Spec)
	var containerEdits *cdi.ContainerEdits

	configs := claim.Status.Allocation.Devices.Config
	slices.SortStableFunc(configs, func(a, b resourcev1.DeviceAllocationConfiguration) int {
		if a.Source != b.Source {
			switch a.Source {
			case resourcev1.AllocationConfigSourceClass:
				return -1
			case resourcev1.AllocationConfigSourceClaim:
				return 1
			}
		}
		return 0
	})
	for _, device := range claim.Status.Allocation.Devices.Results {
		if device.Driver != driverName {
			continue
		}

		for i, config := range configs {
			if config.Opaque == nil ||
				config.Opaque.Driver != driverName {
				// not our config
				continue
			}
			if len(config.Requests) > 0 && !slices.Contains(config.Requests, device.Request) {
				// not for this request
				continue
			}
			spec, err := cdi.ParseSpec(config.Opaque.Parameters.Raw)
			if err != nil {
				logger.Error(err, "Error parsing CDI spec in status.allocation.devices.config["+strconv.Itoa(i)+"]", "claim", klog.KObj(claim), "uid", claim.UID)
				return kubeletplugin.PrepareResult{
					Err: fmt.Errorf("parse CDI spec in status.allocation.devices.config[%d]: %w", i, err),
				}, nil
			}
			if mergedSpec.Spec == nil {
				mergedSpec.Spec = new(cdispecs.Spec)
			}
			if mergedSpec.Kind == "" {
				mergedSpec.Kind = spec.Kind
			}
			if mergedSpec.Version == "" {
				mergedSpec.Version = spec.Version
			}
			mergedSpec.Devices = append(mergedSpec.Devices, spec.Devices...)
			maps.Copy(mergedSpec.Annotations, spec.Annotations)
			containerEdits = containerEdits.Append(&cdi.ContainerEdits{ContainerEdits: &spec.ContainerEdits})
		}

		if containerEdits == nil {
			return result, nil
		}
	}

	mergedSpec.ContainerEdits = *containerEdits.ContainerEdits
	name, err := cdi.GenerateNameForTransientSpec(mergedSpec.Spec, string(claim.UID))
	if err != nil {
		logger.Error(err, "Error generating name for CDI spec", "claim", klog.KObj(claim), "uid", claim.UID)
		return kubeletplugin.PrepareResult{
			Err: err,
		}, nil
	}
	logger.V(5).Info("Writing CDI spec", "cdi", klog.Format(mergedSpec))
	if err := cdi.GetDefaultCache().WriteSpec(mergedSpec.Spec, name); err != nil {
		logger.Error(err, "Error writing CDI spec", "claim", klog.KObj(claim), "uid", claim.UID)
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("write CDI spec: %w", err),
		}, nil
	}

	for _, device := range claim.Status.Allocation.Devices.Results {
		if device.Driver != driverName {
			continue
		}
		cdiVendor, cdiClass := parser.ParseQualifier(mergedSpec.Kind)
		result.Devices = append(result.Devices, kubeletplugin.Device{
			Requests:   []string{device.Request},
			PoolName:   device.Pool,
			DeviceName: device.Device,
			// DRA device name must match CDI device name
			CDIDeviceIDs: []string{parser.QualifiedName(cdiVendor, cdiClass, device.Device)},
			ShareID:      device.ShareID,
		})
	}

	return result, nil
}

// unprepareClaim unprepares a single ResourceClaim. It returns two errors. The
// first indicates a failure unpreparing this individual claim. The second
// indicates a failure that potentially affects other claims.
func (d *driver) unprepareClaim(ctx context.Context, claim kubeletplugin.NamespacedObject) (claimErr error, unprepareErr error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Unpreparing claim", "claim", klog.KRef(claim.Namespace, claim.Name), "uid", claim.UID)

	checkpoint, err := readCheckpoint(d.checkpointPath, d.checkpointDecoder)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	defer func() {
		if err := writeCheckpoint(d.checkpointPath, d.checkpointEncoder, checkpoint); err != nil {
			// A failure to write the checkpoint doesn't affect only this claim.
			// None of the claims being prepared now should be considered valid.
			unprepareErr = multierror.Append(unprepareErr, fmt.Errorf("write checkpoint: %w", err))
		}
	}()

	// TODO: store path in checkpoint
	err = os.Remove(filepath.Join(cdi.DefaultDynamicDir, fmt.Sprintf("%s-%s_%s.yaml", driverName, "configured", claim.UID)))
	if errors.Is(err, fs.ErrNotExist) {
		logger.V(3).Info("CDI spec file does not exist", "claim", claim, "uid", claim.UID)
		err = nil
	}
	if err != nil {
		return fmt.Errorf("remove CDI spec file for claim %s/%s, UID=%s: %w", claim.Namespace, claim.Name, claim.UID, err), nil
	}

	if err := writeCheckpoint(d.checkpointPath, d.checkpointEncoder, checkpoint); err != nil {
		// A failure to write the checkpoint doesn't affect only this claim.
		// None of the claims being unprepared now should be considered fully
		// unprepared.
		return nil, fmt.Errorf("write checkpoint: %w", err)
	}

	return nil, nil
}
