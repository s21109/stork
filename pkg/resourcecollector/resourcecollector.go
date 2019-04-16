package resourcecollector

import (
	"fmt"

	"github.com/heptio/ark/pkg/discovery"
	"github.com/heptio/ark/pkg/util/collections"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// ResourceCollector is used to collect and process unstructured objects in namespaces and using label selectors
type ResourceCollector struct {
	Driver           volume.Driver
	discoveryHelper  discovery.Helper
	dynamicInterface dynamic.Interface
}

// Init initializes the resource collector
func (r *ResourceCollector) Init() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	aeclient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error getting apiextention client, %v", err)
	}

	discoveryClient := aeclient.Discovery()
	r.discoveryHelper, err = discovery.NewHelper(discoveryClient, logrus.New())
	if err != nil {
		return err
	}
	err = r.discoveryHelper.Refresh()
	if err != nil {
		return err
	}
	r.dynamicInterface, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	return nil
}

func resourceToBeCollected(resource metav1.APIResource) bool {
	// Deployment is present in "apps" and "extensions" group, so ignore
	// "extensions"
	if resource.Group == "extensions" && resource.Kind == "Deployment" {
		return false
	}

	switch resource.Kind {
	case "PersistentVolumeClaim",
		"PersistentVolume",
		"Deployment",
		"StatefulSet",
		"ConfigMap",
		"Service",
		"Secret",
		"DaemonSet",
		"ServiceAccount",
		"ClusterRole",
		"ClusterRoleBinding":
		return true
	default:
		return false
	}
}

// GetResources gets all the resources in the given list of namespaces which match the labelSelectors
func (r *ResourceCollector) GetResources(namespaces []string, labelSelectors map[string]string) ([]runtime.Unstructured, error) {
	err := r.discoveryHelper.Refresh()
	if err != nil {
		return nil, err
	}
	allObjects := make([]runtime.Unstructured, 0)

	for _, group := range r.discoveryHelper.Resources() {
		groupVersion, err := schema.ParseGroupVersion(group.GroupVersion)
		if err != nil {
			return nil, err
		}
		if groupVersion.Group == "extensions" {
			continue
		}

		// Map to prevent collection of duplicate objects
		resourceMap := make(map[types.UID]bool)
		for _, resource := range group.APIResources {
			if !resourceToBeCollected(resource) {
				continue
			}

			for _, ns := range namespaces {
				var dynamicClient dynamic.ResourceInterface
				if !resource.Namespaced {
					dynamicClient = r.dynamicInterface.Resource(groupVersion.WithResource(resource.Name))
				} else {
					dynamicClient = r.dynamicInterface.Resource(groupVersion.WithResource(resource.Name)).Namespace(ns)
				}

				var selectors string
				// PVs don't get the labels from their PVCs, so don't use
				// the label selector
				if resource.Kind != "PersistentVolume" {
					selectors = labels.Set(labelSelectors).String()
				}
				objectsList, err := dynamicClient.List(metav1.ListOptions{
					LabelSelector: selectors,
				})
				if err != nil {
					return nil, err
				}
				objects, err := meta.ExtractList(objectsList)
				if err != nil {
					return nil, err
				}
				for _, o := range objects {
					runtimeObject, ok := o.(runtime.Unstructured)
					if !ok {
						return nil, fmt.Errorf("error casting object: %v", o)
					}

					collect, err := r.objectToBeCollected(labelSelectors, resourceMap, runtimeObject, ns)
					if err != nil {
						return nil, fmt.Errorf("error processing object %v: %v", runtimeObject, err)
					}
					if !collect {
						continue
					}
					metadata, err := meta.Accessor(runtimeObject)
					if err != nil {
						return nil, err
					}
					allObjects = append(allObjects, runtimeObject)
					resourceMap[metadata.GetUID()] = true
				}
			}
		}
	}

	err = r.prepareResources(allObjects)
	if err != nil {
		return nil, err
	}
	return allObjects, nil
}

// Returns whether an object should be collected or not for the requested
// namespace
func (r *ResourceCollector) objectToBeCollected(
	labelSelectors map[string]string,
	resourceMap map[types.UID]bool,
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}

	// Skip if we've already processed this object
	if _, ok := resourceMap[metadata.GetUID()]; ok {
		return false, nil
	}

	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return false, err
	}

	switch objectType.GetKind() {
	case "Service":
		// Don't migrate the kubernetes service
		metadata, err := meta.Accessor(object)
		if err != nil {
			return false, err
		}
		if metadata.GetName() == "kubernetes" {
			return false, nil
		}
	case "PersistentVolumeClaim":
		metadata, err := meta.Accessor(object)
		if err != nil {
			return false, err
		}
		pvcName := metadata.GetName()
		pvc, err := k8s.Instance().GetPersistentVolumeClaim(pvcName, namespace)
		if err != nil {
			return false, err
		}
		// Only collect Bound PVCs
		if pvc.Status.Phase != v1.ClaimBound {
			return false, nil
		}

		// Don't collect PVCs not owned by the driver
		if !r.Driver.OwnsPVC(pvc) {
			return false, nil
		}
		return true, nil
	case "PersistentVolume":
		phase, err := collections.GetString(object.UnstructuredContent(), "status.phase")
		if err != nil {
			return false, err
		}
		// Only collect Bound PVs
		if phase != string(v1.ClaimBound) {
			return false, nil
		}

		// Collect only PVs which have a reference to a PVC in the namespace
		// requested
		pvcName, err := collections.GetString(object.UnstructuredContent(), "spec.claimRef.name")
		if err != nil {
			return false, err
		}
		if pvcName == "" {
			return false, nil
		}

		pvcNamespace, err := collections.GetString(object.UnstructuredContent(), "spec.claimRef.namespace")
		if err != nil {
			return false, err
		}
		if pvcNamespace != namespace {
			return false, nil
		}

		pvc, err := k8s.Instance().GetPersistentVolumeClaim(pvcName, pvcNamespace)
		if err != nil {
			return false, err
		}
		// Collect only if the PVC bound to the PV is owned by the driver
		if !r.Driver.OwnsPVC(pvc) {
			return false, nil
		}

		// Also check the labels on the PVC since the PV doesn't inherit the
		// labels
		if len(pvc.Labels) == 0 && len(labelSelectors) > 0 {
			return false, nil
		}

		if !labels.AreLabelsInWhiteList(labels.Set(labelSelectors),
			labels.Set(pvc.Labels)) {
			return false, nil
		}
		return true, nil
	case "ClusterRoleBinding":
		name := metadata.GetName()
		crb, err := k8s.Instance().GetClusterRoleBinding(name)
		if err != nil {
			return false, err
		}
		// Check if there is a subject for the namespace which is requested
		for _, subject := range crb.Subjects {
			if subject.Namespace == namespace {
				return true, nil
			}
		}
		return false, nil
	case "ClusterRole":
		name := metadata.GetName()
		crbs, err := k8s.Instance().ListClusterRoleBindings()
		if err != nil {
			return false, err
		}
		// Find the corresponding ClusterRoleBinding and see if if belongs to
		// the requested namespace
		for _, crb := range crbs.Items {
			if crb.RoleRef.Name == name {
				for _, subject := range crb.Subjects {
					if subject.Namespace == namespace {
						return true, nil
					}
				}
			}
		}
		return false, nil

	case "ServiceAccount":
		// Don't migrate the default service account
		name := metadata.GetName()
		if name == "default" {
			return false, nil
		}
	}

	return true, nil
}

func (r *ResourceCollector) preparePVResource(
	object runtime.Unstructured,
) error {
	spec, err := collections.GetMap(object.UnstructuredContent(), "spec")
	if err != nil {
		return err
	}

	// Delete the claimRef so that the collected resource can be rebound
	delete(spec, "claimRef")

	// Storage class needs to be removed so that it can rebind to an
	// existing PV
	delete(spec, "storageClassName")

	return nil
}

func (r *ResourceCollector) prepareServiceResource(
	object runtime.Unstructured,
) error {
	spec, err := collections.GetMap(object.UnstructuredContent(), "spec")
	if err != nil {
		return err
	}
	// Don't delete clusterIP for headless services
	if ip, err := collections.GetString(spec, "clusterIP"); err == nil && ip != "None" {
		delete(spec, "clusterIP")
	}

	return nil
}

func (r *ResourceCollector) prepareResources(
	objects []runtime.Unstructured,
) error {
	for _, o := range objects {
		content := o.UnstructuredContent()
		// Status shouldn't be retained when collecting resources
		delete(content, "status")

		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}

		switch o.GetObjectKind().GroupVersionKind().Kind {
		case "PersistentVolume":
			err := r.preparePVResource(o)
			if err != nil {
				return fmt.Errorf("error preparing PV resource %v: %v", metadata.GetName(), err)
			}
		case "Service":
			err := r.prepareServiceResource(o)
			if err != nil {
				return fmt.Errorf("error preparing Service resource %v/%v: %v", metadata.GetNamespace(), metadata.GetName(), err)
			}
		}
		metadataMap, err := collections.GetMap(content, "metadata")
		if err != nil {
			return fmt.Errorf("error getting metadata for resource %v: %v", metadata.GetName(), err)
		}
		// Remove all metadata except some well-known ones
		for key := range metadataMap {
			switch key {
			case "name", "namespace", "labels", "annotations":
			default:
				delete(metadataMap, key)
			}
		}
	}
	return nil
}
