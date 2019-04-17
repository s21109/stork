package controllers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	//"github.com/heptio/ark/pkg/util/collections"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	//"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
)

const (
	resyncPeriod                      = 30 * time.Second
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
)

// ApplicationBackupController reconciles applicationbackup objects
type ApplicationBackupController struct {
	Driver               volume.Driver
	Recorder             record.EventRecorder
	ResourceCollector    resourcecollector.ResourceCollector
	backupAdminNamespace string
}

// Init Initialize the application backup controller
func (a *ApplicationBackupController) Init(backupAdminNamespace string) error {
	err := a.createCRD()
	if err != nil {
		return err
	}

	a.backupAdminNamespace = backupAdminNamespace
	if err := a.performRuleRecovery(); err != nil {
		logrus.Errorf("Failed to perform recovery for backup rules: %v", err)
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.ApplicationBackup{}).Name(),
		},
		"",
		resyncPeriod,
		a)
}

func setKind(snap *stork_api.ApplicationBackup) {
	snap.Kind = "ApplicationBackup"
	snap.APIVersion = stork_api.SchemeGroupVersion.String()
}

// performRuleRecovery terminates potential background commands running pods for
// all applicationBackup objects
func (a *ApplicationBackupController) performRuleRecovery() error {
	applicationBackups, err := k8s.Instance().ListApplicationBackups(v1.NamespaceAll)
	if err != nil {
		logrus.Errorf("Failed to list all application backups during rule recovery: %v", err)
		return err
	}

	if applicationBackups == nil {
		return nil
	}

	var lastError error
	for _, applicationBackup := range applicationBackups.Items {
		setKind(&applicationBackup)
		err := rule.PerformRuleRecovery(&applicationBackup)
		if err != nil {
			lastError = err
		}
	}
	return lastError
}

// Handle updates for ApplicationBackup objects
func (a *ApplicationBackupController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *stork_api.ApplicationBackup:
		backup := o
		if event.Deleted {
			return nil
			// TODO
			// return a.cancelBackup(backup)
		}

		// Check whether namespace is allowed to be backed before each stage
		// Restrict backup to only the namespace that the object belongs
		// except for the namespace designated by the admin
		if !a.namespaceBackupAllowed(backup) {
			err := fmt.Errorf("Spec.Namespaces should only contain the current namespace")
			log.ApplicationBackupLog(backup).Errorf(err.Error())
			a.Recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				err.Error())
			return nil
		}

		var terminationChannels []chan bool
		var err error

		switch backup.Status.Stage {
		case stork_api.ApplicationBackupStageInitial:
			// Make sure the namespaces exist
			for _, ns := range backup.Spec.Namespaces {
				_, err := k8s.Instance().GetNamespace(ns)
				if err != nil {
					backup.Status.Status = stork_api.ApplicationBackupStatusFailed
					backup.Status.Stage = stork_api.ApplicationBackupStageFinal
					backup.Status.FinishTimestamp = metav1.Now()
					err = fmt.Errorf("error getting namespace %v: %v", ns, err)
					log.ApplicationBackupLog(backup).Errorf(err.Error())
					a.Recorder.Event(backup,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						err.Error())
					err = sdk.Update(backup)
					if err != nil {
						log.ApplicationBackupLog(backup).Errorf("Error updating")
					}
					return nil
				}
			}
			// Make sure the rules exist if configured
			if backup.Spec.PreExecRule != "" {
				_, err := k8s.Instance().GetRule(backup.Spec.PreExecRule, backup.Namespace)
				if err != nil {
					message := fmt.Sprintf("Error getting PreExecRule %v: %v", backup.Spec.PreExecRule, err)
					log.ApplicationBackupLog(backup).Errorf(message)
					a.Recorder.Event(backup,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						message)
					return nil
				}
			}
			if backup.Spec.PostExecRule != "" {
				_, err := k8s.Instance().GetRule(backup.Spec.PostExecRule, backup.Namespace)
				if err != nil {
					message := fmt.Sprintf("Error getting PostExecRule %v: %v", backup.Spec.PreExecRule, err)
					log.ApplicationBackupLog(backup).Errorf(message)
					a.Recorder.Event(backup,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						message)
					return nil
				}
			}
			fallthrough
		case stork_api.ApplicationBackupStagePreExecRule:
			terminationChannels, err = a.runPreExecRule(backup)
			if err != nil {
				message := fmt.Sprintf("Error running PreExecRule: %v", err)
				log.ApplicationBackupLog(backup).Errorf(message)
				a.Recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					message)
				backup.Status.Stage = stork_api.ApplicationBackupStageInitial
				backup.Status.Status = stork_api.ApplicationBackupStatusInitial
				err := sdk.Update(backup)
				if err != nil {
					return err
				}
				return nil
			}
			fallthrough
		case stork_api.ApplicationBackupStageVolumes:
			err := a.backupVolumes(backup, terminationChannels)
			if err != nil {
				message := fmt.Sprintf("Error backing up volumes: %v", err)
				log.ApplicationBackupLog(backup).Errorf(message)
				a.Recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					message)
				return nil
			}
		case stork_api.ApplicationBackupStageApplications:
			err := a.backupResources(backup)
			if err != nil {
				message := fmt.Sprintf("Error migrating resources: %v", err)
				log.ApplicationBackupLog(backup).Errorf(message)
				a.Recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					message)
				return nil
			}

		case stork_api.ApplicationBackupStageFinal:
			// Do Nothing
			return nil
		default:
			log.ApplicationBackupLog(backup).Errorf("Invalid stage for backup: %v", backup.Status.Stage)
		}
	}
	return nil
}

func (a *ApplicationBackupController) namespaceBackupAllowed(backup *stork_api.ApplicationBackup) bool {
	// Restrict backups to only the namespace that the object belongs
	// except for the namespace designated by the admin
	if backup.Namespace != a.backupAdminNamespace {
		for _, ns := range backup.Spec.Namespaces {
			if ns != backup.Namespace {
				return false
			}
		}
	}
	return true
}

func (a *ApplicationBackupController) backupVolumes(backup *stork_api.ApplicationBackup, terminationChannels []chan bool) error {
	defer func() {
		for _, channel := range terminationChannels {
			channel <- true
		}
	}()

	backup.Status.Stage = stork_api.ApplicationBackupStageVolumes
	if backup.Status.Volumes == nil {
		volumeInfos, err := a.Driver.StartBackup(backup)
		if err != nil {
			message := fmt.Sprintf("Error starting ApplicationBackup for volumes: %v", err)
			log.ApplicationBackupLog(backup).Errorf(message)
			a.Recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				message)
			return nil
		}
		backup.Status.Volumes = volumeInfos
		backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
		err = sdk.Update(backup)
		if err != nil {
			return err
		}

		// Terminate any background rules that were started
		for _, channel := range terminationChannels {
			channel <- true
		}
		terminationChannels = nil

		// Run any post exec rules once backup is triggered
		if backup.Spec.PostExecRule != "" {
			err = a.runPostExecRule(backup)
			if err != nil {
				message := fmt.Sprintf("Error running PostExecRule: %v", err)
				log.ApplicationBackupLog(backup).Errorf(message)
				a.Recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					message)

				// TODO: Cancel the backup and mark it as failed if the postExecRule failed
				err := a.Driver.CancelBackup(backup)
				if err != nil {
					log.ApplicationBackupLog(backup).Errorf("Error cancelling backups: %v", err)
				}
				backup.Status.Stage = stork_api.ApplicationBackupStageFinal
				backup.Status.FinishTimestamp = metav1.Now()
				backup.Status.Status = stork_api.ApplicationBackupStatusFailed
				err = sdk.Update(backup)
				if err != nil {
					return err
				}
				return fmt.Errorf("%v", message)
			}
		}
	}

	inProgress := false
	// Skip checking status if no volumes are being backed up
	if len(backup.Status.Volumes) != 0 {
		var err error
		volumeInfos, err := a.Driver.GetBackupStatus(backup)
		if err != nil {
			return err
		}
		if volumeInfos == nil {
			volumeInfos = make([]*stork_api.ApplicationBackupVolumeInfo, 0)
		}
		backup.Status.Volumes = volumeInfos
		// Store the new status
		err = sdk.Update(backup)
		if err != nil {
			return err
		}

		// Now check if there is any failure or success
		// TODO: On failure of one volume cancel other backups?
		for _, vInfo := range volumeInfos {
			if vInfo.Status == stork_api.ApplicationBackupStatusInProgress {
				log.ApplicationBackupLog(backup).Infof("Volume backup still in progress: %v", vInfo.Volume)
				inProgress = true
			} else if vInfo.Status == stork_api.ApplicationBackupStatusFailed {
				a.Recorder.Event(backup,
					v1.EventTypeWarning,
					string(vInfo.Status),
					fmt.Sprintf("Error migrating volume %v: %v", vInfo.Volume, vInfo.Reason))
				backup.Status.Stage = stork_api.ApplicationBackupStageFinal
				backup.Status.FinishTimestamp = metav1.Now()
				backup.Status.Status = stork_api.ApplicationBackupStatusFailed
			} else if vInfo.Status == stork_api.ApplicationBackupStatusSuccessful {
				a.Recorder.Event(backup,
					v1.EventTypeNormal,
					string(vInfo.Status),
					fmt.Sprintf("Volume %v backed up successfully", vInfo.Volume))
			}
		}
	}

	// Return if we have any volume backups still in progress
	if inProgress {
		return nil
	}

	// If the backup hasn't failed move on to the next stage.
	if backup.Status.Status != stork_api.ApplicationBackupStatusFailed {
		backup.Status.Stage = stork_api.ApplicationBackupStageApplications
		backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
		// Update the current state and then move on to backing up resources
		err := sdk.Update(backup)
		if err != nil {
			return err
		}
		err = a.backupResources(backup)
		if err != nil {
			log.ApplicationBackupLog(backup).Errorf("Error migrating resources: %v", err)
			return err
		}
	}

	err := sdk.Update(backup)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApplicationBackupController) runPreExecRule(backup *stork_api.ApplicationBackup) ([]chan bool, error) {
	if backup.Spec.PreExecRule == "" {
		backup.Status.Stage = stork_api.ApplicationBackupStageVolumes
		backup.Status.Status = stork_api.ApplicationBackupStatusPending
		err := sdk.Update(backup)
		if err != nil {
			return nil, err
		}
		return nil, nil
	} else if backup.Status.Stage == stork_api.ApplicationBackupStageInitial {
		backup.Status.Stage = stork_api.ApplicationBackupStagePreExecRule
		backup.Status.Status = stork_api.ApplicationBackupStatusPending
	}

	if backup.Status.Stage == stork_api.ApplicationBackupStagePreExecRule {
		if backup.Status.Status == stork_api.ApplicationBackupStatusPending {
			backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
			err := sdk.Update(backup)
			if err != nil {
				return nil, err
			}
		} else if backup.Status.Status == stork_api.ApplicationBackupStatusInProgress {
			a.Recorder.Event(backup,
				v1.EventTypeNormal,
				string(stork_api.ApplicationBackupStatusInProgress),
				fmt.Sprintf("Waiting for PreExecRule %v", backup.Spec.PreExecRule))
			return nil, nil
		}
	}
	terminationChannels := make([]chan bool, 0)
	for _, ns := range backup.Spec.Namespaces {
		r, err := k8s.Instance().GetRule(backup.Spec.PreExecRule, ns)
		if err != nil {
			for _, channel := range terminationChannels {
				channel <- true
			}
			return nil, err
		}

		ch, err := rule.ExecuteRule(r, rule.PreExecRule, backup, ns)
		if err != nil {
			for _, channel := range terminationChannels {
				channel <- true
			}
			return nil, fmt.Errorf("error executing PreExecRule for namespace %v: %v", ns, err)
		}
		if ch != nil {
			terminationChannels = append(terminationChannels, ch)
		}
	}
	return terminationChannels, nil
}

func (a *ApplicationBackupController) runPostExecRule(backup *stork_api.ApplicationBackup) error {
	for _, ns := range backup.Spec.Namespaces {
		r, err := k8s.Instance().GetRule(backup.Spec.PostExecRule, ns)
		if err != nil {
			return err
		}

		_, err = rule.ExecuteRule(r, rule.PostExecRule, backup, ns)
		if err != nil {
			return fmt.Errorf("error executing PreExecRule for namespace %v: %v", ns, err)
		}
	}
	return nil
}

/*
func (a *ApplicationBackupController) prepareResources(
	backup *stork_api.ApplicationBackup,
	objects []runtime.Unstructured,
) error {
	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}

		switch o.GetObjectKind().GroupVersionKind().Kind {
		case "PersistentVolume":
			updatedObject, err := a.preparePVResource(o)
			if err != nil {
				return fmt.Errorf("Error preparing PV resource %v: %v", metadata.GetName(), err)
			}
			o = updatedObject
		}
	}
	return nil
}

func (a *ApplicationBackupController) updateResourceStatus(
	backup *stork_api.ApplicationBackup,
	object runtime.Unstructured,
	status stork_api.ApplicationBackupStatusType,
	reason string,
) {
	for _, resource := range backup.Status.Resources {
		metadata, err := meta.Accessor(object)
		if err != nil {
			continue
		}
		gkv := object.GetObjectKind().GroupVersionKind()
		if resource.Name == metadata.GetName() &&
			resource.Namespace == metadata.GetNamespace() &&
			(resource.Group == gkv.Group || (resource.Group == "core" && gkv.Group == "")) &&
			resource.Version == gkv.Version &&
			resource.Kind == gkv.Kind {
			resource.Status = status
			resource.Reason = reason
			eventType := v1.EventTypeNormal
			if status == stork_api.ApplicationBackupStatusFailed {
				eventType = v1.EventTypeWarning
			}
			eventMessage := fmt.Sprintf("%v %v/%v: %v",
				gkv,
				resource.Namespace,
				resource.Name,
				reason)
			a.Recorder.Event(backup, eventType, string(status), eventMessage)
			return
		}
	}
}

func (a *ApplicationBackupController) prepareServiceResource(
	backup *stork_api.ApplicationBackup,
	object runtime.Unstructured,
) (runtime.Unstructured, error) {
	spec, err := collections.GetMap(object.UnstructuredContent(), "spec")
	if err != nil {
		return nil, err
	}
	// Don't delete clusterIP for headless services
	if ip, err := collections.GetString(spec, "clusterIP"); err == nil && ip != "None" {
		delete(spec, "clusterIP")
	}

	return object, nil
}

func (a *ApplicationBackupController) preparePVResource(
	object runtime.Unstructured,
) (runtime.Unstructured, error) {
	return a.Driver.UpdateMigratedPersistentVolumeSpec(object)
}
*/

func (a *ApplicationBackupController) backupResources(
	backup *stork_api.ApplicationBackup,
) error {
	return nil
}

func (a *ApplicationBackupController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.ApplicationBackupResourceName,
		Plural:  stork_api.ApplicationBackupResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationBackup{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
