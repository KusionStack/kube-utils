/*
 * Copyright 2024-2025 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xset

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	clientutil "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/controller/history"
	"kusionstack.io/kube-utils/controller/mixin"
	"kusionstack.io/kube-utils/xset/api"
	"kusionstack.io/kube-utils/xset/api/validation"
	"kusionstack.io/kube-utils/xset/resourcecontexts"
	"kusionstack.io/kube-utils/xset/revisionowner"
	"kusionstack.io/kube-utils/xset/subresources"
	"kusionstack.io/kube-utils/xset/synccontrols"
	"kusionstack.io/kube-utils/xset/xcontrol"
)

type xSetCommonReconciler struct {
	mixin.ReconcilerMixin

	XSetController api.XSetController
	meta           metav1.TypeMeta
	finalizerName  string
	xsetGVK        schema.GroupVersionKind

	// reconcile logic helpers
	cacheExpectations      *expectations.CacheExpectations
	targetControl          xcontrol.TargetControl
	pvcControl             subresources.PvcControl
	syncControl            synccontrols.SyncControl
	revisionManager        history.HistoryManager
	resourceContextControl resourcecontexts.ResourceContextControl
}

func SetUpWithManager(mgr ctrl.Manager, xsetController api.XSetController) error {
	if err := validation.ValidateXSetController(xsetController); err != nil {
		return err
	}
	resourceContextAdapter := resourcecontexts.GetResourceContextAdapter(xsetController)
	if err := validation.ValidateResourceContextAdapter(resourceContextAdapter); err != nil {
		return err
	}

	reconcilerMixin := mixin.NewReconcilerMixin(xsetController.ControllerName(), mgr)
	xsetLabelManager := api.GetXSetLabelAnnotationManager(xsetController)
	xsetMeta := xsetController.XSetMeta()
	xsetGVK := xsetMeta.GroupVersionKind()
	resourceContextMeta := resourceContextAdapter.ResourceContextMeta()
	resourceContextGVK := resourceContextMeta.GroupVersionKind()
	targetMeta := xsetController.XMeta()

	targetControl, err := xcontrol.NewTargetControl(reconcilerMixin, xsetController)
	if err != nil {
		return err
	}
	cacheExpectations := expectations.NewxCacheExpectations(reconcilerMixin.Client, reconcilerMixin.Scheme, clock.RealClock{})
	resourceContextControl := resourcecontexts.NewRealResourceContextControl(reconcilerMixin.Client, xsetController, resourceContextAdapter, resourceContextGVK, cacheExpectations)
	pvcControl, err := subresources.NewRealPvcControl(reconcilerMixin, cacheExpectations, xsetLabelManager, xsetController)
	if err != nil {
		return errors.New("failed to create pvc control")
	}
	syncControl := synccontrols.NewRealSyncControl(reconcilerMixin, xsetController, targetControl, pvcControl, xsetLabelManager, resourceContextControl, cacheExpectations)
	revisionControl := history.NewRevisionControl(reconcilerMixin.Client, reconcilerMixin.Client)
	revisionOwner := revisionowner.NewRevisionOwner(xsetController, targetControl)
	revisionManager := history.NewHistoryManager(revisionControl, revisionOwner)

	reconciler := &xSetCommonReconciler{
		targetControl:          targetControl,
		ReconcilerMixin:        *reconcilerMixin,
		XSetController:         xsetController,
		meta:                   xsetController.XSetMeta(),
		finalizerName:          xsetController.FinalizerName(),
		pvcControl:             pvcControl,
		syncControl:            syncControl,
		revisionManager:        revisionManager,
		resourceContextControl: resourceContextControl,
		cacheExpectations:      cacheExpectations,
		xsetGVK:                xsetGVK,
	}

	c, err := controller.New(xsetController.ControllerName(), mgr, controller.Options{
		MaxConcurrentReconciles: 5,
		Reconciler:              reconciler,
	})
	if err != nil {
		return fmt.Errorf("failed to create controller: %s", err.Error())
	}

	if err := c.Watch(&source.Kind{Type: xsetController.NewXSetObject()}, &handler.EnqueueRequestForObject{}); err != nil {
		return fmt.Errorf("failed to watch %s: %s", xsetController.XSetMeta().Kind, err.Error())
	}

	if err := c.Watch(&source.Kind{Type: xsetController.NewXObject()}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    xsetController.NewXSetObject(),
	}, predicate.Funcs{
		CreateFunc: func(event event.CreateEvent) bool {
			return synccontrols.IsControlledByXSet(xsetLabelManager, event.Object)
		},
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			return synccontrols.IsControlledByXSet(xsetLabelManager, updateEvent.ObjectNew) ||
				synccontrols.IsControlledByXSet(xsetLabelManager, updateEvent.ObjectOld)
		},
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			return synccontrols.IsControlledByXSet(xsetLabelManager, deleteEvent.Object)
		},
		GenericFunc: func(genericEvent event.GenericEvent) bool {
			return synccontrols.IsControlledByXSet(xsetLabelManager, genericEvent.Object)
		},
	}); err != nil {
		return fmt.Errorf("failed to watch %s: %s", targetMeta.Kind, err.Error())
	}

	return nil
}

func (r *xSetCommonReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	kind := r.meta.Kind
	key := req.String()
	ctx = logr.NewContext(ctx, r.Logger.WithValues(kind, key))
	logger := logr.FromContext(ctx)
	instance := r.XSetController.NewXSetObject()
	if err := r.Client.Get(ctx, req.NamespacedName, instance); err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to find object")
			return reconcile.Result{}, err
		}

		logger.Info("object deleted")
		r.cacheExpectations.DeleteExpectations(req.String())
		return ctrl.Result{}, nil
	}

	// if cacheExpectation not fulfilled, shortcut this reconciling till informer cache is updated.
	if satisfied := r.cacheExpectations.SatisfiedExpectations(req.String()); !satisfied {
		logger.Info("not satisfied to reconcile")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if instance.GetDeletionTimestamp() != nil {
		if controllerutil.ContainsFinalizer(instance, r.finalizerName) {
			// reclaim owner IDs in ResourceContextControl
			if err := r.resourceContextControl.UpdateToTargetContext(ctx, instance, nil); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.ensureReclaimTargetsDeletion(ctx, instance); err != nil {
				// reclaim targets deletion before remove finalizers
				return ctrl.Result{}, err
			}
			// reclaim target sub resources before remove finalizers
			if err := r.ensureReclaimTargetSubResources(ctx, instance); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, clientutil.RemoveFinalizerAndUpdate(ctx, r.Client, instance, r.finalizerName)
	}

	if !controllerutil.ContainsFinalizer(instance, r.finalizerName) {
		return ctrl.Result{}, clientutil.AddFinalizerAndUpdate(ctx, r.Client, instance, r.finalizerName)
	}

	currentRevision, updatedRevision, revisions, collisionCount, _, err := r.revisionManager.ConstructRevisions(ctx, instance)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("fail to construct revision for %s %s: %s", kind, key, err.Error())
	}

	xsetStatus := r.XSetController.GetXSetStatus(instance)
	newStatus := xsetStatus.DeepCopy()
	newStatus.UpdatedRevision = updatedRevision.Name
	newStatus.CurrentRevision = currentRevision.Name
	newStatus.CollisionCount = &collisionCount
	syncContext := &synccontrols.SyncContext{
		Revisions:       revisions,
		CurrentRevision: currentRevision,
		UpdatedRevision: updatedRevision,
		NewStatus:       newStatus,
	}

	requeueAfter, syncErr := r.doSync(ctx, instance, syncContext)
	if syncErr != nil {
		logger.Error(syncErr, "failed to sync")
	}

	newStatus = r.syncControl.CalculateStatus(ctx, instance, syncContext)
	// update status anyway
	if err := r.updateStatus(ctx, instance, newStatus); err != nil {
		return requeueResult(requeueAfter), fmt.Errorf("fail to update status of %s %s: %s", kind, req, err.Error())
	}
	return requeueResult(requeueAfter), syncErr
}

func (r *xSetCommonReconciler) doSync(ctx context.Context, instance api.XSetObject, syncContext *synccontrols.SyncContext) (*time.Duration, error) {
	synced, err := r.syncControl.SyncTargets(ctx, instance, syncContext)
	if err != nil || synced {
		return nil, err
	}

	err = r.syncControl.Replace(ctx, instance, syncContext)
	if err != nil {
		return nil, err
	}

	_, scaleRequeueAfter, scaleErr := r.syncControl.Scale(ctx, instance, syncContext)
	_, updateRequeueAfter, updateErr := r.syncControl.Update(ctx, instance, syncContext)
	patcherErr := synccontrols.ApplyTemplatePatcher(ctx, r.XSetController, r.Client, instance, syncContext.TargetWrappers)

	err = errors.Join(scaleErr, updateErr, patcherErr)
	if updateRequeueAfter != nil && (scaleRequeueAfter == nil || *updateRequeueAfter < *scaleRequeueAfter) {
		return updateRequeueAfter, err
	}
	return scaleRequeueAfter, err
}

func (r *xSetCommonReconciler) ensureReclaimTargetSubResources(ctx context.Context, xset api.XSetObject) error {
	if _, enabled := subresources.GetSubresourcePvcAdapter(r.XSetController); enabled {
		err := r.ensureReclaimPvcs(ctx, xset)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *xSetCommonReconciler) ensureReclaimPvcs(ctx context.Context, xset api.XSetObject) error {
	if !r.pvcControl.RetainPvcWhenXSetDeleted(xset) {
		return nil
	}
	var needReclaimPvcs []*corev1.PersistentVolumeClaim
	pvcs, err := r.pvcControl.GetFilteredPvcs(ctx, xset)
	if err != nil {
		return err
	}
	// reclaim pvcs if RetainPvcWhenXSetDeleted
	for i := range pvcs {
		owned := pvcs[i].OwnerReferences != nil && len(pvcs[i].OwnerReferences) > 0
		if owned {
			needReclaimPvcs = append(needReclaimPvcs, pvcs[i])
		}
	}
	for i := range needReclaimPvcs {
		if err := r.pvcControl.OrphanPvc(ctx, xset, needReclaimPvcs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *xSetCommonReconciler) ensureReclaimTargetsDeletion(ctx context.Context, instance api.XSetObject) error {
	xSetSpec := r.XSetController.GetXSetSpec(instance)
	targets, err := r.targetControl.GetFilteredTargets(ctx, xSetSpec.Selector, instance)
	if err != nil {
		return fmt.Errorf("fail to get filtered Targets: %s", err.Error())
	}
	return r.syncControl.BatchDeleteTargetsByLabel(ctx, r.targetControl, targets)
}

func (r *xSetCommonReconciler) updateStatus(ctx context.Context, instance api.XSetObject, status *api.XSetStatus) error {
	r.XSetController.SetXSetStatus(instance, status)
	if err := r.Client.Status().Update(ctx, instance); err != nil {
		return fmt.Errorf("fail to update status of %s: %s", instance.GetName(), err.Error())
	}
	return r.cacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(instance), r.xsetGVK, instance.GetNamespace(), instance.GetName(), instance.GetResourceVersion())
}

func requeueResult(requeueTime *time.Duration) reconcile.Result {
	if requeueTime != nil {
		if *requeueTime == 0 {
			return reconcile.Result{Requeue: true}
		}
		return reconcile.Result{RequeueAfter: *requeueTime}
	}
	return reconcile.Result{}
}
