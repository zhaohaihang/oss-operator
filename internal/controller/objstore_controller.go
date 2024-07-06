/*
Copyright 2024.

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

package controller

import (
	"context"
	"fmt"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cninfossv1alpha1 "github.com/zhaohaihang/oss-operator/api/v1alpha1"
)

const (
	configMapName = "%s-cm"
	finalizer = "objstore.oss.fjj.com/finalizer"
)
// ObjStoreReconciler reconciles a ObjStore object
type ObjStoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	OssSvc *oss.Client
}

// +kubebuilder:rbac:groups=cninf-oss.oss.fjj.com,resources=objstores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cninf-oss.oss.fjj.com,resources=objstores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cninf-oss.oss.fjj.com,resources=objstores/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete 

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ObjStore object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.2/pkg/reconcile
func (r *ObjStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	objStore := &cninfossv1alpha1.ObjStore{}
	if err := r.Get(ctx, req.NamespacedName, objStore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if objStore.ObjectMeta.DeletionTimestamp.IsZero(){
		if objStore.Status.State == "" {
			objStore.Status.State = cninfossv1alpha1.PENDING_STATE
			if err := r.Status().Update(ctx, objStore); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		
		}
		controllerutil.AddFinalizer(objStore,finalizer)
		if err := r.Update(ctx,objStore);err != nil{
			return ctrl.Result{},  err
		}

		if objStore.Status.State == cninfossv1alpha1.PENDING_STATE {
			if err := r.createResources(ctx,objStore); err != nil{
				objStore.Status.State = cninfossv1alpha1.ERROR_STATE
				r.Status().Update(ctx, objStore)
				return ctrl.Result{},  err
			}
		}

	}else{
		log.Info("删除")
		if err := r.deleteResources(ctx,objStore); err != nil{
			objStore.Status.State = cninfossv1alpha1.ERROR_STATE
			r.Status().Update(ctx, objStore)
			return ctrl.Result{},  err
		}
		controllerutil.RemoveFinalizer(objStore,finalizer)
		if err := r.Update(ctx,objStore);err != nil{
			return ctrl.Result{},  err
		}
		 
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ObjStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cninfossv1alpha1.ObjStore{}).
		Complete(r)
}

func (r *ObjStoreReconciler) createResources(ctx context.Context,objStore *cninfossv1alpha1.ObjStore) error {
	objStore.Status.State = cninfossv1alpha1.CREATING_STATE
	if err := r.Status().Update(ctx,objStore);err != nil {
		return err
	}

	if err := r.OssSvc.CreateBucket(objStore.Spec.Name);err != nil {
		return err
	}

	bucketInfo,err := r.OssSvc.GetBucketInfo(objStore.Spec.Name)
	if err != nil {
		return err
	}

	data := make(map[string]string,0)
	data["bucketName"] = objStore.Spec.Name
	data["endpoint"] = bucketInfo.BucketInfo.ExtranetEndpoint

	configMap := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind: "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf(configMapName,objStore.Name),
			Namespace: objStore.Namespace,
		},
		Data: data,
	}
	if err := r.Create(ctx,configMap);err != nil {
		return err
	}

	objStore.Status.State = cninfossv1alpha1.CREATED_STATE
	if err :=  r.Status().Update(ctx,objStore);err != nil {
		return err
	}

	return nil
}

func (r * ObjStoreReconciler) deleteResources (ctx context.Context,objStore *cninfossv1alpha1.ObjStore) error {
	if err := r.OssSvc.DeleteBucket(objStore.Spec.Name);err != nil {
		return err
	}

	configMap := &v1.ConfigMap{}

	if err := r.Get(ctx ,client.ObjectKey{
		Name: fmt.Sprintf(configMapName,objStore.Name),
		Namespace: objStore.Namespace,
	},configMap); err != nil {
		return err
	}
	
	if err := r.Delete(ctx,configMap); err != nil {
		return err
	}

	return nil 
}