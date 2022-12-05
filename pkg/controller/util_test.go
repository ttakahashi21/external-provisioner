package controller

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-csi/csi-lib-utils/rpc"
	"github.com/kubernetes-csi/external-provisioner/pkg/features"
	"github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned/fake"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	utilfeaturetesting "k8s.io/component-base/featuregate/testing"
	csitrans "k8s.io/csi-translation-lib"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
	fakegateway "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/fake"
	gatewayInformers "sigs.k8s.io/gateway-api/pkg/client/informers/externalversions"
	referenceGrantv1beta1 "sigs.k8s.io/gateway-api/pkg/client/listers/apis/v1beta1"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/v8/controller"
)

// newRefGrant returns a new ReferenceGrant with given attributes
func tempnewRefGrant(name, fromNamespace, toNamespace, toGroup, toKind string) *gatewayv1beta1.ReferenceGrant {
	return &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fromNamespace,
		},
		Spec: gatewayv1beta1.ReferenceGrantSpec{
			From: []gatewayv1beta1.ReferenceGrantFrom{
				{
					Group:     gatewayv1beta1.Group(""),
					Kind:      gatewayv1beta1.Kind("PersistentVolumeClaim"),
					Namespace: gatewayv1beta1.Namespace(toNamespace),
				},
			},
			To: []gatewayv1beta1.ReferenceGrantTo{
				{
					Group: gatewayv1beta1.Group(toGroup),
					Kind:  gatewayv1beta1.Kind(toKind),
				},
			},
		},
	}
}

func TestProvisionFromCrossNamespacedataSource(t *testing.T) {
	var requestedBytes int64 = 1000
	snapapiGrp := "snapshot.storage.k8s.io"
	snapapiKind := "VolumeSnapshot"
	snapName := "test-snapshot"
	fakeSc1 := "fake-sc-1"
	//fakeSc2 := "fake-sc-2"
	//storageClassName := "test-storageclass"
	coreapiGrp := ""
	pvcapiKind := "PersistentVolumeClaim"
	//unsupportedAPIGrp := "unsupported.group.io"
	timeNow := time.Now().UnixNano()
	metaTimeNowUnix := &metav1.Time{
		Time: time.Unix(0, timeNow),
	}
	deletePolicy := v1.PersistentVolumeReclaimDelete
	fromNamespace := "ns1"
	frompvcName := "test-pvc"
	toNamespace := "ns2"

	type pvSpec struct {
		Name          string
		ReclaimPolicy v1.PersistentVolumeReclaimPolicy
		AccessModes   []v1.PersistentVolumeAccessMode
		Capacity      v1.ResourceList
		CSIPVS        *v1.CSIPersistentVolumeSource
	}

	type testcase struct {
		dataSoucetype       string
		volOpts             controller.ProvisionOptions
		snapshotStatusReady bool
		expectErr           bool
		expectCSICall       bool
		expectedPVSpec      *pvSpec
		xnsEnabled          bool
		refGrant            *gatewayv1beta1.ReferenceGrant
		snapNamespace       string
		expectFinalizers    bool
		sourcePVStatusPhase v1.PersistentVolumePhase // set to change source PV Status.Phase, default "Bound"
		cloneUnsupported    bool
		notPopulated        bool
	}
	testcases := map[string]testcase{
		"provision with xns PersitentVolumeClaim data source with refgrant when CrossNamespaceVolumeDataSource feature enabled": {
			dataSoucetype: pvcapiKind,
			volOpts: controller.ProvisionOptions{
				StorageClass: &storagev1.StorageClass{
					ReclaimPolicy: &deletePolicy,
					Parameters:    map[string]string{},
					Provisioner:   "test-driver",
				},
				PVName: "test-name",
				PVC: &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						UID:         "testid",
						Annotations: driverNameAnnotation,
						Namespace:   toNamespace,
					},
					Spec: v1.PersistentVolumeClaimSpec{
						StorageClassName: &fakeSc1,
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceName(v1.ResourceStorage): resource.MustParse(strconv.FormatInt(requestedBytes, 10)),
							},
						},
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
						DataSourceRef: &v1.TypedObjectReference{
							Name:      frompvcName,
							Kind:      pvcapiKind,
							APIGroup:  &coreapiGrp,
							Namespace: &fromNamespace,
						},
					},
				},
			},
			expectFinalizers: true,
			xnsEnabled:       true,
			refGrant:         tempnewRefGrant("refGrant1", fromNamespace, toNamespace, coreapiGrp, pvcapiKind),
			expectedPVSpec: &pvSpec{
				Name:          pvName,
				ReclaimPolicy: v1.PersistentVolumeReclaimDelete,
				AccessModes:   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): bytesToQuantity(requestedBytes),
				},
				CSIPVS: &v1.CSIPersistentVolumeSource{
					Driver:       "test-driver",
					VolumeHandle: "test-volume-id",
					FSType:       "ext4",
					VolumeAttributes: map[string]string{
						"storage.kubernetes.io/csiProvisionerIdentity": "test-provisioner",
					},
				},
			},
		},
		"provision with same ns volume snapshot data source without refgrant when CrossNamespaceVolumeDataSource feature enabled": {
			dataSoucetype: snapapiKind,
			volOpts: controller.ProvisionOptions{
				StorageClass: &storagev1.StorageClass{
					ReclaimPolicy: &deletePolicy,
					Parameters:    map[string]string{},
					Provisioner:   "test-driver",
				},
				PVName: "test-name",
				PVC: &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						UID:         "testid",
						Annotations: driverNameAnnotation,
						Namespace:   fromNamespace,
					},
					Spec: v1.PersistentVolumeClaimSpec{
						StorageClassName: &fakeSc1,
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceName(v1.ResourceStorage): resource.MustParse(strconv.FormatInt(requestedBytes, 10)),
							},
						},
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
						DataSourceRef: &v1.TypedObjectReference{
							Name:      snapName,
							Kind:      snapapiKind,
							APIGroup:  &snapapiGrp,
							Namespace: &fromNamespace,
						},
					},
				},
			},
			snapshotStatusReady: true,
			expectedPVSpec: &pvSpec{
				Name:          "test-testi",
				ReclaimPolicy: v1.PersistentVolumeReclaimDelete,
				AccessModes:   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): bytesToQuantity(requestedBytes),
				},
				CSIPVS: &v1.CSIPersistentVolumeSource{
					Driver:       "test-driver",
					VolumeHandle: "test-volume-id",
					FSType:       "ext4",
					VolumeAttributes: map[string]string{
						"storage.kubernetes.io/csiProvisionerIdentity": "test-provisioner",
					},
				},
			},
			expectCSICall: true,
			xnsEnabled:    true,
			snapNamespace: fromNamespace,
		},
	}

	tmpdir := tempDir(t)
	defer os.RemoveAll(tmpdir)
	mockController, driver, _, controllerServer, csiConn, err := createMockServer(t, tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	defer mockController.Finish()
	defer driver.Stop()

	doit := func(t *testing.T, tc testcase) {
		var clientSet *fakeclientset.Clientset
		var pv *v1.PersistentVolume

		client := &fake.Clientset{}

		var refGrantLister referenceGrantv1beta1.ReferenceGrantLister
		var stopChan chan struct{}
		var gatewayClient *fakegateway.Clientset
		if tc.refGrant != nil {
			gatewayClient = fakegateway.NewSimpleClientset(tc.refGrant)
		} else {
			gatewayClient = fakegateway.NewSimpleClientset()
		}

		if tc.xnsEnabled {
			gatewayFactory := gatewayInformers.NewSharedInformerFactory(gatewayClient, ResyncPeriodOfReferenceGrantInformer)
			referenceGrants := gatewayFactory.Gateway().V1beta1().ReferenceGrants()
			refGrantLister = referenceGrants.Lister()

			stopChan := make(chan struct{})
			gatewayFactory.Start(stopChan)
			gatewayFactory.WaitForCacheSync(stopChan)
		}
		defer func() {
			if stopChan != nil {
				close(stopChan)
			}
		}()

		defer utilfeaturetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.CrossNamespaceVolumeDataSource, tc.xnsEnabled)()

		var pluginCaps rpc.PluginCapabilitySet
		var controllerCaps rpc.ControllerCapabilitySet
		var out *csi.CreateVolumeResponse
		// Create a fake claim as our PVC DataSource
		claim := fakeClaim(frompvcName, fromNamespace, "fake-claim-uid", requestedBytes, pvName, v1.ClaimBound, &fakeSc1, "")

		switch tc.dataSoucetype {
		case snapshotKind:
			client.AddReactor("get", "volumesnapshots", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				namespace := "default"
				if tc.snapNamespace != "" {
					namespace = tc.snapNamespace
				}
				snap := newSnapshot(snapName, namespace, fakeSc1, "snapcontent-snapuid", "snapuid", "claim", tc.snapshotStatusReady, nil, metaTimeNowUnix, resource.NewQuantity(requestedBytes, resource.BinarySI))
				return true, snap, nil
			})

			client.AddReactor("get", "volumesnapshotcontents", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				namespace := "default"
				if tc.snapNamespace != "" {
					namespace = tc.snapNamespace
				}
				content := newContent("snapcontent-snapuid", namespace, fakeSc1, "sid", "pv-uid", "volume", "snapuid", snapName, &requestedBytes, &timeNow)
				return true, content, nil
			})

			pluginCaps, controllerCaps = provisionFromSnapshotCapabilities()

			clientSet = fakeclientset.NewSimpleClientset(claim)

			// Phase: setup responses based on test case parameters
			out = &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					CapacityBytes: requestedBytes,
					VolumeId:      "test-volume-id",
				},
			}

		case pvcKind:

			// Phase: setup fake objects for test
			pvPhase := v1.VolumeBound
			if tc.sourcePVStatusPhase != "" {
				pvPhase = tc.sourcePVStatusPhase
			}
			pv = &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: pvName,
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       driverName,
							VolumeHandle: "test-volume-id",
							FSType:       "ext3",
							VolumeAttributes: map[string]string{
								"storage.kubernetes.io/csiProvisionerIdentity": "test-provisioner",
							},
						},
					},
					ClaimRef: &v1.ObjectReference{
						Kind:      "PersistentVolumeClaim",
						Namespace: fromNamespace,
						Name:      frompvcName,
						UID:       types.UID("fake-claim-uid"),
					},
					StorageClassName: fakeSc1,
				},
				Status: v1.PersistentVolumeStatus{
					Phase: pvPhase,
				},
			}

			// Create a fake claim as our PVC DataSource
			claim := fakeClaim(frompvcName, fromNamespace, "fake-claim-uid", requestedBytes, pvName, v1.ClaimBound, &fakeSc1, "")

			clientSet = fakeclientset.NewSimpleClientset(claim, pv)

			// Phase: setup responses based on test case parameters
			out = &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					CapacityBytes: requestedBytes,
					VolumeId:      "test-volume-id",
				},
			}
			pluginCaps, controllerCaps = provisionFromPVCCapabilities()
			if tc.cloneUnsupported {
				pluginCaps, controllerCaps = provisionCapabilities()
			}
			var volumeSource csi.VolumeContentSource_Volume
			if !tc.expectErr {
				if tc.xnsEnabled {
					if tc.volOpts.PVC.Spec.DataSourceRef != nil {
						volumeSource = csi.VolumeContentSource_Volume{
							Volume: &csi.VolumeContentSource_VolumeSource{
								VolumeId: tc.volOpts.PVC.Spec.DataSourceRef.Name,
							},
						}
					} else if tc.volOpts.PVC.Spec.DataSource != nil {
						volumeSource = csi.VolumeContentSource_Volume{
							Volume: &csi.VolumeContentSource_VolumeSource{
								VolumeId: tc.volOpts.PVC.Spec.DataSource.Name,
							},
						}
					}
				} else {
					volumeSource = csi.VolumeContentSource_Volume{
						Volume: &csi.VolumeContentSource_VolumeSource{
							VolumeId: tc.volOpts.PVC.Spec.DataSource.Name,
						},
					}
				}
				out.Volume.ContentSource = &csi.VolumeContentSource{
					Type: &volumeSource,
				}
				controllerServer.EXPECT().CreateVolume(gomock.Any(), gomock.Any()).Return(out, nil).Times(1)
			}

		}

		_, _, _, claimLister, _, _ := listers(clientSet)

		csiProvisioner := NewCSIProvisioner(clientSet, 5*time.Second, "test-provisioner", "test", 5, csiConn.conn,
			client, driverName, pluginCaps, controllerCaps, "", false, true, csitrans.New(), nil, nil, nil, claimLister, nil, refGrantLister, false, defaultfsType, nil, true, true)

		// Setup mock call expectations.
		// If tc.restoredVolSizeSmall is true, or tc.wrongDataSource is true, or
		// tc.snapshotStatusReady is false,  create volume from snapshot operation will fail
		// early and therefore CreateVolume is not expected to be called.
		// When the following if condition is met, it is a valid create volume from snapshot
		// operation and CreateVolume is expected to be called.
		if tc.expectCSICall {
			if tc.notPopulated {
				out.Volume.ContentSource = nil
				controllerServer.EXPECT().CreateVolume(gomock.Any(), gomock.Any()).Return(out, nil).Times(1)
				controllerServer.EXPECT().DeleteVolume(gomock.Any(), &csi.DeleteVolumeRequest{
					VolumeId: "test-volume-id",
				}).Return(&csi.DeleteVolumeResponse{}, nil).Times(1)
			} else {
				snapshotSource := csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{
						SnapshotId: "sid",
					},
				}
				out.Volume.ContentSource = &csi.VolumeContentSource{
					Type: &snapshotSource,
				}
				controllerServer.EXPECT().CreateVolume(gomock.Any(), gomock.Any()).Return(out, nil).Times(1)
			}
		}

		pv, _, err := csiProvisioner.Provision(context.Background(), tc.volOpts)
		if tc.expectErr && err == nil {
			t.Errorf("Expected error, got none")
		}

		if !tc.expectErr && err != nil {
			t.Errorf("got error: %v", err)
		}

		if tc.volOpts.PVC.Spec.DataSourceRef != nil || tc.volOpts.PVC.Spec.DataSource != nil {
			var claim *v1.PersistentVolumeClaim
			if tc.volOpts.PVC.Spec.DataSourceRef != nil {
				claim, _ = claimLister.PersistentVolumeClaims(tc.volOpts.PVC.Namespace).Get(tc.volOpts.PVC.Spec.DataSourceRef.Name)
			} else if tc.volOpts.PVC.Spec.DataSource != nil {
				claim, _ = claimLister.PersistentVolumeClaims(tc.volOpts.PVC.Namespace).Get(tc.volOpts.PVC.Spec.DataSource.Name)
			}
			if claim != nil {
				set := checkFinalizer(claim, pvcCloneFinalizer)
				if tc.expectFinalizers && !set {
					t.Errorf("Claim %s does not have clone protection finalizer set", claim.Name)
				} else if !tc.expectFinalizers && set {
					t.Errorf("Claim %s should not have clone protection finalizer set", claim.Name)
				}
			}
		}

		if tc.expectedPVSpec != nil {
			if pv != nil {
				if pv.Name != tc.expectedPVSpec.Name {
					t.Errorf("expected PV name: %q, got: %q", tc.expectedPVSpec.Name, pv.Name)
				}

				if !reflect.DeepEqual(pv.Spec.Capacity, tc.expectedPVSpec.Capacity) {
					t.Errorf("expected capacity: %v, got: %v", tc.expectedPVSpec.Capacity, pv.Spec.Capacity)
				}

				if tc.expectedPVSpec.CSIPVS != nil {
					if !reflect.DeepEqual(pv.Spec.PersistentVolumeSource.CSI, tc.expectedPVSpec.CSIPVS) {
						t.Errorf("expected PV: %v, got: %v", tc.expectedPVSpec.CSIPVS, pv.Spec.PersistentVolumeSource.CSI)
					}
				}
			}
		}

	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			doit(t, tc)
		})
	}

}
