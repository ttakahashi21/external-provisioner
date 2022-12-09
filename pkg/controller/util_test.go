package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/v8/controller"
)

// newRefGrantList returns a new ReferenceGrant with given attributes
func newRefGrantList(name, fromNamespace, fromGrop, fromKind, toNamespace, toGroup, toKind, toName string, settoName bool) []*gatewayv1beta1.ReferenceGrant {
	ReferenceGrantList := []*gatewayv1beta1.ReferenceGrant{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: fromNamespace,
			},
			Spec: gatewayv1beta1.ReferenceGrantSpec{
				From: []gatewayv1beta1.ReferenceGrantFrom{
					{
						Group:     gatewayv1beta1.Group(fromGrop),
						Kind:      gatewayv1beta1.Kind(fromKind),
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
		},
	}

	if settoName {
		objectName := gatewayv1beta1.ObjectName(toName)
		ReferenceGrantList[0].Spec.To[0].Name = &objectName
	}

	return ReferenceGrantList
}

func TestIsGranted(t *testing.T) {
	ctx := context.Background()
	var requestedBytes int64 = 1000
	snapapiGrp := "snapshot.storage.k8s.io"
	snapapiKind := "VolumeSnapshot"
	coreapiGrp := ""
	pvcapiKind := "PersistentVolumeClaim"
	anyvolumedatasourceapiGrp := "hello.k8s.io/v1alpha1"
	anyvolumedatasourceapiKind := "Hello"
	fromNamespace := "ns1"
	fromsrcName := "test-dataSource"
	toNamespace := "ns2"

	type testcase struct {
		expectErr    bool
		volOpts      controller.ProvisionOptions
		refGrantList []*gatewayv1beta1.ReferenceGrant
	}
	testcases := map[string]testcase{
		"Allowed to access dataSource for xns PVC with refGrant": {
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, coreapiGrp, pvcapiKind, "", false),
		},
		"Allowed to access dataSource for xns PVC with refGrant of specify toName": {
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, coreapiGrp, pvcapiKind, fromsrcName, true),
		},
		"Allowed to access dataSource for xns PVC with refGrant of specify toName of non": {
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, coreapiGrp, pvcapiKind, "", true),
		},
		"Allowed to access dataSource for xns Snapshot with refGrant": {
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, snapapiGrp, snapapiKind, "", false),
		},
		"Allowed to access dataSource for xns AnyVolumeDataSource with refGrant": {
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", anyvolumedatasourceapiKind, &fromNamespace, &anyvolumedatasourceapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, anyvolumedatasourceapiGrp, anyvolumedatasourceapiKind, "", false),
		},
		"Not allowed to access dataSource for xns PVC without refGrant": {
			expectErr: true,
			volOpts:   generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
		},
		"Not allowed to access dataSource for xns Snapshot without refGrant": {
			expectErr: true,
			volOpts:   generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
		},
		"Not allowed to access dataSource for xns AnyVolumeDataSource without refGrant": {
			expectErr: true,
			volOpts:   generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", anyvolumedatasourceapiKind, &fromNamespace, &anyvolumedatasourceapiGrp, requestedBytes, "", true),
		},
		"Not allowed to access dataSource for xns PVC with refGrant of wrong create resource": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", toNamespace, coreapiGrp, pvcapiKind, fromNamespace, coreapiGrp, pvcapiKind, "", false),
		},
		"Not allowed to access dataSource for xns PVC with refGrant of wrong namespace reference": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, "badnamespace", coreapiGrp, pvcapiKind, "", false),
		},
		"Not allowed to access dataSource for xns PVC with refGrant of wrong apiGroup": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, "badapi.group.io", pvcapiKind, "", false),
		},
		"Not allowed to access dataSource for xns PVC with refGrant of wrong apiKind": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, coreapiGrp, "BadapiKind", "", false),
		},
		"Not allowed to access dataSource for xns PVC with refGrant of wrong toName": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, coreapiGrp, coreapiGrp, "bad-datasource", true),
		},
		"Not allowed to access PVC dataSource for xns PVC with refGrant of SnapShot dataSource": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, snapapiGrp, snapapiKind, "", false),
		},
		"Not allowed to access Snapshot dataSource for xns PVC with refGrant of PVC dataSource": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, coreapiGrp, pvcapiKind, "", false),
		},
		"Not allowed to access except PersistentVolumeClaim kind": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, "Example", toNamespace, coreapiGrp, pvcapiKind, "", false),
		},
		"Not allowed to access except coreapiGroup": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, "example.k8s.io", pvcapiKind, toNamespace, coreapiGrp, pvcapiKind, "", false),
		},
		"Not allowed to access except PersistentVolumeClaim kind and coreapiGroup": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, "example.k8s.io", "Example", toNamespace, coreapiGrp, pvcapiKind, "", false),
		},
		"Not allowed to access dataSource for xns PVC with refGrant of nil toGroup": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, coreapiGrp, pvcapiKind, toNamespace, "", "", "", false),
		},
	}

	doit := func(t *testing.T, tc testcase) {
		allowed, err := IsGranted(ctx, tc.volOpts.PVC, tc.refGrantList)

		if tc.expectErr && (err == nil || allowed) {
			t.Errorf("Expected error, got none")
		}
		if !tc.expectErr && (err != nil || !allowed) {
			t.Errorf("got error: %v", err)
		}
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			doit(t, tc)
		})
	}
}
