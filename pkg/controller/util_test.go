package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/v8/controller"
)

// newRefGrantList returns a new ReferenceGrant with given attributes
func newRefGrantList(name, fromNamespace, toNamespace, toGroup, toKind string) []*gatewayv1beta1.ReferenceGrant {
	ReferenceGrantList := []*gatewayv1beta1.ReferenceGrant{
		{
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
		},
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
	//unsupportedAPIGrp := "unsupported.group.io"
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
			refGrantList: newRefGrantList("refGrant1", fromNamespace, toNamespace, coreapiGrp, pvcapiKind),
		},
		"Allowed to access dataSource for xns Snapshot with refGrant": {
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, toNamespace, snapapiGrp, snapapiKind),
		},
		"Not allowed to access dataSource for xns PVC without refGrant": {
			expectErr: true,
			volOpts:   generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
		},
		"Not allowed to access dataSource for xns Snapshot with refGrant": {
			expectErr: true,
			volOpts:   generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", snapapiKind, &fromNamespace, &snapapiGrp, requestedBytes, "", true),
		},
		"Not allowed to access dataSource for xns PVC with bad namespace refGrant": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, "badnamespace", coreapiGrp, pvcapiKind),
		},
		"Not allowed to access dataSource for xns PVC with bad apiGroup refGrant": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, toNamespace, "badapi.group.io", pvcapiKind),
		},
		"Not allowed to access dataSource for xns PVC with bad apiKind refGrant": {
			expectErr:    true,
			volOpts:      generatePVCForProvisionFromXnsdataSource(toNamespace, fromsrcName, "", pvcapiKind, &fromNamespace, &coreapiGrp, requestedBytes, "", true),
			refGrantList: newRefGrantList("refGrant1", fromNamespace, toNamespace, coreapiGrp, "BadapiKind"),
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
