package controller

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

// TODO: This function should eventually be replaced by a common kubernetes library.
func IsGranted(ctx context.Context, claim *v1.PersistentVolumeClaim, referenceGrants []*gatewayv1beta1.ReferenceGrant) (bool, error) {
	var allowed bool
	// Check that accessing to {namespace}/{name} is allowed.
	for _, grant := range referenceGrants {
		var validFrom bool
		for _, from := range grant.Spec.From {
			if from.Group == "" && from.Kind == pvcKind && string(from.Namespace) == claim.Namespace {
				validFrom = true
				break
			}
		}
		// Skip unrelated policy by checking From field
		if !validFrom {
			continue
		}

		for _, to := range grant.Spec.To {
			if (claim.Spec.DataSourceRef.APIGroup != nil && string(to.Group) != *claim.Spec.DataSourceRef.APIGroup) ||
				(claim.Spec.DataSourceRef.APIGroup == nil && len(to.Group) > 0) ||
				string(to.Kind) != claim.Spec.DataSourceRef.Kind {
				continue
			}
			if to.Name == nil || string(*to.Name) == "" || string(*to.Name) == claim.Spec.DataSourceRef.Name {
				allowed = true
				break
			}
		}

		if allowed {
			break
		}
	}

	if !allowed {
		return false, fmt.Errorf("accessing %s/%s of %s dataSource from %s/%s isn't allowed", *claim.Spec.DataSourceRef.Namespace, claim.Spec.DataSourceRef.Name, claim.Spec.DataSourceRef.Kind, claim.Namespace, claim.Name)
	}

	return allowed, nil
}
