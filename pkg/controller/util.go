package controller

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// TODO: This function should eventually be replaced by a common kubernetes library.
func (p *csiProvisioner) IsGranted(ctx context.Context, claim *v1.PersistentVolumeClaim) (bool, error) {
	// Get all ReferenceGrants in data source's namespace
	referenceGrants, err := p.referenceGrantLister.ReferenceGrants(*claim.Spec.DataSourceRef.Namespace).List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("error getting ReferenceGrants in %s namespace from api server: %v", *claim.Spec.DataSourceRef.Namespace, err)
	}

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
		return allowed, nil
	}

	return true, nil
}
