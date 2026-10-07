package main

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
)

type roleReadinessQuery interface {
	Query(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
}
type coordinatorManifestRoleResolver struct{ admin roleReadinessQuery }

func (r coordinatorManifestRoleResolver) ResolveManifestRoles(ctx context.Context, m backlog.Manifest) (map[string]domain.RoleSelection, error) {
	plan, err := campaign.Project(m, campaign.Options{})
	if err != nil {
		return nil, err
	}
	request, err := campaignViabilityRequest(plan, 0, 0, "")
	if err != nil {
		return nil, err
	}
	return queryRoleSelections(ctx, r.admin, request)
}
func queryRoleSelections(ctx context.Context, admin roleReadinessQuery, request backlogadmin.ViabilityRequest) (map[string]domain.RoleSelection, error) {
	if admin == nil {
		return nil, fmt.Errorf("%w: coordinator role readiness unavailable", backlog.ErrValidationUnavailable)
	}
	response, err := admin.Query(ctx, backlogadmin.Query{Version: backlogadmin.ExtendedReadVersion, Kind: backlogadmin.QueryViability, Principal: backlogadmin.Principal{ID: "coordinator", Roles: []string{backlogadmin.LocalAdminRole}}, Viability: &request})
	if err != nil {
		return nil, err
	}
	if response.Viability == nil {
		return nil, fmt.Errorf("%w: coordinator role readiness returned no matrix", backlog.ErrValidationUnavailable)
	}
	selections := map[string]domain.RoleSelection{}
	for _, task := range request.Tasks {
		if task.Role == "" {
			continue
		}
		found := false
		for _, result := range response.Viability.Tasks {
			if result.Task != task.Name {
				continue
			}
			if result.RoleSelection != nil {
				selections[task.Name] = domain.CloneRoleSelection(*result.RoleSelection)
				found = true
			}
			if !found {
				var reasons []string
				for _, reason := range result.Reasons {
					reasons = append(reasons, reason.Code+": "+reason.Detail)
				}
				return nil, fmt.Errorf("role %s task %s unresolved: %s", task.Role, task.Name, strings.Join(reasons, "; "))
			}
			break
		}
		if !found {
			return nil, fmt.Errorf("coordinator does not support role: for task %s; upgrade the coordinator", task.Name)
		}
	}
	return selections, nil
}
