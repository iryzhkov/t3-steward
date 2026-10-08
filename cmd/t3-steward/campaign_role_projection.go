package main

import "github.com/iryzhkov/t3-steward/internal/campaign"

func campaignRoleProducers(task campaign.Task) []string {
	var producers []string
	for _, binding := range task.InputsFrom {
		producers = append(producers, binding.Producer)
	}
	return producers
}
