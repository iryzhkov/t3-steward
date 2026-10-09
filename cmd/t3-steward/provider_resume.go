package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
)

// coordinatorProviderResume is the coordinator's maximum for its workers'
// in-session resumes after provider-side errors, from
// backlog_v2.provider_resume.
func coordinatorProviderResume(settings config.BacklogV2) backlog.ProviderResumeLimits {
	resumes, delay := settings.ProviderResume.Maximum()
	return backlog.ProviderResumeLimits{Configured: true, MaxResumes: resumes, MaxDelay: delay}
}
