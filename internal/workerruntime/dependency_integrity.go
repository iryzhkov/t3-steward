package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Recovery can observe the published checkout after its sibling input tree has
// disappeared. Never equate an existing checkout with a complete preparation.
func (d *LocalDriver) ensureDependencyIntegrity(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	if d.Config.DryRun || len(pkg.Dependencies) == 0 && len(pkg.CommitBundles) == 0 {
		return nil
	}
	if err := d.verifyDependencyIntegrity(ctx, pkg, workspace); err == nil {
		return nil
	}
	lock, err := lockDependencyRepair(ctx, workspace)
	if err != nil {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
	}
	defer lock.Close()
	// Another caller may have repaired the view while this caller waited.
	if err := d.verifyDependencyIntegrity(ctx, pkg, workspace); err == nil {
		return nil
	}
	// Retry materialization once from checksum-verified custody. Stage the whole
	// tree, so a failed download or commit resolution leaves the old view intact.
	if d.Source == nil {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: errors.New("artifact source is unavailable")}
	}
	for _, input := range pkg.CommitBundles {
		if input.Bundle != nil {
			if _, err := d.cacheArtifact(ctx, *input.Bundle, pkg.Limits.MaxArtifactBytes); err != nil {
				return &backlog.DependencyIntegrityError{Producer: input.TaskID, Artifact: input.Name, Err: err}
			}
		}
	}
	parent := filepath.Dir(workspace)
	metadata := filepath.Join(workspace, ".t3")
	if info, err := os.Lstat(metadata); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: errors.New("metadata directory is not a real directory")}
	}
	stage, err := os.MkdirTemp(parent, ".dependency-repair-")
	if err != nil {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
	}
	defer removeReadOnlyTree(stage)
	for _, dependency := range pkg.Dependencies {
		for _, object := range dependency.Artifacts {
			relative, err := dependencyObjectPath(dependency, object)
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			if _, err = d.cacheArtifact(ctx, object, pkg.Limits.MaxArtifactBytes); err != nil {
				return dependencyFailure(dependency, object, err)
			}
			source, err := openRegular(d.objectPath(object))
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			destination := filepath.Join(stage, relative)
			if err = os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
				source.Close()
				return dependencyFailure(dependency, object, err)
			}
			output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0400)
			if err != nil {
				source.Close()
				return dependencyFailure(dependency, object, err)
			}
			_, copyErr := io.Copy(output, source)
			closeErr := output.Close()
			source.Close()
			if copyErr != nil {
				return dependencyFailure(dependency, object, copyErr)
			}
			if closeErr != nil {
				return dependencyFailure(dependency, object, closeErr)
			}
			if err = d.restoreDependencyCommit(ctx, pkg, dependency, destination, workspace); err != nil {
				return dependencyFailure(dependency, object, err)
			}
		}
	}
	if err := filepath.WalkDir(stage, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0500)
		}
		return nil
	}); err != nil {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
	}
	final := filepath.Join(parent, "dependencies")
	if info, err := os.Lstat(final); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: errors.New("dependency target is not a real directory")}
		}
		// Rename the previous view into our temporary directory; never remove
		// paths obtained by following a damaged or task-edited metadata symlink.
		old := stage + ".old"
		if _, err := os.Lstat(old); !errors.Is(err, os.ErrNotExist) {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: errors.New("repair backup already exists")}
		}
		if err := os.Rename(final, old); err != nil {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
		}
		if err := os.Rename(stage, final); err != nil {
			_ = os.Rename(old, final)
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
		}
		if err := removeReadOnlyTree(old); err != nil {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := os.Rename(stage, final); err != nil {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
		}
	} else {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
	}
	link := filepath.Join(metadata, "dependencies")
	if info, err := os.Lstat(link); err == nil {
		if info.Mode()&os.ModeSymlink == 0 {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: errors.New("dependency view is not a symlink")}
		}
		if err := os.Remove(link); err != nil {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
	}
	if err := os.Symlink("../../dependencies", link); err != nil {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
	}
	return d.verifyDependencyIntegrity(ctx, pkg, workspace)
}

func dependencyObjectPath(dependency workerproto.DependencyInput, object workerproto.ArtifactObject) (string, error) {
	parts := strings.Split(filepath.ToSlash(object.Path), "/")
	if len(parts) < 3 || parts[0] != "dependencies" {
		return "", fmt.Errorf("invalid dependency path %q", object.Path)
	}
	relative := filepath.FromSlash(strings.Join(parts[1:], "/"))
	if !filepath.IsLocal(relative) || strings.Contains(object.Path, "\\") {
		return "", fmt.Errorf("invalid dependency path %q", object.Path)
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid dependency path %q", object.Path)
		}
	}
	return relative, nil
}
func dependencyFailure(dependency workerproto.DependencyInput, object workerproto.ArtifactObject, err error) error {
	parts := strings.SplitN(filepath.ToSlash(object.Path), "/", 3)
	producer, name := dependency.TaskID, object.Path
	if len(parts) == 3 && parts[0] == "dependencies" {
		producer, name = parts[1], parts[2]
	}
	return &backlog.DependencyIntegrityError{Producer: producer, Artifact: name, Err: err}
}

func (d *LocalDriver) verifyDependencyIntegrity(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	for _, directory := range []string{workspace, filepath.Join(workspace, ".t3"), filepath.Join(filepath.Dir(workspace), "dependencies")} {
		if info, err := os.Lstat(directory); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: errors.New("dependency view root is not a real directory")}
		}
	}
	link := filepath.Join(workspace, ".t3", "dependencies")
	resolved, err := filepath.EvalSymlinks(link)
	expected, expectedErr := filepath.EvalSymlinks(filepath.Join(filepath.Dir(workspace), "dependencies"))
	if err != nil || expectedErr != nil || resolved != expected {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: errors.New("dependency directory does not resolve to the prepared view")}
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return &backlog.DependencyIntegrityError{Artifact: ".t3/dependencies", Err: err}
	}
	defer root.Close()
	commits := make(map[string]bool)
	for _, dependency := range pkg.Dependencies {
		for _, object := range dependency.Artifacts {
			relative, err := dependencyObjectPath(dependency, object)
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			info, err := root.Lstat(relative)
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			if !info.Mode().IsRegular() {
				return dependencyFailure(dependency, object, errors.New("input is not a regular file"))
			}
			file, err := root.Open(relative)
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			err = workerproto.VerifyArtifact(file, object, pkg.Limits.MaxArtifactBytes)
			file.Close()
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			// Provenance records are small JSON documents. Large ordinary
			// artifacts need checksum streaming, not a second full allocation.
			if info.Size() > 1<<20 {
				continue
			}
			provenanceFile, err := root.Open(relative)
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			data, err := io.ReadAll(io.LimitReader(provenanceFile, (1<<20)+1))
			provenanceFile.Close()
			if err != nil {
				return dependencyFailure(dependency, object, err)
			}
			if len(data) > 1<<20 {
				continue
			}
			provenance, parseErr := backlog.ParseCommitProvenance(data)
			if parseErr != nil && backlog.LooksLikeCommitProvenance(data) {
				return dependencyFailure(dependency, object, parseErr)
			}
			if parseErr == nil {
				if err := validateDependencyProvenance(pkg, dependency, provenance); err != nil {
					return dependencyFailure(dependency, object, err)
				}
				commits[provenance.Ref] = true
				git := d.Workspace.GitBinary
				if git == "" {
					git = "git"
				}
				raw, err := exec.CommandContext(ctx, git, "-C", workspace, "rev-parse", "--verify", provenance.Ref+"^{commit}").Output()
				if err != nil || strings.TrimSpace(string(raw)) != provenance.Commit {
					return dependencyFailure(dependency, object, errors.New("declared commit is not materialized at its recorded ref"))
				}
			}
		}
	}
	for _, input := range pkg.CommitBundles {
		if !commits[backlog.CampaignRef(input.WorkflowRunID, input.TaskID, input.Name)] && !commits[failedCommitBundleRef(pkg, input)] {
			return &backlog.DependencyIntegrityError{Producer: input.TaskID, Artifact: input.Name, Err: errors.New("declared commit provenance is not materialized")}
		}
	}
	return nil
}

func (d *LocalDriver) restoreDependencyCommit(ctx context.Context, pkg workerproto.ExecutionPackage, dependency workerproto.DependencyInput, path, workspace string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > 1<<20 {
		return nil
	}
	file, err := openRegular(path)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return nil
	}
	provenance, err := backlog.ParseCommitProvenance(data)
	if err != nil {
		if backlog.LooksLikeCommitProvenance(data) {
			return err
		}
		return nil
	}
	if err := validateDependencyProvenance(pkg, dependency, provenance); err != nil {
		return err
	}
	bundles := commitBundleDeliveries(pkg, func(object workerproto.ArtifactObject) (io.ReadCloser, error) {
		return openRegular(d.objectPath(object))
	})
	delivery, hasDelivery := bundles[provenance.Ref]
	var input *backlog.CommitBundleDelivery
	if hasDelivery {
		input = &delivery
	}
	if err := d.Workspace.CampaignRefs.Obtain(ctx, workspace, provenance, input, io.Discard); err != nil {
		return err
	}
	return d.Workspace.CampaignRefs.FetchInto(ctx, workspace, provenance, io.Discard)
}

func validateDependencyProvenance(pkg workerproto.ExecutionPackage, dependency workerproto.DependencyInput, provenance backlog.CommitProvenance) error {
	var source backlog.DependencySource
	if dependency.Provenance != nil {
		source = backlog.DependencySource{WorkflowRunID: dependency.Provenance.RunID, TaskID: dependency.Provenance.TaskID, AttemptID: dependency.Provenance.AttemptID}
	}
	if err := backlog.ValidateFailedCommitSource(provenance, source); err != nil {
		return err
	}
	run, task := pkg.Identity.WorkflowRunID, dependency.TaskID
	if dependency.Provenance != nil {
		run, task = dependency.Provenance.RunID, dependency.Provenance.TaskID
	}
	if provenance.WorkflowRunID != run || provenance.TaskID != task {
		return errors.New("declared commit provenance does not match its dependency source")
	}
	if pkg.Environment.Repository != "" && provenance.Repository != pkg.Environment.Repository {
		return errors.New("declared commit provenance belongs to another repository")
	}
	return nil
}
