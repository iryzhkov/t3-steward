//go:build linux

package directoryresource

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"golang.org/x/sys/unix"
)

// Open pins the registered inode and its ancestry using descriptor-relative,
// no-symlink traversal. The returned descriptor is suitable for a bind-fd mount;
// consumers must retain it through mount creation and close it afterwards.
// Unsupported kernels/filesystems fail closed rather than weakening identity.
func Open(r Registration) (*os.File, Identity, error) {
	if err := r.Validate(); err != nil {
		return nil, Identity{}, err
	}
	fd, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, Identity{}, err
	}
	ancestors := []Object{}
	for _, part := range strings.Split(strings.TrimPrefix(r.Path, "/"), "/") {
		parent, _, err := identify(fd)
		if err != nil {
			unix.Close(fd)
			return nil, Identity{}, err
		}
		ancestors = append(ancestors, parent)
		next, err := unix.Openat(fd, part, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if err != nil {
			return nil, Identity{}, fmt.Errorf("open registered directory %q: %w", r.Path, err)
		}
		fd = next
	}
	object, mount, err := identify(fd)
	if err != nil {
		unix.Close(fd)
		return nil, Identity{}, err
	}
	return os.NewFile(uintptr(fd), r.Path), Identity{Registration: r, Object: object, MountID: mount, Ancestors: ancestors}, nil
}

func identify(fd int) (Object, uint64, error) {
	var st unix.Statx_t
	const mask = unix.STATX_INO | unix.STATX_TYPE | unix.STATX_BTIME | unix.STATX_MNT_ID
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW, mask, &st); err != nil {
		return Object{}, 0, fmt.Errorf("inspect directory identity: %w", err)
	}
	if st.Mask&mask != mask || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mnt_id == 0 {
		return Object{}, 0, fmt.Errorf("directory identity requires inode, birth time and mount ID (mask %x)", st.Mask)
	}
	return Object{Device: unix.Mkdev(st.Dev_major, st.Dev_minor), Inode: st.Ino, BirthSeconds: st.Btime.Sec, BirthNanos: st.Btime.Nsec}, st.Mnt_id, nil
}

// Reopen is for preparation/resume. It refuses replacements, mount changes,
// changed ancestry or changed registration. It never retargets a live execution.
func Reopen(expected Identity, r Registration) (*os.File, error) {
	if expected.Registration != r {
		return nil, fmt.Errorf("directory registration changed")
	}
	file, actual, err := Open(r)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(actual, expected) {
		file.Close()
		return nil, fmt.Errorf("directory identity changed: %s", r.ResourceID)
	}
	return file, nil
}
