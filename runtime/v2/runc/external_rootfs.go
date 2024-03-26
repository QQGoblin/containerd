package runc

import (
	"encoding/json"
	"fmt"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/oci"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
)

const (
	MountConfigName = "mounts.json"
)

// WriteMounts writes the runtime information into the path
func WriteMounts(path string, mounts []mount.Mount) error {

	if len(mounts) == 0 {
		return nil
	}
	b, err := json.Marshal(mounts)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, "mounts.json"), b, 0600)
}

type ExternalRootFSDriver string

const (
	ExternalRootFSPrefix                             = "EXTERNAL_ROOTFS_"
	ExternalRootFSDriverOverlay ExternalRootFSDriver = "overlay"
	ExternalRootFSDriverDevice  ExternalRootFSDriver = "device"
)

type ExternalRootFSConfig struct {
	Driver          ExternalRootFSDriver `json:"EXTERNAL_ROOTFS_DRIVER"`
	OverlayLower    string               `json:"EXTERNAL_ROOTFS_OVERLAY_LOWER_PATH"`
	OverlayUpper    string               `json:"EXTERNAL_ROOTFS_OVERLAY_UPPER_PATH"`
	Device          string               `json:"EXTERNAL_ROOTFS_DEVICE_NAME"`
	DeviceFSType    string               `json:"EXTERNAL_ROOTFS_DEVICE_FSTYPE"`
	DeviceConfig    string               `json:"EXTERNAL_ROOTFS_DEVICE_CONFIG"`
	DeviceMountOpts string               `json:"EXTERNAL_ROOTFS_DEVICE_MOUNT_OPTS"`
}

type ExtDevice struct {
	Device         string `json:"device"`
	FilesystemType string `json:"fs_type"`
}

func ReadExternalRootFSConfigFromENV(configFile string) (*ExternalRootFSConfig, error) {

	spec, err := oci.ReadSpec(configFile)
	if err != nil {
		return nil, err
	}

	configMap := make(map[string]string)
	for _, env := range spec.Process.Env {
		if !strings.HasPrefix(env, ExternalRootFSPrefix) {
			continue
		}
		t := strings.SplitN(env, "=", -1)
		if len(t) < 2 {
			continue
		}
		configMap[t[0]] = t[1]
	}

	strConfig, err := json.Marshal(configMap)
	if err != nil {
		return nil, err
	}
	externalRootFSConfig := &ExternalRootFSConfig{}

	if err := json.Unmarshal(strConfig, externalRootFSConfig); err != nil {
		return nil, err
	}

	return externalRootFSConfig, nil

}

func ParseOverlayOption(m mount.Mount) (string, string, string) {

	var upperdir, lowerdir, workdir string

	for _, option := range m.Options {
		if strings.HasPrefix(option, "upperdir") {
			upperdir = strings.TrimPrefix(option, "upperdir=")
			continue
		}
		if strings.HasPrefix(option, "lowerdir") {
			lowerdir = strings.TrimPrefix(option, "lowerdir=")
			continue
		}
		if strings.HasPrefix(option, "workdir") {
			workdir = strings.TrimPrefix(option, "workdir=")
			continue
		}
	}

	return upperdir, lowerdir, workdir
}

func HookMounts(mounts []mount.Mount, configPath string) ([]mount.Mount, error) {

	extRootfsConfig, err := ReadExternalRootFSConfigFromENV(configPath)
	if err != nil {
		return nil, err
	}

	logrus.WithField("ExtRootfsConfig", extRootfsConfig).Info("Read External Rootfs Config")

	// TODO: 这里判断 containerd 使用 overlay
	if extRootfsConfig.Driver == ExternalRootFSDriverOverlay && (len(mounts) == 1 && mounts[0].Type == "overlay") {

		upperdir, lowdir, workdir := ParseOverlayOption(mounts[0])

		if extRootfsConfig.OverlayLower != "" {
			lowdir = extRootfsConfig.OverlayLower
		}

		if extRootfsConfig.OverlayUpper != "" {
			upperdir = extRootfsConfig.OverlayUpper
			if err = os.MkdirAll(upperdir, 0777); err != nil {
				return nil, err
			}
		}

		newMount := mount.Mount{
			Type:   "overlay",
			Source: "overlay",
			Options: []string{
				fmt.Sprintf("upperdir=%s", upperdir),
				fmt.Sprintf("lowerdir=%s", lowdir),
				fmt.Sprintf("workdir=%s", workdir),
				"index=off",
			},
		}

		return []mount.Mount{newMount}, nil
	}

	if extRootfsConfig.Driver == ExternalRootFSDriverDevice {
		return externalDevice(extRootfsConfig)
	}

	return mounts, nil
}

func externalDevice(c *ExternalRootFSConfig) ([]mount.Mount, error) {

	var (
		fsType   = "xfs"
		device   = ""
		mountOpt = []string{"rw"}
	)

	if c.DeviceConfig != "" {

		ed := ExtDevice{}

		b, err := os.ReadFile(c.DeviceConfig)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &ed); err != nil {
			return nil, err
		}

		if ed.FilesystemType != "" {
			fsType = ed.FilesystemType
		}
		device = ed.Device
	}

	if c.DeviceFSType != "" {
		fsType = c.DeviceFSType
	}

	if c.Device != "" {
		device = c.Device
	}

	if c.DeviceMountOpts != "" {
		mountOpt = strings.Split(c.DeviceMountOpts, ",")
	}

	if device == "" {
		return nil, fmt.Errorf("use external deivce as rootfs, but config is empty")
	}

	// TODO : 这里需要判断 device 设备是否存在

	return []mount.Mount{{
		Type:    fsType,
		Source:  device,
		Options: mountOpt,
	}}, nil

}
