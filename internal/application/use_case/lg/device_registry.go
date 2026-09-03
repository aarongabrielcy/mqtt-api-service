package lg_service

import "sync"

type deviceRegistry struct {
	mu      sync.RWMutex
	devices map[string]*ManagedDevice
}

func newDeviceRegistry() *deviceRegistry {
	return &deviceRegistry{
		devices: make(map[string]*ManagedDevice),
	}
}

func (r *deviceRegistry) Get(deviceID string) (*ManagedDevice, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.devices[deviceID]
	return d, ok
}

func (r *deviceRegistry) Set(deviceID string, device *ManagedDevice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.devices[deviceID] = device
}

func (r *deviceRegistry) Delete(deviceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.devices, deviceID)
}

func (r *deviceRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.devices)
}

type deviceEntry struct {
	DeviceID string
	Device   *ManagedDevice
}

func (r *deviceRegistry) Snapshot() []deviceEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]deviceEntry, 0, len(r.devices))
	for id, d := range r.devices {
		entries = append(entries, deviceEntry{DeviceID: id, Device: d})
	}
	return entries
}
