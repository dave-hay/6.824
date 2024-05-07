package kvraft

import "sync"

type KVMap struct {
	mu    sync.Mutex
	items map[string]string
}

func makeKVMap() *KVMap {
	return &KVMap{}
}

func (kvm *KVMap) get(key string) string {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	DPrintf("DB get key: %s", key)
	return kvm.items[key]
}

func (kvm *KVMap) put(key string, val string) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	DPrintf("DB put key: %s, val: %s", key, val)
	kvm.items[key] = val
}

func (kvm *KVMap) append(key string, val string) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	DPrintf("DB put key: %s, val: %s", key, val)
	kvm.items[key] += val
}

func (kvm *KVMap) del(key string) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	delete(kvm.items, key)
}

type Resp struct {
	Data chan string
	Err  chan Err
}

type ItemStatus int

const (
	NEW ItemStatus = iota
	PROCESSING
	COMPLETED
)

type ChanMapEntry struct {
	ch     chan bool
	status ItemStatus
}

type KVChanMap struct {
	mu    sync.Mutex
	items map[int64]ChanMapEntry
}

func makeKVChanMap() *KVChanMap {
	return &KVChanMap{items: make(map[int64]ChanMapEntry)}
}

func (kvm *KVChanMap) get(key int64) ChanMapEntry {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	DPrintf("get items[key](%d): %v", key, kvm.items[key])
	return kvm.items[key]
}

func (kvm *KVChanMap) contains(key int64) bool {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	_, ok := kvm.items[key]
	DPrintf("contains key: %d: %t", key, ok)
	return ok
}

func (kvm *KVChanMap) add(key int64) ChanMapEntry {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	DPrintf("adding key: %d", key)
	kvm.items[key] = ChanMapEntry{ch: make(chan bool, 1), status: NEW}
	return kvm.items[key]
}

func (kvm *KVChanMap) del(key int64) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	DPrintf("deleting key: %d", key)
	delete(kvm.items, key)
}

// func (kvm *KVChanMap) setStatus(key int64, status ItemStatus) {
// 	kvm.mu.Lock()
// 	defer kvm.mu.Unlock()
// 	item := kvm.items[key]
// 	item.status = status
// 	kvm.items[key] = item
// }
