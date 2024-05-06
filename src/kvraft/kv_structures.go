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
	return kvm.items[key]
}

func (kvm *KVMap) put(key string, val string) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	kvm.items[key] = val
}

func (kvm *KVMap) append(key string, val string) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
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
	DNE ItemStatus = iota
	NEW
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
	return kvm.items[key]
}

func (kvm *KVChanMap) add(key int64) ChanMapEntry {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	val, ok := kvm.items[key]
	if ok {
		return val
	}
	kvm.items[key] = ChanMapEntry{ch: make(chan bool, 1), status: NEW}
	return kvm.items[key]
}

func (kvm *KVChanMap) del(key int64) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	kvm.items[key] = ChanMapEntry{status: COMPLETED}
}
