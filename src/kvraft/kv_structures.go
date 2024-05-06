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

type KVChanMap struct {
	mu    sync.Mutex
	items map[int64]chan bool
}

func makeKVChanMap() *KVChanMap {
	return &KVChanMap{}
}

func (kvm *KVChanMap) get(key int64) chan bool {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	return kvm.items[key]
}

func (kvm *KVChanMap) add(key int64) chan bool {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	kvm.items[key] = make(chan bool, 1)
	return kvm.items[key]
}

func (kvm *KVChanMap) del(key int64) {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	delete(kvm.items, key)
}
