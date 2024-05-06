package kvraft

import "sync"

type KVQueue struct {
	mu    sync.Mutex
	items []Op
}

func makeKVQueue() *KVQueue {
	return &KVQueue{}
}

func (kvq *KVQueue) length() int {
	kvq.mu.Lock()
	defer kvq.mu.Unlock()
	return len(kvq.items)
}

func (kvq *KVQueue) append(item Op) {
	kvq.mu.Lock()
	defer kvq.mu.Unlock()
	kvq.items = append(kvq.items, item)
}

func (kvq *KVQueue) pop() {
	kvq.mu.Lock()
	defer kvq.mu.Unlock()
	if kvq.length() == 0 {
		return
	}

	kvq.items = kvq.items[1:]
}

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
