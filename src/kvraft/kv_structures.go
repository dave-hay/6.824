package kvraft

import "sync"

type KVChanMap struct {
	mu    sync.Mutex
	items map[int64]chan Op
}

func makeKVChanMap() *KVChanMap {
	return &KVChanMap{items: make(map[int64]chan Op)}
}

func (kvm *KVChanMap) get(key int64) chan Op {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	return kvm.items[key]
}

func (kvm *KVChanMap) contains(key int64) bool {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	_, ok := kvm.items[key]
	return ok
}

func (kvm *KVChanMap) add(key int64) chan Op {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	kvm.items[key] = make(chan Op, 1)
	return kvm.items[key]
}
