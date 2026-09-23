package keyedmutex

import "sync"

type KeyedMutex struct {
	mut    sync.Mutex
	keyed  map[string]*sync.RWMutex
	writer map[string]int
	reader map[string]int
	inited map[string]bool
}

func NewMutex() *KeyedMutex {
	return &KeyedMutex{
		keyed:  make(map[string]*sync.RWMutex),
		writer: make(map[string]int),
		reader: make(map[string]int),
		inited: make(map[string]bool),
	}
}

func (k *KeyedMutex) Lock(key string) {
	k.mut.Lock()

	m, ok := k.keyed[key]

	if !ok {
		k.keyed[key] = &sync.RWMutex{}
		m = k.keyed[key]
	}

	k.writer[key]++

	k.mut.Unlock()

	m.Lock()
}

func (k *KeyedMutex) RLock(key string) {
	k.mut.Lock()

	m, ok := k.keyed[key]

	if !ok {
		k.keyed[key] = &sync.RWMutex{}
		m = k.keyed[key]
	}

	k.reader[key]++

	k.mut.Unlock()

	m.RLock()
}

func (k *KeyedMutex) Unlock(key string) {
	k.mut.Lock()

	m := k.keyed[key]
	k.writer[key]--

	if !k.inited[key] && k.writer[key] == 0 && k.reader[key] == 0 {
		delete(k.keyed, key)
		delete(k.writer, key)
		delete(k.reader, key)
	}

	k.mut.Unlock()

	m.Unlock()
}

func (k *KeyedMutex) RUnlock(key string) {
	k.mut.Lock()

	m := k.keyed[key]
	k.reader[key]--

	if !k.inited[key] && k.writer[key] == 0 && k.reader[key] == 0 {
		delete(k.keyed, key)
		delete(k.writer, key)
		delete(k.reader, key)
	}

	k.mut.Unlock()

	m.RUnlock()
}

func (k *KeyedMutex) InitKey(key string) {
	k.mut.Lock()

	k.inited[key] = true
	k.keyed[key] = &sync.RWMutex{}

	k.mut.Unlock()
}

func (k *KeyedMutex) ClearKey(key string) {
	k.mut.Lock()

	if k.inited[key] && k.writer[key] == 0 && k.reader[key] == 0 {
		delete(k.keyed, key)
		delete(k.writer, key)
		delete(k.reader, key)
		delete(k.inited, key)
	}

	k.mut.Unlock()
}

type MutexMapShard struct {
	mut    sync.Mutex
	keyed  map[string]*sync.RWMutex
	writer map[string]int
	reader map[string]int
	inited map[string]bool
}

type ShardedKeyedMutex struct {
	shards [32]*MutexMapShard
}

func NewShardedMutex() *ShardedKeyedMutex {
	m := &ShardedKeyedMutex{}

	for i := 0; i < 32; i++ {
		m.shards[i] = &MutexMapShard{
			keyed:  make(map[string]*sync.RWMutex),
			writer: make(map[string]int),
			reader: make(map[string]int),
			inited: make(map[string]bool),
		}
	}

	return m
}

func (k *ShardedKeyedMutex) getShard(key string) *MutexMapShard {
	hash := uint32(2166136261)

	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}

	return k.shards[hash%32]
}

func (k *ShardedKeyedMutex) Lock(key string) {
	shard := k.getShard(key)

	shard.mut.Lock()

	m, ok := shard.keyed[key]

	if !ok {
		shard.keyed[key] = &sync.RWMutex{}
		m = shard.keyed[key]
	}

	shard.writer[key]++

	shard.mut.Unlock()

	m.Lock()
}

func (k *ShardedKeyedMutex) RLock(key string) {
	shard := k.getShard(key)

	shard.mut.Lock()

	m, ok := shard.keyed[key]

	if !ok {
		shard.keyed[key] = &sync.RWMutex{}
		m = shard.keyed[key]
	}

	shard.reader[key]++

	shard.mut.Unlock()

	m.RLock()
}

func (k *ShardedKeyedMutex) Unlock(key string) {
	shard := k.getShard(key)

	shard.mut.Lock()

	m := shard.keyed[key]
	shard.writer[key]--

	if !shard.inited[key] && shard.writer[key] == 0 && shard.reader[key] == 0 {
		delete(shard.keyed, key)
		delete(shard.writer, key)
		delete(shard.reader, key)
	}

	shard.mut.Unlock()

	m.Unlock()
}

func (k *ShardedKeyedMutex) RUnlock(key string) {
	shard := k.getShard(key)

	shard.mut.Lock()

	m := shard.keyed[key]
	shard.reader[key]--

	if !shard.inited[key] && shard.writer[key] == 0 && shard.reader[key] == 0 {
		delete(shard.keyed, key)
		delete(shard.writer, key)
		delete(shard.reader, key)
	}

	shard.mut.Unlock()

	m.RUnlock()
}

func (k *ShardedKeyedMutex) InitKey(key string) {
	shard := k.getShard(key)

	shard.mut.Lock()

	shard.inited[key] = true
	shard.keyed[key] = &sync.RWMutex{}

	shard.mut.Unlock()
}

func (k *ShardedKeyedMutex) ClearKey(key string) {
	shard := k.getShard(key)

	shard.mut.Lock()

	if shard.inited[key] && shard.writer[key] == 0 && shard.reader[key] == 0 {
		delete(shard.keyed, key)
		delete(shard.writer, key)
		delete(shard.reader, key)
		delete(shard.inited, key)
	}

	shard.mut.Unlock()
}

type PooledMutex struct {
	shards [1024]sync.RWMutex
}

func (k *PooledMutex) getShard(key string) *sync.RWMutex {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}

	return &k.shards[hash%1024]
}

func NewPooledMutex() *PooledMutex {
	return &PooledMutex{}
}

func (k *PooledMutex) Lock(key string) {
	k.getShard(key).Lock()
}

func (k *PooledMutex) Unlock(key string) {
	k.getShard(key).Unlock()
}

func (k *PooledMutex) RLock(key string) {
	k.getShard(key).RLock()
}

func (k *PooledMutex) RUnlock(key string) {
	k.getShard(key).RUnlock()
}
