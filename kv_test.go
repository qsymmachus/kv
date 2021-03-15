package kv

import (
	"sync"
	"testing"

	"github.com/qsymmachus/ranger"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	store := NewStore()
	testData := ranger.Int(1, 100)
	for _, n := range testData {
		store.Set(n, n)
	}

	for _, k := range testData {
		v, found := store.Get(k)
		assert.True(t, found)
		assert.Equal(t, v, k)
	}
}

// Test that ensures that concurrent sets are handled one-by-one, without using
// a mutex lock, thanks to the singular update queue.
func TestConcurrentSets(t *testing.T) {
	store := NewStore()
	testData := ranger.Int(1, 1000)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(v int, wg *sync.WaitGroup) {
			ok := store.Set("value", v)
			assert.True(t, ok)
			wg.Done()
		}(val, &wg)
	}
	wg.Wait()

	_, ok := store.Get("value")
	assert.True(t, ok)
}
