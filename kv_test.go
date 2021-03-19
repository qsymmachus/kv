package kv

import (
	"sync"
	"testing"

	"github.com/qsymmachus/ranger"
	"github.com/stretchr/testify/assert"
)

func TestSetAndGet(t *testing.T) {
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

func TestUnset(t *testing.T) {
	store := NewStore()
	store.Set("name", "Toby")
	store.Unset("name")
	_, found := store.Get("name")

	assert.False(t, found)
}

// Test that ensures that concurrent updates are handled one-by-one, without using
// a mutex lock, thanks to the singular update queue.
func TestConcurrentUpdates(t *testing.T) {
	store := NewStore()
	testData := ranger.Int(1, 1000)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(v int, wg *sync.WaitGroup) {
			err := store.Set("value", v)
			assert.NoError(t, err)
			wg.Done()
		}(val, &wg)
	}
	wg.Wait()

	_, ok := store.Get("value")
	assert.True(t, ok)
}
