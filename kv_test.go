package kv

import (
	"os"
	"sync"
	"testing"

	"github.com/qsymmachus/ranger"
	"github.com/stretchr/testify/assert"
)

const logPath = "./test.log"

func TestSetAndGet(t *testing.T) {
	store, _ := NewStore[int, int]()
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
	store, _ := NewStore[string, string]()
	store.Set("name", "Toby")
	store.Unset("name")
	_, found := store.Get("name")

	assert.False(t, found)
}

// Test that ensures that concurrent updates are handled one-by-one, without using
// a mutex lock, thanks to the singular update queue.
func TestConcurrentUpdates(t *testing.T) {
	store, _ := NewStore[string, int]()
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

func TestWriteAheadLog(t *testing.T) {
	defer os.Remove(logPath)

	first, err := NewStore[string, string](LogPath(logPath))
	assert.NoError(t, err)
	first.Set("a", "a")
	first.Set("b", "b")
	first.Set("c", "c")
	first.Unset("b")

	// Replay the log into a second store:
	second, err := NewStore[string, string](LogPath((logPath)))
	assert.NoError(t, err)
	assert.Equal(t, first.GetAll(), second.GetAll())
	v, ok := second.Get("a")
	assert.Equal(t, "a", v)
	v, ok = second.Get("c")
	assert.Equal(t, "c", v)
	_, ok = second.Get("b")
	assert.False(t, ok)
}

func BenchmarkWithoutLog(b *testing.B) {
	store, _ := NewStore[int, int]()

	for i := 0; i < b.N; i++ {
		testData := ranger.Int(1, 10000)
		for _, n := range testData {
			store.Set(n, n)
			store.Get(n)
		}
	}
}

func BenchmarkWithLog(b *testing.B) {
	defer os.Remove(logPath)

	store, _ := NewStore[int, int](LogPath(logPath))

	for i := 0; i < b.N; i++ {
		testData := ranger.Int(1, 10000)
		for _, n := range testData {
			store.Set(n, n)
			store.Get(n)
		}
	}
}
