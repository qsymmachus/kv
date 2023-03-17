package kv

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// A key/value store that stores its data in-memory, and optionally in a file.
// When storing to a file, its data will be durable between restarts.
type KVStore[K comparable, V any] interface {
	// Gets a value from the store using the provided key. If there is no matching
	// key in the store, `found` will be false.
	Get(key K) (value V, found bool)

	// Sets a key/value pair in the store. Returns an error if it failed.
	Set(key K, value V) error

	// Unsets a key/value pair in the store. REturns an error if it failed.
	Unset(key K) error

	// Gets all data in the store as a map.
	GetAll() map[K]V
}

// Underlying implementation of the key/value store.
type kvStore[K comparable, V any] struct {
	// The backing key/value map.
	data map[K]V
	// A singular update queue, implemented as a channel, that receives
	// update messages and applies them to the store.
	updates chan (update[K, V])
	// `log` is a write-ahead log where the store writes all updates so they can be
	// replayed, providing durability between restarts.
	log *os.File
	// Options for the store.
	options *optionsData
}

// Enum of all types of updates to the store.
type updateType uint8

const (
	set   updateType = 0
	unset updateType = 1
)

// Request to update the state of the store.
type update[K comparable, V any] struct {
	UpdateType updateType
	Key        K
	Value      V
	append     bool
	result     chan (updateResult)
}

// The result of an update operation.
type updateResult struct {
	ok  bool
	err error
}

// Instantiates an empty store and starts a goroutine to read
// messages sent to the `updates` queue.
func NewStore[K comparable, V any](options ...option) (KVStore[K, V], error) {
	store := kvStore[K, V]{
		data:    make(map[K]V),
		updates: make(chan (update[K, V])),
		options: applyOptions(options...),
	}

	// Start receiving updates:
	go store.readUpdates()

	// If a path to a write-ahead log was specified, replay it:
	if store.options.logPath != "" {
		log, err := os.OpenFile(store.options.logPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}

		store.log = log
		if err := store.replayUpdatesFromLog(); err != nil {
			return nil, err
		}
	}

	return &store, nil
}

func (s *kvStore[K, V]) Get(key K) (value V, found bool) {
	value, found = s.data[key]
	return value, found
}

func (s *kvStore[K, V]) Set(key K, value V) error {
	append := s.log != nil
	return s.queueUpdate(update[K, V]{0, key, value, append, make(chan (updateResult))})
}

func (s *kvStore[K, V]) Unset(key K) error {
	append := s.log != nil
	var zeroValue V
	return s.queueUpdate(update[K, V]{1, key, zeroValue, append, make(chan (updateResult))})
}

func (s *kvStore[K, V]) GetAll() map[K]V {
	return s.data
}

// Sends an update to the `updates` channel and waits for the result.
func (s *kvStore[K, V]) queueUpdate(u update[K, V]) error {
	s.updates <- u
	result := <-u.result

	if !result.ok && result.err != nil {
		return result.err
	}

	return nil
}

// Appends an update to the write-ahead log.
func (s *kvStore[K, V]) appendUpdate(u update[K, V]) error {
	if s.log == nil {
		return errors.New("Failed to append update, store has no log")
	}

	json, err := json.Marshal(u)
	if err != nil {
		return errors.New("Failed to marshal update into JSON for the log")
	}

	if _, err := s.log.WriteString(fmt.Sprintf("%s\n", string(json))); err != nil {
		return err
	}

	return nil
}

// "Replays" the store's write-ahead log by reading update data from the log and
// sending updates to the `updates` queue.
func (s *kvStore[K, V]) replayUpdatesFromLog() error {
	if s.log == nil {
		return errors.New("Cannot replay updates, store has no log")
	}

	scanner := bufio.NewScanner(s.log)
	result := make(chan (updateResult))
	for scanner.Scan() {
		update := update[K, V]{}
		updateJson := scanner.Text()
		if err := json.Unmarshal([]byte(updateJson), &update); err != nil {
			return err
		}

		update.result = result
		s.queueUpdate(update)
	}

	return nil
}

// Reads updates from the store's singular update queue. This ensures that only
// one update is processed at a time, in the order they're received.
func (s *kvStore[K, V]) readUpdates() {
	for update := range s.updates {
		if update.append {
			err := s.appendUpdate(update)
			if err != nil {
				update.result <- updateResult{false, err}
				continue
			}
		}

		switch update.UpdateType {
		case set:
			s.data[update.Key] = update.Value
			update.result <- updateResult{true, nil}
		case unset:
			delete(s.data, update.Key)
			update.result <- updateResult{true, nil}
		default:
			err := fmt.Errorf("Unknown update type %d", update.UpdateType)
			update.result <- updateResult{false, err}
		}
	}
}
