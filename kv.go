package kv

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Underlying implementation of the key/value store.
//
// `data` is our backing key/value map.
//
// `updates` is a singular update queue, implemented as a channel, that receives
// update messages and applies them to the store.
//
// `log` is a write-ahead log where the store writes all updates so they can be
// replayed, providing durability between restarts.
type kvStore struct {
	data    map[interface{}]interface{}
	updates chan (update)
	log     *os.File
	options *optionsData
}

// Enum of all types of updates to the store.
type updateType uint8

const (
	set   updateType = 0
	unset updateType = 1
)

// Request to update the state of the store.
type update struct {
	UpdateType updateType
	Key        interface{}
	Value      interface{}
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
func NewStore(options ...option) (*kvStore, error) {
	store := kvStore{
		data:    make(map[interface{}]interface{}),
		updates: make(chan (update)),
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

// Gets a value from the store using the provided key. If there is no matching
// key in the store, `found` will be false.
func (s *kvStore) Get(key interface{}) (value interface{}, found bool) {
	value, found = s.data[key]
	return value, found
}

// Sets a key/value pair in the store. Returns an error if it failed.
func (s *kvStore) Set(key interface{}, value interface{}) error {
	append := s.log != nil
	return s.queueUpdate(update{0, key, value, append, make(chan (updateResult))})
}

// Unsets a key/value pair in the store. REturns an error if it failed.
func (s *kvStore) Unset(key interface{}) error {
	append := s.log != nil
	return s.queueUpdate(update{1, key, nil, append, make(chan (updateResult))})
}

// Sends an update to the `updates` channel and waits for the result.
func (s *kvStore) queueUpdate(u update) error {
	s.updates <- u
	result := <-u.result

	if !result.ok && result.err != nil {
		return result.err
	}

	return nil
}

// Appends an update to the write-ahead log.
func (s *kvStore) appendUpdate(u update) error {
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
func (s *kvStore) replayUpdatesFromLog() error {
	if s.log == nil {
		return errors.New("Cannot replay updates, store has no log")
	}

	scanner := bufio.NewScanner(s.log)
	result := make(chan (updateResult))
	for scanner.Scan() {
		update := update{}
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
func (s *kvStore) readUpdates() {
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
