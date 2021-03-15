package kv

// Underlying implementation of the key/value store.
//
// `data` is our backing key/value map. `updates` is a singular update queue,
// implemented as a channel, that receives update messages and applies them
// to the store.
type kvStore struct {
	data    map[interface{}]interface{}
	updates chan (update)
}

// Enum of all types of updates to the store.
type updateType uint8

const (
	set updateType = 0
)

// Message to update the state of the store.
type update struct {
	updateType updateType
	key        interface{}
	value      interface{}
	ok         chan (bool)
}

// Instantiates an empty store and starts a goroutine to read
// messages sent to the `updates` queue.
func NewStore() *kvStore {
	store := kvStore{
		data:    make(map[interface{}]interface{}),
		updates: make(chan (update)),
	}

	go store.readUpdates()

	return &store
}

// Gets a value from the store using the provided key. If there is no matching
// key in the store, `found` will be false.
func (s *kvStore) Get(key interface{}) (value interface{}, found bool) {
	value, found = s.data[key]
	return value, found
}

// Sets a key/value pair in the store. Returns true if the update succeeded,
// false otherwise.
func (s *kvStore) Set(key interface{}, value interface{}) (ok bool) {
	update := update{0, key, value, make(chan (bool))}
	s.updates <- update
	ok = <-update.ok
	return ok
}

// Reads updates from the store's singular update queue. This ensures that only
// one update is processed at a time, in the order they're received.
func (s *kvStore) readUpdates() {
	for update := range s.updates {
		switch update.updateType {
		case set:
			s.data[update.key] = update.value
			update.ok <- true
		default:
			update.ok <- false
		}
	}
}
