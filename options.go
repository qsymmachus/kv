package kv

// Options for the key/value store.
type optionsData struct {
	// `logPath` points to a write-ahead log to make the store durable. If it is set,
	// the store will read any updates from the log when it starts to restore an
	// initial state. It will write all subsequent updates to the log to provide a
	// durability guarantee.
	logPath string
}

// An option is a function that mutates the state of `optionsData`.
type option func(optsData *optionsData)

// Option that sets a path to a write-ahead log the store will use.
func LogPath(filepath string) option {
	return func(optsData *optionsData) {
		optsData.logPath = filepath
	}
}

// Returns a pointer to `optionsData` that is the result of applying
// a series of options
func applyOptions(options ...option) (optsData *optionsData) {
	optsData = &optionsData{}
	for _, opt := range options {
		opt(optsData)
	}

	return optsData
}
