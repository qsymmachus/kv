kv
==

I use this project to practice implementing some distributed programming patterns. 

`KVStore` is an in-memory key/value store. All updates are handled by a singular update queue, guaranteeing that only one write will be applied at a time. This means updates are thread-safe, and can be made in concurrent goroutines. Updates are processed in the order they're received.

The store can optionally copy all its updates to a file as a write-ahead log. You can replay the log the next time you start the store, providing data durability between restarts.

Usage
-----

When you instantiate a store, you can provide an optional `LogPath` if you want to save all data to a file. If you omit a `LogPath`, data will only be stored in memory.

```go
import "github.com/qsymmachus/kv"

store, _ := kv.NewStore[string, string](kv.LogPath("./kv.log"))
```

The key and value types are specified as type parameters. To store mixed data types, use `NewStore[any, any]`.

To set and get values:

```go
err := store.Set("name", "ralph")

v, found := store.get("name") // => "ralph", true
v, found = store.get("favorite food") // => "", false
```

To delete a value:

```go
err := store.Unset("name")
```

You can retrieve all data from the store as a `map[K]V`:

```go
allData := store.GetAll()
```

Development
-----------

To run all unit tests:

```sh
go test -v .
```

To run benchmarks:

```sh
go test -bench .
```
