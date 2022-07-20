# `hoard`

`hoard` is an interface for key-value store abstractions that allows applications to be agnostic to storage backends.
The current version of `hoard` supports the following backends: memory, filesystems, redis, and AWS S3.

## Quickstart

Use a `hoard` like a python dictionary.
In the following example, assume `h` is a hoard of some kind (see next section for hoard types).

```python
# store an object in the hoard
h['hoard_key'] = 'hoard_value'

# access an object stored in the hoard
assert h['hoard_key'] == 'hoard_value'

# Delete an object from the hoard
del h['hoard_key']

# Iterate over keys, values, or items
list(h.keys())
list(h.values())
list(h.items())
```

## Hoard types

The data management principles of the storage system underlying each of the hoard types would apply to the choice of hoard type to use.

### Dictionary (`hoard.DictHoard`)
In-memory storage. NOT PERSISTENT. Only use this for testing or caching (see caching below).

### Filesystem (`hoard.FSHoard`)

Stores data on a filesystem.

#### Creation
```python
FSHoard.new(path, compression=None, remove_existing=False, serializer='pickle)
```
*Parameters*
- `path` - root path to storage directory
- `compression` - `None` or `'gzip'`. Compression method
- `remove_existing` - if `True` and a directory already exists at `path`,
the existing directory will be removed and the hoard will be initialized.
Otherwise, an exception will be raised.
- `serializer` - serialization method (see [**Serialization**](#serialization))

Upon creation, a `config.yaml` file will be created in the root directory (`path`).

#### Usage
```python
FSHoard(path, partition=None)
```
*Parameters*
- `path` - as above
- `partition` - the `FSHoard` supports multiple partitions, separating data into separate subdirectories (`data.PARTITION`) at the top level. All partitions of the same hoard share one config.
The `None` partition points to a directory `data` (without suffix).


#### Hashed storage pattern (`hoard.HashedFSHoard`)
If you use a filesystem that does not scale well with a large number of files/subdirectories in a single directory,
use the `HashedFSHoard` which organizes files into a hierarchy of `depth` levels of subdirectories
with a maximum of 100 subdirectories per node.
Files are placed into and accessed from the leaf subdirectories based on the `sha1` hash of their hoard keys.

### Redis (`hoard.RedisHoard`)

Stores data in a redis hash.

#### Creation
```python
RedisHoard.new(redis_key, redis_kwargs={}, remove_existing=False, serializer='pickle')
```
*Parameters*
- `redis_key` - key to the redis hash.
- `redis_kwargs` - arguments to the `Redis` client [constructor](https://github.com/redis/redis-py/blob/4b0543d567aef36ac467ce495d831a24575d8d5b/redis/client.py#L900)
- `remove_existing` - if `True` and `redis_key` already exists, the existing redis key will be deleted and the key initialized.
Otherwise, an exception will be raised.
- `serializer` - serialization method (see [**Serialization**](#serialization))

#### Usage
```python
RedisHoard(redis_key, redis_kwargs)
```
`redis_key`, `redis_kwargs` parameters as above in `RedisHoard.new`

#### Least-recently-used redis hoard (`hoard.LRURedisHoard`)

#### Creation
```python
LRURedisHoard.new(redis_key, maxsize, redis_kwargs={}, remove_existing=False, serializer='pickle')
```
A redis hoard with a limited number of items (suitable for caching).

*Parameters*
- `maxsize` - maximum number of items in this hoard
- others - as above in `RedisHoard`


### AWS S3 (`hoard.S3Hoard`)

AWS S3 storage backend.
Requires an existing s3 bucket.
Application must be running under an AWS identity permissioned for the appropriate S3 resource access.

#### Usage
```python
S3Hoard(bucket_name, partition='root')
```
*Parameters*
- `bucket_name` - S3 bucket
- `parittion` - a prefix added to the hoard key to create the S3 object key

## Serialization

See `hoard.serialize`. The following serialization methods are supported:

### `pickle`
- Uses python's pickle serialization and supports any object that can be pickled.
Hoards using this would obviously be limited to python applcations.

### `bytes`
- Expects a bytes object or byte stream as stored object. Stores data in binary format.

### `text`
- Expects a string/text stream. Stores data as text.

### `json`
- Expects a JSON serializable object. Stores data as text.

## Caching

A simple caching mechanism enables a hoard to act as a cache for another hoard.

### Usage

```python
CachedHoard(base, cache=None)
```
*Parameters*
- `base` - the base hoard
- `cache` - the cache hoard

### Caching caveats
The cache is unaware of changes to the base hoard, and therefore caching should only be implemented when the base hoard is guaranteed to remain unchanged.
The only exception to this is when the **base hoard is modified through the cache** (and no other writers are modifying the base hoard).
This issue of stale caches may be improved in future versions of `hoard`.


## Composite hoards

Two or more hoards can be unified in two ways: `CompositeHoard` and `HoardSet`

### `CompositeHoard`

The `CompositeHoard` is an ordered collection of hoards.
Key lookups are performed on the child hoards in their order, and the first hoard containing the key will be accessed for the value.

#### Usage
```python
CompositeHoard(hoards, write_idx=None)
```
*Parameters*
- `hoards` - ordered list of hoards
- `write_idx` - if `None`, the `CompositeHoard` is read-only. Otherwise, specifies the index of the child hoard to pass a `__setitem__` to.


### `HoardSet`

A mapping into one or more child hoards. Keys of the `HoardSet` are a 2-tuple of `(CHILD_HOARD, KEY_IN_CHILD_HOARD)`.

#### Usage
```python
HoardSet(hoards)
```
*Parameters*
- `hoard` - a dictionary of hoards

## Read-only hoard (`hoard.ReadOnlyHoard`)

Wraps a hoard, exposing it as a hoard with writes and deletes disabled.

```python
ReadOnlyHoard(base)
```
*Parameters*
- `base` the hoard to write-protect

## `HoardView`

Wraps a hoard and exposes it with keys remapped.
Can be used for conditional access of data or creating a subset of a hoard.

```python
HoardView(hoard, remap)
```
*Parameters*
- `hoard` the hoard to wrap
- `remap` a callable that maps the keys of the `HoardView` to the corresponding key of the underlying hoard.


## Remote hoard RPC (`RemoteHoard`)

Hoards can be exposed on a (trusted) network using the `RemoteHoardServer`.
The `RemoteHoard` client is a proxy for the hosted hoard and can be used identically.
This is an experimental feature.

### Server (`hoard.RemoteHoardServer`)
Runs in a thread. Exposes multiple hoards.
#### Usage
```python
RemoteHoardServer(hoards, host='0.0.0.0', port=DEFAULT_PORT)
```
*Parameters*
- `hoard` - dictionary of hoards to host, indexed by keys
- `host`, `port` - [`XMLRPCServer` parameters](https://docs.python.org/3/library/xmlrpc.html)


### Client

#### Usage

```python
RemoteHoard(hoard, host='localhost', port=DEFAULT_PORT)
```
*Parameters*
- `hoard` - the name (key) of the hoard among the hoards hosted by the sever
- `host`, `port` - host and port the remote hoard server is listening on

## Other languages
With the exception of python-pickled data (`pickle` serializer), stored hoard data can be made compatible with other languages, though no implementations exist yet.
