# Specification

## DataPath

`DataPath` works similarly as `pathlib.Path`, and can be used as traversing inside
a database.

```
# returns a 'DataPath('/path/to/data/root#/')'
root = stappy.open('/path/to/data/root', format='barez')

# any path can be referred to, as long as it is well-formed
# does not matter whether it actually exists
group  = root['path/to/group']
parent = group['..']

# read/write processes
# read from an entry
data    = group.get_dataset('data_name')
mapping = group.get_mapping('dict_name')

# write to an entry
child = group['group/path'].create() # creates a group
group.put_dataset('data_name', data)
group.put_mapping('dict_name', mapping)

# ODML-type entry manipulation
data_entry = group.get_entry('data_name', format='dataset')
type(data_entry) # ODMLEntry(definition='...', value=..., unit='...')
group.put_entry('data_name', data_entry, format='dataset')

# attribute access
group['data_name'].attrs['path/to/attr/value'] = value

# attribute access in the form of ODML
group['data_name'].attrs['path/to/parent'].put_entry('attr', value, 'definition', 'unit')
group['data_name'].attrs.put_entry('path/to/parent/attr', value, 'definition', 'unit')
attr = group['data_name'].attrs['path/to/parent'].get_entry('attr')
attr = group['data_name'].attrs.get_entry('path/to/parent/attr')
type(attr) # ODMLEntry(definition='...', value=..., unit='...')
value = attr.value
```

## DataHandler

`as_dataset` or `as_mapping` calls are delegated to a corresponding `DataHandler` object,
which is supposed to encode/decode Python objects in a certain way.
Each `DataHandler` object knows:

- its specialized _format_ (e.g. `dataset`, `mapping`, `table`, `file`)
- the database-internal path to store the data object
- a dedicated pair of `get()` and `put()` methods that handles the actual I/O.

For example, a `DataHandler` for a `dataset` format may write the dataset into
path `example/path/spec` as a file `example/path/spec.npy`.

It is likely that a `DataHandler` class is restricted to a certain form of databases
(e.g. those that store data in a type of file system) and may not be interoperable
with the other forms.
Conversely, a format of database may have its own set of `DataHandler`s that handle
basic format I/O.

The root `DataPath` object has the `format -> handler_class` mapping as a `dict` object,
and passes it on to its descendant `DataPath` objects.

### Representative formats

- `dataset`: stores N-dimensional array (most likely being `np.ndarray`).
- `mapping`: stores key-value hierarchy (e.g. a `dict` or `OrderedDict` object).
- `table`: stores data-frame (most likely being `pd.DataFrame`).
- `file`: stores binary/character file (most likely a path-like object that represents
    the location of the file). this is the most generic way of storing data in a database.

### `attrs` codec

Among others, the `attrs` data codec is a special one, in a sense that it allows path-like
name specification on its own.
Upon setting and getting of a value, it refers to a hierarchy of names inside its data representation.

## ODML support

both `DataPath` and `attrs` must have the `get_entry()` and `put_entry()` functions
that eases the generation of entries in the ODML format.

## `open` function

`open` is the most basic function to generate the root `DataPath` object.
You must specify the `format` of the database so that appropriate set of `DataHandler` classes.

## Some class/function specs

### open

#### parameters

- `path` : path-like object to specify the location of the database root.
- `format` : `str` to specify the format of the database.

#### returns

- a root `DataPath` object loaded with appropriate `format -> handler_class` mappings.

### DataPath

#### properties

- `handlers`: the `format -> handler_class` mappings.
- `path` : in the form of `tuple`
- `name` : (read-only) the last fragment of `path`
- `parent`: (read-only) generates the new `DataPath` without this object's `name` in its `path`

#### methods

- `__setitem__`: (_TODO_) cannot call??
- `__getitem__`: returns its descendant `DataPath` object.
- `get_abc`: alias to the `get` method of the corresponding `abc` handler
- `put_abc`: alias to the `put` method of the corresponding `abc` handler

### DataHandler

#### properties

- `path` : in the form of `tuple`

#### methods

- `get`: retrieves a value (in its format) at `path`
- `put`: sets (if possible) the value at `path` as specified

### DatabaseFormat

Internally called from `open()`, and helps create a root `DataPath` object.
It works behind the scenes whenever a `DataHandler` is in action.

#### properties

- `handlers`: passed on to the deriving `DataPath` objects.

#### methods

- `get_group(path)`
- `create_group(path)`
- `get_attribute_manager(path)`
- `set_attribute_manager(path)`

#### methods (specific to `FileSystemDatabase`)

- `get_filepath(path)`
