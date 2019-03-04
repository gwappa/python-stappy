# MIT License
#
# Copyright (c) 2019 Keisuke Sehara
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys as _sys
import pathlib as _pathlib
import json as _json
import zlib as _zlib
from collections import OrderedDict as _OrderedDict
from functools import wraps

import numpy as _np

"""
stappy -- a storage-access protocol in python.

currently byte-order (i.e. endian-ness) support is write-only.
`stappy` does not support reading dataset that differ from native byte order.

TODO:

BIDS may need to be supported.

it may have been a bad idea to prepare "get/put/delete_xxx" for all types of data.
should work like e.g.:

```
base.create["path/to/entry"]
base["path/to/entry"].dataset["name"]    = data
# or: base.dataset["path/to/entry/name"] = data

base["path/to/entry"].table["name"]      = table
base["path/to/entry"].keyvalue["name"]   = mapped
base["path/to/entry"].image["name"]      = img
```

Ideally, these virtual attributes could be used ad hoc e.g. by calling:

```
class SubInterface(AbstractInterface):
    def __getdata__(self, name, type):
        ...

    def __setdata__(self, name, type, value):
        ...

    def __deldata__(self, name, type):
        ...
```
"""

VERSION_STR = "0.5"
DEBUG       = False

SEP         = '/'
INFO_TYPES  = (int, float, str)

def debug(msg):
    if DEBUG == True:
        print(f"[DEBUG] {msg}")

def abstractmethod(meth):
    @wraps(meth)
    def __invalid_call__(self, *args, **kwargs):
        raise NotImplementedError(meth.__name__)
    return __invalid_call__

def is_namedtuple_struct(obj):
    if isinstance(obj, tuple):
        if hasattr(obj, '_fields'):
            if all(isinstance(getattr(obj, fld),  INFO_TYPES + (_np.ndarray,)) for fld in obj._fields):
                return True
    return False

def is_mapping(obj):
    for attr in ('keys', 'values', 'items', '__getitem__'):
        if not hasattr(obj, attr) or not callable(getattr(obj, attr)):
            return False
    return True

def infer_format(value):
    """infers data format from the object type of `value`."""
    pass

class PathFormat:
    sep     = '/'
    parent  = '..'
    current = '.'

    @classmethod
    def to_string(cls, path):
        return cls.sep + cls.sep.join(self._path)

    @classmethod
    def from_string(cls, pathrepr):
        return pathrepr.split(cls.sep)

    @classmethod
    def compute_name(cls, path):
        if len(path) == 0:
            return None
        else:
            return path[-1]

    @classmethod
    def compute_parent(cls, path):
        if len(path) == 0:
            return tuple()
        else:
            return tuple(path[:-1])

    @classmethod
    def concatenate(cls, basepath, additions):
        if isinstance(additions, str):
            additions = cls.from_string(additions)
        path = basepath
        for elem in additions:
            if len(elem) == 0:
                raise ValueError('cannot concatenate a zero-length path component.')
            elif elem == cls.parent:
                path = cls.compute_parent(path)
            elif elem == cls.current:
                pass
            else:
                path = path + (elem,)
        return path

class Structure:
    """the base interface for structured database access."""
    create_attribute_default = False

    def __init__(self, path, context=None):
        if context is None:
            raise ValueError("context cannot be None")
        self._path    = path
        self._context = context

    def __repr__(self):
        cls = self.__class__.__name__
        cxt = self._context.get_repr()
        pathrepr = PathFormat.to_string(self._path)
        return f"{cls}{cxt}[{repr(pathrepr)}]"

    def __getitem__(self, path):
        return self.__class__(PathFormat.concatenate(self._path, path), self._context)

    def __getattr__(self, name):
        if name == 'parent':
            return self.__class__(PathFormat.compute_parent(self._path), self._context)
        elif name == 'attr':
            return self._context.get_attribute_root(self._path, create=create_attribute_default)
        elif name == 'format':
            return self._context.get_format(self._path)
        elif name == 'name':
            return PathFormat.compute_name(self._path)
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        # TODO: may be settable for 'name' or 'format'
        cls = self.__class__.__name__
        raise RuntimeError(f"cannot use bracket-form set methods for {cls}"+
                        f" (use {cls}.store() instead)")

    def exists(self, format=None):
        """returns if any entry exists at this path (with the specified format(s),
        or None without specification)."""
        return self._context.entry_exists(self._path, format=format)

    def create(self, parents=True):
        """create a group that has the corresponding path."""
        self._context.create_group(self._path, parents=parents)
        return self

    def load(self, format='infer'):
        """retrieves the content of this entry."""
        if (format is None) or (format == 'infer'):
            format = self._context.get_format(self._path)
        return self._context.get_value(self, format=format)

    def store(self, value, format='infer', parents=True):
        """stores the value as the content of this entry."""
        if (format is None) or (format == 'infer'):
            format = infer_format(value)
        self._context.set_value(self._path, value, format=format, parents=parents)

    def keys(self, format=None):
        """iterates child names (with or without specified format(s))."""
        return self._context.child_names(self._path, format=format)

    def values(self, format=None, unwrap=False):
        """returns a generator that iterates child values (with or without specified format(s)).

        if `unwrap` is True, `entry.load()` is called for each of children."""
        for name in self._context.child_names(self._path, format=format):
            entry = self[name]
            if unwrap == True:
                entry = entry.load()
            yield entry

    def items(self, format=None, as_path=False, unwrap=False):
        """returns a generator that iterates child names/values (with or without specified format).

        if `unwrap` is True, `entry.load()` is called for each of children.
        if `as_path` is True, child names are given as the path tuple from the context root.
        """
        for name in self._context.child_names(self._path, format=format):
            entry = self[name]
            if as_path == True:
                path = entry._path
            else:
                path = name
            if unwrap == True:
                entry = entry.load()
            yield path, entry

class Context:
    """the base class defining the structure context.
    e.g. EntryContext defines the context for 'proper' database context,
    whereas AttributeContext defines the context for attriubtes of entries."""
    name    = None

    @abstractmethod
    def get_attribute_root(self, path, create=False):
        """returns the root attribute context for the entry at the path.

        if `create` is True, it creates the base entry whenever `entry_exists` returns False.
        for attribute contexts, this method likely raises a RuntimeError."""
        pass

    @abstractmethod
    def get_repr(self):
        """returns a proper representation of this context that can be used
        for repr(Structure)"""
        pass

    @abstractmethod
    def entry_exists(self, path, format=None):
        """returns if an entry exists at path (with or without specified formats)."""
        pass

    @abstractmethod
    def get_format(self, path):
        """returns the format of the entry at the path"""
        pass

    @abstractmethod
    def create_group(self, path, parents=True):
        """creates a data group at path. if `parents` is True, calling this also
        creates any missing parent groups."""
        pass

    @abstractmethod
    def get_value(self, path, format=None):
        """load a data at path (with or without specified formats)."""
        pass

    @abstractmethod
    def set_value(self, path, value, format=None, parents=True):
        """stores a value at path (with or without specified formats)."""
        if (format is None) or (format == 'infer'):
            format = infer_format(value)
        pass

    @abstractmethod
    def child_names(self, path, format=None):
        """iterates names of child entries (if any) under this path."""
        pass

class Transaction:
    """represents the locked state to a context."""
    def __init__(self, context):
        self._context = context
        self._marked  = False
        self._context._transaction = self

    def mark(self):
        """flags this transaction 'dirty'."""
        self._marked  = True

    def commit(self):
        if self._marked == True:
            self._context.writeall()
            self._marked = False

    def rollback(self):
        if self._marked == True:
            self._context.readall()
            self._marked = False

class __Context:
    """abstract context structure for dealing with raw driver, apart from the
    default database-traversal functionality.

    For example, the Context object for 'dataset' will be generated
    when one calls as follows:

    ```
    data.dataset["path/to/dataset"]
    ```

    Scope and format
    ----------------

    Each Context object works with `scope` and `format`,
    where `format` being one of (DATASET, TABLE, IMAGE, KEYVALUE, ATTRS),
    and `scope` being the internal path of the context within the database.

    Upon retrieval/updating the objects within the context, the Context object
    supplies the parent driver interface with its corresponding `scope` and
    `format` info, along with the method call.

    Every root instance can specify the exact implementation of `scope`.

    The method calls for driver instance include:

    - `children()``
    - `read()`
    - `write()`
    - `subscope()`
    - `readall()`
    - `writeall()`
    - `lock()`
    - `unlock()`

    Note that the transaction supports on `read` and `write` methods depend on
    individual database implementations. Some database may be able to make all
    reads/writes atomic during a transaction; others may only support such atomic
    I/O for part of the formats; the rest may totally ignore transaction-related
    features (in which cases it is encouraged to emit warnings).

    Path spec and scope
    -------------------

    `root` as it is specified in Context's initializer refers to the "root object"
    that handles the path specification.
    In other words, if you have two different path specs in
    your database, then the `root` object can differ depending on it.

    For example, in h5py, you would write as follows:

    ```
    with h5py.File('your.hdf5', 'r') as src:
        src["path/to/group"].attrs["attribute/path"] = "comment"
    ```

    Here the path space for `"path/to/group"` and that for `"attribute/path"`
    is different (in a sense `src["path/to/group/attribute"].attrs["path"]` does
    not refer to a same data).

    For the former part of the path spec, `src` is the root object for the path;
    for the latter part, `src["path/to/group"].attrs` is the root object.

    On the other hand, the "path" part specifies the 'scope' of the context.
    For example, 'scope' for `src["path/to/group"]` is `"path/to/group"`.

    Instantiation of concrete data items
    ------------------------------------

    `Context` works like `pathlib.Path` objects in a sense that existence of a
    `Context` object does not imply the existence of corresponding physical entity.

    Entry creation / retrieval occurs *only when* either of `read` or `write` calls
    occurs.

    ContextManager style
    --------------------

    `with` statements can be used with or without `lock()`ing.
    If the database is locked using this Context object, the lock object will be
    automatically released as the procedures exits from the `with` block.

    """

    NONE      = 'none'
    GROUPS    = 'create'
    DATASET   = 'dataset'
    TABLE     = 'table'
    IMAGE     = 'image'
    KEYVALUE  = 'keyvalue'
    ATTRIBUTE = 'attrs'

    available = (DATASET, TABLE, IMAGE, KEYVALUE, ATTRS)

    def __init__(self, root, scope, format='none', sep=SEP, transaction=None):
        self._root          = root
        self._scope         = scope
        self._format        = format
        self._sep           = sep
        self._transaction   = transaction

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._transaction is not None:
            if exc_type is None:
                self._transaction.commit()
            else:
                self._transaction.rollback()
            self.unlock(transaction=self._transaction)

    def __getitem__(self, keypath):
        entry, key = self.resolve(keypath)
        return entry.read()

    def __setitem__(self, keypath, value):
        entry, key = self.resolve(keypath)
        debug(f"{self.__class__.__name__}: {repr(keypath)} <- {repr(value)}")
        entry.write(value)

    def copy(self, root=None, scope=None, format=None,
                    sep=None, transaction=None):
        return self.__class__(  root=self._root if root is None else root,
                                scope=self._scope if scope is None else scope,
                                format=self._format if format is None else format,
                                sep=self._sep if sep is None else sep,
                                transaction=self._transaction if transaction is None else transaction)

    def keys(self):
        """retrieves list of names associated with this format directly within this context."""
        return self._root.children(contexts=[self], keys=True, values=False)

    def values(self):
        """retrieves list of values associated with this format directly within this context."""
        return self._root.children(contexts=[self], keys=False, values=False)

    def items(self):
        return self._root.children(contexts=[self], keys=True, value=True)

    def read(self, contexts=None):
        """retrieve the content of this context."""
        if contexts is None:
            contexts = [self]
        else:
            contexts.append(self)
        return self._root.read(contexts=contexts)

    def write(self, value, contexts=None):
        """updates the content of this context."""
        if contexts is None:
            contexts = [self]
        else:
            contexts.append(self)
        return self._root.write(value, contexts=contexts)

    def readall(self, contexts=None):
        if contexts is None:
            contexts = [self]
        else:
            contexts.append(self)
        return self._root.readall(contexts=contexts)

    def writeall(self, contexts=None):
        if contexts is None:
            contexts = [self]
        else:
            contexts.append(self)
        return self._root.writeall(contexts=contexts)

    def subscope(self, key, contexts=None):
        """retrieves the existing sub-scope structure for the format within this context."""
        if contexts is None:
            contexts = [self]
        else:
            contexts.append(self)
        scope = self._root.subscope(key, contexts=contexts)
        return self.copy(scope=scope)

    def resolve(self, path):
        context = self
        keypath = path.split(self._sep)
        # traverse to the deepest entry
        for key in keypath:
            entry = context.subscope(key)
        return entry

    def lock(self, contexts=None):
        """should return a Context that is locked."""
        if contexts is None:
            contexts = [self]
        else:
            contexts.append(self)
        return self.copy(transaction=self._root.lock(contexts=contexts))

    def unlock(self, transaction=None, contexts=None):
        """free resources for new transactions."""
        if contexts is None:
            contexts = [self]
            if self._transaction:
                transaction = self._transaction
        else:
            contexts.append(self)
            if (not transaction) and (self._transaction):
                transaction = self._transaction
        self._transaction = self._root.unlock(transaction=transaction, contexts=contexts)


class AttributeManager:
    """interface for editing entry attributes."""
    def __init__(self, interface):
        self._interface = interface
        self._updating  = False
        self._dirtyflag = False

    def lock(self):
        if self._updating == True:
            return False
        self._updating  = True
        return True

    def flag(self):
        if self._updating  == True:
            self._dirtyflag = True
        else:
            self._interface._store_info()

    def commit(self):
        self._updating  = False
        if self._dirtyflag == True:
            self._interface._store_info()
            self._dirtyflag = False

    def rollback(self):
        self._updating   = False
        if self._dirtyflag == True:
            self._interface._load_info()
            self._dirtyflag = False

    def keys(self):
        return self._interface._info.keys()

    def values(self):
        return self._interface._info.values()

    def items(self):
        return self._interface._info.items()

    def __getitem__(self, keypath):
        entry, key = self.__resolve_keypath(keypath, create=False)
        return entry[key]

    def __setitem__(self, keypath, value):
        entry, key = self.__resolve_keypath(keypath, create=True)
        entry[key] = value
        debug(f"AttributeManager: {repr(keypath)} <- {repr(value)}")
        self.flag()

    def __delitem__(self, keypath):
        entry, key = self.__resolve_keypath(keypath, create=False)
        del entry[key]
        debug(f"AttributeManager: `rm` {repr(keypath)}")
        self.flag()

    def __resolve_keypath(self, keypath, create=True):
        entry   = self._interface._info
        keypath = keypath.split(SEP)
        # traverse to the deepest entry
        for key in keypath[:-1]:
            if key not in entry.keys():
                if create == True:
                    entry[key] = _OrderedDict()
                else:
                    raise KeyError(key)
            entry = entry[key]
        return entry, keypath[-1]

class EntryCreation:
    """utility interface for creating entries. called as AbstractInterface.create[<keypath>]."""
    def __init__(self, interface):
        self._interface = interface

    def __getitem__(self, keypath):
        entry, key = self._interface.resolve_path(keypath, create=True)
        if key in entry.dataset_names():
            return entry.get_dataset(key)
        else:
            return entry.get_entry(key, create=True)

    def __setitem__(self, keypath, value):
        raise NotImplementedError("use AbstractInterface[<keypath>] to modify entries/datasets")

class EntryRetrieval:
    """utility interface for retrieving entries. called as AbstractInterface.get[<keypath>]."""
    def __init__(self, interface):
        self._interface = interface

    def __getitem__(self, keypath):
        entry, key = self._interface.resolve_path(keypath, create=False)
        if key in entry.dataset_names():
            return entry.get_dataset(key)
        elif key in entry.child_names():
            return entry.get_entry(key, create=False)
        else:
            raise KeyError(key)

    def __setitem__(self, keypath, value):
        raise NotImplementedError("use AbstractInterface[<keypath>] to modify entries/datasets")

class AbstractInterface:
    """base class that provides common functionality.

    The default implementation is:

    - it uses the JSON-format file for storing dataset information.
    - it uses the directory structure to organize entry hierarchy.

    Subclasses must implement (at minimum):

    - `_data_suffix`: to distinguish dataset file from the other child entries.
    - `_load_child_dataset`: to deserialize datasets into numpy.ndarrays.
    - `_store_child_dataset`: to serialize numpy.ndarrays.
    - `_delete_child_dataset`: to remove datasets from the storage.

    If the subclasses intend to use the structure other than the file system,
    they must implement the other methods:

    - `_open_root_repr`
    - `_free_root_repr`
    - `_get_volatile_repr`
    - `_list_contents`
    - `_load_info`
    - `_store_info`
    - `_delete_info`
    - `_store_child_entry`:
    - `_delete_child_entry`:
    """
    _info_suffix = ".json"
    _data_suffix = None
    _byteorders = {
        '<': 'little',
        '>': 'big',
        '=': _sys.byteorder,
        '|': 'NA'
    }

    @classmethod
    def _open_root_repr(cls, rootpath):
        """initializes the physical representation of the root at `rootpath`.
        returns (new, obj) tuple, where `new` indicates whether the root `obj`
        is newly created."""
        raise NotImplementedError("_root_repr")

    @classmethod
    def _free_root_repr(cls, rootrepr):
        raise NotImplementedError("_free_root_repr")

    @abstractmethod
    def _get_volatile_repr(self, parent, name):
        """creates `self`'s physical representation based on
        `parent` and `name` information.

        note that `parent` may not have a justified type.
        if `parent` is None, it implies that this is the root entry."""
        pass

    @abstractmethod
    def _load_info(self):
        """reads info from the existing physical representation,
        and stores it in the instance's `info` attribute."""
        pass

    @abstractmethod
    def _store_info(self):
        """writes the current `info` attribute to its physical
        representation."""
        pass

    @abstractmethod
    def _delete_info(self):
        """deletes the information for the entry.
        this is only supposed to occur during the deletion of the entry itself."""
        pass

    @abstractmethod
    def _list_contents(self, children=True, datasets=True):
        """returns the contents of the entry as a sequence."""
        pass

    @abstractmethod
    def _get_child_entry(self, name):
        """tries to get the specified child entry in this entry.
        it returns the corresponding AbstractInterface object."""
        pass

    @abstractmethod
    def _delete_child_entry(self, name, child):
        """remove the child entry `child` that has `name`."""
        pass

    @abstractmethod
    def _load_child_dataset(self, name):
        """tries to get the specified child dataset in this entry.
        it must return the corresponding numpy.ndarray object."""
        pass

    @abstractmethod
    def _store_child_dataset(self, name, value):
        """store `value` with the specified `name`."""
        pass

    @abstractmethod
    def _delete_child_dataset(self, name):
        """remove the dataset that has `name`."""
        pass

    @abstractmethod
    def _load_child_dict(self, name):
        pass

    @abstractmethod
    def _store_child_dict(self, name):
        pass

    @abstractmethod
    def _delete_child_dict(self, name):
        pass

    @classmethod
    def open(cls, rootpath, **kwargs):
        """returns the 'root' entry (that has different terminology)."""
        root                = cls("", parent=None)
        created, root._repr = cls._open_root_repr(rootpath)
        root._root          = root._repr
        root._path          = ''
        if not created:
            root._load_info()

        def _update(src):
            return root.__class__._copy_from_another_root(src=src, dest=root)
        def _close():
            return root.__class__.close(root)
        root.update = _update
        root.close  = _close
        if len(kwargs) > 0:
            for key, value in kwargs:
                setattr(root, key, value)
        return root

    @classmethod
    def close(cls, rootobj=None):
        """free the physical representation of this root object."""
        if not rootobj.is_root():
            raise ValueError("close() not applied to the root object")
        else:
            cls._free_root_repr(rootobj._repr)
            rootobj.invalidate()

    @classmethod
    def _copy_from_another_root(cls, src=None, dest=None):
        if (not src.is_root()) or (not dest.is_root()):
            raise ValueError("invalid call to copy()")
        for name, value in src.items():
            dest[name] = value

    def __init__(self, name, parent=None):
        """creates (ensures) the directory with the matched name.
        `parent` must be either None (root) or the instance of the same classs.

        `info` will be only used when """
        if (len(name.strip()) == 0) and (parent is not None):
            raise ValueError("entry name cannot be empty")
        if SEP in name:
            if parent is None:
                raise ValueError("use of path is not allowed for the root name")
            else:
                comps = name.split(SEP)
                parent = self.__class__(SEP.join(comps[:-1]), parent=parent)
                name   = comps[-1]

        self._name  = name
        self._info  = _OrderedDict()
        self._parent= parent
        if parent is not None:
            self._root  = parent._root
            self._repr  = self._get_volatile_repr(parent, name)
            self._path  = f"{parent._path}{SEP}{name}"
            self._load_info()
        self.attrs  = AttributeManager(self)
        self._valid = True

    def __repr__(self):
        if self._valid == True:
            return f"{self.__class__.__name__}({repr(str(self._root))})[{repr(str(self._path))}]"
        else:
            return f"{self.__class__.__name__}(#invalid)"

    def __getattr__(self, name):
        if name == 'create':
            return EntryCreation(self)
        elif name == 'get':
            return EntryRetrieval(self)
        else:
            return super().__getattr__(name)

    def __getitem__(self, keypath):
        entry, key = self.resolve_path(keypath, create=False)
        if key in entry.child_names():
            return entry.get_entry(key, create=False)
        elif key in entry.dataset_names():
            return entry.get_dataset(key)
        else:
            raise KeyError(key)

    def __setitem__(self, keypath, value):
        if (not isinstance(value, (AbstractInterface, _np.ndarray))) \
            and (not is_namedtuple_struct(value)) and (not is_mapping(value)):
            raise ValueError(f"stappy only accepts entry-types, numpy arrays, array-based named tuples, or mappings, but got {value.__class__}")
        entry, key = self.resolve_path(keypath, create=True)
        if isinstance(value, AbstractInterface):
            entry.put_entry(key, value)
        elif isinstance(value, _np.ndarray):
            entry.put_dataset(key, value)
        elif is_namedtuple_struct(value):
            entry.put_namedtuple_struct(key, value)
        elif is_mapping(value):
            entry.put_dict(key, value)
        else:
            raise RuntimeError("fatal error: class assertion failed")

    def __delitem__(self, keypath):
        entry, key = self.resolve_path(keypath, create=False)
        if key in entry.child_names():
            # entry
            entry.delete_entry(key)
        elif key in entry.dataset_names():
            # dataset
            entry.delete_dataset(key)
        else:
            raise KeyError(key)

    def __contains__(self, keypath):
        try:
            entry, key = self.resolve_path(keypath, create=False)
        except KeyError:
            return False
        try:
            entry = entry.get_entry(key, create=False)
            return True
        except NameError:
            return False

    def resolve_path(self, keypath, create=True):
        """returns (dparent, key), where `dparent` indicates the
        direct parent of the value specified by `keypath`."""
        keys = keypath.split(SEP)
        entry = self
        for key in keys[:-1]:
            entry = entry.get_entry(key, create=create)
        return entry, keys[-1]

    def invalidate(self):
        """makes this object invalid as a reference."""
        self._name   = None
        self._root   = None
        self._parent = None
        self._info   = None
        self._repr   = None
        self._valid  = False

    def is_root(self):
        return (self._parent is None)

    def keys(self):
        """returns a sequence of names of children (child entries and datasets irrelevant)."""
        return self._list_contents(children=True, datasets=True)

    def child_names(self):
        """returns a sequence of its child entries."""
        return self._list_contents(children=True, datasets=False)

    def dataset_names(self):
        """returns a sequence of datasets that this entry contains."""
        return self._list_contents(children=False, datasets=True)

    def values(self):
        """returns a generator of children (entries and datasets)."""
        for name in self.keys():
            yield self.__getitem__(name)

    def children(self):
        for name in self.child_names():
            yield self.get_entry(name, create=False)

    def datasets(self):
        for name in self.dataset_names():
            yield self.get_dataset(name)

    def items(self):
        for name in self.keys():
            yield name, self.__getitem__(name)

    def get_entry(self, name, create=True):
        """returns the specified child entry.
        if `create` is True and the entry does not exist,
        the entry is newly generated before being returned."""
        if name in self.child_names():
            entry = self._get_child_entry(name)
        else:
            if create == False:
                raise NameError(f"name not found: {name}")
            entry = self.__class__(name, parent=self)
        return entry

    def put_entry(self, name, entry, overwrite=True, deletesource=False):
        """puts `entry` to this entry with `name`."""
        if name in self.child_names():
            if overwrite == False:
                raise NameError(f"entry '{name}' already exists")
            else:
                self.delete_entry(name)

        # copy recursively
        child = self.get_entry(name, create=True)
        child._info.update(entry._info)
        for dataname in entry.dataset_names():
            child.put_dataset(dataname, entry.get_dataset(dataname))
        for grandchild in entry.child_names():
            child.put_entry(grandchild, entry.get_entry(grandchild))
        child._store_info()

        if deletesource == True:
            if entry._parent is None:
                # TODO: remove the root file
                pass
            else:
                entry._parent.delete_entry(entry.name)

    def delete_entry(self, name):
        """deletes a child entry with 'name' from this entry."""
        if name not in self.child_names():
            raise NameError(f"entry '{name}' does not exist")
        child = self.get_entry(name, create=False)

        # deletes grandchildren recursively
        for dataname in child.dataset_names():
            child.delete_dataset(dataname)
        for grandchild in child.child_names():
            child.delete_entry(grandchild)
        child._delete_info()

        self._delete_child_entry(name, child)
        child.invalidate()

    def get_dataset(self, name):
        """returns the dataset with the specified name."""
        if name not in self.dataset_names():
            raise NameError(f"dataset not found: {name}")
        data = self._load_child_dataset(name)
        locked = self.attrs.lock()
        self.attrs[f"{name}/dtype"]       = str(data.dtype)
        self.attrs[f"{name}/shape"]       = data.shape
        self.attrs[f"{name}/byteorder"]   = self._byteorders[data.dtype.byteorder]
        if locked == True:
            self.attrs.commit()
        return data

    def put_dataset(self, name, value, overwrite=True):
        """puts `value` to this entry with `name`."""
        if name in self.dataset_names():
            if overwrite == False:
                raise NameError(f"the dataset '{name}' already exists")
            else:
                self.delete_dataset(name)
        self._store_child_dataset(name, value)
        locked = self.attrs.lock()
        self.attrs[f"{name}/dtype"] = str(value.dtype)
        self.attrs[f"{name}/shape"] = value.shape
        self.attrs[f"{name}/byteorder"]   = self._byteorders[value.dtype.byteorder]
        if locked == True:
            self.attrs.commit()

    def put_namedtuple_struct(self, name, value, overwrite=True):
        if not is_namedtuple_struct(value):
            raise ValueError(f"not conforming to the 'named-tuple structure': {value.__class__}")
        if name in self.child_names():
            if overwrite == False:
                raise NameError(f"the entry '{name}' already exists")
            else:
                self.delete_entry(name)
        entry = self.get_entry(name, create=True)
        locked = entry.attrs.lock()
        entry.attrs["type"] = value.__class__.__name__
        for field in value._fields:
            item = getattr(value, field)
            if isinstance(item, INFO_TYPES):
                entry.attrs[field] = item
            else:
                # must be np.ndarray b/c of is_namedtuple_struct() impl
                entry.put_dataset(field, item, overwrite=True)
        if locked == True:
            entry.attrs.commit()

    def delete_dataset(self, name):
        """deletes a child dataset with 'name' from this entry."""
        self._delete_child_dataset(name)

        locked = self.attrs.lock()
        del self.attrs[f"{name}/dtype"]
        del self.attrs[f"{name}/shape"]
        del self.attrs[f"{name}/byteorder"]
        if locked == True:
            self.attrs.commit()

    def get_dict(self, name):
        return self._load_child_dict(name)

    def put_dict(self, name, value):
        if not isinstance(value, (dict, _OrderedDict)): # FIXME: how to check if it is mapping type?
            raise ValueError(f"expected dict, got '{value.__class__}'")
        self._store_child_dict(name, value)

    def delete_dict(self, name):
        self._delete_child_dict(name)

class FileSystemInterface(AbstractInterface):
    """base class that provides a file system-based data access.

    it proides implementations for some abstract functions in AbstractInterface:

    - `open`
    - `_get_volatile_repr`
    - `_list_contents`
    - `_load_info`
    - `_store_info`
    - `_delete_info`
    - `_store_child_entry`
    - `_delete_child_entry`
    - `_delete_child_dataset`

    subclasses still needs to implement the following methods:

    - `_data_suffix`: to distinguish dataset file from the other child entries.
    - `_load_child_dataset`: to deserialize datasets into numpy.ndarrays.
    - `_store_child_dataset`: to serialize numpy.ndarrays.

    """

    _meta_base   = "entry_metadata"
    _info_suffix = ".json"
    _data_suffix = None

    def _datafile(self, name):
        return self._repr / f"{name}{self._data_suffix}"

    def _get_volatile_repr(self, parent, name):
        if parent is None:
            # root; necessary paths must have been already initialized
            return
        else:
            file = _pathlib.Path(parent._repr) / name

        if file.is_file():
            raise FileExistsError("cannot create another entry (file in place of directory)")
        if not file.exists():
            file.mkdir()
            debug(f"FileSystemInterface._get_volatile_repr: created '{name}' under '{str(parent)}'")
        return file

    def _load_child_dict(self, name):
        dictfile = self._repr / f"{name}.json"
        if not dictfile.exists():
            raise FileNotFoundError(str(dictfile))
        else:
            with open(dictfile, 'r') as src:
                return _json.load(src, object_hook=_OrderedDict)

    def _store_child_dict(self, name, value):
        dictfile = self._repr / f"{name}.json"
        with open(dictfile, 'w') as out:
            _json.dump(value, out, indent=4)

    def _delete_child_dict(self, name):
        dictfile = self._repr / f"{name}.json"
        if dictfile.exists():
            dictfile.unlink()

    def _load_info(self):
        infofile = self._repr / f"{self._meta_base}{self._info_suffix}"
        if not infofile.exists():
            debug(f"FileSystemInterface._load_info: {repr(str(infofile))} was not found; leave the info empty.")
            self._info = _OrderedDict()
        else:
            with open(infofile, 'r') as info:
                self._info = _json.load(info, object_hook=_OrderedDict)
            debug(f"FileSystemInterface._load_info: loaded from '{self._name}': '{self._info}'")

    def _store_info(self):
        if len(self._info) > 0:
            with open(self._repr / f"{self._meta_base}{self._info_suffix}", 'w') as out:
                _json.dump(self._info, out, indent=4)
            debug(f"FileSystemInterface._store_info: stored into '{self._name}': '{self._info}'")

    def _delete_info(self):
        infofile = self._repr / f"{self._meta_base}{self._info_suffix}"
        if not infofile.exists():
            return
        else:
            infofile.unlink()

    def _list_contents(self, children=True, datasets=True):
        _listed = []
        for path in self._repr.iterdir():
            if path.name.startswith('.'):
                # hidden
                pass
            elif path.suffix == self._info_suffix:
                # info
                pass
            elif path.suffix == self._data_suffix:
                # data
                if datasets == True:
                    _listed.append(path.stem)
            else:
                # child
                if children == True:
                    _listed.append(path.name)
        return tuple(_listed)

    def _get_child_entry(self, name):
        return self.__class__(name, parent=self)

    def _delete_child_entry(self, name, child):
        child._repr.rmdir()

    @abstractmethod
    def _load_child_dataset(self, name):
        """tries to get the specified child dataset in this entry.
        it must return the corresponding numpy.ndarray object."""
        pass

    @abstractmethod
    def _store_child_dataset(self, name, value):
        """stores `value` with the specified `name` (with appropriate
        suffix, if you use the `_data_suffix` functionality)."""
        pass

    def _delete_child_dataset(self, name):
        """removes the dataset that has `name` (with appropriate suffix,
        if you use the `_data_suffix` functionality)."""
        self._datafile(name).unlink()

    @classmethod
    def _open_root_repr(cls, rootpath):
        rootrepr = _pathlib.Path(rootpath)
        if not rootrepr.exists():
            created = True
            rootrepr.mkdir(parents=True)
        else:
            created = False
        return created, rootrepr

    @classmethod
    def _free_root_repr(cls, rootrepr):
        pass


class NPYInterface(FileSystemInterface):
    _data_suffix = '.npy'

    def __init__(self, name, parent=None):
        super().__init__(name, parent=parent)

    def _load_child_dataset(self, name):
        data = _np.load(str(self._datafile(name)))
        return data

    def _store_child_dataset(self, name, value):
        _np.save(str(self._datafile(name)), value)

class BareZInterface(FileSystemInterface):
    _data_suffix = ".zarr"
    _default_compression_level = 6
    compression_level = None

    def __init__(self, name, parent=None):
        super().__init__(name, parent=parent)
        if parent is not None:
            if hasattr(parent, 'compression_level'):
                self.compression_level = parent.compression_level
        if self.compression_level is None:
            self.compression_level = self._default_compression_level

    def _load_child_dataset(self, name):
        dtype = _np.dtype(self.attrs[f"{name}/dtype"])
        shape = self.attrs[f"{name}/shape"]
        file  = str(self._datafile(name))
        with open(file, 'rb') as src:
            binary = _zlib.decompress(src.read())
        return _np.frombuffer(binary, dtype=dtype).reshape(shape, order='C')

    def _store_child_dataset(self, name, value):
        self.attrs[f"{name}/compression"] = 'zlib'
        with open(self._datafile(name), 'wb') as dst:
            dst.write(_zlib.compress(value.tobytes(order='C'), level=self.compression_level))
