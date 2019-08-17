#
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
#

import sys as _sys
import pathlib as _pathlib
import json as _json
import zlib as _zlib
from collections import OrderedDict as _OrderedDict, namedtuple as _namedtuple
from functools import wraps

import numpy as _np

from .utils import assert_type as _assert_type
from .core import Protocol

"""
stappy -- a storage-access protocol in python.

currently byte-order (i.e. endian-ness) support is write-only.
`stappy` does not support reading dataset that differ from native byte order.

TODO:

BIDS may need to be supported.

it may have been a bad idea to prepare "get/put/delete_xxx" for all types of data.
should work like e.g.:

```
base["path/to/entry"].as_dataset    = data
base["path/to/entry"].as_table      = table
base["path/to/entry"].as_keyvalue   = mapped
base["path/to/entry"].as_image      = img
base["path/to/entry"].as_binary     = data
base["path/to/entry"].attrs['key']  = value
base["path/to/entry"].type # returns entry type
base["path/to/entry"].delete() # deletes this entry
```

Each `Entry` or `AttributeEntry` item is just like
a Path instance in `pathlib`;
It does not have to have a physical counterpart up to the point
any write operation is performed.

Any write operations are delegated to the Database interface such as
`Database.__writedata__(root, path, type, value)` or
`Database.__readattr__(entry, path, value)`


Ideally, these virtual attributes could be used ad hoc e.g. by calling:

```
class SubProtocol(Protocol):
    def __readdata__(self, root, path, type):
        ...

    def __writedata__(self, root, path, type, value):
        ...

    def __removedata__(self, root, path):
        ...
```
"""

VERSION_STR = "1.0a1"
DEBUG       = False
fileopen    = open

class __ProtocolEnumeration(object):
    def __init__(self, *args):
        self.__protocols = []
        for cls in args:
            self.register(cls)

    def register(self, cls, position=0):
        if not issubclass(cls, Protocol):
            raise ValueError("a protocol must be a subclass of Protocol, got "+cls.__name__)
        if cls.name in self.names:
            raise ValueError("name already used: "+cls.name)
        if position is None:
            self.__protocols.append(cls)
        else:
            self.__protocols.insert(int(position), cls)

    def unregister(self, name):
        if issubclass(name, Protocol):
            try:
                idx = self.__protocols.index(name)
                del self.__protocols[idx]
            except ValueError:
                raise ValueError("not registered: "+str(name))
        elif isinstance(name, str):
            try:
                del self.__protocols[self.names.index(name)]
            except ValueError:
                raise ValueError("not registered: "+name)
        else:
            raise ValueError("expected str or a subclass of Protocol, got "+name.__class__.__name__)

    def lookup(self, name):
        if isinstance(name, str):
            try:
                return self.__protocols[self.names.index(name)]
            except ValueError:
                raise ValueError("protocol not found: "+name)
        elif issubclass(name, Protocol):
            return name
        else:
            raise ValueError("could not look up a protocol by a value: "+ str(name))

    def from_path(self, path):
        for proto in self.__protocols:
            if proto.accepts_path(path):
                return proto
        return Protocol

    def __getattr__(self, name):
        if name == 'names':
            return tuple(proto.name for proto in self.__protocols)
        else:
            raise AttributeError(name)

protocols = __ProtocolEnumeration()

def open(filepath, mode='r', protocol=None):
    from .core import RootEntry
    if protocol is None:
        proto = protocols.from_path(filepath)
    else:
        proto = protocols.lookup(protocol)
        if not proto.accepts_path(filepath):
            raise ValueError("protocol '{proto}' does not accept this path type: {path}".format(
                proto=proto.name, path=filepath
            ))
    return RootEntry(proto.open(filepath, mode=mode))

# data types
# class DataFormats:
#     Entry   = 'entry'
#     NDArray = 'ndarray'
#     Table   = 'table'
#     Struct  = 'struct'
#     Binary  = 'binary'
#
# INFO_TYPES  = (int, float, str)
#
# def debug(msg):
#     if DEBUG == True:
#         print(f"[DEBUG] {msg}")
#
# def abstractmethod(meth):
#     @wraps(meth)
#     def __invalid_call__(self, *args, **kwargs):
#         raise NotImplementedError(meth.__name__)
#     return __invalid_call__
#
# def is_namedtuple_struct(obj):
#     if isinstance(obj, tuple):
#         if hasattr(obj, '_fields'):
#             if all(isinstance(getattr(obj, fld),  INFO_TYPES + (_np.ndarray,)) for fld in obj._fields):
#                 return True
#     return False
#
# def is_mapping(obj):
#     for attr in ('keys', 'values', 'items', '__getitem__'):
#         if not hasattr(obj, attr) or not callable(getattr(obj, attr)):
#             return False
#     return True
#
# def infer_format(value):
#     """infers data format from the object type of `value`."""
#     pass
#
# class PathFormat:
#     sep     = '/'
#     parent  = '..'
#     current = '.'
#
#     @classmethod
#     def to_string(cls, path):
#         return cls.sep + cls.sep.join(self._path)
#
#     @classmethod
#     def from_string(cls, pathrepr):
#         return pathrepr.split(cls.sep)
#
#     @classmethod
#     def compute_name(cls, path):
#         if len(path) == 0:
#             return None
#         else:
#             return path[-1]
#
#     @classmethod
#     def compute_parent(cls, path):
#         if len(path) == 0:
#             return tuple()
#         else:
#             return tuple(path[:-1])
#
#     @classmethod
#     def concatenate(cls, basepath, additions, method=None):
#         if isinstance(additions, str):
#             additions = cls.from_string(additions)
#         if method is None:
#             method    = cls.append
#         path = basepath
#         for elem in additions:
#             path = method(path, elem)
#         return path
#
#     @classmethod
#     def append(cls, path, elem):
#         if len(elem) == 0:
#             raise ValueError('cannot concatenate a zero-length path component.')
#         elif elem == cls.parent:
#             path = cls.compute_parent(path)
#         elif elem == cls.current:
#             pass
#         else:
#             path = path + (elem,)
#         return path
#
#
# class DataEntry:
#     """the base interface for structured database access."""
#     create_attribute_default = False
#
#     def __init__(self, path, context=None):
#         if driver is None:
#             raise ValueError("driver cannot be None")
#         self.path    = path
#         self.context = context
#
#     def __repr__(self):
#         cls = self.__class__.__name__
#         cxt = self.context.get_repr()
#         pathrepr = PathFormat.to_string(self.path)
#         return f"{cls}{cxt}[{repr(pathrepr)}]"
#
#     def __getitem__(self, path):
#         return self.__class__(PathFormat.concatenate(self.path, path), self.context)
#
#     def __getattr__(self, name):
#         if name == 'parent':
#             return self.__class__(PathFormat.compute_parent(self.path), self.context)
#         elif name == 'attr':
#             return self.context.get_attribute_root(self.path, create=create_attribute_default)
#         elif name == 'format':
#             return self.context.get_format(self.path)
#         elif name == 'name':
#             return PathFormat.compute_name(self.path)
#         else:
#             raise AttributeError(name)
#
#     def __setattr__(self, name, value):
#         # TODO: may be settable for 'name' or 'format'
#         cls = self.__class__.__name__
#         raise RuntimeError(f"cannot use bracket-form set methods for {cls}"+
#                         f" (use {cls}.store() instead)")
#
#     def exists(self, format=None):
#         """returns if any entry exists at this path (with the specified format(s),
#         or None without specification)."""
#         return self.context.entry_exists(self.path, format=format)
#
#     def create(self, parents=True, ignore_existing=True):
#         """create a group that has the corresponding path."""
#         self.context.create_group(self.path, parents=parents, ignore_existing=ignore_existing)
#         return self
#
#     def load(self, format='infer'):
#         """retrieves the content of this entry."""
#         if (format is None) or (format == 'infer'):
#             format = self.context.get_format(self.path)
#         return self.context.get_value(self, format=format)
#
#     def store(self, value, format='infer', parents=True):
#         """stores the value as the content of this entry."""
#         if (format is None) or (format == 'infer'):
#             format = infer_format(value)
#         self.context.set_value(self.path, value, format=format, parents=parents)
#
#     def keys(self, format=None):
#         """iterates child names (with or without specified format(s))."""
#         return self.context.child_names(self.path, format=format)
#
#     def values(self, format=None, unwrap=False):
#         """returns a generator that iterates child values (with or without specified format(s)).
#
#         if `unwrap` is True, `entry.load()` is called for each of children."""
#         for name in self.context.child_names(self.path, format=format):
#             entry = self[name]
#             if unwrap == True:
#                 entry = entry.load()
#             yield entry
#
#     def items(self, format=None, as_path=False, unwrap=False):
#         """returns a generator that iterates child names/values (with or without specified format).
#
#         if `unwrap` is True, `entry.load()` is called for each of children.
#         if `as_path` is True, child names are given as the path tuple from the context root.
#         """
#         for name in self.context.child_names(self.path, format=format):
#             entry = self[name]
#             if as_path == True:
#                 path = entry.path
#             else:
#                 path = name
#             if unwrap == True:
#                 entry = entry.load()
#             yield path, entry
#
# class DataContext:
#     """the base class defining the structure context."""
#     name    = None
#
#     @abstractmethod
#     def get_attribute_root(self, path, create=False):
#         """returns the root attribute entry for the entry at the path.
#         if `create` is True, it creates the base entry whenever `entry_exists` returns False."""
#         pass
#
#     @abstractmethod
#     def get_repr(self):
#         """returns a proper representation of this context that can be used
#         for repr(Structure)"""
#         pass
#
#     @abstractmethod
#     def child_names(self, path, format=None):
#         """iterates names of child entries (if any) under this path."""
#         pass
#
#     @abstractmethod
#     def entry_exists(self, path, format=None):
#         """returns if an entry exists at path (with or without specified formats)."""
#         pass
#
#     @abstractmethod
#     def get_format(self, path):
#         """returns the format of the entry at the path"""
#         pass
#
#     @abstractmethod
#     def create_group(self, path, parents=True, ignore_existing=True):
#         """creates a data group at path. if `parents` is True, calling this also
#         creates any missing parent groups."""
#         pass
#
#     @abstractmethod
#     def get_value(self, path, format=None):
#         """load a data at path (with or without specified formats)."""
#         pass
#
#     @abstractmethod
#     def set_value(self, path, value, format=None, parents=True):
#         """stores a value at path (with or without specified formats)."""
#         if (format is None) or (format == 'infer'):
#             format = infer_format(value)
#         pass
#
# class FileSystemContext(DataContext):
#     """a database scheme based on the OS file system."""
#     format        = "barez"
#     suffix        = ".barezdb"
#
#     metadata_stem = ".metadata"
#
#     ATTRIBUTE_KEY = ".attribute"
#     ENTRIES_KEY   = ".entries"
#
#     FORMAT_KEY    = '.format'
#     ENTRYTYPE_KEY = 'type'
#     ENTRYFILE_KEY = 'file'
#     DATATYPE_KEY  = 'dtype'
#     DATASHAPE_KEY = 'shape'
#
#     format_dict     = {}
#     metadata_suffix = '.json'
#     ndarray_suffix  = '.zarr'
#     table_suffix    = '.csv'
#     struct_suffix   = '.json'
#
#     NDARRAY_OPTS  = {'compression': 'zlib'}
#     TABLE_OPTS    = {'format': 'csv'}
#     STRUCT_OPTS   = {'format': 'json'}
#
#     attr_object   = _OrderedDict
#
#     rootpath      = None
#
#     @classmethod
#     def append_path(cls, path, elem):
#         return path / elem
#
#     @classmethod
#     def as_filepath(cls, rootpath, path):
#         return PathFormat.concatenate(rootpath, path, method=cls.append_path)
#
#     @classmethod
#     def init_format_dict(cls):
#         """loads cls.format_dict with `suffix -> (DataFormats, options)` dictionary."""
#         cls.format_dict[cls.ndarray_suffix] = (DataFormats.NDArray, cls.NDARRAY_OPTS)
#         cls.format_dict[cls.table_suffix]   = (DataFormats.Table, cls.TABLE_OPTS)
#         cls.format_dict[cls.struct_suffix]  = (DataFormats.Struct, cls.STRUCT_OPTS)
#
#     @classmethod
#     def is_metadata_filename(cls, name):
#         return name in (cls.metadata_stem,)
#
#     @classmethod
#     def load_metadata_file(cls, entry_path):
#         metafile = (entry_path / cls.metadata_stem).with_suffix(cls.metadata_suffix)
#         attrtype = cls.attr_object
#         if metafile.exists():
#             with open(metafile, 'r') as src:
#                 metadata = _json.load(src, object_hook=attrtype)
#         else:
#             metadata = attrtype()
#             metadata[cls.ATTRIBUTE_KEY] = attrtype()
#             metadata[cls.ENTRIES_KEY]   = attrtype()
#         return metadata
#
#     @classmethod
#     def store_metadata_file(cls, entry_path, metadata):
#         """overwrites the metadata file under entry"""
#         metafile = (entry_path / cls.metadata_stem).with_suffix(cls.metadata_format)
#         with open(metafile, 'w') as out:
#             _json.dump(metadata, indent=4)
#
#     @classmethod
#     def create_entry_metadata(cls, path, format, dtype=None, shape=None, **kwargs):
#         if isinstance(path, _pathlib.Path):
#             name = path.name
#         elif isinstance(path, (tuple, list)):
#             name = path[-1]
#         else:
#             name = path
#         metadata = cls.attr_object()
#         metadata[cls.FORMAT_KEY] = cls.attr_object()
#         fmt = metadata[cls.FORMAT_KEY]
#         fmt[cls.ENTRYFILE_KEY] = name
#         fmt[cls.ENTRYTYPE_KEY] = format
#         if dtype is not None:
#             fmt[cls.DATATYPE_KEY]  = dtype
#         if shape is not None:
#             fmt[cls.DATASHAPE_KEY] = shape
#         for key, val in kwargs.items():
#             fmt[key] = val
#         return metadata
#
#     @classmethod
#     def infer_metadata(cls, path):
#         """infer data format and other important info about the file at path."""
#         if len(cls.format_dict) == 0:
#             cls.init_format_dict()
#         if path.is_dir():
#             return cls.create_entry_metadata(path, DataFormats.Entry)
#         else:
#             suffix = path.suffix
#             for key, val in cls.format_dict.items():
#                 if suffix == key:
#                     fmt, opts = val
#                     if fmt == DataFormats.NDArray:
#                         _warn(f"file at '{path}' misses information about data type and shape. the data may not be read properly.")
#                     return cls.create_entry_metadata(path, val, **opts)
#             # no match
#             return cls.create_entry_metadata(path, val, DataFormats.Binary)
#
#     @classmethod
#     def ensure_metadata(cls, path):
#         """creates/checks/repairs metadata (where necessary).
#         path may not be existent (or not a directory) at the time of calling the method."""
#         if not path.exists():
#             raise FileNotFoundError(path)
#         elif not path.is_dir():
#             raise NotADirectoryError(path)
#         metadata = cls.load_metadata_file(path)
#         entries  = metadata[cls.ENTRIES_KEY]
#
#         # check existence of the corresponding entry for each child files
#         for child_path in path.iterdir():
#             child_name = child_path.stem
#             if cls.is_metadata_filename(child_name):
#                 continue
#             if not child_name in entries.keys():
#                 entries[child_name] = cls.infer_metadata(child_path)
#         cls.store_metadata_file(path, metadata)
#         return metadata
#
#     @classmethod
#     def find_metadata(cls, rootpath, path):
#         return cls.ensure_metadata(cls.as_filepath(rootpath, path))
#
#     def __init__(self, rootpath):
#         self.rootpath = _pathlib.Path(rootpath)
#
#     def get_repr(self):
#         return f"({repr(str(self.rootpath))}, format={repr(self.format)})"
#
#     def child_names(self, path, format=None):
#         metadata = self.__class__.find_metadata(self.rootpath, path)
#         found = []
#         for name, val in metadata[self.__class__.ENTRIES_KEY].items():
#             type = val[self.__class__.FORMAT_KEY][self.__class__.ENTRYTYPE_KEY]
#             if (format is None) or (format == type):
#                 found.append(name)
#         return tuple(found)
#
#     def get_format(self, path):
#         """returns the format of the entry at the path"""
#         metadata = self.__class__.find_metadata(self.rootpath, path[:-1])
#         if path[-1] not in metadata[self.__class__.ENTRIES_KEY].keys():
#             raise KeyError(path[-1])
#         else:
#             entry = metadata[self.__class__.ENTRIES_KEY][path[-1]]
#             return entry[self.__class__.FORMAT_KEY][self.__class__.ENTRYTYPE_KEY]
#
#     def entry_exists(self, path, format=None):
#         """returns if an entry exists at path (with or without specified formats)."""
#         try:
#             fmt = self.get_format(path)
#             if format is None:
#                 return True
#             elif isinstance(format, str):
#                 format = (format,)
#             return fmt in format
#         except FileNotFoundError:
#             return False
#         except NotADirectoryError:
#             return False
#         except KeyError:
#             return False
#
#     def add_entry_metadata(self, parent_entrypath, child_name, format, **kwargs):
#         parent_filepath = self.__class__.as_filepath(self.rootpath, parent_entrypath)
#         parent_meta     = self.__class__.load_metadata_file(parent_filepath)
#         parent_meta[cls.ENTRIES_KEY][child_name] = self.__class__.create_entry_metadata(child_name, format, **kwargs)
#         self.__class__.store_metadata_file(parent_filepath, parent_meta)
#
#     def create_group(self, path, parents=True, ignore_existing=True):
#         """creates a data group at path. if `parents` is True, calling this also
#         creates any missing parent groups."""
#         # handle cases when an entry exists at `path`
#         if self.entry_exists(path):
#             fmt = self.get_format(path)
#             if fmt != DataFormats.Entry:
#                 parent = PathFormat.to_string(PathFormat.compute_parent(path))
#                 raise FileExistsError(f"a '{fmt}' entry with the same name already exists in '{parent}'")
#             elif ignore_existing != True:
#                 raise FileExistsError(f"group '{path[-1]}' already exists.")
#             else:
#                 # just returns
#                 return
#
#         # otherwise ensure existence of the parent entry
#         parent = path[:-1]
#         if not self.entry_exists(parent):
#             if parents != True:
#                 parent = PathFormat.to_string(parent)
#                 raise FileNotFoundError(f"parent directory '{}' not found")
#             else:
#                 self.create_group(parent, parents=True, ignore_existing=True)
#         else:
#             fmt = self.get_format(parent)
#             if fmt != DataFormats.Entry:
#                 gparent = PathFormat.to_string(PathFormat.compute_parent(parent))
#                 raise FileExistsError(f"a '{fmt}' entry with the same name already exists in '{gparent}'")
#
#         # create entry group
#         filepath = self.__class__.as_filepath(self.rootpath, path)
#         filepath.mkdir()
#         self.__class__.ensure_metadata(filepath)
#         self.add_entry_metadata(parent, path[-1], DataFormats.Entry)
#
#     @abstractmethod
#     def get_value(self, path, format=None):
#         """load a data at path (with or without specified formats)."""
#         pass
#
#     @abstractmethod
#     def set_value(self, path, value, format=None, parents=True):
#         """stores a value at path (with or without specified formats)."""
#         if (format is None) or (format == 'infer'):
#             format = infer_format(value)
#         pass
#
#     @abstractmethod
#     def get_attribute_root(self, path, create=False):
#         raise NotImplementedError("get_attribute_root")
#
# class Transaction:
#     """represents the locked state to a context."""
#     def __init__(self, context):
#         self._context = context
#         self._marked  = False
#         self._context._transaction = self
#
#     def mark(self):
#         """flags this transaction 'dirty'."""
#         self._marked  = True
#
#     def commit(self):
#         if self._marked == True:
#             self._context.writeall()
#             self._marked = False
#
#     def rollback(self):
#         if self._marked == True:
#             self._context.readall()
#             self._marked = False
#
# class __Context:
#     """abstract context structure for dealing with raw driver, apart from the
#     default database-traversal functionality.
#
#     For example, the Context object for 'dataset' will be generated
#     when one calls as follows:
#
#     ```
#     data.dataset["path/to/dataset"]
#     ```
#
#     Scope and format
#     ----------------
#
#     Each Context object works with `scope` and `format`,
#     where `format` being one of (DATASET, TABLE, IMAGE, KEYVALUE, ATTRS),
#     and `scope` being the internal path of the context within the database.
#
#     Upon retrieval/updating the objects within the context, the Context object
#     supplies the parent driver interface with its corresponding `scope` and
#     `format` info, along with the method call.
#
#     Every root instance can specify the exact implementation of `scope`.
#
#     The method calls for driver instance include:
#
#     - `children()``
#     - `read()`
#     - `write()`
#     - `subscope()`
#     - `readall()`
#     - `writeall()`
#     - `lock()`
#     - `unlock()`
#
#     Note that the transaction supports on `read` and `write` methods depend on
#     individual database implementations. Some database may be able to make all
#     reads/writes atomic during a transaction; others may only support such atomic
#     I/O for part of the formats; the rest may totally ignore transaction-related
#     features (in which cases it is encouraged to emit warnings).
#
#     Path spec and scope
#     -------------------
#
#     `root` as it is specified in Context's initializer refers to the "root object"
#     that handles the path specification.
#     In other words, if you have two different path specs in
#     your database, then the `root` object can differ depending on it.
#
#     For example, in h5py, you would write as follows:
#
#     ```
#     with h5py.File('your.hdf5', 'r') as src:
#         src["path/to/group"].attrs["attribute/path"] = "comment"
#     ```
#
#     Here the path space for `"path/to/group"` and that for `"attribute/path"`
#     is different (in a sense `src["path/to/group/attribute"].attrs["path"]` does
#     not refer to a same data).
#
#     For the former part of the path spec, `src` is the root object for the path;
#     for the latter part, `src["path/to/group"].attrs` is the root object.
#
#     On the other hand, the "path" part specifies the 'scope' of the context.
#     For example, 'scope' for `src["path/to/group"]` is `"path/to/group"`.
#
#     Instantiation of concrete data items
#     ------------------------------------
#
#     `Context` works like `pathlib.Path` objects in a sense that existence of a
#     `Context` object does not imply the existence of corresponding physical entity.
#
#     Entry creation / retrieval occurs *only when* either of `read` or `write` calls
#     occurs.
#
#     ContextManager style
#     --------------------
#
#     `with` statements can be used with or without `lock()`ing.
#     If the database is locked using this Context object, the lock object will be
#     automatically released as the procedures exits from the `with` block.
#
#     """
#
#     NONE      = 'none'
#     GROUPS    = 'create'
#     DATASET   = 'dataset'
#     TABLE     = 'table'
#     IMAGE     = 'image'
#     KEYVALUE  = 'keyvalue'
#     ATTRIBUTE = 'attrs'
#
#     available = (DATASET, TABLE, IMAGE, KEYVALUE, ATTRS)
#
#     def __init__(self, root, scope, format='none', sep=SEP, transaction=None):
#         self._root          = root
#         self._scope         = scope
#         self._format        = format
#         self._sep           = sep
#         self._transaction   = transaction
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_value, traceback):
#         if self._transaction is not None:
#             if exc_type is None:
#                 self._transaction.commit()
#             else:
#                 self._transaction.rollback()
#             self.unlock(transaction=self._transaction)
#
#     def __getitem__(self, keypath):
#         entry, key = self.resolve(keypath)
#         return entry.read()
#
#     def __setitem__(self, keypath, value):
#         entry, key = self.resolve(keypath)
#         debug(f"{self.__class__.__name__}: {repr(keypath)} <- {repr(value)}")
#         entry.write(value)
#
#     def copy(self, root=None, scope=None, format=None,
#                     sep=None, transaction=None):
#         return self.__class__(  root=self._root if root is None else root,
#                                 scope=self._scope if scope is None else scope,
#                                 format=self._format if format is None else format,
#                                 sep=self._sep if sep is None else sep,
#                                 transaction=self._transaction if transaction is None else transaction)
#
#     def keys(self):
#         """retrieves list of names associated with this format directly within this context."""
#         return self._root.children(contexts=[self], keys=True, values=False)
#
#     def values(self):
#         """retrieves list of values associated with this format directly within this context."""
#         return self._root.children(contexts=[self], keys=False, values=False)
#
#     def items(self):
#         return self._root.children(contexts=[self], keys=True, value=True)
#
#     def read(self, contexts=None):
#         """retrieve the content of this context."""
#         if contexts is None:
#             contexts = [self]
#         else:
#             contexts.append(self)
#         return self._root.read(contexts=contexts)
#
#     def write(self, value, contexts=None):
#         """updates the content of this context."""
#         if contexts is None:
#             contexts = [self]
#         else:
#             contexts.append(self)
#         return self._root.write(value, contexts=contexts)
#
#     def readall(self, contexts=None):
#         if contexts is None:
#             contexts = [self]
#         else:
#             contexts.append(self)
#         return self._root.readall(contexts=contexts)
#
#     def writeall(self, contexts=None):
#         if contexts is None:
#             contexts = [self]
#         else:
#             contexts.append(self)
#         return self._root.writeall(contexts=contexts)
#
#     def subscope(self, key, contexts=None):
#         """retrieves the existing sub-scope structure for the format within this context."""
#         if contexts is None:
#             contexts = [self]
#         else:
#             contexts.append(self)
#         scope = self._root.subscope(key, contexts=contexts)
#         return self.copy(scope=scope)
#
#     def resolve(self, path):
#         context = self
#         keypath = path.split(self._sep)
#         # traverse to the deepest entry
#         for key in keypath:
#             entry = context.subscope(key)
#         return entry
#
#     def lock(self, contexts=None):
#         """should return a Context that is locked."""
#         if contexts is None:
#             contexts = [self]
#         else:
#             contexts.append(self)
#         return self.copy(transaction=self._root.lock(contexts=contexts))
#
#     def unlock(self, transaction=None, contexts=None):
#         """free resources for new transactions."""
#         if contexts is None:
#             contexts = [self]
#             if self._transaction:
#                 transaction = self._transaction
#         else:
#             contexts.append(self)
#             if (not transaction) and (self._transaction):
#                 transaction = self._transaction
#         self._transaction = self._root.unlock(transaction=transaction, contexts=contexts)
#
#
# class AttributeManager:
#     """interface for editing entry attributes."""
#     def __init__(self, interface):
#         self._interface = interface
#         self._updating  = False
#         self._dirtyflag = False
#
#     def lock(self):
#         if self._updating == True:
#             return False
#         self._updating  = True
#         return True
#
#     def flag(self):
#         if self._updating  == True:
#             self._dirtyflag = True
#         else:
#             self._interface._store_info()
#
#     def commit(self):
#         self._updating  = False
#         if self._dirtyflag == True:
#             self._interface._store_info()
#             self._dirtyflag = False
#
#     def rollback(self):
#         self._updating   = False
#         if self._dirtyflag == True:
#             self._interface._load_info()
#             self._dirtyflag = False
#
#     def keys(self):
#         return self._interface._info.keys()
#
#     def values(self):
#         return self._interface._info.values()
#
#     def items(self):
#         return self._interface._info.items()
#
#     def __getitem__(self, keypath):
#         entry, key = self.__resolve_keypath(keypath, create=False)
#         return entry[key]
#
#     def __setitem__(self, keypath, value):
#         entry, key = self.__resolve_keypath(keypath, create=True)
#         entry[key] = value
#         debug(f"AttributeManager: {repr(keypath)} <- {repr(value)}")
#         self.flag()
#
#     def __delitem__(self, keypath):
#         entry, key = self.__resolve_keypath(keypath, create=False)
#         del entry[key]
#         debug(f"AttributeManager: `rm` {repr(keypath)}")
#         self.flag()
#
#     def __resolve_keypath(self, keypath, create=True):
#         entry   = self._interface._info
#         keypath = keypath.split(SEP)
#         # traverse to the deepest entry
#         for key in keypath[:-1]:
#             if key not in entry.keys():
#                 if create == True:
#                     entry[key] = _OrderedDict()
#                 else:
#                     raise KeyError(key)
#             entry = entry[key]
#         return entry, keypath[-1]
#
# class EntryCreation:
#     """utility interface for creating entries. called as AbstractInterface.create[<keypath>]."""
#     def __init__(self, interface):
#         self._interface = interface
#
#     def __getitem__(self, keypath):
#         entry, key = self._interface.resolve_path(keypath, create=True)
#         if key in entry.dataset_names():
#             return entry.get_dataset(key)
#         else:
#             return entry.get_entry(key, create=True)
#
#     def __setitem__(self, keypath, value):
#         raise NotImplementedError("use AbstractInterface[<keypath>] to modify entries/datasets")
#
# class EntryRetrieval:
#     """utility interface for retrieving entries. called as AbstractInterface.get[<keypath>]."""
#     def __init__(self, interface):
#         self._interface = interface
#
#     def __getitem__(self, keypath):
#         entry, key = self._interface.resolve_path(keypath, create=False)
#         if key in entry.dataset_names():
#             return entry.get_dataset(key)
#         elif key in entry.child_names():
#             return entry.get_entry(key, create=False)
#         else:
#             raise KeyError(key)
#
#     def __setitem__(self, keypath, value):
#         raise NotImplementedError("use AbstractInterface[<keypath>] to modify entries/datasets")
#
# class AbstractInterface:
#     """base class that provides common functionality.
#
#     The default implementation is:
#
#     - it uses the JSON-format file for storing dataset information.
#     - it uses the directory structure to organize entry hierarchy.
#
#     Subclasses must implement (at minimum):
#
#     - `_data_suffix`: to distinguish dataset file from the other child entries.
#     - `_load_child_dataset`: to deserialize datasets into numpy.ndarrays.
#     - `_store_child_dataset`: to serialize numpy.ndarrays.
#     - `_delete_child_dataset`: to remove datasets from the storage.
#
#     If the subclasses intend to use the structure other than the file system,
#     they must implement the other methods:
#
#     - `_open_root_repr`
#     - `_free_root_repr`
#     - `_get_volatile_repr`
#     - `_list_contents`
#     - `_load_info`
#     - `_store_info`
#     - `_delete_info`
#     - `_store_child_entry`:
#     - `_delete_child_entry`:
#     """
#     _info_suffix = ".json"
#     _data_suffix = None
#     _byteorders = {
#         '<': 'little',
#         '>': 'big',
#         '=': _sys.byteorder,
#         '|': 'NA'
#     }
#
#     @classmethod
#     def _open_root_repr(cls, rootpath):
#         """initializes the physical representation of the root at `rootpath`.
#         returns (new, obj) tuple, where `new` indicates whether the root `obj`
#         is newly created."""
#         raise NotImplementedError("_root_repr")
#
#     @classmethod
#     def _free_root_repr(cls, rootrepr):
#         raise NotImplementedError("_free_root_repr")
#
#     @abstractmethod
#     def _get_volatile_repr(self, parent, name):
#         """creates `self`'s physical representation based on
#         `parent` and `name` information.
#
#         note that `parent` may not have a justified type.
#         if `parent` is None, it implies that this is the root entry."""
#         pass
#
#     @abstractmethod
#     def _load_info(self):
#         """reads info from the existing physical representation,
#         and stores it in the instance's `info` attribute."""
#         pass
#
#     @abstractmethod
#     def _store_info(self):
#         """writes the current `info` attribute to its physical
#         representation."""
#         pass
#
#     @abstractmethod
#     def _delete_info(self):
#         """deletes the information for the entry.
#         this is only supposed to occur during the deletion of the entry itself."""
#         pass
#
#     @abstractmethod
#     def _list_contents(self, children=True, datasets=True):
#         """returns the contents of the entry as a sequence."""
#         pass
#
#     @abstractmethod
#     def _get_child_entry(self, name):
#         """tries to get the specified child entry in this entry.
#         it returns the corresponding AbstractInterface object."""
#         pass
#
#     @abstractmethod
#     def _delete_child_entry(self, name, child):
#         """remove the child entry `child` that has `name`."""
#         pass
#
#     @abstractmethod
#     def _load_child_dataset(self, name):
#         """tries to get the specified child dataset in this entry.
#         it must return the corresponding numpy.ndarray object."""
#         pass
#
#     @abstractmethod
#     def _store_child_dataset(self, name, value):
#         """store `value` with the specified `name`."""
#         pass
#
#     @abstractmethod
#     def _delete_child_dataset(self, name):
#         """remove the dataset that has `name`."""
#         pass
#
#     @abstractmethod
#     def _load_child_dict(self, name):
#         pass
#
#     @abstractmethod
#     def _store_child_dict(self, name):
#         pass
#
#     @abstractmethod
#     def _delete_child_dict(self, name):
#         pass
#
#     @classmethod
#     def open(cls, rootpath, **kwargs):
#         """returns the 'root' entry (that has different terminology)."""
#         root                = cls("", parent=None)
#         created, root._repr = cls._open_root_repr(rootpath)
#         root._root          = root._repr
#         root._path          = ''
#         if not created:
#             root._load_info()
#
#         def _update(src):
#             return root.__class__._copy_from_another_root(src=src, dest=root)
#         def _close():
#             return root.__class__.close(root)
#         root.update = _update
#         root.close  = _close
#         if len(kwargs) > 0:
#             for key, value in kwargs:
#                 setattr(root, key, value)
#         return root
#
#     @classmethod
#     def close(cls, rootobj=None):
#         """free the physical representation of this root object."""
#         if not rootobj.is_root():
#             raise ValueError("close() not applied to the root object")
#         else:
#             cls._free_root_repr(rootobj._repr)
#             rootobj.invalidate()
#
#     @classmethod
#     def _copy_from_another_root(cls, src=None, dest=None):
#         if (not src.is_root()) or (not dest.is_root()):
#             raise ValueError("invalid call to copy()")
#         for name, value in src.items():
#             dest[name] = value
#
#     def __init__(self, name, parent=None):
#         """creates (ensures) the directory with the matched name.
#         `parent` must be either None (root) or the instance of the same classs.
#
#         `info` will be only used when """
#         if (len(name.strip()) == 0) and (parent is not None):
#             raise ValueError("entry name cannot be empty")
#         if SEP in name:
#             if parent is None:
#                 raise ValueError("use of path is not allowed for the root name")
#             else:
#                 comps = name.split(SEP)
#                 parent = self.__class__(SEP.join(comps[:-1]), parent=parent)
#                 name   = comps[-1]
#
#         self._name  = name
#         self._info  = _OrderedDict()
#         self._parent= parent
#         if parent is not None:
#             self._root  = parent._root
#             self._repr  = self._get_volatile_repr(parent, name)
#             self._path  = f"{parent._path}{SEP}{name}"
#             self._load_info()
#         self.attrs  = AttributeManager(self)
#         self._valid = True
#
#     def __repr__(self):
#         if self._valid == True:
#             return f"{self.__class__.__name__}({repr(str(self._root))})[{repr(str(self._path))}]"
#         else:
#             return f"{self.__class__.__name__}(#invalid)"
#
#     def __getattr__(self, name):
#         if name == 'create':
#             return EntryCreation(self)
#         elif name == 'get':
#             return EntryRetrieval(self)
#         else:
#             return super().__getattr__(name)
#
#     def __getitem__(self, keypath):
#         entry, key = self.resolve_path(keypath, create=False)
#         if key in entry.child_names():
#             return entry.get_entry(key, create=False)
#         elif key in entry.dataset_names():
#             return entry.get_dataset(key)
#         else:
#             raise KeyError(key)
#
#     def __setitem__(self, keypath, value):
#         if (not isinstance(value, (AbstractInterface, _np.ndarray))) \
#             and (not is_namedtuple_struct(value)) and (not is_mapping(value)):
#             raise ValueError(f"stappy only accepts entry-types, numpy arrays, array-based named tuples, or mappings, but got {value.__class__}")
#         entry, key = self.resolve_path(keypath, create=True)
#         if isinstance(value, AbstractInterface):
#             entry.put_entry(key, value)
#         elif isinstance(value, _np.ndarray):
#             entry.put_dataset(key, value)
#         elif is_namedtuple_struct(value):
#             entry.put_namedtuple_struct(key, value)
#         elif is_mapping(value):
#             entry.put_dict(key, value)
#         else:
#             raise RuntimeError("fatal error: class assertion failed")
#
#     def __delitem__(self, keypath):
#         entry, key = self.resolve_path(keypath, create=False)
#         if key in entry.child_names():
#             # entry
#             entry.delete_entry(key)
#         elif key in entry.dataset_names():
#             # dataset
#             entry.delete_dataset(key)
#         else:
#             raise KeyError(key)
#
#     def __contains__(self, keypath):
#         try:
#             entry, key = self.resolve_path(keypath, create=False)
#         except KeyError:
#             return False
#         try:
#             entry = entry.get_entry(key, create=False)
#             return True
#         except NameError:
#             return False
#
#     def resolve_path(self, keypath, create=True):
#         """returns (dparent, key), where `dparent` indicates the
#         direct parent of the value specified by `keypath`."""
#         keys = keypath.split(SEP)
#         entry = self
#         for key in keys[:-1]:
#             entry = entry.get_entry(key, create=create)
#         return entry, keys[-1]
#
#     def invalidate(self):
#         """makes this object invalid as a reference."""
#         self._name   = None
#         self._root   = None
#         self._parent = None
#         self._info   = None
#         self._repr   = None
#         self._valid  = False
#
#     def is_root(self):
#         return (self._parent is None)
#
#     def keys(self):
#         """returns a sequence of names of children (child entries and datasets irrelevant)."""
#         return self._list_contents(children=True, datasets=True)
#
#     def child_names(self):
#         """returns a sequence of its child entries."""
#         return self._list_contents(children=True, datasets=False)
#
#     def dataset_names(self):
#         """returns a sequence of datasets that this entry contains."""
#         return self._list_contents(children=False, datasets=True)
#
#     def values(self):
#         """returns a generator of children (entries and datasets)."""
#         for name in self.keys():
#             yield self.__getitem__(name)
#
#     def children(self):
#         for name in self.child_names():
#             yield self.get_entry(name, create=False)
#
#     def datasets(self):
#         for name in self.dataset_names():
#             yield self.get_dataset(name)
#
#     def items(self):
#         for name in self.keys():
#             yield name, self.__getitem__(name)
#
#     def get_entry(self, name, create=True):
#         """returns the specified child entry.
#         if `create` is True and the entry does not exist,
#         the entry is newly generated before being returned."""
#         if name in self.child_names():
#             entry = self._get_child_entry(name)
#         else:
#             if create == False:
#                 raise NameError(f"name not found: {name}")
#             entry = self.__class__(name, parent=self)
#         return entry
#
#     def put_entry(self, name, entry, overwrite=True, deletesource=False):
#         """puts `entry` to this entry with `name`."""
#         if name in self.child_names():
#             if overwrite == False:
#                 raise NameError(f"entry '{name}' already exists")
#             else:
#                 self.delete_entry(name)
#
#         # copy recursively
#         child = self.get_entry(name, create=True)
#         child._info.update(entry._info)
#         for dataname in entry.dataset_names():
#             child.put_dataset(dataname, entry.get_dataset(dataname))
#         for grandchild in entry.child_names():
#             child.put_entry(grandchild, entry.get_entry(grandchild))
#         child._store_info()
#
#         if deletesource == True:
#             if entry._parent is None:
#                 # TODO: remove the root file
#                 pass
#             else:
#                 entry._parent.delete_entry(entry.name)
#
#     def delete_entry(self, name):
#         """deletes a child entry with 'name' from this entry."""
#         if name not in self.child_names():
#             raise NameError(f"entry '{name}' does not exist")
#         child = self.get_entry(name, create=False)
#
#         # deletes grandchildren recursively
#         for dataname in child.dataset_names():
#             child.delete_dataset(dataname)
#         for grandchild in child.child_names():
#             child.delete_entry(grandchild)
#         child._delete_info()
#
#         self._delete_child_entry(name, child)
#         child.invalidate()
#
#     def get_dataset(self, name):
#         """returns the dataset with the specified name."""
#         if name not in self.dataset_names():
#             raise NameError(f"dataset not found: {name}")
#         data = self._load_child_dataset(name)
#         locked = self.attrs.lock()
#         self.attrs[f"{name}/dtype"]       = str(data.dtype)
#         self.attrs[f"{name}/shape"]       = data.shape
#         self.attrs[f"{name}/byteorder"]   = self._byteorders[data.dtype.byteorder]
#         if locked == True:
#             self.attrs.commit()
#         return data
#
#     def put_dataset(self, name, value, overwrite=True):
#         """puts `value` to this entry with `name`."""
#         if name in self.dataset_names():
#             if overwrite == False:
#                 raise NameError(f"the dataset '{name}' already exists")
#             else:
#                 self.delete_dataset(name)
#         self._store_child_dataset(name, value)
#         locked = self.attrs.lock()
#         self.attrs[f"{name}/dtype"] = str(value.dtype)
#         self.attrs[f"{name}/shape"] = value.shape
#         self.attrs[f"{name}/byteorder"]   = self._byteorders[value.dtype.byteorder]
#         if locked == True:
#             self.attrs.commit()
#
#     def put_namedtuple_struct(self, name, value, overwrite=True):
#         if not is_namedtuple_struct(value):
#             raise ValueError(f"not conforming to the 'named-tuple structure': {value.__class__}")
#         if name in self.child_names():
#             if overwrite == False:
#                 raise NameError(f"the entry '{name}' already exists")
#             else:
#                 self.delete_entry(name)
#         entry = self.get_entry(name, create=True)
#         locked = entry.attrs.lock()
#         entry.attrs["type"] = value.__class__.__name__
#         for field in value._fields:
#             item = getattr(value, field)
#             if isinstance(item, INFO_TYPES):
#                 entry.attrs[field] = item
#             else:
#                 # must be np.ndarray b/c of is_namedtuple_struct() impl
#                 entry.put_dataset(field, item, overwrite=True)
#         if locked == True:
#             entry.attrs.commit()
#
#     def delete_dataset(self, name):
#         """deletes a child dataset with 'name' from this entry."""
#         self._delete_child_dataset(name)
#
#         locked = self.attrs.lock()
#         del self.attrs[f"{name}/dtype"]
#         del self.attrs[f"{name}/shape"]
#         del self.attrs[f"{name}/byteorder"]
#         if locked == True:
#             self.attrs.commit()
#
#     def get_dict(self, name):
#         return self._load_child_dict(name)
#
#     def put_dict(self, name, value):
#         if not isinstance(value, (dict, _OrderedDict)): # FIXME: how to check if it is mapping type?
#             raise ValueError(f"expected dict, got '{value.__class__}'")
#         self._store_child_dict(name, value)
#
#     def delete_dict(self, name):
#         self._delete_child_dict(name)
#
# class FileSystemInterface(AbstractInterface):
#     """base class that provides a file system-based data access.
#
#     it proides implementations for some abstract functions in AbstractInterface:
#
#     - `open`
#     - `_get_volatile_repr`
#     - `_list_contents`
#     - `_load_info`
#     - `_store_info`
#     - `_delete_info`
#     - `_store_child_entry`
#     - `_delete_child_entry`
#     - `_delete_child_dataset`
#
#     subclasses still needs to implement the following methods:
#
#     - `_data_suffix`: to distinguish dataset file from the other child entries.
#     - `_load_child_dataset`: to deserialize datasets into numpy.ndarrays.
#     - `_store_child_dataset`: to serialize numpy.ndarrays.
#
#     """
#
#     _meta_base   = "entry_metadata"
#     _info_suffix = ".json"
#     _data_suffix = None
#
#     def _datafile(self, name):
#         return self._repr / f"{name}{self._data_suffix}"
#
#     def _get_volatile_repr(self, parent, name):
#         if parent is None:
#             # root; necessary paths must have been already initialized
#             return
#         else:
#             file = _pathlib.Path(parent._repr) / name
#
#         if file.is_file():
#             raise FileExistsError("cannot create another entry (file in place of directory)")
#         if not file.exists():
#             file.mkdir()
#             debug(f"FileSystemInterface._get_volatile_repr: created '{name}' under '{str(parent)}'")
#         return file
#
#     def _load_child_dict(self, name):
#         dictfile = self._repr / f"{name}.json"
#         if not dictfile.exists():
#             raise FileNotFoundError(str(dictfile))
#         else:
#             with open(dictfile, 'r') as src:
#                 return _json.load(src, object_hook=_OrderedDict)
#
#     def _store_child_dict(self, name, value):
#         dictfile = self._repr / f"{name}.json"
#         with open(dictfile, 'w') as out:
#             _json.dump(value, out, indent=4)
#
#     def _delete_child_dict(self, name):
#         dictfile = self._repr / f"{name}.json"
#         if dictfile.exists():
#             dictfile.unlink()
#
#     def _load_info(self):
#         infofile = self._repr / f"{self._meta_base}{self._info_suffix}"
#         if not infofile.exists():
#             debug(f"FileSystemInterface._load_info: {repr(str(infofile))} was not found; leave the info empty.")
#             self._info = _OrderedDict()
#         else:
#             with open(infofile, 'r') as info:
#                 self._info = _json.load(info, object_hook=_OrderedDict)
#             debug(f"FileSystemInterface._load_info: loaded from '{self._name}': '{self._info}'")
#
#     def _store_info(self):
#         if len(self._info) > 0:
#             with open(self._repr / f"{self._meta_base}{self._info_suffix}", 'w') as out:
#                 _json.dump(self._info, out, indent=4)
#             debug(f"FileSystemInterface._store_info: stored into '{self._name}': '{self._info}'")
#
#     def _delete_info(self):
#         infofile = self._repr / f"{self._meta_base}{self._info_suffix}"
#         if not infofile.exists():
#             return
#         else:
#             infofile.unlink()
#
#     def _list_contents(self, children=True, datasets=True):
#         _listed = []
#         for path in self._repr.iterdir():
#             if path.name.startswith('.'):
#                 # hidden
#                 pass
#             elif path.suffix == self._info_suffix:
#                 # info
#                 pass
#             elif path.suffix == self._data_suffix:
#                 # data
#                 if datasets == True:
#                     _listed.append(path.stem)
#             else:
#                 # child
#                 if children == True:
#                     _listed.append(path.name)
#         return tuple(_listed)
#
#     def _get_child_entry(self, name):
#         return self.__class__(name, parent=self)
#
#     def _delete_child_entry(self, name, child):
#         child._repr.rmdir()
#
#     @abstractmethod
#     def _load_child_dataset(self, name):
#         """tries to get the specified child dataset in this entry.
#         it must return the corresponding numpy.ndarray object."""
#         pass
#
#     @abstractmethod
#     def _store_child_dataset(self, name, value):
#         """stores `value` with the specified `name` (with appropriate
#         suffix, if you use the `_data_suffix` functionality)."""
#         pass
#
#     def _delete_child_dataset(self, name):
#         """removes the dataset that has `name` (with appropriate suffix,
#         if you use the `_data_suffix` functionality)."""
#         self._datafile(name).unlink()
#
#     @classmethod
#     def _open_root_repr(cls, rootpath):
#         rootrepr = _pathlib.Path(rootpath)
#         if not rootrepr.exists():
#             created = True
#             rootrepr.mkdir(parents=True)
#         else:
#             created = False
#         return created, rootrepr
#
#     @classmethod
#     def _free_root_repr(cls, rootrepr):
#         pass
#
#
# class NPYInterface(FileSystemInterface):
#     _data_suffix = '.npy'
#
#     def __init__(self, name, parent=None):
#         super().__init__(name, parent=parent)
#
#     def _load_child_dataset(self, name):
#         data = _np.load(str(self._datafile(name)))
#         return data
#
#     def _store_child_dataset(self, name, value):
#         _np.save(str(self._datafile(name)), value)
#
# class BareZInterface(FileSystemInterface):
#     _data_suffix = ".zarr"
#     _default_compression_level = 6
#     compression_level = None
#
#     def __init__(self, name, parent=None):
#         super().__init__(name, parent=parent)
#         if parent is not None:
#             if hasattr(parent, 'compression_level'):
#                 self.compression_level = parent.compression_level
#         if self.compression_level is None:
#             self.compression_level = self._default_compression_level
#
#     def _load_child_dataset(self, name):
#         dtype = _np.dtype(self.attrs[f"{name}/dtype"])
#         shape = self.attrs[f"{name}/shape"]
#         file  = str(self._datafile(name))
#         with open(file, 'rb') as src:
#             binary = _zlib.decompress(src.read())
#         return _np.frombuffer(binary, dtype=dtype).reshape(shape, order='C')
#
#     def _store_child_dataset(self, name, value):
#         self.attrs[f"{name}/compression"] = 'zlib'
#         with open(self._datafile(name), 'wb') as dst:
#             dst.write(_zlib.compress(value.tobytes(order='C'), level=self.compression_level))
