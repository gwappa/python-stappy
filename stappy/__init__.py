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

import pathlib as _pathlib
import json as _json
from collections import OrderedDict as _OrderedDict
from functools import wraps

import numpy as _np

VERSION_STR = "0.0.1"

SEP = '/'
DEBUG = True

def debug(msg):
    if DEBUG == True:
        print(f"[DEBUG] {msg}")

class DELETE_KEY:
    """used to delete a key in the `info` dictionary."""
    pass

def abstractmethod(meth):
    @wraps(meth)
    def __invalid_call__(self, *args, **kwargs):
        raise NotImplementedError(meth.__name__)
    return __invalid_call__

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

    - `_root_repr`
    - `_get_repr`
    - `_list_contents`
    - `_load_info`
    - `_store_info`
    - `_delete_info`
    - `_store_child_entry`:
    - `_delete_child_entry`:
    """
    _info_suffix = ".json"
    _data_suffix = None

    @abstractmethod
    def _get_repr(self, parent, name):
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

    @classmethod
    def open(cls, rootpath):
        """returns the 'root' entry (that has different terminology)."""
        root                = cls("", parent=None)
        created, root._repr = cls._root_repr(rootpath)
        root._root          = root._repr
        if not created:
            root.load_info()
        return root

    @classmethod
    def _root_repr(cls, rootpath):
        """initializes the physical representation of the root at `rootpath`.
        returns (new, obj) tuple, where `new` indicates whether the root `obj`
        is newly created."""
        raise NotImplementedError("_root_repr")

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
            self._repr  = self._get_repr(parent, name)
            self.load_info()
        self._valid = True

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self._repr)})@<{str(self._root)}>"

    def __getitem__(self, keypath):
        keys = keypath.split(SEP)
        entry = self
        for key in keys[:-1]:
            if key in entry.children():
                entry = entry.get_entry(key, create=False)
            else:
                raise KeyError(key)
        key = keys[-1]
        if key in entry.children():
            return entry.get_entry(key, create=False)
        elif key in entry.datasets():
            return entry.get_dataset(key)
        else:
            raise KeyError(key)

    def invalidate(self):
        """makes this object invalid as a reference."""
        self._name   = None
        self._root   = None
        self._parent = None
        self._info   = None
        self._repr   = None
        self._valid  = False

    def children(self):
        """returns a sequence of its child entries."""
        return self._list_contents(children=True, datasets=False)

    def datasets(self):
        """returns a sequence of datasets that this entry contains."""
        return self._list_contents(children=False, datasets=True)

    def update_info(self, keypath, value, write=True):
        """updates the `info` attribute and overwrite the stored data.
        `keypath` may be split by '/' for describing its hierarchy.

        use `astore.DELETE_KEY` in place of `value` when you want to delete
        value(s) at `keypath`.

        if write is True (default), the updated `info` may be immediately
        written to its storage."""

        entry   = self._info
        keypath = keypath.split(SEP)
        # traverse to the deepest entry
        for key in keypath[:-1]:
            if key not in entry.keys():
                entry[key] = _OrderedDict()
            entry = entry[key]
        if value == DELETE_KEY:
            del entry[keypath[-1]]
            debug(f"AbstractInterface._update_info: `rm` {repr(keypath)}")
        else:
            entry[keypath[-1]] = value
            debug(f"AbstractInterface._update_info: {repr(keypath)} <- {repr(value)}")
        if write == True:
            self.store_info()

    def get_entry(self, name, create=True):
        """returns the specified child entry.
        if `create` is True and the entry does not exist,
        the entry is newly generated before being returned."""
        if name in self.children():
            entry = self._get_child_entry(name)
        else:
            if create == False:
                raise NameError(f"name not found: {name}")
            entry = self.__class__(name, parent=self)
        return entry

    def put_entry(self, name, entry, overwrite=True, deletesource=False):
        """puts `entry` to this entry with `name`."""
        if name in self.children():
            if overwrite == False:
                raise NameError(f"entry '{name}' already exists")
            else:
                self.delete_entry(name)

        # copy recursively
        child = self.get_entry(name, create=True)
        child._info.update(entry._info)
        for dataname in entry.datasets():
            child.put_dataset(dataname, entry.get_dataset(dataname))
        for grandchild in entry.children():
            child.put_entry(grandchild, entry.get_entry(grandchild))
        child.store_info()

        if deletesource == True:
            if entry._parent is None:
                # TODO: remove the root file
                pass
            else:
                entry._parent.delete_entry(entry.name)

    def delete_entry(self, name):
        """deletes a child entry with 'name' from this entry."""
        if name not in self.children():
            raise NameError(f"entry '{name}' does not exist")
        child = self.get_entry(name, create=False)

        # deletes grandchildren recursively
        for dataname in entry.datasets():
            child.delete_dataset(dataname)
        for grandchild in child.children():
            child.delete_entry(grandchild)
        child._delete_info()

        self._delete_child_entry(name, child)
        child.invalidate()

    def get_dataset(self, name):
        """returns the dataset with the specified name."""
        if name not in self.datasets():
            raise NameError(f"dataset not found: {name}")
        return self._load_child_dataset(name)

    def put_dataset(self, name, value, overwrite=True):
        """puts `value` to this entry with `name`."""
        if name in self.datasets():
            if overwrite == False:
                raise NameError(f"the dataset '{name}' already exists")
            else:
                self.delete_dataset(name, writeinfo=False)
        self._store_child_dataset(name, value)
        self._update_info(f".datasets/{name}/dtype", str(value.dtype), write=False)
        self._update_info(f".datasets/{name}/shape", value.shape, write=True)

    def delete_dataset(self, name, writeinfo=True):
        """deletes a child dataset with 'name' from this entry."""
        self._delete_child_dataset(name)
        self._update_info(f".datasets/{name}/dtype", DELETE_KEY, write=writeinfo)
        self._update_info(f".datasets/{name}/shape", DELETE_KEY, write=writeinfo)

class FileSystemInterface(AbstractInterface):
    """base class that provides a file system-based data access.

    it proides implementations for some abstract functions in AbstractInterface:

    - `open`
    - `_get_repr`
    - `_list_contents`
    - `_load_info`
    - `_store_info`
    - `_delete_info`
    - `_store_child_entry`
    - `_delete_child_entry`

    subclasses still needs to implement the following methods:

    - `_data_suffix`: to distinguish dataset file from the other child entries.
    - `_load_child_dataset`: to deserialize datasets into numpy.ndarrays.
    - `_store_child_dataset`: to serialize numpy.ndarrays.
    - `_delete_child_dataset`: to remove datasets from the storage.

    """

    _info_suffix = ".json"
    _data_suffix = None

    def _get_repr(self, parent, name):
        if parent is None:
            # root; necessary paths must have been already initialized
            return
        else:
            file = _pathlib.Path(parent._repr) / name

        if file.is_file():
            raise FileExistsError("cannot create another entry (file in place of directory)")
        if not file.exists():
            file.mkdir()
            debug(f"AbstractInterface._get_repr: created '{name}' under '{str(parent)}'")
        return file

    def _load_info(self):
        infofile = self._repr / f"entryinfo{self._info_suffix}"
        if not infofile.exists():
            debug(f"FileSystemInterface._load_info: {repr(str(infofile))} was not found; leave the info empty.")
            self._info = _OrderedDict()
        else:
            with open(infofile, 'r') as info:
                self._info = _json.load(info, object_hook=_OrderedDict)
            debug(f"FileSystemInterface._load_info: loaded from '{self._name}': '{self._info}'")

    def _store_info(self):
        if len(self._info) > 0:
            with open(self._repr / f"entryinfo{self._info_suffix}", 'w') as out:
                _json.dump(self._info, out, indent=4)
            debug(f"FileSystemInterface._store_info: stored into '{self._name}': '{self._info}'")

    def _delete_info(self):
        infofile = self._repr / f"entryinfo{self._info_suffix}"
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

    @abstractmethod
    def _delete_child_dataset(self, name):
        """removes the dataset that has `name` (with appropriate suffix,
        if you use the `_data_suffix` functionality)."""
        pass

    @classmethod
    def _root_repr(cls, rootpath):
        rootrepr = _pathlib.Path(rootpath)
        if not rootrepr.exists():
            created = True
            rootrepr.mkdir(parents=True)
        else:
            created = False
        return created, rootrepr


class NPYInterface(FileSystemInterface):
    _data_suffix = '.npy'

    def __init__(self, name, parent=None):
        super().__init__(name, parent=parent)

    def __datafile(self, name):
        return self._repr / f"{name}.npy"

    def _load_child_dataset(self, name):
        data = _np.load(str(self.__datafile(name)))
        self._update_info(f".datasets/{name}/dtype", str(data.dtype), write=False)
        self._update_info(f".datasets/{name}/shape", data.shape, write=True)
        return data

    def _store_child_dataset(self, name, value):
        _np.save(str(self.__datafile(name)), value)

    def _delete_child_dataset(self, name):
        self.__datafile(name).unlink()
