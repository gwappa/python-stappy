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

"""implementation of core structure: Protocol, Entry and AttributeEntry."""

from .utils import assert_type as _assert_type

VALID_TYPES = ('dataset', 'table', 'keyvalue', 'image', 'binary')
SEP         = '/'
ATTRSEP     = '/'

__valid_slots = tuple('as_'+typ for typ in VALID_TYPES)

def __slot_to_datatype(slot):
    return slot[3:]

def check_datatype(datatype, value):
    return value

class Protocol(object):
    """the base class for the database protocol."""
    name = "NotSpecified"

    @classmethod
    def accepts_path(cls, uri):
        return True

    @classmethod
    def open(cls, uri, mode='r', **kwargs):
        return cls(uri, mode, **kwargs)

    def __init__(self, uri, mode='r', **kwargs):
        if mode not in ('r', 'w', 'r+'):
            raise ValueError("unknown mode: "+mode)
        self._uri  = uri
        self._mode = mode

    def __repr__(self):
        modestr = '<read-only>' if self._mode == 'r' else ''
        return "{cls}{info}('{file}')".format(cls=self.name,
                        file=self._uri,
                        info=modestr)

    def __typeof__(self, path):
        raise NotImplementedError("cannot read data type using this protocol")

    def __readdata__(self, path, type, **kwargs):
        raise NotImplementedError("cannot read data using this protocol")

    def __writedata__(self, path, type, value, **kwargs):
        raise NotImplementedError("cannot write data using this protocol")

    def __removedata__(self, path, **kwargs):
        raise NotImplementedError("cannot remove data using this protocol")

    def __readattr__(self, entry, path, type, **kwargs):
        raise NotImplementedError("cannot read attributes using this protocol")

    def __writeattr__(self, entry, path, type, value, **kwargs):
        raise NotImplementedError("cannot write attributes using this protocol")

    def __removeattr__(self, entry, path, **kwargs):
        raise NotImplementedError("cannot remove attributes using this protocol")


class Entry(object):
    """an abstraction of a path inside a database."""

    def __init__(self, root, path):
        self._root = root
        self._path = path

    def __repr__(self):
        return repr(self._root)+"[\"{path}\"]".format(path=self.path)

    def __getattr__(self, name):
        """for retrieval of unsettable attributes"""
        if name in ('root', 'path'):
            return getattr(self, '_'+name)
        elif name == 'attrs':
            return AttributeEntry(self, '')
        elif name == 'type':
            return self._root._proto.__typeof__(self._path)
        elif name in __valid_slots:
            datatype = __slot_to_datatype(name)
            return self._root._proto.__readdata__(self._path, datatype)
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        """for updating the identity of this entry."""
        if name.startswith('_'):
            super(Entry, self).__setattr__(name, value)
        elif name in __valid_slots:
            datatype = __slot_to_datatype(name)
            self._root._proto.__writedata__(self._path, datatype,
                                    check_datatype(datatype, value))
        else:
            raise AttributeError("cannot set attribute '{name}'".format(name=name))

    def is_root(self):
        return (self._path == SEP)

    def immediate_child(self, child_name):
        return Entry(self._root, self._path + SEP + child_name)

    def __getitem__(self, path):
        """returns its child entry."""
        if len(path) == 0:
            # this entry itself
            return self
        elif path.startswith(SEP):
            # absolute reference
            return self._root[path[1:]]
        else:
            try:
                seppos           = path.index(SEP)
                if seppos == len(path)-1:
                    # only one component in path
                    return self.immediate_child(path)
                child_name, rest = path[:seppos], path[(seppos+1):]
                return self.immediate_child(child_name)[rest]
            except ValueError:
                # no separator inside path
                return self.immediate_child(path)

    def __setitem__(self, path, entry):
        _assert_type(entry, Entry)
        raise NotImplementedError

class RootEntry(Entry):
    """an abstraction of the root entry of a database.
    Its `root` attribute equals to itself, and
    its `path` is always '/'.
    """

    def __init__(self, proto=None):
        Entry.__init__(self, self, SEP)
        self._proto = proto

    def __repr__(self):
        return repr(self._proto) if self._proto is not None else "<NA>"

    def immediate_child(self, child_name):
        return Entry(self, SEP + child_name)

class AttributeEntry(object):
    """an abstraction of an attribute entry."""

    def __init__(self, parent, path):
        self._parent = parent
        self._path   = path

    def __getattr__(self, name):
        if name in ('parent', 'path'):
            return getattr(self, '_'+name)
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super(AttributeEntry, self).__setattr__(name, value)
        elif name == 'value':
            raise NotImplementedError("setting value to an attribute")
        else:
            raise AttributeError("cannot set attribute '{name}'".format(name=name))

    def immediate_child(self, child_name):
        return AttributeEntry(self._parent, self._path + ATTRSEP + child_name)

    def __getitem__(self, path):
        """returns its child entry."""
        if len(path) == 0:
            # this entry itself
            return self
        elif path.startswith(ATTRSEP):
            return self.__getitem__(path[1:])
        else:
            try:
                seppos           = path.index(ATTRSEP)
                if seppos == len(path)-1:
                    # only one component in path
                    return self.immediate_child(path)
                child_name, rest = path[:seppos], path[(seppos+1):]
                return self.immediate_child(child_name)[rest]
            except ValueError:
                # no separator inside path
                return self.immediate_child(path)

    def __setitem__(self, path, value):
        """returns its child entry."""
        if len(path) == 0:
            self.value = value
        elif path.startswith(ATTRSEP):
            self.__setitem__(path[1:], value)
        else:
            try:
                seppos           = path.index(ATTRSEP)
                if seppos == len(path)-1:
                    # only one component in path
                    self.immediate_child(path).value = value
                child_name, rest = path[:seppos], path[(seppos+1):]
                self.immediate_child(child_name)[rest].value = value
            except ValueError:
                # no separator inside path
                self.immediate_child(path).value = value
