# -*- coding: utf-8 -*-
# Source: https://github.com/tsileo/dirtools (copied here because pypi package is not updated)

import logging
import os
import hashlib
from contextlib import closing  # for Python2.6 compatibility
import tarfile
import tempfile
from datetime import datetime
import json

from globster import Globster

log = logging.getLogger("dirtools")

# TODO abs=True args for .files(), .subdirs() ?


def load_patterns(exclude_file=".exclude"):
    """ Load patterns to exclude file from `exclude_file',
    and return a list of pattern.

    :type exclude_file: str
    :param exclude_file: File containing exclude patterns

    :rtype: list
    :return: List a patterns

    """
    return filter(None, open(exclude_file).read().split("\n"))


def _filehash(filepath, blocksize=4096):
    """ Return the hash object for the file `filepath', processing the file
    by chunk of `blocksize'.

    :type filepath: str
    :param filepath: Path to file

    :type blocksize: int
    :param blocksize: Size of the chunk when processing the file

    """
    sha = hashlib.sha256()
    with open(filepath, 'rb') as fp:
        while 1:
            data = fp.read(blocksize)
            if data:
                sha.update(data)
            else:
                break
    return sha


def filehash(filepath, blocksize=4096):
    """ Return the hash hexdigest() for the file `filepath', processing the file
    by chunk of `blocksize'.

    :type filepath: str
    :param filepath: Path to file

    :type blocksize: int
    :param blocksize: Size of the chunk when processing the file

    """
    sha = _filehash(filepath, blocksize)
    return sha.hexdigest()


class File(object):
    def __init__(self, path):
        self.file = os.path.basename(path)
        self.path = os.path.abspath(path)

    def _hash(self):
        """ Return the hash object. """
        return _filehash(self.path)

    def hash(self):
        """ Return the hash hexdigest. """
        return filehash(self.path)

    def compress_to(self, archive_path=None):
        """ Compress the directory with gzip using tarlib.

        :type archive_path: str
        :param archive_path: Path to the archive, if None, a tempfile is created

        """
        if archive_path is None:
            archive = tempfile.NamedTemporaryFile(delete=False)
            tar_args = ()
            tar_kwargs = {'fileobj': archive}
            _return = archive.name
        else:
            tar_args = (archive_path)
            tar_kwargs = {}
            _return = archive_path
        tar_kwargs.update({'mode': 'w:gz'})
        with closing(tarfile.open(*tar_args, **tar_kwargs)) as tar:
            tar.add(self.path, arcname=self.file)

        return _return


class Dir(object):
    """ Wrapper for dirtools arround a path.

    Try to load a .exclude file, ready to compute hashdir,


    :type directory: str
    :param directory: Root directory for initialization

    :type exclude_file: str
    :param exclude_file: File containing exclusion pattern,
        .exclude by default, you can also load .gitignore files.

    :type excludes: list
    :param excludes: List of additionals patterns for exclusion,
        by default: ['.git/', '.hg/', '.svn/']

    """
    def __init__(self, directory=".", exclude_file=".exclude",
                 excludes=['.git/', '.hg/', '.svn/']):
        if not os.path.isdir(directory):
            raise TypeError("Directory must be a directory.")
        self.directory = os.path.basename(directory)
        self.path = os.path.abspath(directory)
        self.parent = os.path.dirname(self.path)
        self.exclude_file = os.path.join(self.path, exclude_file)
        self.patterns = excludes
        if os.path.isfile(self.exclude_file):
            self.patterns.extend(load_patterns(self.exclude_file))
        self.globster = Globster(self.patterns)

    def hash(self, index_func=os.path.getmtime):
        """ Hash for the entire directory (except excluded files) recursively.

        Use mtime instead of sha256 by default for a faster hash.

        >>> dir.hash(index_func=dirtools.filehash)

        """
        # TODO alternative to filehash => mtime as a faster alternative
        shadir = hashlib.sha256()
        for f in self.files():
            try:
                shadir.update(str(index_func(os.path.join(self.path, f))))
            except (IOError, OSError):
                pass
        return shadir.hexdigest()

    def iterfiles(self, pattern=None, abspath=False):
        """ Generator for all the files not excluded recursively.

        Return relative path.

        :type pattern: str
        :param pattern: Unix style (glob like/gitignore like) pattern

        """
        if pattern is not None:
            globster = Globster([pattern])
        for root, dirs, files in self.walk():
            for f in files:
                if pattern is None or (pattern is not None and globster.match(f)):
                    if abspath:
                        yield os.path.join(root, f)
                    else:
                        yield self.relpath(os.path.join(root, f))

    def files(self, pattern=None, sort_key=lambda k: k, sort_reverse=False, abspath=False):
        """ Return a sorted list containing relative path of all files (recursively).

        :type pattern: str
        :param pattern: Unix style (glob like/gitignore like) pattern

        :param sort_key: key argument for sorted

        :param sort_reverse: reverse argument for sorted

        :rtype: list
        :return: List of all relative files paths.

        """
        return sorted(self.iterfiles(pattern, abspath=abspath), key=sort_key, reverse=sort_reverse)

    def get(self, pattern, sort_key=lambda k: k, sort_reverse=False, abspath=False):
        res = self.files(pattern, sort_key=sort_key, sort_reverse=sort_reverse, abspath=abspath)
        if res:
            return res[0]

    def itersubdirs(self, pattern=None, abspath=False):
        """ Generator for all subdirs (except excluded).

        :type pattern: str
        :param pattern: Unix style (glob like/gitignore like) pattern

        """
        if pattern is not None:
            globster = Globster([pattern])
        for root, dirs, files in self.walk():
            for d in dirs:
                if pattern is None or (pattern is not None and globster.match(d)):
                    if abspath:
                        yield os.path.join(root, d)
                    else:
                        yield self.relpath(os.path.join(root, d))

    def subdirs(self, pattern=None, sort_key=lambda k: k, sort_reverse=False, abspath=False):
        """ Return a sorted list containing relative path of all subdirs (recursively).

        :type pattern: str
        :param pattern: Unix style (glob like/gitignore like) pattern

        :param sort_key: key argument for sorted

        :param sort_reverse: reverse argument for sorted

        :rtype: list
        :return: List of all relative files paths.
        """
        return sorted(self.itersubdirs(pattern, abspath=abspath), key=sort_key, reverse=sort_reverse)

    def size(self):
        """ Return directory size in bytes.

        :rtype: int
        :return: Total directory size in bytes.
        """
        dir_size = 0
        for f in self.iterfiles(abspath=True):
            dir_size += os.path.getsize(f)
        return dir_size

    def is_excluded(self, path):
        """ Return True if `path' should be excluded
        given patterns in the `exclude_file'. """
        match = self.globster.match(self.relpath(path))
        if match:
            log.debug("{0} matched {1} for exclusion".format(path, match))
            return True
        return False

    def walk(self):
        """ Walk the directory like os.path
        (yields a 3-tuple (dirpath, dirnames, filenames)
        except it exclude all files/directories on the fly. """
        for root, dirs, files in os.walk(self.path, topdown=True):
            # TODO relative walk, recursive call if root excluder found???
            # root_excluder = get_root_excluder(root)
            ndirs = []
            # First we exclude directories
            for d in list(dirs):
                if self.is_excluded(os.path.join(root, d)):
                    dirs.remove(d)
                elif not os.path.islink(os.path.join(root, d)):
                    ndirs.append(d)

            nfiles = []
            for fpath in (os.path.join(root, f) for f in files):
                if not self.is_excluded(fpath) and not os.path.islink(fpath):
                    nfiles.append(os.path.relpath(fpath, root))

            yield root, ndirs, nfiles

    def find_projects(self, file_identifier=".project"):
        """ Search all directory recursively for subdirs
        with `file_identifier' in it.

        :type file_identifier: str
        :param file_identifier: File identier, .project by default.

        :rtype: list
        :return: The list of subdirs with a `file_identifier' in it.

        """
        projects = []
        for d in self.subdirs():
            project_file = os.path.join(self.directory, d, file_identifier)
            if os.path.isfile(project_file):
                projects.append(d)
        return projects

    def relpath(self, path):
        """ Return a relative filepath to path from Dir path. """
        return os.path.relpath(path, start=self.path)

    def compress_to(self, archive_path=None):
        """ Compress the directory with gzip using tarlib.

        :type archive_path: str
        :param archive_path: Path to the archive, if None, a tempfile is created

        """
        if archive_path is None:
            archive = tempfile.NamedTemporaryFile(delete=False)
            tar_args = []
            tar_kwargs = {'fileobj': archive}
            _return = archive.name
        else:
            tar_args = [archive_path]
            tar_kwargs = {}
            _return = archive_path
        tar_kwargs.update({'mode': 'w:gz'})
        with closing(tarfile.open(*tar_args, **tar_kwargs)) as tar:
            tar.add(self.path, arcname='', exclude=self.is_excluded)

        return _return


class DirState(object):
    """ Hold a directory state / snapshot meta-data for later comparison. """
    def __init__(self, _dir=None, state=None, index_cmp=os.path.getmtime):
        self._dir = _dir
        self.index_cmp = index_cmp
        self.state = state or self.compute_state()

    def compute_state(self):
        """ Generate the index. """
        data = {}
        data['directory'] = self._dir.path
        data['files'] = list(self._dir.files())
        data['subdirs'] = list(self._dir.subdirs())
        data['index'] = self.index()
        return data

    def index(self):
        index = {}
        for f in self._dir.iterfiles():
            try:
                index[f] = self.index_cmp(os.path.join(self._dir.path, f))
            except Exception as exc:
                print(f, exc)
        return index

    def __sub__(self, other):
        """ Compute diff with operator overloading.

        >>> path = DirState(Dir('/path'))
        >>> path_copy = DirState(Dir('/path_copy'))
        >>> diff =  path_copy - path
        >>> # Equals to
        >>> diff = compute_diff(path_copy.state, path.state)

        """
        if self.index_cmp != other.index_cmp:
            raise Exception('Both DirState instance must have the same index_cmp.')
        return compute_diff(self.state, other.state)

    def to_json(self, base_path='.', dt=None, fmt=None):
        if fmt is None:
            fmt = '{0}@{1}.json'
        if dt is None:
            dt = datetime.utcnow()
        path = fmt.format(self._dir.path.strip('/').split('/')[-1],
                          dt.isoformat())
        path = os.path.join(base_path, path)

        with open(path, 'wb') as f:
            f.write(json.dumps(self.state))

        return path

    @classmethod
    def from_json(cls, path):
        with open(path, 'rb') as f:
            return cls(state=json.loads(f.read()))


def compute_diff(dir_base, dir_cmp):
    """ Compare `dir_base' and `dir_cmp' and returns a list with
    the following keys:
     - deleted files `deleted'
     - created files `created'
     - updated files `updated'
     - deleted directories `deleted_dirs'

    """
    data = {}
    data['deleted'] = list(set(dir_cmp['files']) - set(dir_base['files']))
    data['created'] = list(set(dir_base['files']) - set(dir_cmp['files']))
    data['updated'] = []
    data['deleted_dirs'] = list(set(dir_cmp['subdirs']) - set(dir_base['subdirs']))

    for f in set(dir_cmp['files']).intersection(set(dir_base['files'])):
        if dir_base['index'][f] != dir_cmp['index'][f]:
            data['updated'].append(f)

    return data
