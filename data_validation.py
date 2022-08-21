# -*- coding: utf-8 -*-
R"""Tools for validating neuropixels data files from ecephys recording sessions.

    Some design notes:
    - hash + filesize uniquely identify data, regardless of path 
    
    - the database holds previously-generated checksum hashes for
    large files (because they can take a long time to generate), plus their
    filesize at the time of checksum generation
    
    - small text-like files can have checksums generated on the fly
    so they don't need to live in the database (but they often do)
    
    for a given data file input we want to identify in the database:
        - self:
            - size[0] == size[1]
            - hash[0] == hash[1]
            - path[0] == path[1]
    
        - valid backups:
            - size[0] == size[1]
            - hash[0] == hash[1]
            - path[0] != path[1]
                
            - valid backups, with filename mismatch:
                - filename[0] != filename[1]                
            
        - invalid backups:
            - path[0] != path[1] 
            - filename[0] == filename[1]
            
            - invalid backups, corruption likely:
                - size[0] == size[1]
                - hash[0] != hash[1]
            
            - invalid backups, out-of-sync or incomplete transfer:       
                - size[0] != size[1]
                - hash[0] != hash[1]
                
        - other, assumed unrelated:
            - size[0] != size[1]
            - hash[0] != hash[1]
            - filename[0] != filename[1]
            
    - the basic unit for making these comparsons is a 'DataValidationFile' object, which has the properties above
    - checking the equality of two DVFile objects (ie subject == other) returns an enum specifying which of the relationships
      above is true
    - three or four parameters constitute a DataValidationFile object:
        -filepath
            -ecephys sessionID, which may be inferred from the filepath, 
            required for organization and many other possible uses of the file
        -checksum
        -size
    - not all of the parameters are required    
    - a standard baseclass template exists for connecting to a database, feeding-in file objects and getting matches
    - convenience / helper functions: live in a separate module ?

    
    Typical usage:

    import data_validation as dv
    
    x = dv.CRC32DataValidationFile(
        path=
        R'\\allen\programs\mindscope\workgroups\np-exp\1190290940_611166_20220708\1190258206_611166_20220708_surface-image1-left.png'
    )
    print(f'checksum is auto-generated for small files: {x.checksum}')

    y = dv.CRC32DataValidationFile(
        checksum=x.checksum, 
        size=x.size, 
        path='/dir/1190290940_611166_20220708_foo.png'
    )

    # DataValidationFile objects evaulate to True if they have some overlap between filename (regardless of path),
    # checksum, and size: 
    print(x == y)

    # only files that are unrelated, and have no overlap in filename, checksum, or size,
    # evaluate to False
    
    # connecting to a database:
    db = dv.MongoDataValidationDB()
    db.add_file(x)
    
    # to see large-file checksum performance (~400GB file)
    db.DVFile.generate_checksum('//allen/programs/mindscope/production/incoming/recording_slot3_2.npx2)

    # applying to folders
    local = R'C:\Users\ben.hardcastle\Desktop\1190258206_611166_20220708'
    npexp = R'\\w10dtsm18306\neuropixels_data\1190258206_611166_20220708'
    f = dv.DataValidationFolder(local)
    f.db = dv.MongoDataValidationDB
    f.add_folder_to_db(local)
    f.add_folder_to_db(npexp)

    f.add_backup(npexp)

    f.validate_backups(verbose=True)
"""
from __future__ import annotations

import abc
import configparser
import datetime
import enum
import json
import logging
import logging.handlers
import mmap
import os
import pathlib
import pprint
import random
import re
import shelve
import sys
import tempfile
import threading
import traceback
import zlib
from typing import Any, Callable, Generator, List, Set, Type, Union

try:
    import pymongo
except ImportError:
    print("pymongo not installed")
    
import data_getters as dg  # from corbett's QC repo
import nptk  # utilities for np rigs and data
import strategies  # for interacting with database

# LOG_DIR = fR"//allen/programs/mindscope/workgroups/np-exp/ben/data_validation/logs/"
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.DEBUG,datefmt="%Y-%m-%d %H:%M")
log = logging.getLogger()
logHandler = logging.handlers.RotatingFileHandler('data_validation2.log', maxBytes=10000, backupCount=10)
log.addHandler(logHandler)
log.setLevel(logging.ERROR)


def error(e: TypeError) -> str:
    return ''.join(traceback.TracebackException.from_exception(e).format())


def progressbar(it,
                prefix="",
                size=20,
                file=sys.stdout,
                units: str = None,
                unit_scaler: int = 1,
                display: bool = True):
    # from https://stackoverflow.com/a/34482761
    count = len(it)
    digits = len(str(count*unit_scaler))
    def show(j):
        if display:
            x = int(size * j / (count if count != 0 else 1))
            # file.write("%s[%s%s] %i.2f/%i %s\r" % (prefix, "#" * x, "." *
            #                                     (size-x), j * unit_scaler, count * unit_scaler, units or ""))
            file.write(f'{prefix}[{x * "#"}{"." * (size-x)}] {(digits - len(str(j*unit_scaler)))*"0"}{j * unit_scaler}/{count * unit_scaler} {units or ""}\r')
            file.flush()

    for i, item in enumerate(it):
        yield item
        show(i + 1)
    if display:
        # file.write(" "*(digits*2 + 3 + len(units)+1 + len(prefix) + 2 + size) +"\r\n")
        file.write("\n")
        file.flush()


def chunk_crc32(file:Any=None, fsize=None) -> str:
    """ generate crc32 with for loop to read large files in chunks """
    if isinstance(file, str):
        pass
    elif isinstance(file, type(pathlib.Path)):
        file = str(file)
    elif isinstance(file, DataValidationFile):
        file = file.path
        fsize = file.size

    chunk_size = 65536 # bytes

    # print('using builtin ' + inspect.stack()[0][3])

    # get filesize just once
    if not fsize:
        fsize = os.stat(file).st_size

    # don't show progress bar for small files
    display = True if fsize > 1E06 * chunk_size else False
    display=False #! not compatible with multithread processing of DVFolders
    crc = 0
    with open(str(file), 'rb', chunk_size) as ins:
        for _ in progressbar(range(int((fsize / chunk_size)) + 1),
                             prefix="generating crc32 checksum ",
                             units="B",
                             unit_scaler=chunk_size,
                             display=display):
            crc = zlib.crc32(ins.read(chunk_size), crc)

    return '%08X' % (crc & 0xFFFFFFFF)


def mmap_direct(fpath: Union[str, pathlib.Path], fsize=None) -> str:
    """ generate crc32 with for loop to read large files in chunks """
    # print('using standalone ' + inspect.stack()[0][3])
    print(f'using mmap_direct for {fpath}')
    crc = 0
    with open(str(fpath), 'rb') as ins:
        with mmap.mmap(ins.fileno(), 0, access=mmap.ACCESS_READ) as m:
            crc = zlib.crc32(m.read(), crc)
    return '%08X' % (crc & 0xFFFFFFFF)

def test_crc32_function(func, *args, **kwargs):
    temp = os.path.join(tempfile.gettempdir(), 'checksum_test_' + str(random.randint(0, 1000000)))
    with open(os.path.join(temp), 'wb') as f:
        f.write(b'foo')
    assert func(temp) == "8C736521", "checksum function incorrect"


def valid_crc32_checksum(value: str) -> bool:
    """ validate crc32 checksum """
    if isinstance(value, str) and len(value) == 8 \
        and all(c in '0123456789ABCDEF' for c in value.upper()):
        return True
    return False


class Session:
    """Get session information from any string: filename, path, or foldername"""

    # use staticmethods with any path/string, without instantiating the class:
    #
    #  Session.mouse(
    #  "c:/1234566789_611166_20220708_surface-image1-left.png"
    #   )
    #  >>> "611166"
    #
    # or instantiate the class and reuse the same session:
    #   session = Session(
    #  "c:/1234566789_611166_20220708_surface-image1-left.png"
    #   )
    #   session.id
    #   >>> "1234566789"
    id = None
    mouse = None
    date = None
    
    NPEXP_ROOT = pathlib.Path(R"//allen/programs/mindscope/workgroups/np-exp")

    def __init__(self, path: str):
        if not isinstance(path, (str,pathlib.Path)):
            raise TypeError(f"{self.__class__.__name__} path must be a string or pathlib.Path object")

        self.folder = self.__class__.folder(path)
        # TODO maybe not do this - could be set to class without realizing - just assign for instances

        if self.folder:
            # extract the constituent parts of the session folder
            self.id = self.folder.split('_')[0]
            self.mouse = self.folder.split('_')[1]
            self.date = self.folder.split('_')[2]
        elif 'production' and 'prod0' in str(path):
            self.id = re.search(R'(?<=_session_)\d{10}', str(path)).group(0)
            lims_dg = dg.lims_data_getter(self.id)
            self.mouse = lims_dg.data_dict['external_specimen_name']
            self.date = lims_dg.data_dict['datestring']
            self.folder = ('_').join([self.id, self.mouse, self.date])
        else:
            raise ValueError(f"{self.__class__.__name__} path must contain a valid session folder {path}")

    @classmethod
    def folder(cls, path: Union[str, pathlib.Path]) -> Union[str, None]:
        """Extract [10-digit session ID]_[6-digit mouse ID]_[6-digit date
        str] from a file or folder path"""

        # identify a session based on
        # [10-digit session ID]_[6-digit mouseID]_[6-digit date str]
        session_reg_exp = R"[0-9]{8,}_[0-9]{6}_[0-9]{8}"

        session_folders = re.findall(session_reg_exp, str(path))
        if session_folders:
            if not all(s == session_folders[0] for s in session_folders):
                logging.info(f"{cls.__class__.__name__} Mismatch between session folder strings - file may be in the wrong folder: {path}")
            return session_folders[0]
        else:
            return None
    
    @property
    def npexp_path(self) -> Union[pathlib.Path, None]:
        '''get session folder from path/str and combine with npexp root to get folder path on npexp'''        
        folder = self.folder
        if not folder:
            return None
        return self.NPEXP_ROOT / folder

    @property
    def lims_path(self) -> Union[pathlib.Path, None]:
        '''get lims id from path/str and lookup the corresponding directory in lims'''
        if not (self.folder or self.id):
            return None
        
        try:
            lims_dg = dg.lims_data_getter(self.id)
            WKF_QRY =   '''
                        SELECT es.storage_directory
                        FROM ecephys_sessions es
                        WHERE es.id = {}
                        '''
            lims_dg.cursor.execute(WKF_QRY.format(lims_dg.lims_id))
            exp_data = lims_dg.cursor.fetchall()
            if exp_data and exp_data[0]['storage_directory']:
                return pathlib.Path('/'+exp_data[0]['storage_directory'])
            else:
                return None
            
        except:
            return None


class SessionFile:
    """ Represents a single file belonging to a neuropixels ecephys session """

    session = None

    def __init__(self, path: Union[str, pathlib.Path]):
        """ from the complete file path we can extract some information upon
        initialization """

        if not isinstance(path, (str, pathlib.Path)):
            raise TypeError(f"{self.__class__.__name__}: path must be a str or pathlib.Path pointing to a file: {type(path)}")
        
        path = pathlib.Path(path)

        self.accessible = os.path.exists(path) #pathlib exists() can give user name/pwd incorrect error
        # ensure the path is a file, not directory
        # if the file doesn't exist, we have to assume based on lack of file extension
        if not self.accessible:
            if os.path.splitext(path)[1] == '':
                is_file = False
            else:
                is_file = True
        else:
            is_file = path.is_file()

        if not is_file:
            # TODO some files show accessible=True but at this point don't exist here - presumably deleted
            raise ValueError(f"{self.__class__.__name__}: path must point to a file {path}")
        else:
            self.path = path

        self.name = self.path.name

        # get the name of the folder the file lives in (which may be the same as self.root_path below)
        self.parent = self.path.parent

        # extract the session ID from anywhere in the path
        self.session = Session(self.path)
        if not self.session:
            raise ValueError(f"{self.__class__.__name__}: path does not contain a session ID {self.path.as_posix}")
    
    @property
    def root_path(self) -> str:
        """root path of the file (may be the same as session_folder_path)"""
        # we expect the session_folder string to first appear in the path as
        # a child of some 'repository' of session folders (like npexp), 
        # - split the path at the first session_folder match and call that folder the root
        parts = pathlib.Path(self.path).parts
        while parts:
            if self.session.folder in parts[0]:
                break
            parts = parts[1:]
        else:
            raise ValueError(f"{self.__class__.__name__}: session_folder not found in path {self.path}")

        return pathlib.Path(str(self.path).split(str(parts[0]))[0])


    @property
    def session_folder_path(self) -> Union[str, None]:
        """path to the session folder, if it exists"""
        
        # if a repository (eg npexp) contains session folders, the following location should exist:
        session_folder_path = self.root_path / self.session.folder
        if os.path.exists(session_folder_path):
            return session_folder_path
        # but it might not exist: we could have a file sitting in a folder with a flat structure:
        # assorted files from multiple sessions in a single folder (e.g. LIMS incoming),
        # or a folder which has the session_folder pattern plus extra info
        # appended, eg. _probeABC
        # in that case return the root path
        return self.root_path
    
    
    @property
    def session_relative_path(self) -> pathlib.Path:
        '''filepath relative to a session folder's parent'''
        # wherever the file is, get its path relative to the parent of a
        # hypothetical session folder ie. session_id/.../filename.ext :
        session_relative_path = pathlib.Path(self.path).relative_to(self.root_path)
        if session_relative_path.parts[0] != self.session.folder:
            return pathlib.Path(self.session.folder, str(session_relative_path))
        else:
            return session_relative_path
    
    @property
    def relative_path(self) -> pathlib.Path:
        '''filepath relative to a session folder'''
        return pathlib.Path(self.session_relative_path.relative_to(self.session.folder))
    
    @property
    def npexp_path(self) -> pathlib.Path:
        '''filepath on npexp (might not exist)'''
        if self.session:
            return self.session.NPEXP_ROOT / self.session_relative_path
        else:
            return None
        
    # TODO add lims_path property

    @property
    def z_drive_path(self) -> pathlib.Path:
        """Path to possible backup on 'z' drive (might not exist)
        
        This property getter just prevents repeat calls to find the path
        """
        if hasattr(self, '_z_drive_path'):
            return self._z_drive_path
        else:
            self._z_drive_path = self.get_z_drive_path()
            return self.z_drive_path
            
    def get_z_drive_path(self) -> pathlib.Path:
        """Path to possible backup on 'z' drive (might not exist)"""
        running_on_rig = nptk.COMP_ID if "NP." in nptk.COMP_ID else None
        local_path = str(self.path)[0] not in ["/", "\\"]
        rig_from_path = nptk.Rig.rig_from_path(self.path) 
        
        # get the sync computer's path 
        if (running_on_rig and local_path):
            sync_path = nptk.Rig.Sync.path
        elif rig_from_path:
            sync_path = f"{rig_from_path}-Sync"
        else:
            sync_path = None
            
        if sync_path and sync_path not in self.path:
            # add the z drive/neuropix data folder for this rig
            return pathlib.Path(
                    sync_path, 
                    "neuropixels_data", 
                    self.session.folder
                    ) / self.session_relative_path
                
    
    def __lt__(self, other):
        if self.session.id == other.session.id:
            return self.session_relative_path < other.session_relative_path
        return self.session.id < other.session.id


class DataValidationFile(abc.ABC):
    """ Represents a file to be validated
        
        Not to be used directly, but rather subclassed.
        Can be subclassed easily to change the checksum database/alogrithm
        
        Call <superclass>.__init__(path, checksum, size) in subclass __init__  
        
    """
    #TODO hash refers to path, size, checksum: add setters that freeze these attributes and set only during init
    # as a result it won't be possible to add checksums - a new instance will have to be created with the checksum 
    
    # DB: DataValidationDB = NotImplemented

    checksum_threshold: int = 50 * 1024**2
    # filesizes below this will have checksums auto-generated on init

    checksum_name: str = None
    # used to identify the checksum type in the databse, e.g. a key in a dict

    checksum_generator: Callable[[str], str] = NotImplementedError()
    # implementation of algorithm for generating checksums, accepts a path and
    # returns a checksum string

    checksum_test: Callable[[Callable], None] = NotImplementedError()
    # a function that confirms checksum_generator is working as expected,
    # accept a function, return nothing but raise exception if test fails

    checksum_validate: Callable[[str], bool] = NotImplementedError()

    # a function that accepts a string and confirms it conforms to the checksum
    # format, return True or False

    def __init__(self, path: str = None, checksum: str = None, size: int = None):
        """ setup depending on the inputs """

        if not (path or checksum):
            raise ValueError(f"{self.__class__.__name__}: either path or checksum must be set")

        if path and not isinstance(path, str):
            raise TypeError(f"{self.__class__.__name__}: path must be a str pointing to a file: {type(path)}")

        self.accessible = os.path.exists(path)
        # ensure the path is a file, not directory
        # if the file doesn't exist, we have to assume based on lack of file extension
        if not self.accessible:
            if os.path.splitext(path)[1] == '':
                is_file = False
            else:
                is_file = True
        else:
            is_file = os.path.isfile(path)

        if not is_file:
            raise ValueError(f"{self.__class__.__name__}: path must point to a file {path}")
        else:
            self.path = pathlib.Path(path).as_posix()

        # we have a mix in the databases of posix paths with and without the double fwd slash
        if self.path[0] == '/' and self.path[1] != '/':
            self.path = '/' + self.path
            
        self.name = os.path.basename(self.path)
        
        # if a file lives in a probe folder (_probeA, or _probeABC) it may have the same name, size (and even checksum) as
        # another file in a corresponding folder (_probeB, or _probeDEF) - the data are identical if all the above
        # match, but it would still be preferable to keep track of these files separately -> this property indicates 
        probe = re.search(R"(?<=_probe)_?(([A-F]+)|([0-5]{1}))", self.path.split(self.name)[0])
        if probe:
            probe_name = probe[0]
            # only possibile probe_names here are [A-F](any combination), [0-5](single digit) 
            if len(probe_name) == 1:
                # convert single-digit probe numbers to letters 
                if ord('0') <= ord(probe_name) <= ord('5'):
                    probe_name = chr(ord('A') + int(probe_name))
                assert ord('A') <= ord(probe_name) <= ord('F'), logging.error("{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(probe_name))
            else:
                assert all(letter in "ABCDEF" for letter in probe_name), logging.error("{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(probe_name))
        self.probe_dir = probe_name if probe else None
        
        if path and not size and self.accessible: # TODO replace exists check, race condition
            self.size = os.path.getsize(path)
        elif size and isinstance(size, int):
            self.size = size
        elif size and not isinstance(size, int):
            raise ValueError(f"{self.__class__.__name__}: size must be an integer {size}")
        else:
            self.size = None

        if checksum:
            self.checksum = checksum

        if not checksum \
            and self.path and os.path.exists(self.path) \
            and self.size and self.size < self.checksum_threshold \
            :
            self.checksum = self.__class__.generate_checksum(self.path, self.size) # change to use instance method if available

    @classmethod
    def generate_checksum(cls, path, size=None) -> str:
        cls.checksum_test(cls.checksum_generator)
        return cls.checksum_generator(path, size)

    @property
    def checksum(self) -> str:
        if not hasattr(self, '_checksum'):
            return None
        return self._checksum
    
    @checksum.setter
    def checksum(self, value: str):
        if self.__class__.checksum_validate(value):
            # print(f"setting {self.checksum_name} checksum: {value}")
            self._checksum = value
        else:
            raise ValueError(f"{self.__class__.__name__}: trying to set an invalid {self.checksum_name} checksum")
    
    def report(self, other: Union[DataValidationFile, List[DataValidationFile]]):
        """Log a report on the comparison with one or more files"""
        if isinstance(other, list):
            for others in other:
                self.report(others)
        else:
            result = self.Match(self==other).name
            logging.info(f"{result} | {self.path} {other.path} | {self.checksum} {other.checksum} | {self.size} {other.size} bytes")
    
    def __repr__(self):
        return f"(path='{self.path or ''}', checksum='{self.checksum or ''}', size={self.size or ''})"

    def __lt__(self, other):
        if self.name and other.name:
            if self.name == other.name:
                return self.checksum < other.checksum \
                    or self.size < other.size
            else:
                return self.name < other.name
        else:
            return self.checksum < other.checksum \
                or self.size < other.size

    @enum.unique
    class Match(enum.IntFlag):
        """Integer enum for DataValidationFile equality - test for (file==other)>0 for
        matches of interest and >20 for valid backups"""
        UNRELATED = 0
        UNKNOWN = -1
        SELF = 5
        #! watch out: SELF_NO_CHECKSUM and OTHER_NO_CHECKSUM
        #! depend on the order of objects in the inequality
        SELF_NO_CHECKSUM = 6
        OTHER_NO_CHECKSUM = 7
        CHECKSUM_COLLISION = 10
        UNSYNCED_DATA = 11
        UNSYNCED_CHECKSUM = 12
        UNSYNCED_OR_CORRUPT_DATA = 13
        VALID_COPY_SAME_NAME = 21
        VALID_COPY_RENAMED = 22

    def __eq__(self, other):
        """Test equality of two DataValidationFile objects"""
        # size and path fields are required entries in a DVF entry in database -
        # checksum is optional, so we need to check for it in both objects
        if self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.path.lower() == other.path.lower()) \
            : # self
            return self.__class__.Match.SELF.value

        #! watch out: SELF_NO_CHECKSUM and OTHER_NO_CHECKSUM
        # depend on the order of objects in the inequality
        elif (self.size == other.size) \
            and (self.path.lower() == other.path.lower()) \
            and (not self.checksum) \
            and (other.checksum) \
            : # self without checksum confirmation (self missing)
            return self.__class__.Match.SELF_NO_CHECKSUM.value
        #! watch out: SELF_NO_CHECKSUM and OTHER_NO_CHECKSUM
        # depend on the order of objects in the inequality
        elif (self.size == other.size) \
            and (self.path.lower() == other.path.lower()) \
            and (self.checksum) \
            and not (other.checksum) \
            : # self without checksum confirmation (other missing)
            return self.__class__.Match.OTHER_NO_CHECKSUM.value

        elif self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name.lower() == other.name.lower()) \
            and (self.path.lower() != other.path.lower()) \
            and (self.probe_dir == other.probe_dir) \
            : # valid copy, not self
            return self.__class__.Match.VALID_COPY_SAME_NAME.value

        elif self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size == other.size) \
            and (self.name.lower() != other.name.lower()) \
            and (self.path.lower() != other.path.lower()) \
            and (self.probe_dir == other.probe_dir) \
            : # valid copy, different name
            return self.__class__.Match.VALID_COPY_RENAMED.value

        elif self.checksum and other.checksum \
            and (self.name.lower() == other.name.lower()) \
            and (self.path.lower() != other.path.lower()) \
            and (self.probe_dir == other.probe_dir) \
            : # invalid copy ( multiple categories)

            if (self.size != other.size) \
                and (self.checksum != other.checksum) \
                and (self.probe_dir == other.probe_dir) \
                : # out-of-sync copy or incorrect data named as copy
                return self.__class__.Match.UNSYNCED_DATA.value

            if (self.size != other.size) \
                and (self.checksum == other.checksum) \
                and (self.probe_dir == other.probe_dir) \
                : # out-of-sync copy or incorrect data named as copy
                # plus checksum which needs updating
                # (different size with same checksum isn't possible)
                return self.__class__.Match.UNSYNCED_CHECKSUM.value

            if (self.size == other.size) \
                and (self.checksum != other.checksum) \
                and (self.probe_dir == other.probe_dir) \
                : # possible data corruption, or checksum needs updating
                return self.__class__.Match.UNSYNCED_OR_CORRUPT_DATA.value

        elif self.checksum and other.checksum \
            and (self.checksum == other.checksum) \
            and (self.size != other.size) \
            and (self.name.lower() != other.name.lower()) \
            : # possible checksum collision
            return self.__class__.Match.CHECKSUM_COLLISION.value

        elif self.checksum and other.checksum \
            and (self.checksum != other.checksum) \
            and (self.size != other.size) \
            and (self.name.lower() != other.name.lower()) \
            :# apparently unrelated files (different name && checksum && size)
            return self.__class__.Match.UNRELATED.value

        else:      # insufficient information
            return self.__class__.Match.UNKNOWN.value

    def __hash__(self):
        # this might be a bad idea: added to allow for set() operations on DVFiles to remove duplicates when getting
        # a database - but DVFiles are mutable
        return hash(self.checksum) ^ hash(self.size) ^ hash(self.path)

class CRC32DataValidationFile(DataValidationFile, SessionFile):

    # DB: DataValidationDB = CRC32JsonDataValidationDB()
    checksum_threshold: int = 0 # don't generate checksum for any files by default
    checksum_name: str = "crc32"
    # used to identify the checksum type in the databse, e.g. a key in a dict

    checksum_generator: Callable[[str], str] = chunk_crc32
    # implementation of algorithm for generating checksums, accept a path and return a checksum

    checksum_test: Callable[[Callable], None] = test_crc32_function
    # a test Callable that confirms checksum_generator is working as expected, accept a function, return nothing (raise exception if test fails)

    checksum_validate: Callable[[str], bool] = valid_crc32_checksum

    # a function that accepts a string and validates it conforms to the checksum format, returning boolean

    def __init__(self, path: str = None, checksum: str = None, size: int = None):
        # if the path doesn't contain a session_id, this will raise an error:
        # try:
        SessionFile.__init__(self, path)
        DataValidationFile.__init__(self, path=path, checksum=checksum, size=size)
        # if not hasattr(self, "accessible"):
        #     self.accessible = os.path.exists(self.path)


class DataValidationDB(abc.ABC):
    """ Represents a database of files with validation metadata

    serves as a template for interacting with a database of filepaths,
    filesizes, and filehashes, for validating data integrity
    
    not to be used directly, but subclassed: make a new subclass that implements
    each of the "abstract" methods specified in this class
    
    as long as the subclass methods accept the same inputs and output the
    expected results, a new database subclass can slot in to replace an old one
    in some other code without needing to make any other changes to that code
    
    """
    #* methods:
    #* add_file(file: DataValidationFile)
    #* get_matches(file: DataValidationFile) -> List[DataValidationFile]
    #   the file above can be compared with entries in the returned list for further details

    DVFile: DataValidationFile = NotImplemented

    #? both of these could be staticmethods

    @abc.abstractmethod
    def add_file(self, file: DataValidationFile):
        """ add a file to the database """
        raise NotImplementedError

    @abc.abstractmethod
    def get_matches(self,
                    file: DataValidationFile,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: int = None) -> List[DataValidationFile]: #, Optional[List[int]]:
        """search database for entries that match any of the given arguments 
        """
        raise NotImplementedError


class ShelveDataValidationDB(DataValidationDB):
    """
    A database that stores data in a shelve database
    """
    DVFile: DataValidationFile = CRC32DataValidationFile
    db = "//allen/programs/mindscope/workgroups/np-exp/ben/data_validation/db/shelve_by_session_id"

    @classmethod
    def add_file(
        cls,
        file: DataValidationFile = None,
        path: str = None,
        size: int = None,
        checksum: str = None,
    ):
        """ add an entry to the database """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        key = file.session.id

        with shelve.open(cls.db, writeback=True) as db:
            if key in db and \
                (
                    [x for x in db[key] if (file == x) == cls.DVFile.Match.SELF] \
                    or [x for x in db[key] if (file == x) == cls.DVFile.Match.SELF_NO_CHECKSUM] \
                ):
                print(f'skipped {file.session.folder}/{file.name} in Shelve database')
                return

            if key in db:
                db[key].append(file)
            else:
                db[key] = [file]

            print(f'added {file.session.folder}/{file.name} to Shelve database')

    # @classmethod
    # def save(cls):
    #     self.db.sync()

    @classmethod
    def get_matches(cls,
                    file: DataValidationFile = None,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: int = None) -> List[DataValidationFile]: #, Optional[List[int]]:
        """search database for entries that match any of the given arguments 
        """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        key = file.session.id

        with shelve.open(cls.db, writeback=False) as db:
            if key in db:
                matches = db[key]

        if match and isinstance(match, int) and \
            (match in [x.value for x in cls.DVFile.Match]
             or match in [x for x in cls.DVFile.Match]):
            return [o for o in matches if (file == o) == match > 0], \
                [(file == o) for o in matches if (file == o) == match > 0]
        else:
            return [o for o in matches if (file == o) > 0], \
                [(file == o) for o in matches if (file == o) > 0]

    # def __del__(self):
    #     self.db.close()


class MongoDataValidationDB(DataValidationDB):
    """
    A database that stores validation data in mongodb 
    """
    DVFile: DataValidationFile = CRC32DataValidationFile
    db_address = "mongodb://10.128.50.77:27017/"
    db = pymongo.MongoClient(db_address).prod.snapshots

    @classmethod
    def add_file(
        cls,
        file: DataValidationFile = None,
        path: str = None,
        size: int = None,
        checksum: str = None,
    ):
        """ add an entry to the database """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        # check an identical entry doesn't exist already
        matches = cls.get_matches(file)
        match_type = [(file == match) for match in matches] if matches else []
        if (cls.DVFile.Match.SELF in match_type) \
            or (cls.DVFile.Match.SELF_NO_CHECKSUM in match_type):
            # print(f'skipped {file.session.folder}/{file.name} in Mongo database')
            return

        cls.db.insert_one({
            "session_id": file.session.id,
            "path": file.path,
            "checksum": file.checksum,
            "size": file.size,
            "type": file.checksum_name,
        })
        logging.info(f'added {file.session.folder}/{file.name} to Mongo database')

    @classmethod
    def get_matches(cls,
                    file: DataValidationFile = None,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: Union[int, Type[enum.IntEnum]] = None) -> List[DataValidationFile]: #, Optional[List[int]]:
        """search database for entries that match any of the given arguments 
        """
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)
        
        entries = list(cls.db.find({
            "session_id": file.session.id,
        }))
        
        if not entries:
            return None
                 
        matches = set(
            cls.DVFile(
                path=entry['path'],
                checksum=entry['checksum'],
                size=entry['size'],
            ) for entry in entries
        )

        def filter_on_match_type(match_type: int) -> List[DataValidationFile]:
            if isinstance(match_type, int) and \
                (match_type in [x.value for x in cls.DVFile.Match]
                or match_type in [x for x in cls.DVFile.Match]):
                return [o for o in matches if (file == o) == match_type > 0]

        if not match:
            return [o for o in matches if (file == o) > 0]
        
        filtered_matches = []
        match = [match] if not isinstance(match, list) else match
        for m in match:
            filtered_matches += (filter_on_match_type(m))
        return filtered_matches



class CRC32JsonDataValidationDB(DataValidationDB):
    """ Represents a database of files with validation metadata in JSON format
    
    This is a subclass of DataValidationDB that stores the data in a JSON
    file.
    
    The JSON file is a dictionary of dictionaries, with the following keys:
        - dir_name/filename.extension: 
                - windows: the path to the file with \\
                - posix: the path to the file with /
                - size: the size of the file in bytes
                - crc32: the checksum of the file
    
    """

    DVFile = CRC32DataValidationFile

    path = '//allen/ai/homedirs/ben.hardcastle/crc32_data_validation_db.json'

    db: List[DataValidationFile] = None

    def __init__(self, path: str = None):
        if path:
            self.path = path
        self.load(self.path)

    def load(self, path: str = None):
        """ load the database from disk """

        # persistence in notebooks causes db to grow every execution
        if not self.db:
            self.db = []

        if not path:
            path = self.path

        if os.path.basename(path) == 'checksums.sums' \
            or os.path.splitext(path)[-1] == '.sums':
            # this is a text file exported by openhashtab

            # first line might be a header (optional)
            """
            crc32#implant_info.json#1970.01.01@00.00:00
            C8D91EAB *implant_info.json
            crc32#check_crc32_db.py#1970.01.01@00.00:00
            427608DB *check_crc32_db.py
            ...
            """
            root = pathlib.Path(path).parent.as_posix()

            with open(path, 'r') as f:
                lines = f.readlines()

            if not ("@" or "1970") in lines[0]:
                # this is probably a header line, skip it
                lines = lines[1:]

            for idx in range(0, len(lines), 2):
                line0 = lines[idx].rstrip()
                line1 = lines[idx + 1].rstrip()

                if "crc32" in line0:
                    crc32, *args = line1.split(' ')
                    filename = ' '.join(args)

                    if filename[0] == "*":
                        filename = filename[1:]
                    path = '/'.join([root, filename])

                    try:
                        file = self.DVFile(path=path, checksum=crc32)
                        self.add_file(file=file)
                    except ValueError as e:
                        print('skipping file with no session_id')
                        # return

        else:
            # this is one of my simple flat json databases - exact format
            # changed frequently, try to account for all possibilities

            if os.path.exists(path):

                with open(path, 'r') as f:
                    items = json.load(f)

                for item in items:
                    keys = items[item].keys()

                    if 'linux' in keys:
                        path = items[item]['linux']
                    elif 'posix' in keys:
                        path = items[item]['posix']
                    elif 'windows' in keys:
                        path = items[item]['windows']
                    else:
                        path = None

                    checksum = items[item][self.DVFile.checksum_name] \
                        if self.DVFile.checksum_name in keys else None
                    size = items[item]['size'] if 'size' in keys else None

                    try:
                        file = self.DVFile(path=path, checksum=checksum, size=size)
                        if ".npx2" or ".dat" in path:
                            self.add_file(file=file, checksum=checksum, size=size) # takes too long to check sizes here
                        else:
                            self.add_file(file=file)
                    except ValueError as e:
                        print('skipping file with no session_id')
                        # return

    def save(self):
        """ save the database to disk as json file """

        with open(self.path, 'r') as f:
            dump = json.load(f)

        for file in self.db:

            item_name = pathlib.Path(file.path).as_posix()

            item = {
                item_name: {
                    'windows': str(pathlib.PureWindowsPath(file.path)),
                    'posix': pathlib.Path(file.path).as_posix(),
                }
            }

            if file.checksum:
                item[item_name][self.DVFile.checksum_name] = file.checksum

            if file.size:
                item[item_name]['size'] = file.size

            dump.update(item)

        with open(self.path, 'w') as f:
            json.dump(dump, f, indent=4)

    def add_folder(self, folder: str, filter: str = None):
        """ add all files in a folder to the database """
        for root, _, files in os.walk(folder):
            for file in files:
                if filter and isinstance(filter, str) and filter not in file:
                    continue
                file = self.DVFile(os.path.join(root, file))
                self.add_file(file=file)
        self.save()

    def add_file(self, file: DataValidationFile = None, path: str = None, checksum: str = None, size: int = None):
        """ add a validation file object to the database """

        if not file:
            file = self.DVFile(path=path, checksum=checksum, size=size)
        self.db.append(file)
        print(f'added {file.session.folder}/{file.name} to json database (not saved)')

    #TODO update to classmethod like ShelveDB
    def get_matches(self,
                    file: DataValidationFile = None,
                    path: str = None,
                    size: int = None,
                    checksum: str = None,
                    match: int = None) -> List[DataValidationFile]:
        """search database for entries that match any of the given arguments 
        """
        if not file:
            file = self.DVFile(path=path, checksum=checksum, size=size)
        #! for now we only return equality of File(checksum + size)
        # or partial matches based on other input arguments

        if file and self.db.count(file):
            return [self.db.index(f) for f in self.db if file == f]

        elif path:
            name = os.path.basename(path)
            parent = pathlib.Path(path).parent.parts[-1]

            session_folder = Session.folder(path)

            if not size:
                size = os.path.getsize(path)

            # extract session_id from path if possible and add to comparison
            if size or checksum or (name and parent) or (session_folder and size):
                return [
                    self.db.index(f) \
                        for f in self.db \
                            if f.size == size or \
                                f.checksum == checksum or \
                                (f.name == name and f.parent == parent) or \
                                    (f.session_folder == session_folder and f.size == size)
                ]


class DataValidationFolder:

    db: Type[DataValidationDB] = MongoDataValidationDB
    backup_paths: Set[str] = None # auto-populated with lims, npexp, sync computer folders
    include_subfolders: bool = True
    
    regenerate_threshold_bytes: int = 1 * 1024**2 # MB 
    # - below this file size, checksums will always be generated - even if they're already in the database
    # - above this size, behavior is to get the checksum from the database if it exists for the file (size + path must
    #   be identical), otherwise generate it
    
    min_age_days: int = 0
    # - minimum age of a file for it to be deleted (provided that a valid backup exists)
    
    filename_filter: str = ''
    # - applied to glob search for files in the folder
    
    def __init__(self, path: str):
        """Represents a folder for which we want to checksum the contents and add to database,
        possibly deleting if a valid copy exists elswhere
        """

        # extract the session ID from anywhere in the path (not reqd)
        try:
            self.session = Session(path)
        except:
            self.session = None
            
        # ensure the path is a directory, not a file
        # if the file doesn't exist, we have to assume based on lack of file extension
        self.accessible = os.path.exists(path)
        if not self.accessible:
            if os.path.splitext(path)[1] == '':
                is_file = False
            else:
                is_file = True
        else:
            is_file = os.path.isfile(path)

        if is_file:
            raise ValueError(f"{self.__class__.__name__}: path must point to a folder {path}")
        else:
            self.path = pathlib.Path(path).as_posix()

 
            
    def add_backup_path(self, path: Union[str, List[str]]):
        """Store one or more paths to folders possibly containing backups for the session"""
        if path and (isinstance(path, str) or isinstance(path, pathlib.Path)):
            path = [str(path)]
        elif path and isinstance(path, List): # inequality checks for str type and existence
            pass
        else:
            raise TypeError(f"{self.__class__.__name__}: path must be a string or list of strings")
            # add to list of backup locations as a Folder type object of the same class
        for p in path:
            if str(p) != '':
                if not self.backup_paths:
                    self.backup_paths = set([p])
                else:
                    self.backup_paths.add(str(p))


    def add_standard_backup_paths(self):
        """ 
        Add LIMS, NP-EXP, or neuropixels_data folder, depending on availability.
        
        Priority is LIMS folder - if valid backups are on LIMS, we don't really care about other locations. Next most
        important is NP-EXP, so we add these two by default. If neither of these locations have been made yet, we're likely
        dealing with data that's still on a rig computer prior to any uploads - the local backup path would be
        sync/neuropixels_data for all files except those from open ephys. 
        #TODO add open ephys backup drives/paths... 
        
        We just need to ensure that potential backup path isn't the folder we're trying to validate - ie. itself, which
        is not a backup.
        """
        # get the lims folder for this session and add it to the backup paths
        self.lims_path = self.session.lims_path # must exist if not None
        if self.lims_path and self.lims_path.as_posix() not in self.path:
            self.add_backup_path(self.lims_path.as_posix())
        
        # get the npexp folder for this session and add it to the backup paths (if it exists)
        self.npexp_path = self.session.npexp_path
        if self.npexp_path and os.path.exists(self.npexp_path) \
            and Session.NPEXP_ROOT.as_posix() not in self.path:
            self.add_backup_path(self.npexp_path.as_posix())
            
        if not self.backup_paths and self.session:
            # add only the relevant backup path for this rig (if applicable):
            # currently this is just the neuropix_data folder on the sync computer
            
            # use the first file in the DVFolder to get this path
            file1 = self.file_paths[0]
            File1 = self.db.DVFile(path=file1.as_posix())
            z_drive = File1.z_drive_path
            if z_drive:
                self.add_backup_path(z_drive)
            
    @property
    def filename_filters(self):
        return self.filename_filter.replace('*', '').replace(' ', '').strip().split('|')

    @property
    def file_paths(self) -> List[pathlib.Path]:
        """return a list of files in the folder"""
        if hasattr(self, '_file_paths') and self._file_paths:
            return self._file_paths

        if self.include_subfolders:
            #! yield from needs testing plus modifying the 'backup_paths' code above
            # for now, this will return the full list each time and be slower
            self._file_paths = [
                child for child in pathlib.Path(self.path).rglob('*')
                if not child.is_dir() and any(filters in child.name for filters in self.filename_filters)
            ]
        else:
            self._file_paths = [
                child for child in pathlib.Path(self.path).iterdir()
                if not child.is_dir() and any(filters in child.name for filters in self.filename_filters)
            ]
        return self._file_paths
    
    
    def add_to_db(self):
        "Add all files in folder to database if they don't already exist"
        
        # create threads for each file to be added
        threads = []
        for path in self.file_paths:
            try:
                file = self.db.DVFile(path=path.as_posix())
            except (ValueError, TypeError):
                logging.info(f"{self.__class__.__name__}: could not add to database, likely missing session ID: {path.as_posix()}")
                continue
            
            if file.size <= self.regenerate_threshold_bytes:
                t = threading.Thread(target=strategies.generate_checksum, args=(file,self.db))
            else:
                t = threading.Thread(target=strategies.generate_checksum_if_not_in_db, args=(file,self.db))

            threads.append(t)
            t.start() 
            
        # wait for the threads to complete
        print("- adding files to database...")
        for thread in progressbar(threads, prefix=' ', units='files', size=25):
            thread.join()
            
        
    def clear(self) -> List[int]:
        """Clear the folder of files which are backed-up on LIMS or np-exp, or any added backup paths""" 
    
        def delete_if_valid_backup_in_db(result, idx, file_inst, db, backup_paths):
            files_bytes = strategies.delete_if_valid_backup_in_db(file_inst, db, backup_paths)
            result[idx] = files_bytes
             
        print("- searching for valid backups...")
        deleted_bytes = [0]*len(self.file_paths) # keep a tally of space recovered
        threads = [None]*len(self.file_paths) # following https://stackoverflow.com/a/6894023
        # for path in progressbar(self.file_paths, prefix=' ', units='files', size=25):
        for i, path in enumerate(self.file_paths):
            
            try:
                file = self.db.DVFile(path=path.as_posix())
            except (ValueError, TypeError):
                logging.info(f"{self.__class__.__name__}: could not add to database, likely missing session ID: {path.as_posix()}")
                continue
            
            if int(file.session.date) \
            > int((datetime.datetime.now() - datetime.timedelta(days=self.min_age_days)).strftime('%Y%m%d')) \
                :
                logging.info(f'skipping file less than {self.min_age_days} days old: {file.session.date}')   
                continue
            
            threads[i] = threading.Thread(target=delete_if_valid_backup_in_db, args=(deleted_bytes, i, file, self.db, self.backup_paths))
            threads[i].start()
            
        for thread in progressbar(threads, prefix=' ', units='files', size=25):
            thread.join() if thread else None
        
        # tidy up empty subfolders:
        check_dir_paths = os.walk(self.path, topdown=False, followlinks=False)
        for check_dir in check_dir_paths:
            try:
                os.rmdir(check_dir[0]) # raises error if not empty
                logging.info(f"{self.__class__.__name__}: removed empty folder {check_dir[0]}")
            except OSError:
                continue
        
        # return cumulative sum of bytes deleted from folder
        deleted_bytes = [d for d in deleted_bytes if d != 0]
        print(f"{len(deleted_bytes)} files deleted | {sum(deleted_bytes) / 1024**3 :.1f} GB recovered")
        return deleted_bytes


def test_data_validation_file():
    """ test the data validation file class """

    class Test(DataValidationFile):

        def valid(path):
            return True

        checksum_generator = "12345678"
        checksum_test = None
        checksum_validate = valid

    cls = Test
    path = '//tmp/tmp/test.txt' # currently only working with network drives, which require a folder in the middle between drive/file
    checksum = '12345678'
    size = 10

    self = cls(path=path, checksum=checksum, size=size)

    other = cls(path=path, checksum=checksum, size=size)
    assert (self == self) == self.Match.SELF, "not recognized: self"

    other = cls(path='//tmp2/tmp/test.txt', checksum=checksum, size=size)
    assert (self == other) == self.Match.VALID_COPY_SAME_NAME, "not recgonized: valid copy, not self"

    other = cls(path='//tmp2/tmp/test2.txt', checksum=checksum, size=size)
    assert (self == other) == self.Match.VALID_COPY_RENAMED, "not recognized: valid copy, different name"

    other = cls(path='//tmp2/tmp/test.txt', checksum='87654321', size=20)
    assert (self == other) == self.Match.UNSYNCED_DATA, "not recognized: out-of-sync copy"

    other = cls(path='//tmp2/tmp/test.txt', checksum=checksum, size=20)
    assert (self == other) == self.Match.UNSYNCED_CHECKSUM, "not recognized: out-of-sync copy with incorrect checksum"
    #* note checksum is equal, which could occur if it hasn't been updated in db

    other = cls(path='//tmp2/tmp/test.txt', checksum='87654321', size=size)
    assert (self == other) == self.Match.UNSYNCED_OR_CORRUPT_DATA, "not recognized: corrupt copy"

    other = cls(path='//tmp/tmp/test2.txt', checksum=checksum, size=20)
    assert (self == other) == self.Match.CHECKSUM_COLLISION, "not recognized: checksum collision"

    other = cls(path='//tmp/tmp/test2.txt', checksum='87654321', size=20)
    assert (self == other) == self.Match.UNRELATED, "not recognized: unrelated file"


test_data_validation_file()
        
    
def report_multline_print(file: DataValidationFile, comparisons: List[DataValidationFile]):
    """ report on the contents of the folder, compared to database
        """
    if isinstance(comparisons, DataValidationFile):
        comparisons = [comparisons]

    column_width = 120 # for display of line separators

    def display_name(DVFile: DataValidationFile) -> str:
        min_len_filename = 80
        disp = f"{DVFile.parent}/{DVFile.name}"
        if len(disp) < min_len_filename:
            disp += ' ' * (min_len_filename - len(disp))
        return disp

    def display_str(label: str, DVFile: DataValidationFile) -> str:
        disp = f"{label} : {display_name(DVFile)} | {DVFile.checksum or '  none  '} | {DVFile.size or '??'} bytes"
        return disp

    logging.info("#" * column_width)
    logging.info("\n")
    logging.info(f"subject: {file.path}")
    logging.info("\n")
    logging.info("-" * column_width)

    folder = file.path.split(file.name)[0]
    compare_folder = ""
    for other in comparisons:
        # logging.info new header for each comparison with a new folder
        if compare_folder != other.path.split(other.name)[0]:
            compare_folder = other.path.split(other.name)[0]
            # logging.info("*" * column_width)
            logging.info("folder comparison for")
            logging.info(f"subject : {folder}")
            logging.info(f"other   : {compare_folder}")
            # logging.info("*" * column_width)
            logging.info("-" * column_width)

        logging.info(f"Result  : {file.Match(file==other).name}")
        logging.info(display_str("subject", file))
        logging.info(display_str("other  ", other))
        logging.info("-" * column_width)

    logging.info("\n")
    logging.info("#" * column_width)


def DVFolders_from_dirs(dirs: Union[str, List[str]], only_session_folders=True) -> Generator[DataValidationFolder, None, None]:
    """Generator of DataValidationFolder objects from a list of directories"""
    if not isinstance(dirs, list):
        dirs = [dirs]
        
    def skip(dir) -> bool:
        skip_filters = ["$RECYCLE.BIN", "_temp_", "#recycle"]
        if any(skip in str(dir) for skip in skip_filters):
            return True
        
    for dir in dirs:
        dir_path = pathlib.Path(dir)
        if skip(dir_path):
            continue
        if Session.folder(dir):
            # the dir provided is a session folder: make this into a DVFolder
            yield DataValidationFolder(dir_path.as_posix())
        else:
            # the dir provided is not a session folder itself, but might be a repository of session folders: 
            # we'll check its subfolders and return them as DVFolders where appropriate
            # - but first return the dir provided, as it may contain some loose session files (files not in standard session folder)
            top_level_dir = DataValidationFolder(dir_path.as_posix())
            top_level_dir.include_subfolders = False # if True, we'll rglob through every subfolder from the top_level, when what we want is to loop through individual subfolders as DVFolders below 
            yield top_level_dir
             
            for c in [child for child in dir_path.iterdir() if child.is_dir()]:
                if skip(c) or (only_session_folders and not Session.folder(str(c))):
                    continue
                else:
                    yield DataValidationFolder(c.as_posix())
    
def clear_dirs():
    
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    dirs = [pathlib.Path(d.strip()).resolve().as_posix() for d in config['options']['dirs'].split(',') if d != '']
    
    if os.getenv('AIBS_COMP_ID'):
        # add folders for routine clearing on rig computers
        comp = os.getenv('AIBS_COMP_ID').split('-')[-1].lower()
        if comp in config:
            dirs += [pathlib.Path(d.strip()).resolve().as_posix() for d in config[comp]['dirs'].split(',') if d != '']
    
    if not dirs:
        return
    
    regenerate_threshold_bytes = config['options'].getint('regenerate_threshold_bytes', fallback=1024**2)
    min_age_days = config['options'].getint('min_age_days', fallback=0)
    filename_filter = config['options'].get('filename_filter', fallback='')
    only_session_folders = config['options'].getboolean('only_session_folders', fallback=True)
    exhaustive_search = config['options'].getboolean('exhaustive_search', fallback=False)
    
    total_deleted_bytes = [] # keep a tally of space recovered
    print('Checking:')
    pprint.pprint(dirs, indent=4, compact=False)
    if min_age_days > 0:
        print(f'Skipping files less than {min_age_days} days old')
    
    divider = '\n' + '='*40 + '\n\n'
    
    for F in DVFolders_from_dirs(dirs, only_session_folders):
        if not F:
            continue
        F.regenerate_threshold_bytes = regenerate_threshold_bytes
        F.min_age_days = min_age_days
        F.filename_filter = filename_filter
        if not F.file_paths:
            continue
                
        if F.session:
            F.add_standard_backup_paths()
            
        print(f'{divider}Clearing {F.path}')
        
        F.add_to_db()
        
        deleted_bytes = F.clear()
        
        total_deleted_bytes += deleted_bytes 
        
    print(f"{divider}Finished clearing.\n{len(total_deleted_bytes)} files deleted | {sum(total_deleted_bytes) / 1024**3 :.1f} GB recovered\n")
    
    
if __name__ == "__main__":
    clear_dirs()
    