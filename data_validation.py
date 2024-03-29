# -*- coding: utf-8 -*-
r"""Tools for validating neuropixels data files from ecephys recording sessions.

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
import datetime
import enum
import functools
import hashlib
import itertools
import json
import logging
import logging.handlers
import mmap
import os
import pathlib
import random
import re
import shelve
import shutil
import socket
import sys
import tempfile
import threading
import traceback
import zlib
from typing import Any, Callable, Container, Dict, Generator, List, Literal, Sequence, Set, Tuple, Union

try:
    import pymongo
except ImportError:
    print("pymongo not installed")
import certifi
import requests

import data_getters as dg  # from corbett's QC repo
import nptk  # utilities for np rigs and data
import strategies  # for interacting with database

NPEXP_PATH = pathlib.Path("//allen/programs/mindscope/workgroups/np-exp")
INCOMING_PATH = pathlib.Path("//allen/programs/braintv/production/incoming/neuralcoding")

# setup logging ------------------------------------------------------------------------
# LOG_DIR = fR"//allen/programs/mindscope/workgroups/np-exp/ben/data_validation/logs/"
log_level = logging.INFO
log_format = "%(asctime)s %(threadName)s %(message)s"  # ? %(relativeCreated)6d
log_datefmt = "%Y-%m-%d %H:%M"
log_folder = pathlib.Path("./logs").resolve()
log_folder.mkdir(parents=True, exist_ok=True)
log_filename = "data_validation_main.log"
log_path = log_folder / log_filename

if log_path.exists() and log_path.stat().st_size > 1 * 1024**2:
    log_path.rename(
        log_path.with_stem(
            f"{log_path.stem}_{datetime.datetime.now().strftime('%Y-%m-%d')}"
        )
    )

logging.basicConfig(
    filename=str(log_path),
    level=log_level,
    format=log_format,
    datefmt=log_datefmt,
)
# log = logging.getLogger(__name__)
# logHandler = logging.handlers.RotatingFileHandler(
#     "./logs/clear_dirs.log",
#     maxBytes=10 * 1024**2,
#     backupCount=50,
# )
# log.setFormatter =
# log.addHandler(logHandler)

# get mongodb ready -------------------------------------------------------------------- #
mongo_local_client: pymongo.MongoClient = pymongo.MongoClient(
    "mongodb://10.128.50.77:27017/",
    serverSelectionTimeoutMS=2000,  # default 30s
    maxPoolSize=1,  # default 100
)

# backup cloud location
mongo_cloud_uri = "mongodb+srv://cluster0.rhrmjzu.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
mongo_cloud_certificate = mgc = pathlib.Path(
    "X509-cert-4825098053518902813.pem"
)  # expires Sept 2024
mgc_bkup = (
    lambda host="localhost": pathlib.Path(f"//{host}/C$/ProgramData/MongoDB") / mgc
)
ben_desktop = "W10DTMJ0AK6GM"
if not mgc.resolve().exists() and not mgc_bkup().exists():
    shutil.copy2(mgc_bkup(ben_desktop), mgc.parent)
mongo_cloud_client: pymongo.MongoClient = pymongo.MongoClient(
    host=mongo_cloud_uri,
    tls=True,
    tlsCertificateKeyFile=mgc.as_posix() if mgc.exists() else mgc_bkup().as_posix(),
    maxPoolSize=100,  # 500 max on free plan -default 100
    tlsCAFile=certifi.where(),
)

for client in [mongo_cloud_client]:  # mongo_local_client
    MONGO_COLLECTION = client["prod"]["snapshots"]
    try:
        MONGO_COLLECTION.count_documents({})
        break
    except Exception as e:
        print(f"Could not connect to {client}")
else:
    raise Exception("Could not connect to any mongo clients")
print(f"Connected to {client.address[0]}")

# defining the collection here opens the db connection just once per session (instead of
# repeated open/close for every access) as recommended by MongoDB docs


class SessionError(ValueError):
    """Raised when a session folder string ([lims-id]_[mouse-id]_[date]) can't be found in a
    filepath"""

    pass


class FilepathIsDirError(ValueError):
    """Raised when a directory is specified but a filepath is required"""

    pass


def error(e: TypeError) -> str:
    return "".join(traceback.TracebackException.from_exception(e).format())


def progressbar(
    it,
    prefix="",
    size=20,
    file=sys.stdout,
    units: str = None,
    unit_scaler: int = 1,
    display: bool = True,
):
    # from https://stackoverflow.com/a/34482761
    count = len(it)
    digits = len(str(count * unit_scaler))

    def show(j):
        if display:
            x = int(size * j / (count if count != 0 else 1))
            # file.write("%s[%s%s] %i.2f/%i %s\r" % (prefix, "#" * x, "." *
            #                                     (size-x), j * unit_scaler, count * unit_scaler, units or ""))
            file.write(
                f'{prefix}[{x * "#"}{"." * (size-x)}] {(digits - len(str(j*unit_scaler)))*"0"}{j * unit_scaler}/{count * unit_scaler} {units or ""}\r'
            )
            file.flush()

    for i, item in enumerate(it):
        yield item
        show(i + 1)
    if display:
        # file.write(" "*(digits*2 + 3 + len(units)+1 + len(prefix) + 2 + size) +"\r\n")
        file.write("\n")
        file.flush()


def chunk_crc32(file: Any = None, size=None, *args, **kwargs) -> str:
    """generate crc32 with for loop to read large files in chunks"""
    if isinstance(file, str):
        pass
    elif isinstance(file, type(pathlib.Path)):
        file = str(file)
    elif isinstance(file, DataValidationFile):
        file = file.path.as_posix()
        size = file.size

    chunk_size = 65536  # bytes

    # print('using builtin ' + inspect.stack()[0][3])

    # get filesize just once
    if not size:
        size = os.stat(file).st_size

    # don't show progress bar for small files
    display = True if size > 1e06 * chunk_size else False
    display = False  #! not compatible with multithread processing of DVFolders
    crc = 0
    with open(str(file), "rb", chunk_size) as ins:
        for _ in progressbar(
            range(int((size / chunk_size)) + 1),
            prefix="generating crc32 checksum ",
            units="B",
            unit_scaler=chunk_size,
            display=display,
        ):
            crc = zlib.crc32(ins.read(chunk_size), crc)

    return "%08X" % (crc & 0xFFFFFFFF)


def mmap_direct(fpath: Union[str, pathlib.Path], fsize=None) -> str:
    """generate crc32 with for loop to read large files in chunks"""
    # print('using standalone ' + inspect.stack()[0][3])
    print(f"using mmap_direct for {fpath}")
    crc = 0
    with open(str(fpath), "rb") as ins:
        with mmap.mmap(ins.fileno(), 0, access=mmap.ACCESS_READ) as m:
            crc = zlib.crc32(m.read(), crc)
    return "%08X" % (crc & 0xFFFFFFFF)


def test_crc32_function(func, *args, **kwargs):
    temp = os.path.join(
        tempfile.gettempdir(), "checksum_test_" + str(random.randint(0, 1000000))
    )
    with open(os.path.join(temp), "wb") as f:
        f.write(b"foo")
    assert func(temp) == "8C736521", "checksum function incorrect"


def chunk_hashlib(
    path: Union[str, pathlib.Path],
    hasher_cls=hashlib.sha3_256,
    blocks_per_chunk=128,
    *args,
    **kwargs,
) -> str:
    """
    Use a hashing function on a file as per lims2 copy tool, but return the 32-len
    string hexdigest instead of a 32-len list of integers.
    """
    hasher = hasher_cls()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(hasher.block_size * blocks_per_chunk), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def valid_sha256_checksum(*args, value: str = None, **kwargs) -> bool:
    """Validate sha256/sha3_256 checksum"""
    if (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdef" for c in value.lower())
    ):
        return True
    return False


def test_sha256_function(func, *args, **kwargs):
    temp = os.path.join(
        tempfile.gettempdir(), "checksum_test_" + str(random.randint(0, 1000000))
    )
    with open(os.path.join(temp), "wb") as f:
        f.write(b"foo")
    assert (
        func(temp) == "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
    ), "checksum function incorrect"


def test_sha3_256_function(func, *args, **kwargs):
    temp = os.path.join(
        tempfile.gettempdir(), "checksum_test_" + str(random.randint(0, 1000000))
    )
    with open(os.path.join(temp), "wb") as f:
        f.write(b"foo")
    assert (
        func(temp) == "76d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01"
    ), "checksum function incorrect"


def valid_crc32_checksum(*args, value: str = None, **kwargs) -> bool:
    """validate crc32 checksum"""
    if (
        isinstance(value, str)
        and len(value) == 8
        and all(c in "0123456789ABCDEF" for c in value.upper())
    ):
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

    NPEXP_ROOT = pathlib.Path(r"//allen/programs/mindscope/workgroups/np-exp")

    def __init__(self, path: str | pathlib.Path):
        if not isinstance(path, (str, pathlib.Path)):
            raise TypeError(
                f"{self.__class__.__name__} path must be a string or pathlib.Path object"
            )

        self.folder = self.__class__.folder(path)
        # TODO maybe not do this - could be set to class without realizing - just assign for instances

        if self.folder:
            # extract the constituent parts of the session folder
            self.id = self.folder.split("_")[0]
            self.mouse = self.folder.split("_")[1]
            self.date = self.folder.split("_")[2]
        elif "production" and "prod0" in str(path):
            self.id = re.search(r"(?<=_session_)\d+", str(path)).group(0)
            lims_dg = dg.lims_data_getter(self.id)
            self.mouse = lims_dg.data_dict["external_specimen_name"]
            self.date = lims_dg.data_dict["datestring"]
            self.folder = ("_").join([self.id, self.mouse, self.date])
        else:
            raise SessionError(f"{path} does not contain a valid session folder string")

    @classmethod
    def folder(cls, path: Union[str, pathlib.Path]) -> Union[str, None]:
        """Extract [8+digit session ID]_[6-digit mouse ID]_[6-digit date
        str] from a file or folder path"""
        session_reg_exp = r"[0-9]{8,}_[0-9]{6}_[0-9]{8}"

        session_folders = re.findall(session_reg_exp, str(path))
        if session_folders:
            if not all(s == session_folders[0] for s in session_folders):
                logging.debug(
                    f"{cls.__class__.__name__} Mismatch between session folder strings - file may be in the wrong folder: {path}"
                )
            return session_folders[0]
        else:
            return None

    @property
    def npexp_path(self) -> Union[pathlib.Path, None]:
        """get session folder from path/str and combine with npexp root to get folder path on npexp"""
        folder = self.folder
        if not folder:
            return None
        return self.NPEXP_ROOT / folder

    @property
    def lims_path(self) -> Union[pathlib.Path, None]:
        """get lims id from path/str and lookup the corresponding directory in lims"""
        if not (self.folder or self.id):
            return None
        if not hasattr(self, "_lims_path"):
            try:
                lims_dg = dg.lims_data_getter(self.id)
                WKF_QRY = """
                            SELECT es.storage_directory
                            FROM ecephys_sessions es
                            WHERE es.id = {}
                            """
                lims_dg.cursor.execute(WKF_QRY.format(lims_dg.lims_id))
                exp_data = lims_dg.cursor.fetchall()
                if exp_data and exp_data[0]["storage_directory"]:
                    self._lims_path = pathlib.Path(
                        "/" + exp_data[0]["storage_directory"]
                    )
                else:
                    logging.debug(
                        "lims checked successfully, but no folder uploaded for {}".format(
                            self.id
                        )
                    )
                    self._lims_path = None
            except Exception as e:
                logging.info(
                    "Checking for lims folder failed for {}: {}".format(self.id, e)
                )
                self._lims_path = None
        return self._lims_path
    
    @property
    def lims_session_json(self) -> dict:
        """Content from lims on ecephys_session
        
        This property getter just prevents repeat calls to lims
        """
        if not hasattr(self, '_lims_session_json'):
            self._lims_session_json = self.get_lims_content()
        return self._lims_session_json
    
    def get_lims_content(self) -> dict:
        response = requests.get(f"http://lims2/behavior_sessions/{self.id}.json?")
        if response.status_code == 404:
            response = requests.get(f"http://lims2/ecephys_sessions/{self.id}.json?")
            if response.status_code == 404:
                return None
        elif response.status_code != 200:
            raise requests.RequestException(f"Could not find content for session {self.id} in LIMS")
        
        return response.json()
        
    @property
    def project(self) -> str:
        if self.lims_session_json:
            return self.lims_session_json['project']['code']
        return None

class SessionFile:
    """Represents a single file belonging to a neuropixels ecephys session"""

    session = None

    def __init__(self, path: Union[str, pathlib.Path]):
        """from the complete file path we can extract some information upon
        initialization"""

        if not isinstance(path, (str, pathlib.Path)):
            raise TypeError(
                f"{self.__class__.__name__}: path must be a str or pathlib.Path pointing to a file: {type(path)}"
            )

        path = pathlib.Path(path)

        # ensure the path is a file, not directory
        # ideally we would check the path on disk with pathlib.Path.is_file(), but that only works if the file exists
        # we also can't assume that a file that exists one moment will still exist the next
        # (threaded operations, deleting files etc) - so no 'if exists, .is_file()?'
        # we'll try using the suffix/extension first, but be aware that sorted probe folders named 'Neuropix-PXI-100.1'
        # will give a non-empty suffix here - probably safe to assume that a numeric suffix is never an actual file
        is_file = path.suffix != ""
        is_file = False if path.suffix.isdecimal() else is_file
        try:
            is_file = True if path.is_file() else is_file
            # is_file() returns false if file doesn't exist so only change it if it exists
        except:
            pass

        if not is_file:
            raise FilepathIsDirError(
                f"{self.__class__.__name__}: path must point to a file {path}"
            )
        else:
            try:
                self.path = path  # might be read-only, in the case of DVFiles
            except AttributeError:
                pass

        self.name = self.path.name

        # get the name of the folder the file lives in (which may be the same as self.root_path below)
        self.parent = self.path.parent

        # extract the session ID from anywhere in the path
        self.session = Session(self.path)
        if not self.session:
            raise SessionError(
                f"{self.__class__.__name__}: path does not contain a session ID {self.path.as_posix}"
            )

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
            raise SessionError(
                f"{self.__class__.__name__}: session_folder not found in path {self.path.as_posix()}"
            )

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
        """filepath relative to a session folder's parent"""
        # wherever the file is, get its path relative to the parent of a
        # hypothetical session folder ie. session_id/.../filename.ext :
        session_relative_path = self.path.relative_to(self.root_path)
        if session_relative_path.parts[0] != self.session.folder:
            return pathlib.Path(self.session.folder, session_relative_path.as_posix())
        else:
            return session_relative_path

    @property
    def relative_path(self) -> pathlib.Path:
        """filepath relative to a session folder"""
        return pathlib.Path(self.session_relative_path.relative_to(self.session.folder))

    @property
    def root_relative_path(self) -> pathlib.Path:
        """Filepath relative to the first parent with session string in name.

        #!watch out: Different meaning of 'root' to 'root_path' above

        This property will be most useful when looking for files in lims ecephys_session_XX
        folders, since the 'first parent with session string in name' is often renamed in lims:
        e.g. '123456789_366122_20220618_probeA_sorted' becomes 'job-id/probe-id_probeA'
        - filepaths relative to the renamed folder should be preserved, so we should be
        able to glob for them using this property.
        """
        # TODO update root_path to use the same meaning of 'root'
        for parent in self.path.parents:
            if self.session.folder in parent.parts[-1]:
                return self.path.relative_to(parent)
        else:
            # if no parent with session string in name, we have a file with session
            # string in its filename, sitting in some unknown folder:
            return self.path.relative_to(self.parent)

    @property
    def probe_dir(self) -> str:
        # if a file lives in a probe folder (_probeA, or _probeABC) it may have the same name, size (and even checksum) as
        # another file in a corresponding folder (_probeB, or _probeDEF) - the data are identical if all the above
        # match, but it would still be preferable to keep track of these files separately -> this property indicates
        probe = re.search(
            r"(?<=_probe)_?(([A-F]+)|([0-5]{1}))", self.path.parent.as_posix()
        )
        if probe:
            probe_name = probe[0]
            # only possibile probe_names here are [A-F](any combination) or [0-5](single digit)
            if len(probe_name) == 1:
                if ord("0") <= ord(probe_name) <= ord("5"):
                    # convert single-digit probe numbers to letters
                    probe_name = chr(ord("A") + int(probe_name))
                    # controversial? mostly we store in probe dirs with letter, not digit, so
                    # for finding 'the same filename in a different location' (ie a backup)
                    # it probably makes sense to use the probe letter here to
                    # facilitate comparisons
                assert ord("A") <= ord(probe_name) <= ord("F"), logging.error(
                    "{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(
                        probe_name
                    )
                )
            else:
                assert all(letter in "ABCDEF" for letter in probe_name), logging.error(
                    "{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(
                        probe_name
                    )
                )
            return probe_name
        return None

    # backup paths below are only returned if they exist and are not the same as the
    # current file path (ie. if the file is not already in a backup location) -------------- #
    @property
    def npexp_path(self) -> pathlib.Path:
        """Expected path to a copy on npexp, regardless of whether or not it exists.
        """
        # for symmetry with other paths/backups add the 'cached' property, tho it's not
        # necessary
        self._npexp_path = self.session.NPEXP_ROOT / self.session_relative_path
        return self._npexp_path

    @property
    def npexp_backup(self) -> pathlib.Path:
        """Actual path to backup on npexp if it currently exists, and isn't our current
        file, and our current file isn't on lims"""
        if (
            self.npexp_path
            and self.npexp_path.exists()
            and self.npexp_path != self.path
            and self.session.lims_path not in self.path.parents
        ):
            return self.npexp_path
        return None

    @property
    def lims_path(self) -> pathlib.Path:
        """Expected path to a copy on lims, regardless of whether or not it exists.

        This property getter just prevents repeat calls to find the path.
        """
        if not hasattr(self, "_lims_path"):
            self._lims_path = self.get_lims_path()
        return self._lims_path
    
    @property
    def lims_backup(self) -> pathlib.Path:
        """Actual path to backup on LIMS if it currently exists"""
        if (
            self.lims_path
            and self.lims_path.exists()
            and self.lims_path.as_posix() != self.path.as_posix()
        ):
            return self.lims_path
        return None

    def get_lims_path(self) -> pathlib.Path:
        """Path to backup on Lims (which must exist for this current method to work)"""
        if not self.session.lims_path:
            return None

        # for files in lims 'ecephys_session_XXXX' folders, which aren't in 'job_id' sub-folders:
        if (self.session.lims_path / self.root_relative_path).is_file():
            return self.session.lims_path / self.root_relative_path

        # for files in 'job_id' folders we'll need to glob and take the most recent file
        # version (assuming this == highest job id)
        pattern = f"*/{self.root_relative_path.as_posix()}"
        matches = [
            m.as_posix() for m in self.session.lims_path.glob(pattern)
        ]  # convert to strings for sorting
        if not matches: # try searching one subfolder deeper
            pattern = "*/" + pattern
        matches = [
            m.as_posix() for m in self.session.lims_path.glob(pattern)
        ]  # convert to strings for sorting
        if matches and self.probe_dir:
            matches = [m for m in matches if f"_probe{self.probe_dir}" in m]
        if not matches:
            return None
        return pathlib.Path(sorted(matches)[-1])
    
    @property
    def z_drive_path(self) -> pathlib.Path:
        """Expected path to a copy on 'z' drive, regardless of whether or not it exists.

        This property getter just prevents repeat calls to find the path.
        """
        if not hasattr(self, "_z_drive_path"):
            self._z_drive_path = self.get_z_drive_path()
        return self._z_drive_path
    
    @property
    def z_drive_backup(self) -> pathlib.Path:
        """Path to backup on 'z' drive if it currently exists, also considering the
        location of the current file (e.g. if file is on npexp, z drive is not a backup).
        """
        if (
            self.z_drive_path
            and self.z_drive_path.exists()
            and self.z_drive_path.as_posix() != self.path.as_posix()
            and "neuropixels_data" not in self.path.parts
            and self.path != self.npexp_path
            # if file is on npexp, don't consider z drive as a backup
            and self.session.lims_path not in self.path.parents
            # if file is on lims, don't consider z drive as a backup
        ):
            return self.z_drive_path
        return None

    def get_z_drive_path(self) -> pathlib.Path:
        """Path to possible backup on 'z' drive (might not exist)"""
        # TODO add session method for getting z drive, using rigID from lims
        # then use whichever z drive exists (original vs current)
        running_on_rig = nptk.COMP_ID if "NP." in nptk.COMP_ID else None
        local_path = str(self.path)[0] not in ["/", "\\"]
        rig_from_path = nptk.Rig.rig_from_path(self.path.as_posix())

        # get the sync computer's path
        if running_on_rig and local_path:
            sync_path = nptk.Rig.Sync.path
        elif rig_from_path:
            rig_idx = nptk.Rig.rig_str_to_int(rig_from_path)
            sync_path = (
                "//"
                + nptk.ConfigHTTP.get_np_computers(rig_idx, "sync")[
                    f"NP.{rig_idx}-Sync"
                ]
            )
        else:
            sync_path = None
        # the z drive/neuropix data folder for this rig
        return (
            (
                pathlib.Path(sync_path, "neuropixels_data", self.session.folder)
                / self.session_relative_path
            )
            if sync_path
            else None
        )

    def __lt__(self, other):
        if self.session.id == other.session.id:
            return self.session_relative_path < other.session_relative_path
        return self.session.id < other.session.id

    @property
    def incoming_path(self) -> pathlib.Path:
        """Path to file in incoming folder (may not exist)"""
        return INCOMING_PATH / self.relative_path

class DataValidationFile(abc.ABC):
    """Represents a file to be validated

    Not to be used directly, but rather subclassed.
    Can be subclassed easily to change the checksum database/alogrithm

    Call <superclass>.__init__(path, checksum, size) in subclass __init__

    """

    # TODO add hostname property when path is local

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

    def __init__(
        self,
        path: Union[str, pathlib.Path] = None,
        checksum: str = None,
        size: int = None,
    ):
        """setup depending on the inputs"""

        if not (path or checksum):
            raise ValueError(
                f"{self.__class__.__name__}: either path or checksum must be set"
            )

        if path and not isinstance(path, (str, pathlib.Path)):
            raise TypeError(
                f"{self.__class__.__name__}: path must be a str pointing to a file: {type(path)}"
            )
        if path:
            path = pathlib.Path(path)
            try:
                if path.is_symlink():
                    path.resolve() #! follow symlinks to data
            except OSError:
                # pathlib raises error if inaccessible
                raise ValueError(
                    f"{self.__class__.__name__}: data at end of symlink is inaccessible: {path}"
            )
            # ensure the path is a file, not directory
            # ideally we would check the path on disk with pathlib.Path.is_file(), but that only works if the file exists
            # we also can't assume that a file that exists one moment will still exist the next
            # (threaded operations, deleting files etc) - so no 'if exists, .is_file()?'
            # we'll try using the suffix/extension first, but be aware that sorted probe folders named 'Neuropix-PXI-100.1'
            # will give a non-empty suffix here - probably safe to assume that a numeric suffix is never an actual file
            is_file = path.suffix != ""
            is_file = False if path.suffix.isdecimal() else is_file
            try:
                is_file = True if path.is_file() else is_file
                # is_file() returns false if file doesn't exist so only change it if it exists
            except:
                pass

            if not is_file:
                raise FilepathIsDirError(
                    f"{self.__class__.__name__}: path must point to a file {path}"
                )

            self.name = path.name

            # TODO consolidate file (vs dir) assertion with SessionFile: currently running this twice

            # TODO update lines below using path as str
            path = path.as_posix()

            # we have a mix in the databases of posix paths with and without the double fwd slash
            if path[0] == "/" and path[1] != "/":
                path = "/" + path

            # if a file lives in a probe folder (_probeA, or _probeABC) it may have the same name, size (and even checksum) as
            # another file in a corresponding folder (_probeB, or _probeDEF) - the data are identical if all the above
            # match, but it would still be preferable to keep track of these files separately -> this property indicates
            probe = re.search(
                r"(?<=_probe)_?(([A-F]+)|([0-5]{1}))", path.split(self.name)[0]
            )
            if probe:
                probe_name = probe[0]
                # only possibile probe_names here are [A-F](any combination), [0-5](single digit)
                if len(probe_name) == 1:
                    # convert single-digit probe numbers to letters
                    if ord("0") <= ord(probe_name) <= ord("5"):
                        probe_name = chr(ord("A") + int(probe_name))
                    assert ord("A") <= ord(probe_name) <= ord("F"), logging.error(
                        "{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(
                            probe_name
                        )
                    )
                else:
                    assert all(
                        letter in "ABCDEF" for letter in probe_name
                    ), logging.error(
                        "{} is not a valid probe name: must include a single digit [0-5], or some combination of capital letters [A-F]".format(
                            probe_name
                        )
                    )

        # set read-only property that will be hashed
        self._path = pathlib.Path(path) if path else None

        # set read-only property, won't be hashed
        self._probe_dir = (
            probe_name if path and probe is not None else None
        )  # avoid checking 'if probe' since it could equal 0

        # avoid checking 'if size' since it could equal 0
        if self.path and size is None:
            try:
                size = os.path.getsize(self.path.as_posix())
            except:
                size = None
        if size is not None and not isinstance(size, int):
            if isinstance(size, str) and size.isdecimal():
                size = int(size)
            else:
                raise ValueError(
                    f"{self.__class__.__name__}: size must be an integer {size}"
                )

        # set read-only property that will be hashed
        self._size = size

        if (
            not checksum
            and self.size
            and self.size < self.checksum_threshold
            and os.path.exists(self.path.as_posix())
        ):
            checksum = self.__class__.generate_checksum(
                self.path, self.size
            )  # change to use instance method if available

        if checksum and not self.checksum_validate(value=checksum):
            raise ValueError(
                f"{self.__class__.__name__}: trying to set an invalid {self.checksum_name} checksum"
            )

        # set read-only property that will be hashed
        self._checksum = checksum if checksum else None

    # read-only methods
    @property
    def path(self):
        if hasattr(self, "_path") and self._path:
            return self._path
        return None

    @property
    def size(self):
        if hasattr(self, "_size") and self._size is not None:
            return self._size
        return None

    @property
    def checksum(self):
        if hasattr(self, "_checksum"):
            return self._checksum
        return None

    @property
    def probe_dir(self):
        if hasattr(self, "_probe_dir") and self._probe_dir:
            return self._probe_dir
        return None

    @classmethod
    def generate_checksum(cls, path, size=None) -> str:
        cls.checksum_test(cls.checksum_generator)
        return cls.checksum_generator(path, size=size)

    def report(self, other: Union[DataValidationFile, List[DataValidationFile]]):
        """Log a report on the comparison with one or more files"""
        if isinstance(other, list):
            for others in other:
                self.report(others)
        else:
            result = self.Match(self.compare(other)).name
            logging.info(
                f"{result} | {self.path.as_posix()} {other.path} | {self.checksum} {other.checksum} | {self.size} {other.size} bytes"
            )

    def __repr__(self):
        return f"(path='{self.path.as_posix() or ''}', checksum='{self.checksum or ''}', size={self.size or ''})"

    def __str__(self):
        possible_session = f"{self.session.folder if (hasattr(self,'session') and self.session) else ''}"
        possible_session = possible_session if possible_session not in self.name else ""
        possible_probe = f"{'probe'+self.probe_dir+' ' if self.probe_dir else ''}"
        return f"{possible_session}{possible_probe}{self.name}"

    def __lt__(self, other):
        if self.name and other.name:
            if self.name == other.name:
                return self.checksum < other.checksum or self.size < other.size
            else:
                return self.name < other.name
        else:
            return self.checksum < other.checksum or self.size < other.size

    @enum.unique
    class Match(enum.IntFlag):
        """Integer enum as a shorthand for DataValidationFile comparison.
        - test for file.compare(other)
            5-10 for self
            >10 for matches of interest
            >15 for possible backups (1 or both checksums reqd.)
            >20 for valid backups
        - db.get_matches(file) should return entries in db for which file.compare(db_entry) > 0

        Note: some of the more detailed interpretations require checksum_name to be equal, so
        conditions need updating (Sept'22), but the most used comparisons are still correct:
        self (5/6/7), possible copies (>15), valid backups (>20)
        """

        # =======================================================================================
        # files with nothing in common - these comparisons are generally not useful & filtered out

        UNRELATED = 0
        UNKNOWN = -1
        UNKNOWN_CHECKSUM_TYPE_MISMATCH = (
            -2
        )  # size, name and checksum type are different
        CHECKSUM_COLLISION = -3  # rare case of different files with the same checksum
        SELF_PREVIOUS_VERSION = -5  # path is identical, size or checksum mismatch

        # =======================================================================================
        # files at the same location on disk

        SELF = 5

        #!
        SELF_MISSING_SELF = 6  # self is missing a checksum  #? further info
        SELF_MISSING_OTHER = 7  # other file is missing a checksum  #? further info
        #! watch out: the two above depend on the order of objects in the inequality

        SELF_CHECKSUM_TYPE_MISMATCH = (
            8  # size and path identical, checksum types are different
        )

        # =======================================================================================
        # the most tentative of matches - going off only the size (+ probe letters if applicable)

        POSSIBLE_COPY_RENAMED = 16  # ? further info

        # =======================================================================================
        # category for files with the same name in different locations

        # ---------------------------------------------------------------------------------------
        # mismatched data or db entries need updating

        COPY_UNSYNCED_CHECKSUM = 10  # sizes differ but checksums match
        COPY_UNSYNCED_OR_CORRUPT_DATA = 11  # sizes match, but data differs
        COPY_UNSYNCED_DATA = 12  # checksum and size differ

        # ---------------------------------------------------------------------------------------
        # copies that might be valid, but need checksums to be sure

        COPY_MISSING_BOTH = 17  # ? further info
        COPY_MISSING_SELF = 18  # ? further info
        COPY_MISSING_OTHER = 19  # ? further info
        COPY_CHECKSUM_TYPE_MISMATCH = 20  # ? convert one to the other

        # ---------------------------------------------------------------------------------------
        # matching data - this is generally what we want to search for to validate backups

        VALID_COPY = 21
        VALID_COPY_RENAMED = 22

    SELVES: Tuple[Match] = (
        Match.SELF,
        Match.SELF_MISSING_SELF,
        Match.SELF_MISSING_OTHER,
        Match.SELF_CHECKSUM_TYPE_MISMATCH,
    )
    """`self.compare(other)` will be in the returned list if `self` and `other` are
    suspected to be the same file"""

    VALID_COPIES: Tuple[Match] = (
        Match.VALID_COPY,
        Match.VALID_COPY_RENAMED,
    )
    """`self.compare(other)` will be in the returned list if `other` is a
    checksum-validated copy of `self`"""

    UNCONFIRMED_COPIES: Tuple[Match] = (
        Match.COPY_MISSING_BOTH,
        Match.COPY_MISSING_SELF,
        Match.COPY_MISSING_OTHER,
        Match.COPY_CHECKSUM_TYPE_MISMATCH,
        Match.POSSIBLE_COPY_RENAMED,
    )
    """`self.compare(other)` will be in the returned list if file names and sizes
        suggest `other` is a copy of `self`, and checksums do not contraindicate, but
        additional checksums need to be generated to confirm"""

    INVALID_COPIES: Tuple[Match] = (
        Match.COPY_UNSYNCED_CHECKSUM,
        Match.COPY_UNSYNCED_OR_CORRUPT_DATA,
        Match.COPY_UNSYNCED_DATA,
    )
    """`self.compare(other)` will be in the returned list if `other` has a checksum or
    size that indicates an invalid copy or out-of-date information"""

    IGNORED: Tuple[Match] = (
        Match.UNRELATED,
        Match.UNKNOWN,
        Match.UNKNOWN_CHECKSUM_TYPE_MISMATCH,
        Match.CHECKSUM_COLLISION,
        Match.SELF_PREVIOUS_VERSION,
    )
    """`self.compare(other)` will be in the returned list if `other` has properties
    that suggest it should be ignored for the purposes of validating data"""

    def __hash__(self):
        # this might be a bad idea: added to allow for set() operations on DVFiles to remove duplicates when getting
        # a database - but DVFiles are mutable
        return (
            hash(self.checksum) ^ hash(self.size) ^ hash(self.path.as_posix().lower())
        )

    def __eq__(self, other):
        return hash(self) == hash(other)

    def compare(self, other: DataValidationFile) -> Match:
        """Test equality of two DataValidationFile objects"""
        # size and path fields are required entries in a DVF entry in database -
        # checksum is optional, so we need to check for it in both objects
        if not isinstance(other, DataValidationFile):
            raise TypeError(f"Cannot compare DataValidationFile to {type(other)}")
        # -make use of addtl file check: depends on files existing so isn't always
        # reliable
        samefile = None
        try:
            if self.path.exists() and other.path.exists():
                samefile = self.path.samefile(other.path)
        except OSError:
            pass

        if self is other or hash(self) == hash(other) or (
            self.checksum
            and other.checksum
            and (self.checksum == other.checksum)
            and (self.size == other.size)
            and (
                self.path.as_posix().lower() == other.path.as_posix().lower()
                or samefile is True
            )
            and samefile is not False
        ):  # self
            return self.__class__.Match.SELF

        #! watch out: SELF_MISSING_SELF and SELF_MISSING_OTHER
        # depend on the order of objects in the inequality
        elif (
            (self.size == other.size)
            and (
                self.path.as_posix().lower() == other.path.as_posix().lower()
                or samefile is True
            )
            and (not self.checksum)
            and (other.checksum)
            and samefile is not False
        ):  # self without checksum confirmation (self missing)
            return self.__class__.Match.SELF_MISSING_SELF
        #! watch out: SELF_MISSING_SELF and SELF_MISSING_OTHER
        # depend on the order of objects in the inequality
        elif (
            (self.size == other.size)
            and (
                self.path.as_posix().lower() == other.path.as_posix().lower()
                or samefile is True
            )
            and (self.checksum)
            and not (other.checksum)
            and samefile is not False
        ):  # self without checksum confirmation (other missing)
            return self.__class__.Match.SELF_MISSING_OTHER

        elif (
            (self.size == other.size)
            and (
                self.path.as_posix().lower() == other.path.as_posix().lower()
                or samefile is True
            )
            and (self.checksum and other.checksum)
            and (self.checksum_name != other.checksum_name)
            and samefile is not False
        ):  # self without checksum confirmation (type mismatch)
            return self.__class__.Match.SELF_CHECKSUM_TYPE_MISMATCH

        elif (
            (
                self.size != other.size
                or (
                    self.checksum != other.checksum
                    and self.checksum_name == other.checksum_name
                )
            )
            and (
                self.path.as_posix().lower() == other.path.as_posix().lower()
                or samefile is True
            )
            and samefile is not False
        ):  # an old entry for the same file path
            return self.__class__.Match.SELF_PREVIOUS_VERSION

        elif (
            (not self.checksum and not other.checksum)
            and (self.size == other.size)
            and (self.name.lower() == other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
        ) and samefile is not True:  # copy without checksum confirmation (both missing)
            return self.__class__.Match.COPY_MISSING_BOTH

        elif (
            (self.checksum and not other.checksum)
            and (self.size == other.size)
            and (self.name.lower() == other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
            and samefile is not True
        ):  # copy without checksum confirmation (other missing)
            return self.__class__.Match.COPY_MISSING_OTHER

        elif (
            (not self.checksum and other.checksum)
            and (self.size == other.size)
            and (self.name.lower() == other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
            and samefile is not True
        ):  # copy without checksum confirmation (self missing)
            return self.__class__.Match.COPY_MISSING_SELF

        elif (
            (self.checksum and other.checksum)
            and (self.checksum_name != other.checksum_name)
            and (self.size == other.size)
            and (self.name.lower() == other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
            and samefile is not True
        ):  # copy without checksum confirmation (different types)
            return self.__class__.Match.COPY_CHECKSUM_TYPE_MISMATCH

        elif (
            (
                (not self.checksum or not other.checksum)
                or (self.checksum_name != other.checksum_name)
            )
            and (self.size == other.size)
            and (self.name.lower() != other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
            and samefile is not True
        ):  # possible copy, not self, different name
            return self.__class__.Match.POSSIBLE_COPY_RENAMED

        elif (
            (self.checksum and other.checksum)
            and (self.checksum == other.checksum)
            and (self.size == other.size)
            and (self.name.lower() == other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
            and samefile is not True
        ):  # valid copy, not self, same name
            return self.__class__.Match.VALID_COPY

        elif (
            self.checksum
            and other.checksum
            and (self.checksum == other.checksum)
            and (self.size == other.size)
            and (self.name.lower() != other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
            and samefile is not True
        ):  # valid copy, different name
            return self.__class__.Match.VALID_COPY_RENAMED

        elif (
            self.checksum
            and other.checksum
            and (self.checksum_name == other.checksum_name)
            and (self.name.lower() == other.name.lower())
            and (self.path.as_posix().lower() != other.path.as_posix().lower())
            and (self.probe_dir == other.probe_dir)
            and samefile is not True
        ):  # invalid copy ( multiple categories)

            if (
                (self.size != other.size)
                and (self.checksum != other.checksum)
                and (self.probe_dir == other.probe_dir)
            ):  # out-of-sync copy or incorrect data named as copy
                return self.__class__.Match.COPY_UNSYNCED_DATA

            if (
                (self.size != other.size)
                and (self.checksum == other.checksum)
                and (self.probe_dir == other.probe_dir)
            ):  # out-of-sync copy or incorrect data named as copy
                # plus checksum which needs updating
                # (different size with same checksum isn't possible)
                return self.__class__.Match.COPY_UNSYNCED_CHECKSUM

            if (
                (self.size == other.size)
                and (self.checksum != other.checksum)
                and (self.probe_dir == other.probe_dir)
            ):  # possible data corruption, or checksum needs updating
                return self.__class__.Match.COPY_UNSYNCED_OR_CORRUPT_DATA

        elif (
            self.checksum
            and other.checksum
            and (self.checksum_name == other.checksum_name)
            and (self.checksum == other.checksum)
            and (self.size != other.size)
            and (self.name.lower() != other.name.lower())
            and samefile is not True
        ):  # possible checksum collision
            return self.__class__.Match.CHECKSUM_COLLISION

        elif (
            self.checksum
            and other.checksum
            and (self.checksum != other.checksum)
            and (self.size != other.size)
            and (self.name.lower() != other.name.lower())
            and (self.checksum_name == other.checksum_name)
            and samefile is not True
        ):  # apparently unrelated files (different name && checksum && size)
            return self.__class__.Match.UNRELATED

        else:  # insufficient information
            if self.checksum_name != other.checksum:
                return self.__class__.Match.UNKNOWN_CHECKSUM_TYPE_MISMATCH
            return self.__class__.Match.UNKNOWN


class CRC32DataValidationFile(DataValidationFile, SessionFile):

    checksum_threshold: int = 0  # don't generate checksum for any files by default
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
        DataValidationFile.__init__(self, path=path, checksum=checksum, size=size)
        SessionFile.__init__(self, path)


class SHA256DataValidationFile(DataValidationFile, SessionFile):
    hashlib_func = functools.partial(chunk_hashlib, hasher_cls=hashlib.sha256)

    checksum_threshold: int = 0  # don't generate checksum for any files by default
    checksum_name: str = "sha256"
    checksum_generator: Callable[[str], str] = hashlib_func
    checksum_test: Callable[[Callable], None] = test_sha256_function
    checksum_validate: Callable[[str], bool] = valid_sha256_checksum

    def __init__(self, path: str = None, checksum: str = None, size: int = None):
        DataValidationFile.__init__(self, path=path, checksum=checksum, size=size)
        # if the path doesn't contain a session_id, this will raise an error:
        SessionFile.__init__(self, path)


class SHA3_256DataValidationFile(DataValidationFile, SessionFile):
    hashlib_func = functools.partial(chunk_hashlib, hasher_cls=hashlib.sha3_256)

    checksum_threshold: int = 0  # don't generate checksum for any files by default
    checksum_name: str = "sha3_256"
    checksum_generator: Callable[[str], str] = hashlib_func
    checksum_test: Callable[[Callable], None] = test_sha3_256_function
    checksum_validate: Callable[
        [str], bool
    ] = valid_sha256_checksum  # note that this is the same as the SHA256 checksum validation function

    def __init__(self, path: str = None, checksum: str = None, size: int = None):
        DataValidationFile.__init__(self, path=path, checksum=checksum, size=size)
        # if the path doesn't contain a session_id, this will raise an error:
        SessionFile.__init__(self, path)


class OrphanedDVFile(DataValidationFile):
    """Files with no session identifier, containing only enough information to search
    the database for matches"""

    default_checksum_type = "sha3_256"

    checksum_threshold: int = None
    checksum_name: str = None
    checksum_generator: Callable[[str], str] = None
    checksum_test: Callable[[Callable], None] = None
    checksum_validate: Callable[[str], bool] = None

    def __init__(
        self,
        *args,
        type: Literal["sha3_256", "sha256", "crc32"] = default_checksum_type,
        **kwargs,
    ):
        if type not in available_DVFiles.keys():
            raise ValueError(f"Unknown DVFile type: {type}")
        self.convert(type)
        DataValidationFile.__init__(self, *args, **kwargs)

    def convert(self, type: Literal["sha3_256", "sha256", "crc32"]):
        """Convert class to use specific checksum type"""
        for attr in [
            "checksum_threshold",
            "checksum_name",
            "checksum_generator",
            "checksum_test",
            "checksum_validate",
        ]:
            setattr(self, attr, getattr(available_DVFiles[type], attr))
        self._checksum = None

    def generate_checksum(self, path, size=None) -> str:
        """Overloaded classmethod, since this class has no default generator function"""
        self.checksum_test(self.checksum_generator)
        return self.checksum_generator(path, size=size)


class DataValidationDB(abc.ABC):
    """Represents a database of files with validation metadata

    serves as a template for interacting with a database of filepaths,
    filesizes, and filehashes, for validating data integrity

    not to be used directly, but subclassed: make a new subclass that implements
    each of the "abstract" methods specified in this class

    as long as the subclass methods accept the same inputs and output the
    expected results, a new database subclass can slot in to replace an old one
    in some other code without needing to make any other changes to that code

    """

    DVFile: DataValidationFile = NotImplemented

    # ? both of these could be staticmethods

    @abc.abstractmethod
    def add_file(self, file: DataValidationFile):
        """add a file to the database"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_matches(
        self,
        file: DataValidationFile,
        path: str = None,
        size: int = None,
        checksum: str = None,
        match: int = None,
    ) -> List[DataValidationFile]:  # , Optional[List[int]]:
        """search database for entries that match any of the given arguments"""
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
        """add an entry to the database"""
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        key = file.session.id

        with shelve.open(cls.db, writeback=True) as db:
            if key in db and (
                [x for x in db[key] if file.compare(x) == cls.DVFile.Match.SELF]
                or [
                    x
                    for x in db[key]
                    if file.compare(x) == cls.DVFile.Match.SELF_MISSING_SELF
                ]
            ):
                print(f"skipped {file.session.folder}/{file.name} in Shelve database")
                return

            if key in db:
                db[key].append(file)
            else:
                db[key] = [file]

            print(f"added {file.session.folder}/{file.name} to Shelve database")

    # @classmethod
    # def save(cls):
    #     self.db.sync()

    @classmethod
    def get_matches(
        cls,
        file: DataValidationFile = None,
        path: str = None,
        size: int = None,
        checksum: str = None,
        match: int = None,
    ) -> List[DataValidationFile]:  # , Optional[List[int]]:
        """search database for entries that match any of the given arguments"""
        if not file:
            file = cls.DVFile(path=path, size=size, checksum=checksum)

        key = file.session.id

        with shelve.open(cls.db, writeback=False) as db:
            if key in db:
                matches = db[key]

        if (
            match
            and isinstance(match, int)
            and (
                match in [x.value for x in cls.DVFile.Match]
                or match in [x for x in cls.DVFile.Match]
            )
        ):
            return (
                [o for o in matches if file.compare(o) == match > 0],
                [file.compare(o) for o in matches if file.compare(o) == match > 0],
            )
        else:
            return (
                [o for o in matches if file.compare(o) > 0],
                [file.compare(o) for o in matches if file.compare(o) > 0],
            )

    # def __del__(self):
    #     self.db.close()


class MongoDataValidationDB(DataValidationDB):
    """
    A database that stores validation data in mongodb
    """

    DVFile: DataValidationFile = SHA3_256DataValidationFile  # default
    db = MONGO_COLLECTION  # moved to outer so connection to client is made once per session

    @classmethod
    def add_file(
        cls,
        file: DataValidationFile = None,
        path: Union[str, pathlib.Path] = None,
        size: int = None,
        checksum: str = None,
    ):
        """Add an entry to the database"""
        if not isinstance(file, DataValidationFile):

            if isinstance(
                file, (str, pathlib.Path)
            ):  # path provided as positional argument
                path = file
            try:  # make a new object with the default DVFile class
                file = cls.DVFile(path=path, size=size, checksum=checksum)
            except SessionError:  # if no session string in path
                file = OrphanedDVFile(path=path, size=size, checksum=checksum)
            except Exception:  # anything else we'd rather not halt the program
                return
            logging.debug(
                f"No DVFile provided to add_file() - created {file.__class__.__name__} from path"
            )

        if not file.checksum:
            logging.debug(f"Checksum missing - not entered into MongoDB {file.path}")
            return

        # search for the fields that define a unique entry in db, so only one
        # entry can be returned/replaced
        # * MongoDB has a unique index on path + type, so unique entries are enforced
        existing_entry = {
            "path": file.path.as_posix(),
            "type": file.checksum_name,
            # TODO add hostname if location is on local machine, or convert all paths
        }

        # if an entry for the same file exists but is out of date, we'll replace it
        # otherwise, a new entry is added the database (via upsert=True)
        new_entry = {
            "path": file.path.as_posix(),
            "checksum": file.checksum,
            "type": file.checksum_name,
        }

        if file.size is not None:
            new_entry["size"] = file.size

        if isinstance(file, SessionFile):
            # non-session files are now allowed in db
            new_entry["session_id"] = file.session.id

        # * adding hostnames for future comparison of local paths
        new_entry["hostname"] = socket.gethostname()

        result = cls.db.replace_one(
            filter=existing_entry,  # search for this
            replacement=new_entry,
            upsert=True,  # add new entry if not found
            hint="unique",
        )

        if not result.acknowledged:
            logging.info(f"Failed to add to MongoDB {file}")
            return
        if result.matched_count > 1:
            logging.warning(
                f"Multiple {file.type} entries for {file} in MongoDB - should be unique"
            )
            return
        if result.upserted_id:
            logging.debug(f"Added {file} to MongoDB with {file.checksum_name} checksum")
        elif result.modified_count:
            logging.debug(
                f"Updated {file} in MongoDB with {file.checksum_name} checksum"
            )

    @classmethod
    def get_matches(
        cls,
        file: DataValidationFile = None,
        path: Union[str, pathlib.Path] = None,
        size: int = None,
        checksum: str = None,
        match: Union[int, enum.IntEnum] = None,
    ) -> List[DataValidationFile]:  # , Optional[List[int]]:
        """Search database for entries that match any of the given arguments.

        - search is accelerated by using the session ID as a hint if available
        - if we search with a sessionID available (i.e. a SessionFile), we'll only
          return matches with the same sessionID
        - if we search without a sessionID, matches can be made with entries that have
          no sessionID field (e.g. self)
        - this should work because we generally want to look 'upwards' in the data
          transfer ladder to lims; files without a sessionID are on the bottom rung
        """
        if not file or not isinstance(file, DataValidationFile):
            if isinstance(
                file, (str, pathlib.Path)
            ):  # path provided as positional argument
                path = file
            try:
                # make a new object with the default DVFile class
                file = cls.DVFile(path=path, size=size, checksum=checksum)
            except SessionError:
                # create non-SessionFile DVFile object, use custom get_matches method
                try:
                    file = OrphanedDVFile(path=path, size=size, checksum=checksum)
                except:
                    return []

        match = [match] if match and not isinstance(match, list) else match

        entries = []
        if isinstance(file, SessionFile):  # expected behavior normally
            # TODO update some DataValidationFile type guards to SessionFile, now that
            # we're allowing OrphanedDVFiles
            entries = list(
                cls.db.find(
                    {
                        "session_id": file.session.id,
                    },
                    hint="session_id",
                )
            )
            # perform a quick filter on the list before converting to DVFiles,
            # skip path, which may be normalized by DVFile constructor
            if match and all(
                m in DataValidationFile.SELVES for m in match
            ):  # we'll never want to search for self_missing_other in db, but included here just in case it's in match
                entries = [
                    e
                    for e in entries
                    if (e["size"] == file.size or e["checksum"] == file.checksum)  # *
                ]
            elif match and all(m in DataValidationFile.VALID_COPIES for m in match):
                entries = [
                    e
                    for e in entries
                    if (e["size"] == file.size and e["checksum"] == file.checksum)
                ]
            """
            #*Disabled path == path match as it's too strict: the same file can be
            specified by different paths (UNC vs local, $ etc) - we could use
            pathlib.Path.samefile() for all entries, but we may as well just generate
            the DVFiles
            """

        elif isinstance(file, OrphanedDVFile):
            # for non-SessionFile DVFile objects, we want to find all matches possible
            if file.path:
                entries = list(
                    cls.db.find(
                        {"$or":[
                            {"path": file.path.as_posix()},
                            {"checksum": file.checksum},
                        ]}
                    )
                )
            if not entries and file.size is not None and file.size > 10:
                # small files will have too many matches in the db so we skip this
                entries = list(
                    cls.db.find(
                        {"size": file.size},
                    ),
                )

        if not entries:
            return []

        # * updated Sep '22: we now return set of mixed DVFile types, depending on the
        #  checksum type stored in the database. similar types aren't enforced in DVFile
        #  comparisons becauses collisions across different types (given matching paths,
        #  sizes, sessionIDs etc.) are as approx. as likely as collisions within a
        # type, so we can just compare across DVFile types freely
        matches = set()
        for entry in entries:
            if entry.get("session_id", False):
                DVFile_type = available_DVFiles[entry["type"]]
            else:
                DVFile_type = OrphanedDVFile
            try:
                matches.add(
                    DVFile_type(
                        path=entry["path"],
                        checksum=entry["checksum"],
                        size=entry.get("size", None),  # size not required, may be missing
                        # hostname=entry.get("hostname",None), # hostname may be missing from older entries
                    )
                )
            except:
                continue

        def filter_on_match_type(match_type: int) -> List[DataValidationFile]:
            if isinstance(match_type, int) and (
                match_type in [x.value for x in file.Match]
                or match_type in [x for x in file.Match]
            ):
                return [o for o in matches if file.compare(o) == match_type]
            return []

        if not match:
            return [
                o for o in matches if file.compare(o) not in DataValidationFile.IGNORED
            ]

        filtered_matches = []
        for m in match:
            filtered_matches += filter_on_match_type(m)
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

    path = "//allen/ai/homedirs/ben.hardcastle/crc32_data_validation_db.json"

    db: List[DataValidationFile] = None

    def __init__(self, path: str = None):
        if path:
            self.path = path
        self.load(self.path)

    def load(self, path: str = None):
        """load the database from disk"""

        # persistence in notebooks causes db to grow every execution
        if not self.db:
            self.db = []

        if not path:
            path = self.path

        if (
            os.path.basename(path) == "checksums.sums"
            or os.path.splitext(path)[-1] == ".sums"
        ):
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

            with open(path, "r") as f:
                lines = f.readlines()

            if not ("@" or "1970") in lines[0]:
                # this is probably a header line, skip it
                lines = lines[1:]

            for idx in range(0, len(lines), 2):
                line0 = lines[idx].rstrip()
                line1 = lines[idx + 1].rstrip()

                if "crc32" in line0:
                    crc32, *args = line1.split(" ")
                    filename = " ".join(args)

                    if filename[0] == "*":
                        filename = filename[1:]
                    path = "/".join([root, filename])

                    try:
                        file = self.DVFile(path=path, checksum=crc32)
                        self.add_file(file=file)
                    except SessionError as e:
                        print("skipping file with no session_id")
                        # return

        else:
            # this is one of my simple flat json databases - exact format
            # changed frequently, try to account for all possibilities

            if os.path.exists(path):

                with open(path, "r") as f:
                    items = json.load(f)

                for item in items:
                    keys = items[item].keys()

                    if "linux" in keys:
                        path = items[item]["linux"]
                    elif "posix" in keys:
                        path = items[item]["posix"]
                    elif "windows" in keys:
                        path = items[item]["windows"]
                    else:
                        path = None

                    checksum = (
                        items[item][self.DVFile.checksum_name]
                        if self.DVFile.checksum_name in keys
                        else None
                    )
                    size = items[item]["size"] if "size" in keys else None

                    try:
                        file = self.DVFile(path=path, checksum=checksum, size=size)
                        if ".npx2" or ".dat" in path:
                            self.add_file(
                                file=file, checksum=checksum, size=size
                            )  # takes too long to check sizes here
                        else:
                            self.add_file(file=file)
                    except SessionError as e:
                        print("skipping file with no session_id")
                        # return

    def save(self):
        """save the database to disk as json file"""

        with open(self.path, "r") as f:
            dump = json.load(f)

        for file in self.db:

            item_name = pathlib.Path(file.path).as_posix()

            item = {
                item_name: {
                    "windows": str(pathlib.PureWindowsPath(file.path)),
                    "posix": pathlib.Path(file.path).as_posix(),
                }
            }

            if file.checksum:
                item[item_name][self.DVFile.checksum_name] = file.checksum

            if file.size:
                item[item_name]["size"] = file.size

            dump.update(item)

        with open(self.path, "w") as f:
            json.dump(dump, f, indent=4)

    def add_folder(self, folder: str, filter: str = None):
        """add all files in a folder to the database"""
        for root, _, files in os.walk(folder):
            for file in files:
                if filter and isinstance(filter, str) and filter not in file:
                    continue
                file = self.DVFile(os.path.join(root, file))
                self.add_file(file=file)
        self.save()

    def add_file(
        self,
        file: DataValidationFile = None,
        path: str = None,
        checksum: str = None,
        size: int = None,
    ):
        """add a validation file object to the database"""

        if not file:
            file = self.DVFile(path=path, checksum=checksum, size=size)
        self.db.append(file)
        print(f"added {file.session.folder}/{file.name} to json database (not saved)")

    # TODO update to classmethod like ShelveDB
    def get_matches(
        self,
        file: DataValidationFile = None,
        path: str = None,
        size: int = None,
        checksum: str = None,
        match: int = None,
    ) -> List[DataValidationFile]:
        """search database for entries that match any of the given arguments"""
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
                    self.db.index(f)
                    for f in self.db
                    if f.size == size
                    or f.checksum == checksum
                    or (f.name == name and f.parent == parent)
                    or (f.session_folder == session_folder and f.size == size)
                ]


class LimsDVDatabase(DataValidationDB):
    """Database interface for retrieving checksums generated when files enter lims"""

    DVFile = CRC32DataValidationFile

    @classmethod
    def add_file(cls, *args, **kwargs):
        """Not implemented: information is read-only"""
        pass

    @classmethod
    def get_matches(
        cls,
        file: DataValidationFile = None,
        path: str = None,
        size: int = None,
        checksum: str = None,
        match: int = None,
    ) -> List[DataValidationFile]:
        if isinstance(file, DataValidationFile):
            path = file.path
        return [cls.get_file_with_hash_from_lims(path)]

    @staticmethod
    def hash_type_from_ecephys_upload_input_json(
        json_path: Union[str, pathlib.Path]
    ) -> str:
        """Read LIMS ECEPHYS_UPLOAD_QUEUE _input.json and return the hashlib class."""
        with open(json_path) as f:
            hasher_key = json.load(f).get("hasher_key", None)
        return hasher_key

    @staticmethod
    def lims_list_to_hexdigest(lims_hash: List[int]) -> str:
        lims_list_bytes = b""
        for i in lims_hash:
            lims_list_bytes += (i).to_bytes(1, byteorder="little")
        return lims_list_bytes.hex()

    @staticmethod
    def hashes_from_ecephys_upload_output_json(
        json_path: Union[str, pathlib.Path], hasher_key: str
    ) -> dict[str, str]:
        """Read LIMS ECEPHYS_UPLOAD_QUEUE _output.json and return a dict of {lims filepaths:hashes(hex)}."""
        # hash_cls is specified in output_json, not input json, so we'll need to open that
        # up and feed its value of hash_cls to this function
        # not calling 'hash_class_from_ecephys_upload_input_json' here because this
        # organization of files may change in future, and we need to pass the hash_cls to
        # other functions

        if not json_path and not hasher_key:
            raise ValueError("path and hashlib class must be provided")

        json_path = pathlib.Path(json_path)
        if not hasher_key in lims_available_hashers.keys():
            raise ValueError(
                f"hash_cls must be one of {list(lims_available_hashers.keys())}"
            )

        if not json_path.exists():
            raise FileNotFoundError("path does not exist")

        if not json_path.suffix == ".json":
            raise ValueError("path must be a json file")

        with open(json_path) as f:
            data = json.load(f)

        file_hash = {}
        for file in data["files"]:
            file_hash.update(
                {
                    file["destination"]: __class__.lims_list_to_hexdigest(
                        file["destination_hash"]
                    )
                }
            )
        return file_hash

    @staticmethod
    def upload_jsons_from_ecephys_session_or_file(
        session_or_file: Union[int, str, pathlib.Path, SessionFile]
    ) -> List[Tuple]:
        """Returns a list of tuples of (input_json, output_json) for any given session file
        or session id."""
        if isinstance(session_or_file, (str, pathlib.Path)):
            try:
                lims_dir = Session(session_or_file).lims_path
            except:
                return None
        else:
            if isinstance(session_or_file, (SessionFile)):
                lims_dir = session_or_file.session.lims_path
            elif isinstance(session_or_file, int):
                lims_dir = Session(path=f"{session_or_file}_366122_20220618").lims_path
            else:
                raise ValueError(
                    "session_or_file must be a sessionID or session folder string, or a SessionFile"
                )
        if not lims_dir:
            return None

        input_and_output_jsons = []
        for upload_input_json in itertools.chain(
            lims_dir.rglob("*_UPLOAD_QUEUE_*_input.json"),
            lims_dir.rglob("*EUR_QUEUE_*_input.json"),
        ):
            # get corresponding output json
            matching_output_jsons = upload_input_json.parent.rglob(
                "*" + upload_input_json.name.replace("input", "output")
            )
            upload_output_json = [f for f in matching_output_jsons]
            if not upload_output_json:
                logging.debug(f"No matching output json found for {upload_input_json}")
                continue

            if len(upload_output_json) > 1:
                # multiple upload attempts result in multiple output jsons
                logging.debug(
                    f"Multiple output json files found for {upload_input_json}: {upload_output_json}"
                )
                # use the most recent:
                upload_output_json.sort(key=lambda p: str(p), reverse=True)

            upload_output_json = upload_output_json[0]
            logging.debug(f"Using output json file: {upload_output_json}")

            input_and_output_jsons.append((upload_input_json, upload_output_json))

        return input_and_output_jsons

    @staticmethod
    def file_factory_from_ecephys_session(
        session_or_file: Union[int, str, pathlib.Path, DataValidationFile],
        return_as_dict=False,
    ) -> Union[Dict[str, Dict[str, str]], List[DataValidationFile],]:
        """Return a list of DVFiles with checksums for an ecephys session on lims.

        Provide path to ecephys_session_ dir or any file within it and we'll extract out
        the session dir.

        #! Current understanding is that files can't be overwritten on lims
        so although a file may have been uploaded multiple times (appearing in multiple
        upload queue files with multiple hashes), the path in lims will always be unique (ie
        the same file can live in multiple subfolders) - so we should be able to aggregate
        all filepaths across all upload queue files and get a unique set of files (with some
        possible overlap in data/checksums).

        """

        input_and_output_jsons = __class__.upload_jsons_from_ecephys_session_or_file(
            session_or_file
        )
        if not input_and_output_jsons:
            return

        all_hashes = {}
        for upload_input_json, upload_output_json in input_and_output_jsons:
            # get hash function that was used by lims
            hasher_key = __class__.hash_type_from_ecephys_upload_input_json(
                upload_input_json
            )
            # get hashes from output json
            hashes = __class__.hashes_from_ecephys_upload_output_json(
                upload_output_json, hasher_key
            )

            for file in hashes.keys():
                if all_hashes.get(file, None):
                    pass  # print(f"{file} already in all_hashes")
                all_hashes.update({file: {hasher_key: hashes[file]}})

        if return_as_dict:
            return all_hashes

        # this is slow-ish and only makes sense if we need DVFiles for all objects in a
        # session - otherwise just use the dict
        DVFiles = []
        for file, hashes in all_hashes.items():
            for hasher_key, hash_hexdigest in hashes.items():
                DVFiles.append(
                    available_DVFiles[hasher_key](path=file, checksum=hash_hexdigest)
                )
        return DVFiles

    @staticmethod
    def get_file_with_hash_from_lims(
        file: Union[str, pathlib.Path, SessionFile]
    ) -> DataValidationFile:
        """Return the hash of a file in LIMS, or None if it doesn't exist."""
        if not file:
             return None
        if not isinstance(file, SessionFile):
            try:
                file = SessionFile(path=file)
            except SessionError:
                return None

        if not file.lims_path:
            return None

        lims_file = file.lims_path
        all_hashes = __class__.file_factory_from_ecephys_session(
            lims_file, return_as_dict=True
        )
        if not all_hashes:
            return None
        DVFiles = []
        for lims_file, hashes in all_hashes.items():
            if (
                lims_file == file.lims_path.as_posix()
                or lims_file == file.lims_path.as_posix()[1:]
            ):
                for hasher_key, hash_hexdigest in hashes.items():
                    DVFiles.append(
                        available_DVFiles[hasher_key](
                            path=lims_file, checksum=hash_hexdigest
                        )
                    )
        if len(DVFiles) == 1:
            return DVFiles[0]
        elif len(DVFiles) > 1:
            # multiple checksums found - return first (which should be sha3_256 and is more common)
            return DVFiles[0]
        return None


class DataValidationStatus:
    """Provides a shorthand (enum) that represents the position of a file along the road to LIMS and the existence of
    other related files further along that road - mainly intended to simplify the question of 'can we delete this
    file?'.

    We'll divide the answer to that question into two parts:
        1) are there related files already in the database and do they compare favorably to the file in question?
            - where do they live currently? (LIMS, NP-EXP, or OTHER (considered temporary storage, unless specified))
            - are they valid copies?
            - which storage
        2) are there related files out there on the filesystem that we can find (and add to the database with a checksum,
           time-permitting)?

    """

    db: DataValidationDB = MongoDataValidationDB
    lims_backup: pathlib.Path = None
    npexp_backup: pathlib.Path = None
    z_backup: pathlib.Path = None

    def __init__(
        self,
        file: DataValidationFile = None,
        path: Union[str, pathlib.Path] = None,
        checksum: str = None,
        size: int = None,
    ):
        if isinstance(file, DataValidationFile):
            self.file:DataValidationFile = file
            "Subject file for which we want to find backups."
        if not file or not isinstance(file, DataValidationFile):
            if isinstance(
                file, (str, pathlib.Path)
            ):  # path provided as positional argument
                path = file
            try:
                # make a new object with the default DVFile class
                self.file = self.db.DVFile(path=path, size=size, checksum=checksum)
            except SessionError:
                # create non-SessionFile DVFile object, use custom get_matches method
                self.file = OrphanedDVFile(path=path, size=size, checksum=checksum)

        # get matches from database that have a checksum
        self.matches = self.db.get_matches(
            self.file
        )  # for SessionFiles this only returns other files with session_id in path
    
        # TODO this section is currently slow - either move to a dedicated function or speed-up #
        self.backups: List[DataValidationFile] = []
        highest_backups: List[
            DataValidationFile
        ] = []  # if no checksummed backups in db, go checksum this file
        if isinstance(self.file, SessionFile):
            # create DVfiles for any backup files currently available -
            # we'll descend the backup hierarchy and check against matches from the db
            for attr in ["lims_backup", "npexp_backup", "z_drive_backup"]:
                backup_path = getattr(self.file, attr)
                if not backup_path:
                    continue

                # make a new object with the default DVFile class
                backup_file = self.db.DVFile(path=backup_path)
                # - these must be SessionFiles, since standard _backup paths are
                # constructed from session_id

                if attr == "lims_backup" and not [
                    b for b in self.matches if backup_file.compare(b) in b.SELVES
                ]: # we don't already have a match that corresponds to this backup
                    # grab the lims hash recorded when uploaded to lims
                    lims_files = LimsDVDatabase.get_matches(self.file) or None
                    if lims_files:
                        for lims_file in lims_files:
                            if not lims_file:
                                continue
                            self.db.add_file(lims_file)
                            self.matches.append(lims_file)

                if (
                    attr == "npexp_backup"
                    and self.file.path == self.file.npexp_path
                ):
                    # no point checking for npexp or z drive backups if the file is in
                    # its correct session folder on npexp already
                    break

                # get checksummed matches for backup file
                backup_from_matches = [
                    b for b in self.matches if backup_file.compare(b) in b.SELVES
                ]
                if backup_from_matches and not highest_backups:
                    highest_backups.extend(backup_from_matches)
                elif not highest_backups:
                    highest_backups.append(
                        backup_file
                    )  # store the path in case we need to checksum it

                self.backups.extend(backup_from_matches)
                # if self.backups:
                #     break
                # TODO we found a backup, and could stop looking
                # TODO but if we don't have a checksum (or have the wrong type)
                # this won't count as a 'valid' backup
                # either collect all backups and accept any valid, or collect the
                # highest and either checksum it or out subject

        # TODO check get z drive path is working
        elif isinstance(self.file, OrphanedDVFile):
            # non-session files may have backups on np-exp or lims too, but we'll only
            # find out about them via the db
            #
            # for each of our file's matches, provided it's a sessionfile we can see if its path is
            # the expected np-exp/lims/z-drive path for a backup
            s_m = session_matches = [
                sf for sf in self.matches if isinstance(sf, SessionFile)
            ]
            # while session_matches:
            #     backups = []
            # note: we can't use the properties .lims_backup , .npexp_back etc
            # because they return none if .path == .X_backup to prevent a file being detected as its own backup
            backup_from_matches = [
                b
                for b in s_m
                if isinstance(b, SessionFile)
                and b.path == b.lims_path
                and self.file.compare(b) in b.VALID_COPIES
            ]
            if not backup_from_matches:
                backup_from_matches = [
                    b
                    for b in s_m
                    if isinstance(b, SessionFile)
                    and b.path == b.lims_path
                    and self.file.compare(b) in b.UNCONFIRMED_COPIES
                ]
            # get checksummed matches for backup file
            if backup_from_matches and not highest_backups:
                highest_backups.extend(backup_from_matches)
            self.backups.extend(backup_from_matches)

            # if backups and any(b.path.exists() for b in backups):
            #     break
            # if backups:
            #     highest_backups = backups
            backup_from_matches = [
                b
                for b in s_m
                if b.path == b.npexp_path
                and self.file.compare(b) in b.VALID_COPIES
            ]
            if not backup_from_matches:
                backup_from_matches = [
                    b
                    for b in s_m
                    if b.path == b.npexp_path
                    and self.file.compare(b) in b.UNCONFIRMED_COPIES
                ]
            # get checksummed matches for backup file
            if backup_from_matches and not highest_backups:
                highest_backups.extend(backup_from_matches)
            self.backups.extend(backup_from_matches)

            backup_from_matches = [
                b
                for b in s_m
                if (b.path == b.z_drive_path or "neuropixels_data" in str(b.path))
                and self.file.compare(b) in b.VALID_COPIES
            ]
            if not backup_from_matches:
                backup_from_matches = [
                    b
                    for b in s_m
                    if (
                        b.path == b.z_drive_path
                        or "neuropixels_data" in str(b.path)
                    )
                    and self.file.compare(b) in b.UNCONFIRMED_COPIES
                ]
            # get checksummed matches for backup file
            if backup_from_matches and not highest_backups:
                highest_backups.extend(backup_from_matches)
            self.backups.extend(backup_from_matches)

        for backups in [self.backups, highest_backups]:
            if not backups:
                continue
            # we're getting loose with the discovery of z_drive_backups here so do a
            # quick check that our 'subject' file is not the same as the candidate backup
            # if backups and any(b.path.exists() for b in backups):
            backups = [
                b
                for b in backups
                if not any(
                    s.compare(b) in DataValidationFile.SELVES for s in self.selves
                )
            ]
            backups = [b for b in backups if b.path.exists()]  # filter for extant files

        if not self.backups and highest_backups:
            # no backups with checksums found, but we can checksum the 'best' backup found
            self.backups = highest_backups

    def copy(
        self,
        dest_root_dir_or_filepath: Union[str, pathlib.Path] = NPEXP_PATH,
        add_session_folder=True,
        validate: bool = True,
        recopy: bool = False,
        remove_source: bool = False,
    ):
        """Copy the file to a new location, with optional checksum validation.

        Args:
            dest_root_dir_or_filepath (Union[str,pathlib.Path], optional): Works best
            with a root directory specified, then the source file's relative path will
            be used. Defaults to NPEXP_PATH.

            add_session_folder (bool, optional): If a session folder is missing from
            destination dir or filepath, the source file's session folder will be added if possible. Defaults to True.

            validate (bool, optional): Ensure checksums match for source and destination. Defaults to True.

            recopy (bool, optional): Overwrite existing file if it's suspected to be the
            same, and ignore previous copies made to the same location. Defaults to False.

            remove_source (bool, optional): Automatically sets validate=True. Defaults to False.

        """
        if not self.file.path.exists():
            logging.exception(f"Copy aborted - source file does not exist: {self.file.path}")
            return
        
        if remove_source:
            validate = True

        dest_root_dir_or_filepath = pathlib.Path(dest_root_dir_or_filepath)

        # get from path
        session_folder = Session.folder(dest_root_dir_or_filepath)

        if not session_folder and isinstance(self.file, SessionFile):
            # get from our file that will be copied
            session_folder = self.file.session.folder

        elif session_folder and isinstance(self.file, SessionFile):
            if session_folder != self.file.session.folder:
                raise SessionError(
                    f"session folder mismatch: destination {session_folder} != src {self.file.session.folder}"
                )

        # check if dest is a dir or file
        is_dir = False
        try:
            SessionFile(dest_root_dir_or_filepath)
        except FilepathIsDirError:
            is_dir = True
        except SessionError:
            is_dir = False

        if not is_dir:
            dest_root = dest_root_dir_or_filepath.parent
            dest_relative = dest_root_dir_or_filepath.name

        if is_dir:
            dest_root = dest_root_dir_or_filepath
            dest_relative = None

        if (
            add_session_folder is True
            and session_folder is not None
            and session_folder not in dest_root.parts
        ):
            dest_root = dest_root / session_folder

        if dest_relative is None:

            if not isinstance(self.file, SessionFile):
                dest_relative = self.file.path.name
            elif self.file.probe_dir:
                probe_dir_parent = [f.parent for f in self.file.path.parents if f'_probe{self.file.probe_dir}' in f.parts[-1]][0]
                dest_relative = self.file.path.relative_to(probe_dir_parent)
            elif session_folder not in str(dest_root) + str(self.file.relative_path):
                # we can't allow a session file to be copied without its session
                # identifier somewhere in its path 
                dest_relative = self.file.session_relative_path
            else:
                dest_relative = self.file.relative_path

        final_dest = dest_root / dest_relative

        # make DVFile for proposed dest
        try:
            dest_file = self.db.DVFile(path=final_dest)
        except SessionError:
            dest_file = OrphanedDVFile(path=final_dest)
        except FilepathIsDirError:
            logging.warning(f"Could not copy: not a filepath {self.file.path}")
            return

        if self.file.probe_dir != dest_file.probe_dir:
            logging.warning(
                f"copy aborted - probe_dir mismatch: src {self.file.probe_dir} != dest {dest_file.probe_dir}"
            )
            return

        # check whether the file already exists in the database
        dest_matches = self.db.get_matches(dest_file, match=dest_file.SELVES)
        valid_copies = any(
            s.compare(d) in s.VALID_COPIES
            for s in self.selves
            for d in [dest_file, *dest_matches]
            )
        unconfirmed_copies = any(
            s.compare(d) in s.UNCONFIRMED_COPIES
            for s in self.selves
            for d in [dest_file, *dest_matches]
            )
        invalid_copies = any(
            s.compare(d) in s.INVALID_COPIES
            for s in self.selves
            for d in [dest_file, *dest_matches]
            )
        # determine next action:
        do_copy = False
        if recopy is True:
            do_copy = True  # we'll copy regardless
        elif not dest_file.path.exists() and valid_copies:
            # we copied to this location previously and
            # since cleared the file because it was validated
            do_copy = False
            if invalid_copies:
                do_copy = True
        elif not dest_file.path.exists():
            do_copy = True
        elif self.file.path.stat() == final_dest.stat():
            # we previously copied to this location and the file still exists
            if invalid_copies:
                do_copy = True
            elif not validate:
                return  # we're not validating and the existing dest file looks like a copy according to OS stats
            else:
                do_copy = False  # we have a valid copy already
            
        final_dest.parent.mkdir(parents=True, exist_ok=True)
        
        attempts = 0
        while (do_copy is True or validate is True) and attempts < 3:
            
            attempts += 1

            if do_copy:
                try:
                    logging.info(f"Copying: {self.file} -> {final_dest}")
                    shutil.copy2(self.file.path, final_dest)
                except OSError as e:
                    logging.warning(f"Copy failed - {e}: {self.file} -> {final_dest}")
                    return
                
                if not final_dest.exists():
                    logging.debug(f"Copy failed - re-trying: {self.file} -> {final_dest}")
                    continue
            
                if not validate:
                    logging.debug(
                        f"Copied (without validation): {self.file} -> {final_dest}"
                    )
            
            if not validate:
                break
            
            if do_copy is False:
                dest_file = strategies.exchange_if_checksum_in_db(dest_file, self.db)   
            if do_copy is False and not dest_file.checksum:
                # generate checksum for existing, unconfirmed copy
                dest_file = strategies.generate_checksum(dest_file, self.db)
            if do_copy is True:
                # generate checksum for newly-copied file, regardless
                dest_file = strategies.generate_checksum(dest_file, self.db)
                
            if any(s.checksum for s in self.selves):
                # we have a checksum already so avoid regenerating
                for s in self.selves:
                    if isinstance(s, dest_file.__class__) and s.checksum:
                        # use type specified in db if possible
                        self.file = s
                        break
            if not self.file.checksum:
                try:
                    self.file = dest_file.__class__(path=self.file.path)
                except SessionError:
                    self.file = OrphanedDVFile(path=self.file.path, type=dest_file.checksum_name)
                self.file = strategies.generate_checksum(self.file, self.db)

            if self.file.compare(dest_file) in self.file.VALID_COPIES:
                logging.debug(f"Copied and validated: {self.file} -> {final_dest}")
                self.matches.append(dest_file)
                break
            elif self.file.compare(dest_file) in self.file.INVALID_COPIES:
                logging.info(
                    f"Copy validation failed - retrying: {self.file} -> {final_dest}"
                )
                # the source data may have changed, and we picked up an old checksum
                # from the db - regenerate
                self.file = strategies.generate_checksum(self.file, self.db)            
                if self.file.compare(dest_file) in self.file.VALID_COPIES:
                    break
                recopy = True
                do_copy = True
                continue
            else:
                logging.info(
                    f"Copy validation failed - retrying: {self.file} {self.file.checksum_name} -> {dest_file.checksum_name} {final_dest}"
                )
                do_copy = True
                continue
        else:
            logging.debug(f"Copying or validation failed after {attempts} attempts: {self.file} -> {final_dest}")
        
        if (
            remove_source is True
            and validate is True
            and self.file.compare(dest_file) in self.file.VALID_COPIES
        ):
            try:
                self.file.path.unlink()
                logging.info(f"DELETED source file after copy: {self.file.path}")
            except PermissionError:
                logging.exception(f"Permission denied: could not delete {self.file}")
            except OSError as e:
                logging.exception(f"Failed to remove {self.file} file after copy: {e}")

    def ensure_npexp_backup(self):
        if self.valid_lims or self.valid_npexp: 
            return

        self.copy(validate=True, recopy=False)
        # could use recopy=True 
        if self.status != self.Backup.HAS_POSSIBLE_UNSYNCED_BACKUP:
            self.ensure_backup_checksum()
        if not self.status >= self.Backup.HAS_VALID_BACKUP:
            logging.info(f"Still no valid backups for: {self.file}")
            
    def ensure_backup_checksum(self):
        if self.valid_backups or not self.unconfirmed_backups:
            # skip if there's nothing to checksum
            # * note: it's possible to have the same entry in valid_backups
            #  and unconfirmed_backups:
            #  since we evaluate all entries in self.selves vs all entries in
            #  self.backups, if any of the entries in self.selves don't have a checksum
            #  we will end up with some unconfirmed_backups regardless
            return

        if self.unconfirmed_backups and not self.valid_backups:

            pref_backup = self.unconfirmed_backups[
                0
            ]  # should be the highest (lims>npexp>z_drive)

            if pref_backup.checksum and not any(s.checksum for s in self.selves):
                # our file needs a checksum, and we have a backup with a checksum
                # - convert our file to backup type and generate
                new = pref_backup.__class__(self.file.path)
                self.file = strategies.generate_checksum(new, self.db)

            if not pref_backup.checksum and any(s.checksum for s in self.selves):
                # our backup needs a checksum, and we have a file with a checksum
                # - convert backup to our type and generate
                # - prefer speed with CRC32 vs SHA256:
                if any(
                    isinstance(s, CRC32DataValidationFile) and s.checksum
                    for s in self.selves
                ):
                    new = CRC32DataValidationFile(pref_backup.path)
                elif any(
                    isinstance(s, SHA3_256DataValidationFile) and s.checksum
                    for s in self.selves
                ):
                    new = SHA3_256DataValidationFile(pref_backup.path)
                else:
                    new = [s for s in self.selves if s.checksum][0].__class__(
                        pref_backup.path
                    )
                pref_backup = strategies.generate_checksum(new, self.db)
                self.matches.append(pref_backup)

            if not pref_backup.checksum and not any(s.checksum for s in self.selves):
                # we have no checksums
                # - prefer speed with CRC32 vs SHA256:
                new = CRC32DataValidationFile(self.file.path)
                self.file = strategies.generate_checksum(new, self.db)

                new = CRC32DataValidationFile(pref_backup.path)
                pref_backup = strategies.generate_checksum(new, self.db)
                self.matches.append(pref_backup)

        if self.valid_backups:
            pass

    @property
    def status(self) -> Backup:
        return self.report()
    
    def report(self):
        if not self.matches:
            return self.Backup.NO_MATCHES_IN_DB
        elif not any(
            s.compare(m) in (*s.VALID_COPIES, *s.UNCONFIRMED_COPIES)
            for s in self.selves
            for m in self.matches
        ):
            return self.Backup.NO_COPIES_IN_DB
        elif not self.backups:
            return self.Backup.NO_BACKUPS_IN_FILESYSTEM
        elif not any(s.checksum for s in self.selves):
            return self.Backup.HAS_NO_CHECKSUMS_IN_DB

        if self.valid_lims:
            return self.Backup.VALID_ON_LIMS
        elif self.valid_npexp:
            return self.Backup.VALID_ON_NPEXP
        elif self.valid_backups:
            # print('valid backup(s) exist: safe to delete')
            return self.Backup.HAS_VALID_BACKUP
        elif self.unconfirmed_backups and not self.invalid_backups:
            # print('unconfirmed backup(s) exist: need checksum(s) generated')
            return self.Backup.HAS_UNCONFIRMED_BACKUP
        elif self.invalid_backups:
            # print('only invalid backup(s) exist: data may have changed since backup, be careful!')
            return self.Backup.HAS_POSSIBLE_UNSYNCED_BACKUP
        return self.Backup.UNKNOWN

        print(
            "=" * 50,
            "\nFile\n",
            self.file,
            "\nEvaluation of matches in database:\n",
            self.evaluate_backups_in_db().name,
            # "\nAction:",self.action,
            "\nEvaluation of matches in filesystem:\n",
            "n/a\n",
        )

        if self.evaluate_backups_in_db() < self.Backup.VALID_ON_NPEXP:
            for match in [m for m in self.matches if self.file.compare(m) >= 10]:
                print(
                    "-" * 40,
                    "\n",
                    self.file.Match(self.file.compare(match)).name,
                    "\n",
                    match,
                )
        print("=" * 50)

    def evaluate_backups_in_db(self):

        if not self.matches or all(m < 10 for m in self.match_types):
            return self.Backup.NO_COPIES_IN_DB
        # TODO must also check backups exist now
        elif any(m in DataValidationFile.VALID_COPIES for m in self.match_types):
            # valid copies
            # test whether they are valid backups
            if self.valid_lims and not self.invalid_lims:
                self.action = "delete self"
                return self.Backup.VALID_ON_LIMS
            if self.invalid_lims:
                self.action = "investigate self vs lims copy entries in db"
                return self.Backup.INVALID_ON_LIMS
            if self.valid_npexp and not self.invalid_npexp:
                self.action = "delete self"
                return self.Backup.VALID_ON_NPEXP
            if self.invalid_npexp:
                self.action = "investigate self vs npexp copy entries in db"
                return self.Backup.INVALID_ON_NPEXP
            if self.valid_z and not self.invalid_z:
                self.action = "delete self"
                return self.Backup.VALID_ON_ZDRIVE
            if self.invalid_z:
                self.action = "investigate self vs z copy entries in db"
                return self.Backup.INVALID_ON_ZDRIVE
            if not any(10 < m < 15 for m in self.match_types):
                self.action = "possibly delete self - investigate locations of copies"
                return self.Backup.VALID_ON_OTHER
            self.action = "investigate self vs other valid copy entries in db"
            return self.Backup.INVALID_ON_OTHER

        elif any(m > 15 for m in self.match_types):
            # copies that need checksum info to determine validity
            # generate checksum depending on location
            pass

        elif any(m >= 10 for m in self.match_types):
            # data is out of sync or db is not up to date
            # could checksum again, but likely need attention from user to resolve
            # (otherwise we may generate checksums repeatedly just to find the same result)
            return self.Backup.POSSIBLE_UNSYNCED_COPY

    @property
    def selves(self):
        if not hasattr(self, "matches") or not self.matches:
            return [self.file]
        return list(
            set(
                [
                    f
                    for f in [self.file, *self.matches]
                    if self.file.compare(f) in DataValidationFile.SELVES
                ]
            )
        )

    @property
    def valid_backups(self):
        return list(
            set(
                [
                    backup
                    for s in self.selves
                    for backup in self.backups
                    if (
                        s.compare(backup) in DataValidationFile.VALID_COPIES
                        and backup.path.exists()
                    )
                ]
            )
        )
        # * note: list(set()) means these are no longer in original order
        
    @property 
    def valid_lims(self):
        if not self.valid_backups:
            return False
        return any(b.session.lims_path in b.path.parents for b in self.valid_backups if isinstance(b,SessionFile))
    
    @property 
    def valid_npexp(self):
        if not self.valid_backups:
            return False
        return any(b.session.npexp_path in b.path.parents for b in self.valid_backups if isinstance(b,SessionFile))
    
    @property
    def unconfirmed_backups(self):
        return list(
            set(
                [
                    backup
                    for s in self.selves
                    for backup in self.backups
                    if (
                        s.compare(backup) in DataValidationFile.UNCONFIRMED_COPIES
                        and backup.path.exists()
                    )
                ]
            )
        )

    @property
    def invalid_backups(self):
        return list(
            set(
                [
                    backup
                    for s in self.selves
                    for backup in self.backups
                    if (
                        s.compare(backup) in DataValidationFile.INVALID_COPIES
                        and backup.path.exists()
                    )
                ]
            )
        )
        
    @property
    def incoming(self) -> List[DataValidationFile]:
        """Any matches from database that are at the correct incoming path and currently
        exist"""
        # non-session files won't have the relevant path property
        # and should never be in incoming anyway
        if not isinstance(self.file, SessionFile):
            return []
        incoming = self.db.DVFile(self.file.incoming_path)
        if not incoming.path.exists():
            return []
        return list(
            set(
                [
                    match
                    for s in self.selves
                    for match in self.matches
                    if incoming.compare(match) in DataValidationFile.SELVES
                ]
            )
        )

    @property
    def valid_incoming(self) -> List[DataValidationFile]:
        return [
                valid 
                for valid in self.incoming
                for s in self.selves
                if s.compare(valid) in DataValidationFile.VALID_COPIES
            ]
        
    @property
    def match_types(self) -> List[int]:
        """return a list of match types for the file"""
        return [self.file.compare(match) for match in self.matches]

    @property
    def eval_accessible_db_matches(self) -> DataValidationFile.Match:
        """Return an enum indicating the highest status of a file's matches in the database,
        *only* if they're currently accessible."""
        if self.matches:
            for idx, match in enumerate(self.matches):
                if match.path.is_file():
                    return DataValidationFile.Match(self.match_types[idx])
        return DataValidationFile.Match.UNKNOWN

    @property
    def eval_all_db_matches(self) -> DataValidationFile.Match:
        """Return an enum indicating the highest status of a file's matches in the database,
        regardless of whether they're currently accessible."""
        if self.matches:
            return DataValidationFile.Match(max(self.match_types))
        return DataValidationFile.Match.UNKNOWN

    # @property
    # def eval_backups(self) -> self.Backup:
    #     """Return an enum indicating the status of the file's backups (according to
    #     what's currently accessible on disk or //allen/ - not from entries in the database)"""

    class Backup(enum.IntFlag):
        """Evaluate where a file is in the backup process.

            Using three digits to avoid confusion with DVFile.Match.

              For a given DVFile:
            1) LIMS is the gold standard backup location
            2) NPEXP is large temporary storage, can only be cleared when valid backup is on lims, should be synced with z drive
            3) ZDRIVE is small temporary storage prior to lims upload, can be cleared when valid backup is on lims (safest) or npexp
            4) any other backup location is treated the same as z drive

        Checking how a file is backed-up and can be deleted to recover space:
        (in order of execution)

            - VALID copy on LIMS
                DELETE

            - INVALID copy on LIMS
            - VALID copy on NPEXP (file itself NOT on npexp)
                DELETE (depending on file location, may represent original data - replace lims copy with npexp copy)

            - INVALID copy on LIMS
                NO DELETE (look for other copies to find original checksum)

            - UNKNOWN or no matching files found on LIMS
                NO DELETE (wait for Lims upload or delete manually)

            - VALID copy on NPEXP
                DELETE

            - INVALID copy on NPEXP
                NO DELETE (wait for lims upload or decide which is correct/original data before lims upload)

            - VALID copy in ZDRIVE/other backup location specified (file itself NOT on npexp)
                DELETE

        Need a STATUS enum for each of the above cases that can be combined with whether or not the matched copy is
        accessible or just an entry in the database (ie file may have been deleted).
            - since getting the status requires running 'get_matches' it would be nice to return that list of DVFiles too,
              to present the data or guide next steps
            -


        Also remember that the DB is incomplete and always will be: if we don't find matches in the db
        we can go look for files in known backup locations add add them to the db and re-check status.
        In practice this is less clear-cut than STATUS enum
            - how exhaustively do we want to search for matches? (synology drives + many 10TB disks that aren't indexed)
            - do we checksum first and ask questions later? (slow)
        * a medium/longer-term strategy may be to index all data disks by entering them into the db without checksum info to
        make the db more complete

        """

        # TODO write logic for determining and returning backup status
        # =======================================================================================
        # hierarchy in backup process is taken into account
        # - only the highest (abs) number needs to be considered

        # ---------------------------------------------------------------------------------------
        # copies exist, with full information available
        VALID_ON_LIMS = 601
        INVALID_ON_LIMS = -601

        VALID_ON_NPEXP = VALID_ON_SD = 501
        INVALID_ON_NPEXP = INVALID_ON_SD = -501

        VALID_ON_OTHER = VALID_ON_ZDRIVE = 401
        INVALID_ON_OTHER = INVALID_ON_ZDRIVE = -401

        HAS_VALID_BACKUP = 400
        # ---------------------------------------------------------------------------------------
        # copies exist, more computation is needed to validate

        COPY_ON_LIMS_MISSING_SELF = 303  # checksum self.file
        COPY_ON_LIMS_MISSING_OTHER = 302  # checksum the file on lims
        COPY_ON_LIMS_MISSING_BOTH = 301

        COPY_ON_NPEXP_MISSING_SELF = COPY_ON_SD_MISSING_SELF = 203
        COPY_ON_NPEXP_MISSING_OTHER = COPY_ON_SD_MISSING_OTHER = 202
        COPY_ON_NPEXP_MISSING_BOTH = COPY_ON_SD_MISSING_BOTH = 201

        COPY_ON_OTHER_MISSING_SELF = COPY_ON_ZDRIVE_MISSING_SELF = 103
        COPY_ON_OTHER_MISSING_OTHER = COPY_ON_ZDRIVE_MISSING_OTHER = 102
        COPY_ON_OTHER_MISSING_BOTH = COPY_ON_ZDRIVE_MISSING_BOTH = 101

        HAS_UNCONFIRMED_BACKUP = 300

        # ---------------------------------------------------------------------------------------
        # possible copies with different names exist - need intervention

        POSSIBLE_UNSYNCED_COPY = 100

        HAS_POSSIBLE_UNSYNCED_BACKUP = 100
        """Only invalid backup(s) exist: data may have changed since backup, be careful!"""

        # ---------------------------------------------------------------------------------------
        # no copies found
        HAS_NO_CHECKSUMS_IN_DB = 5

        NO_BACKUPS_IN_DB = 4
        NO_COPIES_IN_DB = 3  # ? find in filesystem
        NO_MATCHES_IN_DB = 2

        NO_BACKUPS_IN_FILESYSTEM = 1
        NO_COPIES_IN_FILESYSTEM = 0  # ? add filesystem locations

        UNKNOWN = -1


class DataValidationFolder:
    """Represents a folder for which we want to checksum the contents and add to database,
    possibly deleting if a valid copy exists elswhere (evalutated using DVStatus)
    """

    db: DataValidationDB = MongoDataValidationDB
    backup_paths: Set[
        str
    ] = None  # auto-populated with lims, npexp, sync computer folders
    include_subfolders: bool = True

    regenerate_threshold_bytes: int = 1 * 1024**2  # MB
    # - below this file size, checksums will always be generated - even if they're already in the database
    # - above this size, behavior is to get the checksum from the database if it exists for the file (size + path must
    #   be identical), otherwise generate it

    min_age_days: int = 0
    # - minimum age of a file for it to be deleted (provided that a valid backup exists)

    filename_include_filter: str = ""
    filename_exclude_filter: str = ""

    # - applied to glob search for files in the folder

    skip_sorting_check: bool = False
    # - by default, raw data won't be deleted from acq A/B drives if sorting hasn't
    #   produced some folders with the same probe letters on npexp

    def __init__(self, path: Union[str, pathlib.Path]):

        # extract the session ID from anywhere in the path (not reqd)
        try:
            self.session = Session(path)
        except:
            self.session = None

        # ensure the path is a directory, not a file
        # piggy back off the SessionFile class to do file check
        try:
            SessionFile(path)
            raise ValueError(
                f"{self.__class__.__name__}: path must point to a folder {path}"
            )
        except ValueError:  # TODO make a file_vs_folder function with its own exception
            self.path = pathlib.Path(path)

    def add_backup_path(self, path: Union[str, List[str]]):
        """Store one or more paths to folders possibly containing backups for the session"""
        if path and (isinstance(path, str) or isinstance(path, pathlib.Path)):
            path = [str(path)]
        elif path and isinstance(
            path, List
        ):  # inequality checks for str type and existence
            pass
        else:
            raise TypeError(
                f"{self.__class__.__name__}: path must be a string or list of strings"
            )
            # add to list of backup locations as a Folder type object of the same class
        for p in path:
            if str(p) != "":
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
        self.lims_path = self.session.lims_path  # must exist if not None
        if self.lims_path and self.lims_path.as_posix() not in str(self.path):
            self.add_backup_path(self.lims_path.as_posix())

        # get the npexp folder for this session and add it to the backup paths (if it exists)
        self.npexp_path = self.session.npexp_path
        if (
            self.npexp_path
            and os.path.exists(self.npexp_path)
            and Session.NPEXP_ROOT.as_posix() not in str(self.path)
        ):
            self.add_backup_path(self.npexp_path.as_posix())

        if not self.backup_paths and self.session:
            # add only the relevant backup path for this rig (if applicable):
            # currently this is just the neuropix_data folder on the sync computer

            # use the first file in the DVFolder to get this path
            file1 = self.file_paths[0]
            File1 = self.db.DVFile(path=file1.as_posix())
            z_drive = File1.z_drive_backup
            if z_drive:
                self.add_backup_path(z_drive)

    @property
    def filename_include_filters(self):
        return self.filename_include_filter.replace("*", "").replace(" ", "").strip().split("|")

    @property
    def filename_exclude_filters(self):
        return self.filename_exclude_filter.replace("*", "").replace(" ", "").strip().split("|")

    @property
    def file_paths(self) -> List[pathlib.Path]:
        """return a list of files in the folder"""
        if hasattr(self, "_file_paths") and self._file_paths:
            return self._file_paths

        if self.include_subfolders:
            #! yield from needs testing plus modifying the 'backup_paths' code above
            # for now, this will return the full list each time and be slower
            self._file_paths = [
                child
                for child in self.path.rglob("*")
                if not child.is_dir()
                and any(filters in str(child) for filters in self.filename_include_filters)
                and (
                    not any(self.filename_exclude_filters)
                    or not any(filters in str(child) for filters in self.filename_exclude_filters)
                )
            ]
        else:
            self._file_paths = [
                child
                for child in self.path.iterdir()
                if not child.is_dir()
                and any(filters in str(child) for filters in self.filename_include_filters)
                and (
                    not any(self.filename_exclude_filters)
                    or not any(filters in str(child) for filters in self.filename_exclude_filters)
                )
            ]
        return self._file_paths
    
    @property
    def probes_in_foldername(self) -> str:
        probe_pattern = "(?<=_probe)([A-F]{1,})"
        if probes := re.search(probe_pattern, self.path.name):
            return probes.group(0)
        return ''
        
    @property
    def is_original_raw_data(self) -> bool:
        if not self.session:
            return False
        original = self.path.drive == 'A:' or self.path.drive == 'B:'
        return original and len(self.probes_in_foldername) >= 3
    
    @functools.cached_property
    def sorted_probe_dirs_on_npexp(self) -> bool:
        if not self.session:
            return False
        probe_pattern = "(?<=_probe)([A-F])(?=_sorted)"
        sorted = ''
        for dir in self.session.npexp_path.iterdir():
            if probe := re.search(probe_pattern, dir.name):
                sorted += probe.group(0)
        return sorted

    @property
    def raw_data_with_no_sorted_on_npexp(self) -> bool:
        return self.is_original_raw_data and not any(
            probe in self.sorted_probe_dirs_on_npexp for probe in self.probes_in_foldername)
            
    def copy_to_npexp(self):
        """Copy all files to the NP-EXP folder"""
        # create threads for each file
        def copy(path):
            DataValidationStatus(path).ensure_npexp_backup()
            
        threads = []
        for path in self.file_paths:
            t = threading.Thread(target=copy, args=(path,))
            threads.append(t)
            t.start()
        # wait for the threads to complete
        for thread in progressbar(threads, prefix=" ", units="files", size=25):
            thread.join()
    
    def copy_to_backup(self, backup_path: pathlib.Path=NPEXP_PATH):
        """Copy all files to a backup location"""
        # create threads for each file
        threads = []
        for path in self.file_paths:
            t = threading.Thread(target=strategies.copy_file, args=(path, backup_path))
            threads.append(t)
            t.start()
        # wait for the threads to complete
        print(f"- copying files to {backup_path}...")
        for thread in progressbar(threads, prefix=" ", units="files", size=25):
            thread.join()
        
    def add_to_db(self):
        "Add all files in folder to database if they don't already exist"

        # create threads for each file to be added
        threads = []
        for path in self.file_paths:
            try:
                file = self.db.DVFile(path=path)
            except SessionError:
                file = OrphanedDVFile(path=path)
            except (ValueError, TypeError):
                logging.debug(
                    f"{self.__class__.__name__}: could not create SessionDVFile: {path.as_posix()}"
                )
                continue

            if file.size <= self.regenerate_threshold_bytes:
                t = threading.Thread(
                    target=strategies.generate_checksum, args=(file, self.db)
                )
            else:
                t = threading.Thread(
                    target=strategies.generate_checksum_if_not_in_db,
                    args=(file, self.db),
                )

            threads.append(t)
            t.start()

        # wait for the threads to complete
        print("- adding files to database...")
        for thread in progressbar(threads, prefix=" ", units="files", size=25):
            thread.join()

    def clear(self) -> List[int]:
        """Clear the folder of files which are backed-up on LIMS or np-exp, or any added backup paths"""
        
        if not self.skip_sorting_check and self.raw_data_with_no_sorted_on_npexp:
            logging.warning(
                f"Skipped clearing of original raw probe data on Acq: no sorted folders on npexp yet for {self.session.folder}."
            )
            return [0]
        
        def delete_if_valid_backup_in_db(result, idx, file_inst, db, backup_paths):

            files_bytes = strategies.delete_if_valid_backup_in_db(
                file_inst, db, backup_paths
            )
            result[idx] = files_bytes
            
        print("- searching for valid backups...")
        deleted_bytes = [0] * len(self.file_paths)  # keep a tally of space recovered
        threads = [None] * len(
            self.file_paths
        )  # following https://stackoverflow.com/a/6894023
        # for path in progressbar(self.file_paths, prefix=' ', units='files', size=25):
        for i, path in enumerate(self.file_paths):

            try:
                file = self.db.DVFile(path=path.as_posix())
            except (ValueError, TypeError):
                logging.debug(
                    f"{self.__class__.__name__}: could not add to database, likely missing session ID: {path.as_posix()}"
                )
                continue

            if int(file.session.date) > int(
                (
                    datetime.datetime.now() - datetime.timedelta(days=self.min_age_days)
                ).strftime("%Y%m%d")
            ):
                logging.debug(
                    f"skipping file less than {self.min_age_days} days old: {file.session.date}"
                )
                continue

            threads[i] = threading.Thread(
                target=delete_if_valid_backup_in_db,
                args=(deleted_bytes, i, file, self.db, self.backup_paths),
            )
            threads[i].start()

        for thread in progressbar(threads, prefix=" ", units="files", size=25):
            thread.join() if thread else None

        # tidy up empty subfolders:
        if self.include_subfolders:
            check_dir_paths = os.walk(self.path, topdown=False, followlinks=False)
        else:
            check_dir_paths = [d for d in self.path.iterdir() if d.is_dir()]
            
        for check_dir in check_dir_paths:
            check_dir = (
                pathlib.Path(check_dir[0]) if self.include_subfolders else check_dir
            )
            try:
                check_dir.rmdir()  # raises error if not empty
                logging.debug(
                    f"{self.__class__.__name__}: removed empty folder {check_dir}"
                )
            except OSError:
                continue

        # return cumulative sum of bytes deleted from folder
        deleted_bytes = [d for d in deleted_bytes if d != 0]
        print(
            f"{len(deleted_bytes)} files deleted | {sum(deleted_bytes) / 1024**3 :.1f} GB recovered"
        )
        # TODO add number of files deleted / total files on disk
        if deleted_bytes:
            logging.info(
                f"{len(deleted_bytes)} files deleted from {self.path} | {sum(deleted_bytes) / 1024**3 :.1f} GB recovered"
            )
        return deleted_bytes


def test_data_validation_file():
    """test the data validation file class"""

    class Test(DataValidationFile):
        def valid(path, *args, **kwargs):
            return True

        checksum_generator = "12345678"
        checksum_test = None
        checksum_validate = valid

    cls = Test
    path = "//tmp/tmp/test.txt"  # currently only working with network drives, which require a folder in the middle between drive/file
    checksum = "12345678"
    size = 10

    self = cls(path=path, checksum=checksum, size=size)

    other = cls(path=path, checksum=checksum, size=size)
    assert self.compare(self) == self.Match.SELF, "not recognized: self"

    other = cls(path="//tmp2/tmp/test.txt", checksum=checksum, size=size)
    assert (
        self.compare(other)
    ) == self.Match.VALID_COPY, "not recgonized: valid copy, not self"

    other = cls(path="//tmp2/tmp/test2.txt", checksum=checksum, size=size)
    assert (
        self.compare(other)
    ) == self.Match.VALID_COPY_RENAMED, "not recognized: valid copy, different name"

    other = cls(path="//tmp2/tmp/test.txt", checksum="87654321", size=20)
    assert (
        self.compare(other)
    ) == self.Match.COPY_UNSYNCED_DATA, "not recognized: out-of-sync copy"

    other = cls(path="//tmp2/tmp/test.txt", checksum=checksum, size=20)
    assert (
        self.compare(other)
    ) == self.Match.COPY_UNSYNCED_CHECKSUM, (
        "not recognized: out-of-sync copy with incorrect checksum"
    )
    # * note checksum is equal, which could occur if it hasn't been updated in db

    other = cls(path="//tmp2/tmp/test.txt", checksum="87654321", size=size)
    assert (
        self.compare(other)
    ) == self.Match.COPY_UNSYNCED_OR_CORRUPT_DATA, "not recognized: corrupt copy"

    other = cls(path="//tmp/tmp/test2.txt", checksum=checksum, size=20)
    assert (
        self.compare(other)
    ) == self.Match.CHECKSUM_COLLISION, "not recognized: checksum collision"

    other = cls(path="//tmp/tmp/test2.txt", checksum="87654321", size=20)
    assert (
        self.compare(other)
    ) == self.Match.UNRELATED, "not recognized: unrelated file"


test_data_validation_file()


def report_multline_print(
    file: DataValidationFile, comparisons: List[DataValidationFile]
):
    """report on the contents of the folder, compared to database"""
    if isinstance(comparisons, DataValidationFile):
        comparisons = [comparisons]

    column_width = 120  # for display of line separators

    def display_name(DVFile: DataValidationFile) -> str:
        min_len_filename = 80
        disp = f"{DVFile.parent}/{DVFile.name}"
        if len(disp) < min_len_filename:
            disp += " " * (min_len_filename - len(disp))
        return disp

    def display_str(label: str, DVFile: DataValidationFile) -> str:
        disp = f"{label} : {display_name(DVFile)} | {DVFile.checksum or '  none  '} | {DVFile.size or '??'} bytes"
        return disp

    logging.debug("#" * column_width)
    logging.debug("\n")
    logging.debug(f"subject: {file.path.as_posix()}")
    logging.debug("\n")
    logging.debug("-" * column_width)

    folder = file.path.split(file.name)[0]
    compare_folder = ""
    for other in comparisons:
        # logging.debug new header for each comparison with a new folder
        if compare_folder != other.path.split(other.name)[0]:
            compare_folder = other.path.split(other.name)[0]
            # logging.debug("*" * column_width)
            logging.debug("folder comparison for")
            logging.debug(f"subject : {folder}")
            logging.debug(f"other   : {compare_folder}")
            # logging.debug("*" * column_width)
            logging.debug("-" * column_width)

        logging.debug(f"Result  : {file.Match(file.compare(other)).name}")
        logging.debug(display_str("subject", file))
        logging.debug(display_str("other  ", other))
        logging.debug("-" * column_width)

    logging.debug("\n")
    logging.debug("#" * column_width)


def DVFolders_from_dirs(
    dirs: Union[str, Sequence[str], Sequence[pathlib.Path]], only_session_folders=True
) -> Generator[DataValidationFolder, None, None]:
    """Generator of DataValidationFolder objects from a list of directories"""
    if isinstance(dirs, (str,pathlib.Path)):
        dirs = (dirs)

    def skip(dir) -> bool:
        mice = [
            "_366122_",
            "_603810_",#NP0 pretest
            "_599657_",#NP1 pretest
            "_598796_",#NP2 pretest
        ]
        skip_filters = ["$RECYCLE.BIN", "_temp_", "#recycle", "pretest", *mice]
        if any(skip in str(dir) for skip in skip_filters):
            return True
        if str(dir).endswith('_sorted') and not tuple(dir.rglob('metrics.csv')):
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
            top_level_dir.include_subfolders = False  # if True, we'll rglob through every subfolder from the top_level, when what we want is to loop through individual subfolders as DVFolders below
            yield top_level_dir

            for c in [child for child in dir_path.iterdir() if child.is_dir()]:
                if skip(c) or (only_session_folders and not Session.folder(str(c))):
                    continue
                else:
                    yield DataValidationFolder(c.as_posix())


# including names from allensdk/brain_observatory/ecephys/copy_utility/_schemas.py:
available_DVFiles = {
    "sha3_256": SHA3_256DataValidationFile,
    "sha256": SHA256DataValidationFile,
    "crc32": CRC32DataValidationFile,
}
# from allensdk/brain_observatory/ecephys/copy_utility/_schemas.py:
lims_available_hashers = {"sha3_256": hashlib.sha3_256, "sha256": hashlib.sha256}

if __name__ == "__main__":
    # f = R"C:\Users\ben.hardcastle\Desktop\New folder (2)\1181283346_625555_20220601\1181283346_625555_20220601.ISIregistration.npz"
    # f = R"\\allen\programs\mindscope\workgroups\np-exp\1182865981_625545_20220608\1182865981_625545_20220608.stim.pkl"
    # f = R"\\allen\programs\mindscope\workgroups\np-exp\1182865981_625545_20220608\1182865981_625545_20220608_surgeryNotes.json"
    # file = SHA256DataValidationFile(f)
    # # f = R"C:\Users\ben.hardcastle\Desktop\New folder (2)\temp_delete_me.ISIregistration.npz"
    # # file = OrphanedDVFile(f)
    # # file = strategies.exchange_if_checksum_in_db(file,MongoDataValidationDB)
    # s = DataValidationStatus(file)
    # s.report()
    MongoDataValidationDB.get_matches("//allen/programs/mindscope/workgroups/np-exp/1168990857_366122_20220405/1168990857_366122_20220405_probeDEF/recording_slot3_3.npx2")
