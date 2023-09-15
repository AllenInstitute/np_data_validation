import itertools
import json
import pathlib
import re
import sys
from typing import Generator, List, Literal
import enum
import json
import pathlib
import sys
from typing import Generator, Literal, Type
import warnings

from data_validation import (NPEXP_PATH, DataValidationStatus,
                             FilepathIsDirError, Session, SessionError,
                             logging)
from platform_json import *

QC_PATH = pathlib.Path("//allen/programs/braintv/workgroups/nc-ophys/corbettb/NP_behavior_pipeline/QC")

class Project(enum.Enum):
    "All specific project names (used on lims) associated with each umbrella project."
    VAR = ('VariabilitySpontaneous', 'VariabilityAim1')
    GLO = ('OpenScopeGlobalLocalOddball',)
    ILLUSION = ('OpenScopeIllusion',)
    DR = (
        'DynamicRoutingSurgicalDevelopment',
        'DynamicRoutingDynamicGating',
        'DynamicRoutingTask1Production',
    )


def eat_warning_lines(msg:str,category:str,*args,**kwargs):
    print("\n{}: {}\n".format(category.__name__, msg))
warnings.showwarning = eat_warning_lines

def probe_folders_in(path: pathlib.Path) -> int:
    probe_folders = []
    if not path or not path.exists():
        return probe_folders
    for probe_group in ["*_probeABC", "*_probeDEF"]:
        if (
            len(glob := list(path.glob(probe_group))) == 1
            and all(p.is_dir() for p in glob) 
        ):
            probe_folders += glob
    return probe_folders


def is_new_ephys_folder(path: pathlib.Path) -> bool:
    return bool(list(path.glob("*_probe*/Record Node*")))

def npexp_sessions(project: Project = None) -> Generator[Session, None, None]:
    for path in NPEXP_PATH.iterdir():
        
        if not path.is_dir():
            continue
        try: 
            session = Session(path)
        except (SessionError, FilepathIsDirError):
            continue
        
        if isinstance(project, Project):
            rename_existing_platform_json_backups(path)
            if not (platform_json_path := path / f"{session.folder}_platformD1.json").exists():
                continue
            if (platform_json := PlatformJson(platform_json_path)).experiment not in project.value:
                continue
            if not platform_json.is_ecephys_session:
                continue
            
        yield session
            
def npexp_sessions_uploaded() -> Generator[str, None, None]:
    for session in npexp_sessions():
        
        if (
            len(probe_folders_in(session.lims_path)) == 2
            and is_new_ephys_folder(session.lims_path)
            and [
                dir_size(f) > 300 * 1024 ** 3
                for f in probe_folders_in(session.lims_path)
            ]
        ):
            yield session

def npexp_sessions_to_upload() -> Generator[Session, None, None]:
    
    uploaded = list(npexp_sessions_uploaded())
    
    for session in npexp_sessions():

        if (
            session not in uploaded
            and len(probe_folders_in(session.npexp_path)) == 2
            and is_new_ephys_folder(session.npexp_path)
            and "366122" not in session.folder
            # these sessions yet to upload from NP.1-Acq
            and not any(s in session.folder for s in ["20220927"])
        ):
            # simple validation of folder sizes:
            size = [
                dir_size(f) // 1024 ** 3 for f in probe_folders_in(session.npexp_path)
            ] 
            if size[0] == size[1] >= 300:
                yield session
                
def npexp_sessions_to_fix() -> Generator[Session, None, None]:
    
    uploaded = list(npexp_sessions_uploaded())
    
    for session in npexp_sessions():

        if (
            (
                session not in uploaded
                and is_new_ephys_folder(session.npexp_path)
            ) 
            and 
            (
                len(probe_folders_in(session.npexp_path)) != 2
                or "366122" in session.folder
                # these sessions yet to upload from NP.1-Acq
                or any(s in session.folder for s in ["20220927"])
            )
        ):
            # simple validation of folder sizes:
            size = [
                dir_size(f) // 1024 ** 3 for f in probe_folders_in(session.npexp_path)
            ] 
            # if size[0] == size[1] >= 300:
            yield session


def npexp_session_D0_json(session) -> pathlib.Path:
    j = session.npexp_path / f"{session.folder}_platformD0.json"
    b = session.npexp_path / f"{session.folder}_platformD0.bak"
    return j if j.exists() else b

def npexp_session_revert_bak_to_json() -> pathlib.Path:
    for session in npexp_sessions():
        rename_existing_platform_json_backups(session.npexp_path)

def rename_existing_platform_jsons(path: pathlib.Path):            
    for existing_platform_json in path.glob(f'*_platform*.json'):
        existing_platform_json.replace(existing_platform_json.with_suffix('.bak'))
        print(f"renamed {existing_platform_json}")
        
def rename_existing_platform_json_backups(path: pathlib.Path):            
    for existing_platform_json in path.glob(f'*_platform*.bak'):
        if not (orig := existing_platform_json.with_suffix('.json')).exists():
            existing_platform_json.replace(orig)
            print(f"renamed {existing_platform_json}")

def dir_size(path:pathlib.Path) -> int:
    if not path.is_dir():
        return None
    dir_size = 0
    dir_size += sum(f.stat().st_size for f in pathlib.Path(path).rglob('*') if pathlib.Path(f).is_file())
    return dir_size

def sorted_probe_folders_in(path: pathlib.Path) -> List[pathlib.Path]:
    probe_folders: List[pathlib.Path] = []
    if not path or not path.exists():
        return probe_folders
    if (
        len(glob := list(path.glob("*sorted"))) > 0
        and all(p.is_dir() for p in glob) 
    ):
        probe_folders += glob
    return probe_folders

def sorted_probe_folders_on_lims(session: Session) -> List[pathlib.Path]:
    paths = []
    if not session.lims_path:
        return []
    for p in session.lims_path.glob("*/*_probe*"):
        if p.is_dir() and not any(d in p.name for d in ("DEF", "ABC")):
            paths.append(p)
    return paths

def is_hab(session: Session) -> bool|None:
    "Return True/False, or None if not enough info to determine"
    if not session.npexp_path:
        return None
    for platform_json in session.npexp_path.glob("*_platformD1.json"):
        if "habituation" in platform_json.read_text():
            return True
        return False
    return None
        
def qc_probe_noise_paths(session: Session) -> List[pathlib.Path]:
    probe_noise = QC_PATH / session.folder / "probe_noise"
    if not probe_noise.exists():
        return []
    return list(probe_noise.glob("*probe*"))

def sessions_with_no_qc():
    for path in NPEXP_PATH.iterdir():
        if not path.is_dir():
            continue
        try: 
            session = Session(path)
        except (SessionError, FilepathIsDirError):
            continue 
        if (not qc_probe_noise_paths(session)
            and not is_hab(session)
        ):
            yield session   
            
            
def npexp_files_to_clear(raw_or_sorted: Literal['raw', 'sorted']) -> Generator[str, None, None]:
    for path in NPEXP_PATH.iterdir():
    
        if not path.is_dir():
            continue
        
        try: 
            session = Session(path)
        except (SessionError, FilepathIsDirError):
            continue
        
        npx2_files = session.npexp_path.glob("_probe*/*.npx2")
        sorted_dat_files: list[pathlib.Path]  = []
        raw_dat_files: list[pathlib.Path] = []
        
        if (noise_paths := qc_probe_noise_paths(session)):
            
            for sorted_dat in session.npexp_path.glob("*_probe*sorted/**/Neuropix-PXI-100.0/**/continuous.dat"):
                probe = re.findall('probe([A-F])',str(sorted_dat.parent))
                probe = probe[0] if probe else None
                if not probe:
                    continue
                if any(f'Probe{probe}' in str(n.name) for n in noise_paths):
                    sorted_dat_files.append(sorted_dat)
                    
        if (
            sorted_probe_folders_on_lims(session)
            or sorted_probe_folders_in(session.npexp_path)
        ):
            raw_abc = session.npexp_path.glob("*_probeABC/**/continuous.dat")
            raw_def = session.npexp_path.glob("*_probeDEF/**/continuous.dat")
            
            raw_dat_files = itertools.chain(raw_abc, raw_def)
            
        if raw_or_sorted == 'raw':
            yield from npx2_files
            yield from raw_dat_files
        elif raw_or_sorted == 'sorted':
            yield from sorted_dat_files
        
def clear_raw_data_on_npexp():
    cleared = float(0)
    for f in npexp_files_to_clear('raw'):
        status = DataValidationStatus(f)
        if status.report() == DataValidationStatus.Backup.VALID_ON_LIMS:
            # print(f"\n\nClearing {f}")
            # print([s.checksum for s in status.selves if s.checksum])
            # print([s.checksum for s in status.valid_backups if s.checksum])
            cleared += f.stat().st_size/1024**3
            f.unlink()
            sys.stdout.write(f"{status.file} cleared: {cleared:9,.1f} GB total\r")
            sys.stdout.flush()
        else:
            logging.debug(f"{status.file} not deleted: {status.report().name}")
            
def clear_sorted_data_on_npexp():
    cleared = float(0)
    for f in npexp_files_to_clear('sorted'):
        cleared += f.stat().st_size/1024**3
        f.unlink()
        sys.stdout.write(f"{f} cleared: {cleared:9,.1f} GB total\r")
        sys.stdout.flush()

if __name__ == "__main__":
    clear_sorted_data_on_npexp()
    clear_raw_data_on_npexp()