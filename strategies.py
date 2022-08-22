"""Strategies for looking up DataValidationFiles in a database and performing some action depending on context."""

from __future__ import annotations

import os
import pathlib
from typing import List, Set, Union

# if TYPE_CHECKING:
import data_validation as dv


def generate_checksum(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    Generate a checksum for a file and add to database.
    """
    checksum = subject.generate_checksum(subject.path.as_posix(), subject.size)
    new_file = db.DVFile(path=subject.path.as_posix(), size=subject.size, checksum=checksum)
    db.add_file(new_file)
    return new_file


def generate_checksum_if_not_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB):
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    matches = db.get_matches(subject, match=accepted_matches)
    if not matches:
        generate_checksum(subject, db)


def ensure_checksum(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    if not subject.checksum:
        subject = exchange_if_checksum_in_db(subject, db)
    if not subject.checksum:
        subject = generate_checksum(subject, db)
    return subject


def find_invalid_copies_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> List[dv.DataValidationFile]:
    """
    Check for invalid copies of the subject file in database.
    """
    matches = db.get_matches(subject)
    match_type = [(subject == match) for match in matches] if matches else []
    return [
        m for i,m in enumerate(matches) 
        if match_type[i] >= subject.Match.CHECKSUM_COLLISION 
        and match_type[i] <= subject.Match.UNSYNCED_OR_CORRUPT_DATA
        ] or None


def find_valid_copies_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> List[dv.DataValidationFile]:
    """
    Check for valid copies of the subject file in database.
    """
    accepted_matches = [subject.Match.VALID_COPY_RENAMED, subject.Match.VALID_COPY_SAME_NAME]
    matches = db.get_matches(subject, match=accepted_matches)
    return matches or None
    

def exchange_if_checksum_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    If the database has an entry for the subject file that already has a checksum, swap 
    the subject for the entry in the database. Saves us regenerating checksums for large files.
    If not, return the subject.
    """
    if subject.checksum:
        return subject
    
    accepted_matches = subject.Match.SELF_NO_CHECKSUM
    matches = db.get_matches(subject, match=accepted_matches)
    
    if not matches:
        return subject
    
    if len(matches) == 1:
        return matches[0]
    else:
        # multiple matches with different checksums: should regenerate checksum
        return subject


def delete_if_valid_backup_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB, backup_paths: Union[List[str],Set[str]]=None) -> int:
    """
    If the database has an entry for the subject file in known backup locations, or a new specified location, we can
    delete the subject. 
    This is just a safety measure to prevent calling 'find_valid_backups' and deleting the returned list of backups!
    """               
    subject = ensure_checksum(subject, db)
    backups = find_valid_backups(subject, db, backup_paths)
    if backups:
        subject.report(backups)
        # a final check before deleting (all items in 'backups' should be valid copies):
        if (subject.checksum != backups[0].checksum or subject.size != backups[0].size):
            raise AssertionError(f"Not a valid backup, something has gone wrong: {subject} {backups[0]}")
        
        # currently, we don't want to delete raw data on A/B drives before the sorted data make it to npexp
        if (subject.path.as_posix().startswith("A:") or subject.path.as_posix().startswith("B:")) \
            and subject.probe_dir and subject.probe_dir in ["ABC", "DEF"] \
            and not (
                (subject.session.npexp_path and any(s for s in subject.session.npexp_path.glob('*_sorted*')))
                or (subject.session.lims_path and any(s for s in subject.session.lims_path.glob('*_sorted*')))
            ):
            dv.logging.debug(f"Skipped deletion of raw probe data on Acq: no sorted folders on npexp or lims yet {subject.session.folder} ")
            return 0
            
        try:
            subject.path.unlink()
            dv.logging.info(f"DELETED {subject.path.as_posix()}")
            
            return subject.size
        
        except PermissionError:
            dv.logging.exception(f"Permission denied: could not delete {subject.path.as_uri()}")
            
    return 0


def find_valid_backups(subject: dv.DataValidationFile, db: dv.DataValidationDB, backup_paths: Union[List[str], Set[str], List[pathlib.Path]] = None) -> List[dv.DataValidationFile]:
    if not backup_paths:
        backup_paths = set()
        if subject.session.lims_path and subject.session.lims_path.as_posix() not in subject.path.as_posix():
            backup_paths.add(subject.session.lims_path.as_posix())
        if subject.npexp_path and subject.session.npexp_path.as_posix() not in subject.path.as_posix():
            backup_paths.add(subject.session.npexp_path.as_posix())
        if not backup_paths \
            and subject.z_drive_path and subject.z_drive_path.as_posix() not in subject.path.as_posix():
            backup_paths.add(subject.z_drive_path.as_posix())
            
    # TODO fix order here so lims folder is first, npexp second: converting to list seems to reorder
    else:
        backup_paths = set(backup_paths)
    backup_paths = list(backup_paths) if not isinstance(backup_paths, list) else backup_paths
    
    assert all(subject.session.folder == dv.Session.folder(bp) for bp in backup_paths if dv.Session.folder(bp)), f"Backup paths look inconsistent: {backup_paths}"
    
    subject = ensure_checksum(subject, db)
    
    invalid_backups = find_invalid_copies_in_db(subject, db)
    
    if invalid_backups and any(ext in invalid_backups[0].path.as_posix() for ext in ['.npx2','.dat']):
        subject.report(invalid_backups)
    
    matches = find_valid_copies_in_db(subject, db)
    
    backups = set()
    if matches:
        for match in matches:
            for backup_path in backup_paths:
                if match.path.as_posix().startswith(backup_path) \
                    and os.path.exists(match.path.as_posix()):
                    backups.add(match)
                else:
                    dv.logging.debug(f"Valid copy - inaccessible or not in a specified backup path: {match.path.as_posix()}")
    
    if not backups:
        for backup_path in backup_paths:
            try_backup = pathlib.Path(backup_path)
            if not try_backup.exists():
                continue
            try_path = try_backup / subject.relative_path
            if try_path.exists():
                    candidate = generate_checksum(db.DVFile(path=try_path.as_posix()), db)
                    if (subject == candidate) in [db.DVFile.Match.VALID_COPY_RENAMED, db.DVFile.Match.VALID_COPY_SAME_NAME]:
                        backups.add(candidate)
                        # could continue here and check all backup paths
                        # to get as much info as possible before deleting the file
                        break # this just saves time
                    
            # now we check for any files in the directory with the same size, since the filename may have changed
            try: 
                dir_contents = os.scandir(backup_path)
            except FileNotFoundError:
                continue
            
            for d in dir_contents:
                if d.is_file() \
                and d.stat().st_size == subject.size:
                    
                    candidate = generate_checksum(db.DVFile(path=d.path, size=subject.size), db)
                    if (subject == candidate) in [db.DVFile.Match.VALID_COPY_RENAMED, db.DVFile.Match.VALID_COPY_SAME_NAME]:
                        backups.add(candidate)
                        # could continue here and check all backup paths
                        # to get as much info as possible before deleting the file
                        break # this just saves time 
                    
    return list(backups) or None
    

def regenerate_checksums_on_mismatch(subject: dv.DataValidationFile, other: dv.DataValidationFile) -> None:
    """
    If the database has an entry for the subject file that has a different checksum, regenerate the checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    #TODO regenerate and check again
    #* need a solution for replacing entries where the checksum is different

