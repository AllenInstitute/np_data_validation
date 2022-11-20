"""Strategies for looking up DataValidationFiles in a database and performing some action depending on context.
    
    Guideline is to only use dv.DataValidationFiles or dv.DataValidationDBs as arguments for functions. 
       
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
    
    Also remember that the DB is incomplete and will never have all files in it: if we don't find matches in the db
    we can go look for files in known backup locations add add them to the db and re-check status.
    In practice this is less clear-cut than STATUS enum 
        - how exhaustively do we want to search for matches? (synology drives + many 10TB disks that aren't indexed)
        - do we checksum first and ask questions later? (slow)
    * a medium/longer-term strategy may be to index all data disks by entering them into the db without checksum info to
    make the db more complete
    
    
"""

from __future__ import annotations

import os
import pathlib
from typing import List, Set, Union

# if TYPE_CHECKING:
import data_validation as dv

def copy_file(
    source: Union[str, pathlib.Path],
    destination: Union[str, pathlib.Path]):
    """
    Copy a file to a destination.
    """
    status = dv.DataValidationStatus(path=source)
    status.copy(destination,validate=True)

def generate_checksum(
    subject: dv.DataValidationFile, db: dv.DataValidationDB
) -> dv.DataValidationFile:
    """
    Generate a checksum for a file and add to database.
    """
    dv.logging.info(f"Generating {subject.checksum_name} checksum for {subject}")
    checksum = subject.generate_checksum(subject.path.as_posix(), subject.size)
    new_file = subject.__class__(
        path=subject.path.as_posix(), size=subject.size, checksum=checksum
    )
    db.add_file(new_file)
    return new_file


def generate_checksum_if_not_in_db(
    subject: dv.DataValidationFile, db: dv.DataValidationDB
):
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    accepted_matches = [subject.Match.SELF_MISSING_SELF]
    matches = db.get_matches(subject, match=accepted_matches)
    if not matches:
        generate_checksum(subject, db)


def ensure_checksum(
    subject: dv.DataValidationFile, db: dv.DataValidationDB
) -> dv.DataValidationFile:
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    if not subject.checksum:
        subject = exchange_if_checksum_in_db(subject, db)
    if not subject.checksum:
        subject = generate_checksum(subject, db)
    return subject


def find_invalid_copies_in_db(
    subject: dv.DataValidationFile, db: dv.DataValidationDB
) -> List[dv.DataValidationFile]:
    """
    Check for invalid copies of the subject file in database.
    """
    matches = db.get_matches(subject)
    match_type = [subject.compare(match) for match in matches] if matches else []
    return [
        m
        for i, m in enumerate(matches)
        if match_type[i] in dv.DataValidationFile.INVALID_COPIES
    ] or None


def find_valid_copies_in_db(
    subject: dv.DataValidationFile, db: dv.DataValidationDB
) -> List[dv.DataValidationFile]:
    """
    Check for valid copies of the subject file in database.
    """
    accepted_matches = subject.VALID_COPIES
    matches = db.get_matches(subject, match=accepted_matches)
    return matches or None


def exchange_if_checksum_in_db(
    subject: dv.DataValidationFile, db: dv.DataValidationDB
) -> dv.DataValidationFile:
    """
    If the database has an entry for the subject file that already has a checksum, swap
    the subject for the entry in the database. Saves us regenerating checksums for large files.
    If not, return the subject.
    """
    if subject.checksum:
        return subject

    accepted_matches = subject.SELVES
    matches = db.get_matches(subject, match=accepted_matches)

    if not matches:
        dv.logging.debug(f"No matches found for {subject.path} in db")
        return subject

    checksums_equal = all(m.checksum == matches[0].checksum for m in matches)
    types_equal = all(m.__class__ == matches[0].__class__ for m in matches)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1 and checksums_equal and types_equal:
        return matches[0]
    if len(matches) > 1 and not types_equal:
        for m in matches:
            submatches = db.get_matches(m,match=dv.DataValidationFile.VALID_COPIES)
            # if any of the matches have a valid copy in the db
            if submatches:
                return m
            if m.__class__ == subject.__class__:
                return m # subject class may have been chosen for a particular reason
            # next follow the order specified
            for cls in dv.available_DVFiles.values():
                if m.__class__ == cls:
                    return m
                
    dv.logging.info(f"Multiple matches for {subject} in db - could not determine SELF for exchange")
    return subject


def delete_if_valid_backup_in_db(
    subject: dv.DataValidationFile,
    db: dv.DataValidationDB,
    backup_paths: Union[List[str], Set[str]] = None,
) -> int:
    """
    If the database has an entry for the subject file in known backup locations, or a new specified location, we can
    delete the subject.
    This is just a safety measure to prevent calling 'find_valid_backups' and deleting the returned list of backups!
    """
    status = dv.DataValidationStatus(subject)
    if status.report() >= dv.DataValidationStatus.Backup.HAS_VALID_BACKUP:
        backups = status.valid_backups
        # subject.report(backups)
        # a final check before deleting (all items in 'backups' should be valid copies):
        if (not any(s.checksum == b.checksum for s in status.selves for b in backups) 
            or not any(s.checksum == b.checksum for s in status.selves for b in backups)
        ):
            raise AssertionError(
                f"Not a valid backup, something has gone wrong: {subject}"
            )

        try:
            subject.path.unlink()
            dv.logging.info(f"DELETED {subject}")

            return subject.size

        except PermissionError:
            dv.logging.exception(
                f"Permission denied: could not delete {subject}"
            )

    return 0


def find_valid_backups(
    subject: dv.DataValidationFile,
    db: dv.DataValidationDB,
    backup_paths: Union[List[str], Set[str], List[pathlib.Path]] = None,
) -> List[dv.DataValidationFile]:
    if not backup_paths:
        backup_paths = set()
        if (
            subject.lims_backup
            and subject.session.lims_path.as_posix() not in subject.path.as_posix()
        ):
            backup_paths.add(subject.lims_backup.as_posix())
        if (
            subject.npexp_backup
            and subject.session.npexp_path.as_posix() not in subject.path.as_posix()
        ):
            backup_paths.add(subject.npexp_backup.as_posix())
        if (
            not backup_paths
            and subject.z_drive_backup
            and subject.z_drive_path.as_posix() not in subject.path.as_posix()
        ):
            backup_paths.add(subject.z_drive_backup.as_posix())

    # TODO fix order here so lims folder is first, npexp second: converting to list seems to reorder
    else:
        backup_paths = set(backup_paths)
    backup_paths = (
        list(backup_paths) if not isinstance(backup_paths, list) else backup_paths
    )

    assert all(
        subject.session.folder == dv.Session.folder(bp)
        for bp in backup_paths
        if dv.Session.folder(bp)
    ), f"Backup paths look inconsistent: {backup_paths}"

    subject = ensure_checksum(subject, db)

    invalid_backups = find_invalid_copies_in_db(subject, db)

    if invalid_backups and any(
        ext in invalid_backups[0].path.as_posix() for ext in [".npx2", ".dat"]
    ):
        subject.report(invalid_backups)
        return

    matches = find_valid_copies_in_db(subject, db)

    backups = set()
    if matches:
        for match in matches:
            for backup_path in backup_paths:
                if match.path.as_posix().startswith(backup_path) and os.path.exists(
                    match.path.as_posix()
                ):
                    backups.add(match)
                else:
                    dv.logging.debug(
                        f"Valid copy - inaccessible or not in a specified backup path: {match.path.as_posix()}"
                    )

    if not backups:
        for backup_path in backup_paths:
            try_backup = pathlib.Path(backup_path)
            if not try_backup.exists():
                continue
            try_path = try_backup / subject.relative_path
            if try_path.exists():
                candidate = generate_checksum(subject.__class__(path=try_path.as_posix()), db)
                if subject.compare(candidate) in dv.DataValidationFile.VALID_COPIES:
                    backups.add(candidate)
                    # could continue here and check all backup paths
                    # to get as much info as possible before deleting the file
                    break  # this just saves time

            # now we check for any files in the directory with the same size, since the filename may have changed
            try:
                dir_contents = os.scandir(backup_path)
            except FileNotFoundError:
                continue

            for d in dir_contents:
                if d.is_file() and d.stat().st_size == subject.size:

                    candidate = generate_checksum(
                        subject.__class__(path=d.path, size=subject.size), db
                    )
                    if subject.compare(candidate) in dv.DataValidationFile.VALID_COPIES:
                        backups.add(candidate)
                        # could continue here and check all backup paths
                        # to get as much info as possible before deleting the file
                        break  # this just saves time

    return list(backups) or None


def regenerate_checksums_on_mismatch(
    subject: dv.DataValidationFile, other: dv.DataValidationFile
) -> None:
    """
    If the database has an entry for the subject file that has a different checksum, regenerate the checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_MISSING_SELF]
    # TODO regenerate and check again
    # * need a solution for replacing entries where the checksum is different
