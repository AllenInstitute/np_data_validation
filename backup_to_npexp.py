import configparser
import logging
import os
import pathlib
import pprint
import sys
import threading

import data_validation as dv
import strategies

# for folder in pathlib.Path("D:/").iterdir():
#     if folder.is_dir() and dv.Session.folder(folder.name):
#         for file in folder.rglob("*"):
#             if file.is_file():
#                 status = dv.DataValidationStatus(file)
#                 status.copy(validate=True,remove_source=True)





CONFIG_FILE = "clear_dirs.cfg" # should live in the same cwd as clear_dirs.py
DB = dv.MongoDataValidationDB()

def config_dirs_from_file():
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), CONFIG_FILE))
    
    # current config for clear_dirs has some universal settings, plus additional dirs that can be cleared for each rig computer type (acq, sync, mon..)
    dirs = [
        pathlib.Path(d.strip()).resolve()
        for d in config["options"]["dirs"].split(",")
        if d != ""
    ]

    if os.getenv("AIBS_COMP_ID"):
        # add folders for routine clearing on rig computers
        comp = os.getenv("AIBS_COMP_ID").split("-")[-1].lower()
        if comp in config:
            dirs += [
                pathlib.Path(d.strip()).resolve()
                for d in config[comp]["dirs"].split(",")
                if d != ""
            ]

    return config, dirs

def delete_if_valid_backup_in_db(result, idx, file_inst, db, backup_paths):

    files_bytes = strategies.delete_if_valid_backup_in_db(
        file_inst, db, backup_paths
    )
    result[idx] = files_bytes
    
def clear_orphan_files():
    
    config, dirs = config_dirs_from_file()
    
    if not dirs:
        return
    # deal with non-session files/dirs ----------------------------------------------------- #
    
    def delete_if_valid_copy_in_db(path):
        if dv.Session.folder(str(path)):
            # skip files/dirs with session folder string in path
            return
        if path.is_dir():
            return # TODO choose: walk directories or skip them
                
        # currrently only one option for non-session files:
        file = dv.OrphanedDVFile(path=path)
        if file.size < 1024 ** 2:
            return # TODO remove at some point - need to clear data right now 
        file = strategies.generate_checksum(file, DB)
        matches = DB.get_matches(file=file)
        if not matches:
            return
        for m in matches:
            if file.compare(m)>=file.Match.VALID_COPY_RENAMED: # we filtered for this in call to get_matches but double-check
                if any(bkup in str(m.path) for bkup in ["np-exp", "prod0"]):
                    print(f"Deleting {file.path} because it matches {m.path}: {file.Match(file.compare(m)).name}")
                    file.path.unlink()
                    return
                    
    for path in [p for d in dirs for p in d.iterdir()]:
        thread = threading.Thread(
            target=delete_if_valid_copy_in_db,
            args=(path,),
        )
        thread.start()
        
            
def move_session_folders_to_npexp():

    config, dirs = config_dirs_from_file()
    
    if not dirs:
        return
    

    regenerate_threshold_bytes = config["options"].getint(
        "regenerate_threshold_bytes", fallback=1024 ** 2
    )
    min_age_days = config["options"].getint("min_age_days", fallback=0)
    filename_include_filter = config["options"].get("filename_include_filter", fallback="")
    filename_exclude_filter = config["options"].get("filename_exclude_filter", fallback="")
    only_session_folders = config["options"].getboolean(
        "only_session_folders", fallback=False
    )
    exhaustive_search = config["options"].getboolean(
        "exhaustive_search", fallback=False
    )
    logging.getLogger().setLevel(
        config["options"].get("logging_level", fallback="INFO")
    )


    total_deleted_bytes = []  # keep a tally of space recovered
    print("Checking:")
    pprint.pprint(dirs, indent=4, compact=False)
    if min_age_days > 0:
        print(f"Skipping files less than {min_age_days} days old")

    divider = "\n" + "=" * 40 + "\n\n"

    for F in dv.DVFolders_from_dirs(dirs, only_session_folders=True):
        if not F:
            continue
        F.min_age_days = min_age_days
        F.regenerate_threshold_bytes = regenerate_threshold_bytes
        F.filename_include_filter = filename_include_filter
        F.filename_exclude_filter = filename_exclude_filter
        
        if not F.file_paths:
            continue
        
        print(f"{divider}Copying {F.path} to np-exp")
        F.copy_to_npexp()
        
        print(f"\nClearing {F.path}")
        deleted_bytes = F.clear()

        total_deleted_bytes += deleted_bytes

    print(
        f"{divider}Finished clearing session folders.\n{len(total_deleted_bytes)} files deleted | {sum(total_deleted_bytes) / 1024**3 :.1f} GB recovered\n"
    )
    if only_session_folders:
        return

def clear_dirs_fast():

    config, dirs = config_dirs_from_file()

    if not dirs:
        return

    deleted = 0

    db = dv.MongoDataValidationDB()
    for path in sorted([paths for dir in dirs for paths in dir.rglob("*")],reverse=True):

        if path.is_dir():
            continue

        if path.stat().st_size <  10*1024 ** 2:
            continue # we don't care about clearing small files right now

        file = strategies.exchange_if_checksum_in_db(dv.OrphanedDVFile(path=path),db)
        if not file.checksum:
            continue

        accepted_matches = [file.Match.VALID_COPY,
                            file.Match.VALID_COPY_RENAMED]
        matches = db.get_matches(file=file,match=accepted_matches)
        if not matches:
            continue
        for m in matches:
            if (
                file.compare(m) > 20
                and any(sub in str(m.path) for sub in ['np-exp','prod0','ecephys_session_'])
            ):  # valid copy
                deleted += file.path.stat().st_size
                sys.stdout.write(f"Deleting {file.path.name} - valid copy in {m.path.parent}\n cumulative cleared {deleted / 1024 ** 3 :.1f} GB\r")
                sys.stdout.flush()
                file.path.unlink()
                break


if __name__ == "__main__":
    move_session_folders_to_npexp()
    # clear_orphan_files()
