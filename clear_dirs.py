import configparser
import itertools
import logging
import os
import pathlib
import pprint

import data_validation as dv
import strategies

CONFIG_FILE = "clear_dirs.cfg" # should live in the same cwd as clear_dirs.py

def config_from_file():
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), CONFIG_FILE))
    
    return config

def clear_dirs():

    config = config_from_file()
    
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

    if not dirs:
        return

    regenerate_threshold_bytes = config["options"].getint(
        "regenerate_threshold_bytes", fallback=1024 ** 2
    )
    min_age_days = config["options"].getint("min_age_days", fallback=0)
    filename_filter = config["options"].get("filename_filter", fallback="")
    only_session_folders = config["options"].getboolean(
        "only_session_folders", fallback=True
    )
    exhaustive_search = config["options"].getboolean(
        "exhaustive_search", fallback=False
    )
    logging.getLogger().setLevel(
        config["options"].getint("regenerate_threshold_bytes", fallback=20)
    )

    total_deleted_bytes = []  # keep a tally of space recovered
    print("Checking:")
    pprint.pprint(dirs, indent=4, compact=False)
    if min_age_days > 0:
        print(f"Skipping files less than {min_age_days} days old")

    divider = "\n" + "=" * 40 + "\n\n"

    for F in dv.DVFolders_from_dirs(dirs, True):
        if not F:
            continue
        F.regenerate_threshold_bytes = regenerate_threshold_bytes
        F.min_age_days = min_age_days
        F.filename_filter = filename_filter
        if not F.file_paths:
            continue

        if F.session:
            F.add_standard_backup_paths()

        print(f"{divider}Clearing {F.path}")

        F.add_to_db()

        deleted_bytes = F.clear()

        total_deleted_bytes += deleted_bytes

    print(
        f"{divider}Finished clearing session folders.\n{len(total_deleted_bytes)} files deleted | {sum(total_deleted_bytes) / 1024**3 :.1f} GB recovered\n"
    )
if __name__ == "__main__":
    clear_dirs()
