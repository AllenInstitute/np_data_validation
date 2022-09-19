import os
import pathlib
import sys
import threading

import clear_dirs
import data_validation as dv
import strategies


def dirs_from_clear_dirs():
    config, dirs = clear_dirs.config_dirs_from_file()
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

def main(dirs:list[str]=None):
    config, usual_dirs = dirs_from_clear_dirs()

    if not dirs:
        dirs = usual_dirs
        
    for path in [p for dir in dirs for p in dir.rglob('*')]:
        if path.is_dir():
            continue
        
        threads = []
        t = threading.Thread(
            target=strategies.generate_checksum_if_not_in_db,
            args=(dv.SHA3_256DataValidationFile(path), dv.MongoDataValidationDB(),),
        )

        threads.append(t)
        t.start()

        # wait for the threads to complete
        print("- adding files to database...")
        for thread in dv.progressbar(threads, prefix=" ", units="files", size=25):
            thread.join()
        
if __name__ == "__main__":
    
    dirs = sys.argv[1:]

    main(dirs)
