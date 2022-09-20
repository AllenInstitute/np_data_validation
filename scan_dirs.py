import pathlib
import pprint
import sys
from typing import Union

import clear_dirs
import data_validation as dv


def main(dirs:Union[list[str],list[pathlib.Path]]=None):

    if not dirs:
        config, dirs = clear_dirs.config_dirs_from_file()
    else:
        dirs = [pathlib.Path(d).resolve() for d in dirs if d != ""]

    if not dirs:
        return

    print("Checking:")
    pprint.pprint(dirs, indent=4, compact=False)

    divider = "\n" + "=" * 40 + "\n\n"

    for F in dv.DVFolders_from_dirs(dirs=dirs, only_session_folders=config["options"].getboolean("only_session_folders", fallback=False)):

        if not F:
            continue

        if not F.file_paths:
            continue

        print(f"{divider}{F.path}")

        F.add_to_db()

    # for path in [p for dir in dirs for p in dir.rglob('*')]:
    #     if path.is_dir():
    #         continue

    #     try:
    #         file = dv.SHA3_256DataValidationFile(path)
    #     except dv.SessionError:
    #         file = dv.OrphanedDVFile(path)

    #     threads = []
    #     t = threading.Thread(
    #         target=strategies.generate_checksum_if_not_in_db,
    #         args=(file, dv.MongoDataValidationDB(),),
    #     )

    #     threads.append(t)
    #     t.start()

    #     # wait for the threads to complete
    #     print("- adding files to database...")
    #     for thread in dv.progressbar(threads, prefix=" ", units="files", size=25):
    #         thread.join()

if __name__ == "__main__":
    main(sys.argv[1:])
