import argparse
import logging

import sys

import data_validation as dv

logging.getLogger().setLevel(logging.INFO)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=True,description="Generate checksum if a valid copy exists in LIMS")
    parser.add_argument("filepath", type=str, help="path to an ecephys session file that will be checked against lims copy (if it exists)")
    args = parser.parse_args()
    if not args or not args.filepath:
        print("path to an ecephys session dir must be provided")
        sys.exit()

    for hasher_cls in dv.available_DVFiles.values():

        folder = dv.DataValidationFolder(path=args.filepath)
        folder.regenerate_threshold_bytes = 0
        folder.db = dv.MongoDataValidationDB
        folder.db.DVFile = hasher_cls
        folder.add_to_db()

