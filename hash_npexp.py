import argparse
import logging
import pathlib
import sys

import data_validation as dv
import strategies

NPEXP_ROOT = R"//allen/programs/mindscope/workgroups/np-exp"

logging.getLogger().setLevel(logging.DEBUG)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(add_help=True,description="Generate checksum if a valid copy exists in LIMS")
    parser.add_argument("filepath", type=str, help="path to an ecephys session file that will be checked against lims copy (if it exists)")
    for f in pathlib.Path(NPEXP_ROOT).rglob('*.npx2'):
        args = parser.parse_args([f.as_posix()])
        if not args or not args.filepath:
            logging.info("Filepath to an ecephys session file must be provided")
            sys.exit()
        
        db = dv.MongoDataValidationDB()
        for hasher_cls in dv.available_DVFiles.values():
            
            db.DVFile = hasher_cls

            file = db.DVFile(path=args.filepath)
            
            if not file.lims_backup:
                sys.exit()
                
            file = strategies.exchange_if_checksum_in_db(file, db)
            
            lims_file = db.DVFile(path=file.lims_backup)
            lims_file = strategies.exchange_if_checksum_in_db(lims_file, db)
                
            comparison = file.compare(lims_file)
            if comparison > 20:
                print('valid copy already found')
                print(file)
                print(lims_file)
                break
            
            if comparison < 17:
                print(db.DVFile.Match(comparison)), print(file)
               
                print(lims_file)
                continue
            
            if comparison in [19,17]:
                print('generating checksum for lims file')
                lims_file = strategies.generate_checksum(lims_file,db)
                print(lims_file.checksum)
                db.add_file(lims_file)
                
            if comparison in [18,17]:
                print('generating checksum for np-exp file')
                file = strategies.generate_checksum(file,db)
                print(file.checksum)
                db.add_file(file)
        