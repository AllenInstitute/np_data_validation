from data_validation import *

DB = MongoDataValidationDB()

def main():
    
    for f in DVFolders_from_dirs(NPEXP_PATH, only_session_folders=True):
        
        if not f.session:
            continue
        if not f.session.lims_path:
            continue
        # platform_json = SessionFile(path=(f.session.npexp_path / f"{f.session.folder}_platformD1.json"))
        
        # if not platform_json.path.exists():
        #     logging.debug(f"Fetch lims hashes - no platformD1.json in {f}")
        #     continue
        
        # if not platform_json.lims_backup:
        #     logging.debug(f"Fetch lims hashes - platformD1.json not found on lims for {f.session.folder}")
        #     continue
        
        # lims_file = strategies.exchange_if_checksum_in_db(platform_json.lims_backup, DB)
        # if lims_file.checksum and lims_file.checksum_name in lims_available_hashers:
        #     logging.debug(f"Fetch lims hashes - platformD1.json already has hash from lims {f.session.folder}")
        #     #  we might want to skip here in future
        #     # continue
        
        # fetch lims hashes for all files in session:
        logging.info("Fetching hashes generated on lims upload for {}".format(f.session.folder))
        lims_files = LimsDVDatabase.file_factory_from_ecephys_session(f.session.folder)
        if not lims_files:
            continue
        for file in lims_files:
            DB.add_file(file)
        
if __name__ == "__main__":
    main()
         