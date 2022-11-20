"""
Get raw data from the Acq drives onto npexp, trigger upload to lims, then checksum
copies on Acq drives. 

The ecephys upload strategy on lims will generate checksums upon copying files (src
first), so as soon as the files are ingested into lims we can look up the checksums
for the copies on npexp.

Takes about 4 hrs for all checksums to be available on lims for 1 TB of data on npexp.

Another process can do the comparison of Acq-npexp checksums and delete Acq copies - we
might not want to delete raw data off the drives until sorting is complete.
"""

import re
import subprocess

from data_validation import *
from platform_json import *


DRIVES = ("A:/", "B:/")
RAW_DATA_DIR_PATTERN = "(?<=_probe)([A-F]{3,})"

def main():
    
    sessions:set[Session] = set()
    
    # copy raw data to npexp --------------------------------------------------------------- #
    for folder in DVFolders_from_dirs(DRIVES, only_session_folders=True): 
        
        if not folder.session:
            continue
        if not re.search(RAW_DATA_DIR_PATTERN, folder.path.name):
            continue
        
        src = folder.path
        dest = folder.session.npexp_path / folder.path.name
        cmd = ["robocopy", src, dest]
        
        # https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/robocopy
        cmd.append("/e") # incl subdirectories (including empty ones)
        cmd.append("/xn") # excl newer src files
        cmd.append("/xo") # excl older src files
        # /xc = excl src with same timestamp, different size
        cmd.append("/j") # unbuffered i/o (for large files)
        cmd.extend(("/r:3", "/w:10")) # retry count, wait between retries (s)
        cmd.append("/mt:24") # multi-threaded: n threads
        
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            if e.returncode >= 8:
                continue
        
        sessions.add(folder.session)
        
        
    # upload successful copies to lims ----------------------------------------------------- #
    for session in sessions:
        
        pj = Files(find_platform_json(session.folder))
        
        if pj.path.parent != session.npexp_path:
            pj.path = pj.path.rename(session.npexp_path / pj.path.name)
            
        pj.upload_d0_only()
        
        
    # checksum folders in chronological order ---------------------------------------------- #
    for folder in sorted(DVFolders_from_dirs(DRIVES, only_session_folders=True), key=lambda x: x.path.name): 
        
        if not folder.session:
            continue
        if not re.search(RAW_DATA_DIR_PATTERN, folder.path.name):
            continue
        
        print(folder.path)
        folder.add_to_db()


if __name__ == "__main__":
    main()
        