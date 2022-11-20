import os
import pathlib


DRIVES = ("A:", "B:")
PATTERNS_FOR_DELETION = (
    "_366122_",
    "_603810_", # NP0 pretest
    "_599657_", # NP1 pretest
    "_598796_", # NP2 pretest
    "_temp_", # dummy rec to unlock Open Ephys
)
MAX_SIZE_FOR_DELETION_GB = 50


def dir_size_gb(path) -> int:
    root_directory = pathlib.Path(path).parent if os.path.isfile(path) else pathlib.Path(path)
    return sum(f.stat().st_size for f in root_directory.glob("**/*") if f.is_file()) // 1024**3

def main():
    for drive in DRIVES:
        
        for folder in pathlib.Path(drive).iterdir():
            
            if not folder.is_dir():
                continue
            if not any(pattern in folder.name for pattern in PATTERNS_FOR_DELETION):
                continue
            if not dir_size_gb(folder) < MAX_SIZE_FOR_DELETION_GB:
                continue
                    
            for dir, _, files in os.walk(folder, topdown=False):
                
                # double-check before deleting
                if not any(pattern in dir for pattern in PATTERNS_FOR_DELETION):
                    continue
                
                for file in files:
                    try:
                        os.unlink(os.path.join(dir, file))
                    except OSError:
                        pass
                    
                try:
                    os.rmdir(dir)
                except OSError:
                    pass
                
        os.unlink(os.path.join(drive, "temp.txt"))
        
if __name__ == "__main__":
    main()