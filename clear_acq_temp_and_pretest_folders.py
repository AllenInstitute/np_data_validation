import os
import pathlib

roots = ("A:", "B:")
delete_patterns = (
    "_366122_",
    "_603810_", # NP0 pretest
    "_599657_", # NP1 pretest
    "_598796_", # NP2 pretest
    "_temp_", # dummy rec to unlock Open Ephys
)
max_size_for_deletion_gb = 50

def dir_size_gb(path) -> int:
    root_directory = pathlib.Path(path).parent if os.path.isfile(path) else pathlib.Path(path)
    return sum(f.stat().st_size for f in root_directory.glob("**/*") if f.is_file()) // 1024**3
    
for root in roots:
    for folder in pathlib.Path(root).iterdir():
        
        if not folder.is_dir():
            continue
        if not any(pattern in folder.name for pattern in delete_patterns):
            continue
        if not dir_size_gb(folder) < max_size_for_deletion_gb:
            continue
                
        for dir, _, files in os.walk(folder, topdown=False):
            
            # double-check before deleting
            if not any(pattern in dir for pattern in delete_patterns):
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