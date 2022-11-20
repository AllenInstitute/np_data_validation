import re

from data_validation import DVFolders_from_dirs

DRIVES = ("A:/", "B:/")

def main():
    for folder in sorted(DVFolders_from_dirs(DRIVES, only_session_folders=True), key=lambda x: x.path.name): 
        
        if not folder.session:
            continue
        if not folder.is_original_raw_data:
            continue
        
        folder.skip_sorting_check = True
        
        print(folder.path)
        folder.clear()
        
        
if __name__ == "__main__":
    main()