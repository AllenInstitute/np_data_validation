import argparse
import configparser
import os
import pathlib
import pprint
import sys
from data_validation import DVFolders_from_dirs

def main(dirs_to_clear, 
         backups=None, 
         include_subfolders=True,
         regenerate_threshold_bytes=1024*1024, 
         min_age_days=0, 
         no_delete=False
         ):
    
    total_deleted_bytes = [] # keep a tally of space recovered
    print('Checking:')
    pprint.pprint(dirs_to_clear, indent=4, compact=False)
    if min_age_days > 0:
        print(f'Skipping files less than {min_age_days} days old')
    
    divider = '\n' + '='*40 + '\n\n'
    
    for F in DVFolders_from_dirs(dirs_to_clear):
  
        # TODO need to be able to set include_subfolders in DVFolders_from_dirs, but also want to leave it as a config
        # option, which shoud be set here 
        # F.include_subfolders = include_subfolders
        F.regenerate_threshold_bytes = regenerate_threshold_bytes
        F.min_age_days = min_age_days
        
        if backups:
            # use backups specified
            F.add_backup_path(backups)
        elif F.session:
            # use LIMS, NPEXP, and rig z-drive backup paths, as applicable
            F.add_standard_backup_paths()
            
        print(f'{divider}Clearing {F.path}')
        F.add_to_db()
        
        # until a standalone 'add_dirs' function is made, use this flag to skip deletion
        if no_delete:
            print(f"'no_delete=True' - skipping clearing {F.path}")
            continue
        
        deleted_bytes = F.clear()
        
        total_deleted_bytes += deleted_bytes 
        
    print(f"{divider}Finished clearing.\n{len(total_deleted_bytes)} files deleted | {sum(total_deleted_bytes) / 1024**3 :.1f} GB recovered\n")
    
    
def args_from_command_line():
    
    parser = argparse.ArgumentParser(description='Clear one or more folders if valid backups can be located')

    parser.add_argument('--clear', required=True, action="extend", nargs="+", type=str, help='directories to clear')
    parser.add_argument('--backup', action="extend", nargs="+", type=str, help='location of possible backups, at equivalent folder-level')
    parser.add_argument('--min_age_days', type=int, help='minimum age of files to clear')
    parser.add_argument('--no_delete', action="store_false", help='do not delete files if True')
    parser.add_argument('--include_subfolders', action="store_true", help='include subfolders in directories to be cleared')
    parser.add_argument('--regenerate_threshold_bytes', type=int, help='minimum size of files to regenerate')
    
    args = parser.parse_args()
    return vars(args)


def args_from_config():
    
    args = {}
    
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    dirs = [pathlib.Path(d.strip()).resolve().as_posix() for d in config['options']['dirs'].split(',') if d != '']
    
    if os.getenv('AIBS_COMP_ID'):
        # get folders for routine clearing on rig computers
        comp = os.getenv('AIBS_COMP_ID').split('-')[-1].lower()
        dirs += [pathlib.Path(d.strip()).resolve().as_posix() for d in config[comp]['dirs'].split(',') if d != '']
    
    if not dirs:
        print("No directories specified to clear")
        exit()
    
    args['clear'] = dirs
    args['include_subfolders'] = config['options'].getboolean('include_subfolders', fallback=True)
    args['include_subfolders'] = config['options'].getboolean('no_delete', fallback=False)
    args['regenerate_threshold_bytes'] = config['options'].getint('regenerate_threshold_bytes', fallback=1024**2)
    args['min_age_days'] = config['options'].getint('min_age_days', fallback=0)
    
    return args
    
    
if __name__ == "__main__":
    # both functions below return args as a dict
    if sys.argv:
        args = args_from_command_line()
    else:
        args = args_from_config()
    main(**args)