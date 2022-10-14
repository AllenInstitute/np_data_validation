import argparse
import pathlib
import sys

import pandas as pd

import platform_json as pj
from data_validation import Session, SessionError


def main(*args, **kwargs):
    argin = args[0].session_dir
    
    try:
        session = Session(argin)
    except SessionError as e:
        print("no session ID found in path")
        session = None
    
    try:
        path = pathlib.Path(argin)
    except:
        path = None
    
    if path and path.is_dir():
        session_folder = path
        jsons = session_folder.glob("*_platform.json")
        if jsons:
            json_path = jsons[0]
    elif path and path.is_file() and "platform" in path.name and path.suffix == ".json":
        session_folder = path.parent
        json_path = path
    elif path and session:
        json_path = pj.find_platform_json(session.folder)
        if json_path:
            session_folder = json_path.parent
    else:
        raise ValueError("no valid session directory or platform json file found")
    
    divider = f"\n{'-' * 80}\n"
    print(divider)
    print(f"Using {json_path}")
    
    files = pj.Files(json_path)
    pj.STAGING = False
    
    print("Checking for files, hold on...")
    d1_df = {}
    d2_df = {}
    # make d1 df --------------------------------------------------------------------------- #
    d1_df[session] = pd.DataFrame(
    data = [ 
            (
            e.suffix, 
            e.origin is not None and e.origin.exists(), #! skip for speed
            e.npexp.exists(),
            e.lims is not None,
            ) 
            for e 
            in files.entries_expected],
    columns=[
            session,
            'at origin', #! skip for speed
            'on npexp',
            'on lims',
            ],
    )
    d1_df[session].loc['SUM'] = d1_df[session].sum()
    d1_df[session].loc['SUM',session] = 'SUM'
    d1_df[session].loc['ALL'] = d1_df[session].all()
    d1_df[session].loc['ALL',session] = 'ALL'

    d1_df[session].set_index(session, inplace=True)

    # make d2 df --------------------------------------------------------------------------- #
    d2_df[session] = pd.DataFrame(
    data=[
            (
            e.suffix, 
            e.npexp.exists(),
            e.lims is not None,
            ) 
            for e 
            in files.entries_d2],
    columns=[
            session,
            'on npexp',
            'on lims',
            ],
    )
    d2_df[session].loc['SUM'] = d2_df[session].sum()
    d2_df[session].loc['SUM',session] = 'SUM'
    d2_df[session].loc['ALL'] = d2_df[session].all()
    d2_df[session].loc['ALL',session] = 'ALL'

    d2_df[session].set_index(session, inplace=True)
    
    print(divider)
    print('D1 files')
    print(d1_df[session])
    print(divider)
    print('D2 files')
    print(d2_df[session])
    

if __name__ == "__main__":
    # ##* for testing
    # sys.argv[1] = r"\\w10dtsm18306\c$\ProgramData\AIBS_MPE\neuropixels_data\1234567890_599657_20221014_pretest\20221014094937_pretest_platformD1.json"
    # sys.argv[1] = "1200879339_634837_20220825"
    parser = argparse.ArgumentParser(add_help=True,description="Using information in a platformD1.json, try to locate and report on missing files.")
    parser.add_argument("session_dir", nargs='?', default=None, type=str, help="path to a session directory, platform json, or session id (123456789_366122_20220618)")
    args = parser.parse_args()
    if not args or not args.session_dir:
        print("path to a session directory, platform json, or session id (123456789_366122_20220618) must be provided")
        sys.exit()
    main(args)
    # clear_orphan_files()