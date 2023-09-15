import argparse
import datetime
import hashlib
import itertools
import json
import logging
import pathlib
import sys
from typing import Dict, List, Tuple, Union

import data_validation as dv

log = logging.getLogger(__name__)

# from allensdk/brain_observatory/ecephys/copy_utility/_schemas.py:
lims_available_hashers = {"sha3_256": hashlib.sha3_256, "sha256": hashlib.sha256}
available_DVFiles = {"sha3_256": dv.SHA3_256DataValidationFile, "sha256": dv.SHA256DataValidationFile}

def hash_type_from_ecephys_upload_input_json(json_path: Union[str, pathlib.Path]) -> str:
    """Read LIMS ECEPHYS_UPLOAD_QUEUE _input.json and return the hashlib class."""
    with open(json_path,'r') as f:
        hasher_key = json.load(f).get("hasher_key", None)
    return hasher_key


def hashes_from_ecephys_upload_output_json(
    json_path: Union[str, pathlib.Path], hasher_key: str
) -> dict[str, str]:
    """Read LIMS ECEPHYS_UPLOAD_QUEUE _output.json and return a dict of {lims filepaths:hashes(hex)}."""
    # hash_cls is specified in output_json, not input json, so we'll need to open that
    # up and feed its value of hash_cls to this function
    # not calling 'hash_class_from_ecephys_upload_input_json' here because this
    # organization of files may change in future, and we need to pass the hash_cls to
    # other functions

    if not json_path and not hasher_key:
        raise ValueError("path and hashlib class must be provided")

    json_path = pathlib.Path(json_path)
    if not hasher_key in lims_available_hashers.keys():
        raise ValueError(f"hash_cls must be one of {list(lims_available_hashers.keys())}")

    if not json_path.exists():
        raise FileNotFoundError("path does not exist")

    if not json_path.suffix == ".json":
        raise ValueError("path must be a json file")

    with open(json_path) as f:
        data = json.load(f)

    file_hash = {}
    for file in data["files"]:
        file_hash.update(
            {file["destination"]: lims_list_to_hexdigest(file["destination_hash"])}
        )
    return file_hash


def lims_list_to_hexdigest(lims_hash: List[int]) -> str:
    lims_list_bytes = b""
    for i in lims_hash:
        lims_list_bytes += (i).to_bytes(1, byteorder="little")
    return lims_list_bytes.hex()


def hash_file(
    path: Union[str, pathlib.Path], hasher_cls=hashlib.sha3_256, blocks_per_chunk=128
) -> str:
    """
    Use a hashing function on a file as per lims2 copy tool, but return the 32-len
    string hexdigest instead of a 32-len list of integers.
    """
    hasher = hasher_cls()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(hasher.block_size * blocks_per_chunk), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def upload_jsons_from_ecephys_session_or_file(session_or_file: Union[int, str, pathlib.Path, dv.DataValidationFile]) -> List[Tuple]:
    """Returns a list of tuples of (input_json, output_json) for any given session file
    or session id."""
    if isinstance(session_or_file,(str,pathlib.Path)):
        try:
            lims_dir = dv.Session(session_or_file).lims_path
        except:
            return None
    else:
        if isinstance(session_or_file,(dv.DataValidationFile)):
            lims_dir = session_or_file.session.lims_path  
        elif isinstance(session_or_file,int):
            lims_dir = dv.Session(path=f"{session_or_file}_366122_20220618").lims_path
        
    if not lims_dir:
        return None
    
    input_and_output_jsons = []
    for upload_input_json in itertools.chain(
        lims_dir.rglob("*_UPLOAD_QUEUE_*_input.json"),
        lims_dir.rglob("*EUR_QUEUE_*_input.json"),
        ):
        # get corresponding output json
        matching_output_jsons = upload_input_json.parent.rglob(
            "*" + upload_input_json.name.replace("input", "output")
        )
        upload_output_json = [f for f in matching_output_jsons]
        if not upload_output_json:
            log.info(f"No matching output json found for {upload_input_json}")
            continue

        if len(upload_output_json) > 1:
            log.info(
                f"Multiple output json files found for {upload_input_json}: {upload_output_json}"
            )
            continue
        
        upload_output_json = upload_output_json[0]

        input_and_output_jsons.append((upload_input_json, upload_output_json))
    
    return input_and_output_jsons
        
    
def file_factory_from_ecephys_session(session_or_file: Union[int, str, pathlib.Path, dv.DataValidationFile], return_as_dict=False) -> Union[Dict[str,Dict[str,str]],List[dv.DataValidationFile],]:
    """Return a list of DVFiles with checksums for an ecephys session on lims.
    
    Provide path to ecephys_session_ dir or any file within it and we'll extract out
    the session dir.
    
    #! Current understanding is that files can't be overwritten on lims
    so although a file may have been uploaded multiple times (appearing in multiple
    upload queue files with multiple hashes), the path in lims will always be unique (ie
    the same file can live in multiple subfolders) - so we should be able to aggregate
    all filepaths across all upload queue files and get a unique set of files (with some
    possible overlap in data/checksums).
    
    """

    input_and_output_jsons = upload_jsons_from_ecephys_session_or_file(session_or_file)
    if not input_and_output_jsons:
        return
    
    all_hashes = {}
    for upload_input_json, upload_output_json in input_and_output_jsons:
        # get hash function that was used by lims
        hasher_key = hash_type_from_ecephys_upload_input_json(upload_input_json)
        # get hashes from output json
        hashes = hashes_from_ecephys_upload_output_json(upload_output_json, hasher_key)
        
        for file in hashes.keys():
            if all_hashes.get(file,None):
                print(f"{file} already in all_hashes")
            all_hashes.update({file:{hasher_key:hashes[file]}})
        
    if return_as_dict:
        return all_hashes

    # this is slow-ish and only makes sense if we need DVFiles for all objects in a
    # session - otherwise just use the dict
    DVFiles = [] 
    for file,hashes in all_hashes.items():
        for hasher_key,hash_hexdigest in hashes.items():
            DVFiles.append(available_DVFiles[hasher_key](path=file,checksum=hash_hexdigest))
    return DVFiles

def get_file_with_hash_from_lims(file: Union[str,pathlib.Path,dv.SessionFile]) -> dv.DataValidationFile:
    """Return the hash of a file in LIMS, or None if it doesn't exist."""
    if not isinstance(file,dv.SessionFile):
        file = dv.SessionFile(path=file)
        
    if not file.lims_backup:
        return None
    
    lims_file = file.lims_backup
    all_hashes = file_factory_from_ecephys_session(lims_file,return_as_dict=True)
    if not all_hashes:
        return None
    DVFiles = []
    for lims_file,hashes in all_hashes.items():
        if lims_file == file.lims_backup.as_posix() or lims_file == file.lims_backup.as_posix()[1:]:
            for hasher_key,hash_hexdigest in hashes.items():
                DVFiles.append(available_DVFiles[hasher_key](path=lims_file,checksum=hash_hexdigest))    
    if len(DVFiles) == 1:
        return DVFiles[0]
    elif len(DVFiles) > 1:
        # multiple checksums found - return first (which should be sha3_256 and is more common)
        return DVFiles[0]
    return None

def delete_file_if_lims_hash_matches(
    file: Union[str, pathlib.Path],
    lims_file: Union[str, pathlib.Path] = None,
) -> int:
    """Compare the hash of a file to the hash of the same file in LIMS. If they match,
    delete the file and return the file size in bytes."""
    file = pathlib.Path(file)
    if not file.is_file():
        print(f"{file.as_posix()} is not a file or does not exist")
        return 0
    if not lims_file:
        lims_file = dv.SessionFile(file).lims_backup
    if not lims_file:
        print(
            f"No lims file specified, and could not find match in lims for {file.as_posix()}"
        )
        return 0
    if not pathlib.Path(lims_file).is_file():
        print(f"lims file {lims_file} does not exist")
        return 0

    file_hash = None  # initialize so we can check its existence, and only generate once
    previous_file_hasher_key = None

    lims_hash_new = None  # do the same for a possible re-generated hash of lims file
    previous_lims_rehasher_key = None
    #! never compare file_hash to lims_hash_new: None == None is True

    for lims_dir in pathlib.Path(lims_file).parents:
        if lims_dir.parts[-1].startswith("ecephys_session_"):
            break
    else:
        print("no ecephys_session_ dir found in parents of ", lims_file)
        return 0

    for upload_input_json in itertools.chain(
        lims_dir.rglob("*_UPLOAD_QUEUE_*_input.json"),
        lims_dir.rglob("*EUR_QUEUE_*_input.json"),
    ):

        # get hash function that was used by lims
        hasher_key = hash_type_from_ecephys_upload_input_json(upload_input_json)

        # get corresponding output json
        matching_output_jsons = upload_input_json.parent.rglob(
            "*" + upload_input_json.name.replace("input", "output")
        )
        upload_output_json = [f for f in matching_output_jsons]

        if not upload_output_json:
            log.info(f"No matching output json found for {upload_input_json}")
            continue

        if len(upload_output_json) > 1:
            log.info(
                f"Multiple output json files found for {upload_input_json}: {upload_output_json}"
            )
            continue

        upload_output_json = upload_output_json[0]

        # get hashes from output json
        hashes = hashes_from_ecephys_upload_output_json(upload_output_json, hasher_key)

        # get hash for our lims file
        # filepaths saved by lims in upload_output_json are posix with single leading fwd-slash
        lims_str = lims_file.as_posix()
        if lims_str[0:2] == "//":
            lims_str = lims_str[1:]

        lims_hash = hashes.get(lims_str, None)

        if not lims_hash:
            continue

        # now hash the file in question (just the first time through the loop)
        if file_hash is None or hasher_key != previous_file_hasher_key:
            file_hash = hash_file(file, lims_available_hashers[hasher_key])
            previous_file_hasher_key = (
                hasher_key  # store the hash function type for comparison
            )

        # compare with lims hash record
        if file_hash != lims_hash:

            if file_hash in hashes.values():
                # not an exact match, but a match with another file in the same session
                matches = [h for h in hashes.keys() if hashes[h] == file_hash]
                if len(matches) == 1:
                    log.info(
                        f"Hash for {file.as_posix()} doesn't match {lims_str}, but does match {matches[0]} - nothing deleted"
                    )
                else:
                    log.info(
                        f"Hash for {file.as_posix()} doesn't match {lims_str}, but matches multiple other files in the same ecephys session folder on lims"
                    )
            else:
                log.info(
                    f"Hash for {file.as_posix()} does not match lims hash at upload time - file data may have been changed, or has been re-uploaded since"
                )

            continue  # other more recent uploads may have happened with matching hashes, continue searching

        # if a file has been on lims for a while and we're concerned about its integrity, we
        # should re-generate hashes instead of relying on the recorded hash at upload time

        # - smaller than 1 MB we may as well re-hash since it takes so little time
        below_size_threshold = lims_file.stat().st_size < 1024**2  # 1 MB

        # - older than some arbitrary age threshold, we will also re-hash
        # TODO revise or remove re-hash thresholds when we have more data to reinforce/allay concerns
        age_threshold = 180  # days
        over_age_threshold = (
            datetime.datetime.now()
            - datetime.datetime.fromtimestamp(lims_file.stat().st_ctime)
        ).days > age_threshold

        rehash_lims = below_size_threshold and over_age_threshold

        # if we've already rehashed lims file we can run this section again for free regardless
        if lims_hash_new is not None and hasher_key == previous_lims_rehasher_key:
            rehash_lims = True

        if rehash_lims:

            if lims_hash_new is None or hasher_key != previous_lims_rehasher_key:
                lims_hash_new = hash_file(lims_file, lims_available_hashers[hasher_key])
                previous_lims_rehasher_key = (
                    hasher_key  # store the hash function type for comparison
                )

            if lims_hash_new != lims_hash:

                if file_hash == lims_hash_new:
                    log.info(
                        f"Hash for {lims_file.as_posix()} has changed since {upload_output_json}, but it matches current hash for {file.as_posix()}"
                    )
                    lims_hash = lims_hash_new
                    log.info(
                        f"{lims_file.as_posix()}|{previous_lims_rehasher_key}|{lims_hash_new}"
                    )
                    # will carry on and delete the file
                else:
                    log.critical(
                        f"A fresh hash for {lims_file.as_posix()} does not match hash in {upload_output_json}. If no re-uploads are found this is suggestive of data corruption in lims"
                    )
                    continue
            else:
                log.info(
                    f"A fresh hash for {lims_file.as_posix()} matches hash at upload time, indicating good data integrity in lims"
                )
                lims_hash = lims_hash_new
                # new hash == old hash on lims so carry on..

        # compare with lims hash from record or newly-generated
        if (
            file_hash == lims_hash
        ):  # don't compare file_hash with lims_hash_new - they were both initialized with None!
            # an exact match
            log.info(f"{file.as_posix()}|{previous_file_hasher_key}|{file_hash}")
            log.info(
                f"Hashes match for {file.as_posix()}. Deleting {file.stat().st_size/1024**3:.1f} Gb."
            )
            file_size = file.stat().st_size
            # delete the file
            file.unlink()
            return file_size

    else:  # upload_intput_json files exhausted
        # if we obtained any new hashes, record them
        if file_hash is not None:
            log.info(f"{file.as_posix()}|{previous_file_hasher_key}|{file_hash}")
        if lims_hash_new is not None:
            log.info(
                f"{lims_file.as_posix()}|{previous_lims_rehasher_key}|{lims_hash_new}"
            )
        log.info(f"No matching hashes found for {file.as_posix()}")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        add_help=True, description="Delete file if a valid copy exists in LIMS"
    )
    parser.add_argument(
        "filepath",
        type=str,
        help="path to an ecephys session file that will be checked against lims copy (if it exists), then deleted if the lims copy checksum matches",
    )
    args = parser.parse_args()
    if not args or not args.filepath:
        print("Filepath to an ecephys session file must be provided")
        sys.exit()
    print("Searching for matching file in LIMS and generating checksum...")
    # deleted = delete_file_if_lims_hash_matches(args.filepath)
    # if deleted == 0:
    #     print("No files deleted")
    # else:
    #     print(f"1 file deleted: {deleted/1024**3:.1f} Gb recovered")

    import pprint
    pprint.pprint(get_file_with_hash_from_lims(args.filepath))
    pprint.pprint(upload_jsons_from_ecephys_session_or_file(args.filepath))
