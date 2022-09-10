import argparse
import datetime
import hashlib
import json
import logging
import pathlib
import sys
from typing import List, Union

import data_validation as dv

log = logging.getLogger(__name__)

# from allensdk/brain_observatory/ecephys/copy_utility/_schemas.py:
available_hashers = {"sha3_256": hashlib.sha3_256, "sha256": hashlib.sha256}


def hash_type_from_ecephys_upload_input_json(path: Union[str, pathlib.Path]) -> str:
    """Read LIMS ECEPHYS_UPLOAD_QUEUE _input.json and return the hashlib class."""
    with open(path) as f:
        hasher_key = json.load(f).get("hasher_key", None)
    return hasher_key


def hashes_from_ecephys_upload_output_json(
    path: Union[str, pathlib.Path], hasher_key: str
) -> dict[str, str]:
    """Read LIMS ECEPHYS_UPLOAD_QUEUE _output.json and return a dict of {lims filepaths:hashes(hex)}."""
    # hash_cls is specified in output_json, not input json, so we'll need to open that
    # up and feed its value of hash_cls to this function
    # not calling 'hash_class_from_ecephys_upload_input_json' here because this
    # organization of files may change in future, and we need to pass the hash_cls to
    # other functions

    if not path and not hasher_key:
        raise ValueError("path and hashlib class must be provided")

    path = pathlib.Path(path)
    if not hasher_key in available_hashers.keys():
        raise ValueError(f"hash_cls must be one of {list(available_hashers.keys())}")

    if not path.exists():
        raise FileNotFoundError("path does not exist")

    if not path.suffix == ".json":
        raise ValueError("path must be a json file")

    with open(path) as f:
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


def delete_file_if_lims_hash_matches(
    file: Union[str, pathlib.Path], lims_file: Union[str, pathlib.Path] = None,
) -> int:
    """Compare the hash of a file to the hash of the same file in LIMS. If they match,
    delete the file and return the file size in bytes."""
    if not lims_file:
        lims_file = dv.SessionFile(file).lims_path
    if not lims_file:
        print(f"No lims file specified - could not find match in lims for {file}")
        return 0
    if not pathlib.Path(lims_file).is_file():
        print(f"lims file {lims_file} does not exist")
        return 0
    
    file = pathlib.Path(file)
    
    for lims_dir in pathlib.Path(lims_file).parents:
        if lims_dir.parts[-1].startswith("ecephys_session_"):
            break
    else:
        print("no ecephys_session_ dir found in parents of ", lims_file)
        return 0

    for upload_input_json in lims_dir.rglob("*ECEPHYS_SESSION_UPLOAD_QUEUE_*_input.json"):

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
        
        if lims_hash:
            break

    else:
        log.info(f"No matching lims file found for {file}")
        return 0

    # now hash the file in question
    file_hash = hash_file(file, available_hashers[hasher_key])
    
    # if a file has been on lims for a while and we're concerned about its integrity, we
    # should re-generate hashes instead of relying on the recorded hash at upload time
    # - smaller than 1 MB we may as well re-hash since it takes so little time
    below_size_threshold = lims_file.stat().st_size < 1024**2 # 1 MB
    # - older than some arbitrary age threshold, we will also re-hash
    # TODO revise or remove re-hash thresholds when we have more data to reinforce/allay concerns
    age_threshold = 90 # days
    over_age_threshold = (datetime.datetime.now() - datetime.datetime.fromtimestamp(lims_file.stat().st_ctime)).days > age_threshold
    rehash_lims = below_size_threshold and over_age_threshold
    if rehash_lims:
        lims_hash_new = hash_file(lims_file, available_hashers[hasher_key])
        if lims_hash_new != lims_hash:
            log.critical(f"fresh hash for {lims_file} does not match hash at upload time, suggesting file corruption in lims")
            return 0
        else:
            log.info(f"fresh hash for {lims_file} matches hash at upload time, indicating file integrity in lims")
            # new_hash == old_hash so continue..
        
    # compare with lims hash record
    if file_hash == lims_hash:
        # an exact match
        log.info(
            f"Hashes match for {file}. Deleting {file.stat().st_size/1024**3:.1f} Gb."
        )
        file_size = file.stat().st_size
        # delete the file
        file.unlink()
        return file_size
    
    elif file_hash in hashes.values():
        # not an exact match, but a match with another file in the same session
        matches = [h for h in hashes.keys() if hashes[h] == file_hash]
        if len(matches) == 1:
            log.info(f"Hash for {file} doesn't match {lims_str}, but does match {matches[0]} - nothing deleted")
            return 0
        else:
            log.info(f"Hash for {file} doesn't match {lims_str}, but matches multiple other files in the same ecephys session folder on lims")
            return 0
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=True,description="Delete file if a valid copy exists in LIMS")
    parser.add_argument("filepath", type=str, help="path to an ecephys session file that will be checked against lims copy (if it exists), then deleted if the lims copy checksum matches")
    args = parser.parse_args()
    if not args or not args.filepath:
        print("Filepath to an ecephys session file must be provided")
        sys.exit()
    print("Searching for matching file in LIMS and generating checksum...")
    deleted = delete_file_if_lims_hash_matches(args.filepath)
    if deleted == 0:
        print("No files deleted")
    else:
        print(f"1 file deleted: {deleted/1024**3:.1f} Gb recovered")