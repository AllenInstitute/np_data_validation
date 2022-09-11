""" Load json file, pop duplicate entries and save them in a second file for comparison (all should exist in original file).

Definition of a duplicate:
    path = path

Deciding which to keep/discard:
    for each path entry, find matching entries
        for each matching entry, compare if checksums exist
            if not self.session_id or not self.path
                discard self
            if self.checksum and not other.checksum:
                discard other
            elif other.checksum and not self.checksum:
                discard self
                break
            elif self.checksum and other.checksum:
                if self.checksum == other.checksum:
                    if self.size == other.size:
                        discard other
                    else:
                        report
                else:
                    report
            elif not self.checksum and not other.checksum:
                if self.size == other.size or self.size and not other.size:
                    discard other
                elif self.size == other.size or other.size and not self.size:
                    discard self
                    break
                else:
                    report
    
    
"""
from ast import Continue
import json
import pathlib
import logging
import sys


logging.basicConfig(filename='remove_duplicates_in_mongo_db.log', level=logging.INFO)
json_backup = R"\\allen\programs\mindscope\workgroups\dynamicrouting\ben\data_validation\db\snapshots_2022-08-30_min_fields.json"
db = pathlib.Path(json_backup)

with open(db) as f:
    orig = json.load(f)

for item in orig:
    if item['path'] is not None \
    and item['path'][0] == '/' \
    and item['path'][1] != '/':
        item['path'] = '/' + item['path']

class Item():
    def __init__(self,item:dict):
        self.checksum = item['checksum']
        self.path = item['path']
        self.session_id = item['session_id']
        self.size = item['size']

new = []
rem = []
who = [Item(i) for i in orig]

def report(s,o):
    logging.info(f"SELF {s.path} | {s.checksum} | {s.size} | {s.session_id}")
    logging.info(f"OTHER {o.path} | {o.checksum} | {o.size} | {o.session_id}")
    # print(f"SELF {s.path} | {s.checksum} | {s.size} | {s.session_id}")
    # print(f"OTHER {o.path} | {o.checksum} | {o.size} | {o.session_id}")
    if True:
        discard(s)
        discard(o)
        if s not in who:
            who.append(s)
        if o not in who:
            who.append(o)
    
def discard(item):
    if item not in rem:
        rem.append(item)
    if item in orig:
        orig.remove(item)

def keep(item):
    if item not in new:
        new.append(item)
    if item in orig:
        orig.remove(item)

def get_paths(dict_list):
    return [item['path'] for item in dict_list]

all_paths = get_paths(orig)
    
for n,i in enumerate(orig):
    sys.stdout.write(f"{n:>06}/{len(orig)}\r")
    sys.stdout.flush()
    item = Item(i)
    if '1234568890_366122_19700101' in item.path:
        continue
    if item.path is None:
        continue
    if not item.session_id:
        continue
    if item.path not in [ii.path for ii in new]:
        new.append(item)
        continue
    idx = [n.path for n in new].index(item.path)
    if new[idx].size and not item.size:
        continue
    if new[idx].size and item.size and new[idx].size != item.size:
        print(f"Size mismatch: {new[idx].size} != {item.size}")
        continue
    new[idx].size = item.size
    if new[idx].checksum and not item.checksum:
        continue
    if new[idx].checksum and item.checksum and new[idx].checksum != item.checksum:
        print(f"checksum mismatch: {new[idx].checksum} != {item.checksum}")
        continue
    new[idx].checksum = item.checksum

d = {}
for item in new:
    if not item.checksum:
        item.checksum = {}
    item.checksum = {'crc32': item.checksum}

for item in new:
    if not item.checksum['crc32']:
        del new['checksum']
for item in new:
    if not item.size:
        del new['size']
    new['size'] = item['size'].values()[0]

# while orig:
#     sys.stdout.write(f"{len(orig):>06} items remaining\r")
#     sys.stdout.flush()
    
#     self_item = orig[-1]
#     self = Item(self_item)
    
#     if not self.session_id or not self.path:
#         discard(self_item)
#         continue
    
#     if all_paths.count(self.path) == 1:
#         keep(self_item)
#         continue
    
#     if '1234568890_366122_19700101' in self.path:
#         # checksum test filenames
#         discard(self_item)
#         continue
    
#     matches = [item for item in orig if item['path'] == self.path]   

#     for other_item in matches:
        
#         other = Item(other_item)
#         if self.checksum and not other.checksum:
#             discard(other_item)
#             continue
#         elif other.checksum and not self.checksum:
#             discard(self_item)
#             continue
#         elif self.checksum and other.checksum:
#             if self.checksum == other.checksum:
#                 if self.size == other.size:
#                     discard(other_item)
#                     continue
#                 else:
#                     report(self,other)
#                     continue
#             else:
#                 report(self,other)
#                 continue
#         elif not self.checksum and not other.checksum:
#             if self.size == other.size or self.size and not other.size:
#                 discard(other_item)
#             elif self.size == other.size or other.size and not self.size:
#                 discard(self_item)
#                 continue
#             else:
#                 report(self,other)
#                 continue
#     else:
#         if self_item not in new:
#             keep(self_item)
#         else:
#             discard(self_item)

# print('sdf')
d = [i.__dict__ for i in new]

# assert not set(get_paths(rem)).difference(get_paths(new)), "Some items were removed and now no longer in original list"
# assert len(get_paths(new))  == len(set(get_paths(new))), "Some duplicate items remain"

# with open('unique_snapshots.json','w') as f:
#     json.dump(rem,f,indent=4,sort_keys=True)
# with open('unique_snapshots.json','w') as f:
#     json.dump(who,f,indent=4,sort_keys=True)
with open('unique_snapshots.json','w') as f:
    json.dump(d,f,indent=4,sort_keys=True)
