import os
import platform
import re
from enum import Enum
from typing import List, Union

import requests

# get AIBS IDs, if set
COMP_ID: str = os.environ.get("AIBS_COMP_ID", os.environ["COMPUTERNAME"]).upper()
RIG_ID: str = os.environ.get("AIBS_RIG_ID",None)

while not RIG_ID:
    
    # extract RIG_ID from COMP_ID if possible
    if "NP." in COMP_ID:
        str_match = re.search(R"NP.[\d]+", COMP_ID)
        if str_match:
            RIG_ID = str_match[0]
            break
    
    # use BTVTest.1 if allowed
    # set with environ var:
    USE_TEST_RIG = os.environ.get("USE_TEST_RIG", True)
    if USE_TEST_RIG:
        RIG_ID = "BTVTest.1"
        break
    
    RIG_ID = "none"
    print("Not running from an NP rig: connections to services won't be made\nTry setting env var USE_TEST_RIG=1")

print(f"Running from {COMP_ID}, connected to {RIG_ID}")



# class Rig(Enum):
#     SYNC = f"{RIG_ID}-Sync"
#     WSE = f"{RIG_ID}-Sync"
#     MON = f"{RIG_ID}-Mon"
#     VIDMON = f"{RIG_ID}-Mon"
#     STIM = f"{RIG_ID}-Stim"
#     CAMSTIM = f"{RIG_ID}-Stim"
#     ACQ = f"{RIG_ID}-Acq" # TODO add btvtest.1-Acq http://mpe-computers/
#     EPHYS = f"{RIG_ID}-Acq"
    
class Rig(Enum):
    
    wse = "Sync"
    sync = Sync = SYNC = "Sync"
    mvr = Mvr = MVR = "Mon"
    mon = Mon = MON = vidmon = Vidmon = VIDMON = "Mon"
    stim = Stim = STIM = "Stim"
    camstim = Camstim = CAMSTIM = "Stim"
    acq = Acq = ACQ = "Acq" # TODO add btvtest.1-Acq http://mpe-computers/
    ephys = Ephys = EPHYS = "Acq"
    oephys = Oephys = OEphys = OEPHYS = "Acq"
    
    def __new__(cls,suffix):
        RIG_ID = None           
        while not RIG_ID:
            
            # extract RIG_ID from COMP_ID if possible
            RIG_ID = cls.rig_str_to_int(COMP_ID)
            if RIG_ID:
                break
            
            # use BTVTest.1 if allowed
            # set with environ var:
            USE_TEST_RIG = os.environ.get("USE_TEST_RIG", True)
            if USE_TEST_RIG:
                RIG_ID = "BTVTest.1"
                break
            
            RIG_ID = "none"
        if not RIG_ID:
            print("Not running from an NP rig: connections to services won't be made\nTry setting env var USE_TEST_RIG=1")
        cls.ID = RIG_ID
        obj = object.__new__(cls)
        obj._value_ = cls.ID + "-" + suffix
        obj.AIBS_ID = f"{RIG_ID}-{suffix}"
        return obj
        # cls(self.name).value = cls.ID + suffix
        
        
    @property
    def host(self):
        if "BTVTest.1-Acq" == self.value: 
            # not in mpe-computers
            return ""
        
        return requests.get(f"http://mpe-computers/v2.0/aibs_comp_id/{self.value}").json()['hostname'].upper()                      


    @property
    def path(self):
        if "BTVTest.1-Acq" == self.value: 
            # not in mpe-computers
            return None

        if platform.system() == "Windows":
            return RF'//{self.host}'
        else:
            return RF'/{self.host}'

    def open(self):
        return os.startfile(self.path)
    
    
    @staticmethod
    def comp():
        """The comp where python is running, regardless of whether on a rig"""
        return COMP_ID
    
    
    @staticmethod
    def comps():
        """All computers on the current rig (if python is running on a rig comp)"""
        return {Rig(x).value: Rig(x).host for x in set(Rig.__members__.values())}
    
    
    @staticmethod
    def all_comps(rigs=[0,1,2]):
        """All computers on all np rigs, or those specified"""
        return ConfigHTTP.get_np_computers(rigs)
    
    @staticmethod
    def rig_from_path(path):
        for idx, rig in enumerate(["NP.0", "NP.1", "NP.2"]):
            for comp in Rig.all_comps(idx).values():
                if comp in path:
                    return rig
        return None
        
    @staticmethod
    def rig_str_to_int(rig:str) -> Union[int,None]:
        # extract RIG_ID from str if possible
        str_match = re.search(R"NP.[\d]+", rig)
        if str_match:
            return str_match[0]
        return None
        
    
class ConfigHTTP:
    
    server = "http://mpe-computers/v2.0"
    # all_pc = requests.get("http://mpe-computers/v2.0").json()

    @staticmethod
    def hostname(comp: str=''):
        if "BTVTest.1-Acq" in comp: # not in mpe-computers
            return None
        else:
            return requests.get(f"{ConfigHTTP.server}/aibs_comp_id/{comp}").json()['hostname'].upper()                      
    
    @staticmethod
    def get_np_computers(rigs: Union[List[int], int]=None, comp: Union[List[str], str]=None):
        if rigs is None:
            rigs = [0, 1, 2]

        if not isinstance(rigs, list):
            rigs = [int(rigs)]

        if comp is None:
            comp = ['sync', 'acq', 'mon', 'stim']

        if not isinstance(comp, list):
            comp = [str(comp)]

        comp = [c.lower() for c in comp]

        np_idx = ["NP." + str(idx) for idx in rigs]

        all_pc = requests.get(ConfigHTTP.server).json()
        a = {}
        for k, v in all_pc['comp_ids'].items():
            if any([sub in k for sub in np_idx]) and any([s in k.lower() for s in comp]):
                a[k] = v['hostname'].upper()
        return a
