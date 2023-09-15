import os
import platform
import re
import socket
from enum import Enum
from typing import List, Union

import requests

# useful lims endpoints ----------------------------------------------------------------
lims_requests = {
    "donor_info": "http://lims2/donors/info/details.json?external_donor_name={}",
    "experiment_id": "http://lims2/specimens/info/details.%s?experiment_id={}",
    "isi_experiment_prod": "http://lims2/specimens/isi_experiment_details/{}.json",
    "isi_experiment_test": "http://testlimswebc7:4800/specimens/isi_experiment_details/{}.json",
    "metadata": "http://lims2/specimen_metadata.json?specimen_id={}",
    "projects_validation": "http://lims2/projects.json?code={}",
    "save_visual_behavior_details": "http://lims2/visual_behavior_details",
    "specimen_id": "http://lims2/specimens/info/details.json?id={}",
    "specimen_name": "http://lims2/specimens/info/details.json?specimen_name={}",
    "user_details": "http://lims2/users.json?login={}",
    "visual_behavior_details": "http://lims2/visual_behavior_details?donor_id={}",
    "well_name": "http://lims2/specimens/info/details.json?carousel_well_name={}",
}
# - ------------------------------------------------------------------------------------
#   #
server = "http://mpe-computers/v2.0"
ALL_RIGS = requests.get(server).json()
ALL_COMPS = requests.get(server+"/aibs_comp_id").json()

# get AIBS IDs, if set
COMP_ID: str = os.environ.get("AIBS_COMP_ID", socket.gethostname()).upper()
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
    
    wse = wse2 = "Sync"
    sync = Sync = SYNC = "Sync"
    mvr = Mvr = MVR = "Mon"
    mon = Mon = MON = vidmon = Vidmon = VIDMON = "Mon"
    cam3d = Cam3d = CAM3D = "Mon"
    camviewer = CamViewer = camViewer = CAMVIEWER = "Mon"
    mousedirector = Mousedirector = mouseDirector = MouseDirector = MOUSEDIRECTOR = "Mon"
    camstim = Camstim = CamStim = CAMSTIM = "Stim"
    stim = Stim = STIM = "Stim"
    acq = Acq = ACQ = "Acq" # TODO add btvtest.1-Acq http://mpe-computers/
    ephys = Ephys = EPhys = EPHYS = "Acq"
    oephys = Oephys = oEphys = OEphys = OEPHYS = "Acq"
    openephys = openEphys = OpenEphys = OPENEPHYS = "Acq"
    
    def __new__(cls,suffix):
        RIG_ID = None           
        while not RIG_ID:
            
            # extract RIG_ID from COMP_ID if possible
            RIG_ID = cls.rig_str_with_digit(COMP_ID)
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
        try:
            return ALL_COMPS.get[self.value]['hostname'].upper()                      
        except KeyError:
            return 

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
        str_match = re.search(R"(?<=NP\.)(\d)", rig.upper())
        if str_match:
            return str_match[0]
        return None
    @staticmethod
    def rig_str_with_digit(rig:str) -> Union[str,None]:
        # extract RIG_ID from str if possible
        str_match = re.search(R"NP.\d", rig.upper())
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
            return ALL_COMPS[comp]['hostname'].upper()                      
    
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

        a = {}
        for k, v in ALL_RIGS['comp_ids'].items():
            if any([sub in k for sub in np_idx]) and any([s in k.lower() for s in comp]):
                a[k] = v['hostname'].upper()
        return a
