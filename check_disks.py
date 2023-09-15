import shutil
from typing import NamedTuple, Tuple, Union

import nptk

"""
A quick overview of free disk space in critical locations on the pipeline rig computers.
TODO rewrite using urllib instead of requests + config.py
"""

RIG_NUMBERS = [0,1,2]
DIVIDER_LENGTH = 30
INDENT_LENGTH = " "*5

def np_comps() -> dict[str,str]:
    "{'NP.2-Acq': 'W10DT713844'}"
    return nptk.ConfigHTTP.get_np_computers(RIG_NUMBERS)

def comp_from_hostname(hostname: str) -> str:
    for comp, host in np_comps().items():
        if host == hostname:
            return comp
    return ""

def rig_from_comp(comp: str) -> str:
    return comp.split(".")[0]

class Drive:
    def __init__(self, letter:str, hostname:str):
        self.letter = letter
        self.hostname = hostname
        self.comp = comp_from_hostname(hostname)
        self.rig = rig_from_comp(self.comp)
        
        self.usage: Union[str,NamedTuple] = "N/A"
        "Disk usage stats, or a str explaining why they can't be accessed."
        try:
            self.usage = shutil.disk_usage(f"//{self.hostname}/{self.letter}$")
        except PermissionError:
            self.usage = "- access denied -"
        except FileNotFoundError:
            self.usage = "- drive not found -"
        
    def __repr__(self):
        return f"{self.__class__.__name__}({self.letter!r}, {self.hostname!r})"
    
    def __str__(self):
        if isinstance(self.usage,str):
            return f"{INDENT_LENGTH}{self.letter}: {self.usage}"
        if isinstance(self.usage,tuple):
            letter = f"{self.letter}: "
            fraction = self.usage.used / self.usage.total
            used = '#'
            free = '-'
            bar_length = DIVIDER_LENGTH // 3
            fill_bar = f"[{used*round(fraction*bar_length)}{free*round((1-fraction)*bar_length)}]"
            free_gb = f"{self.usage.used/1e9:.1f} / {self.usage.total/1e9:.1f} GB"
            return f"{INDENT_LENGTH}{letter} {fill_bar} {free_gb}"
                


if __name__ == "__main__":
    
    RIG_NUMBERS = [0,1]

    first_comp = "Acq"
    
    for name, host in np_comps().items():
        
        if first_comp in name:
            print(f"\n{name.split('-')[0]}\n{'='*DIVIDER_LENGTH}")
            
        print(f"{name.split('-')[1]}")
        
        if 'acq' in name.lower():
            print(Drive("A", host))
            print(Drive("B", host))
            print(Drive("C", host))
            print(Drive("D", host))
        else:
            print(Drive("C", host))