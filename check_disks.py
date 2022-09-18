import shutil
import nptk

"""
A quick overview of free disk space in critical locations on the pipeline rig computers.
TODO rewrite using urllib instead of requests + config.py
"""

ALL_COMPS: dict[str,str] = nptk.ConfigHTTP.get_np_computers([0,1])
DIVIDER_LENGTH = 50

def comp_from_hostname(hostname: str) -> str:
    for comp, host in ALL_COMPS.items():
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
        
    @property
    def usage(self):
        try:
            return shutil.disk_usage(f"//{self.hostname}/{self.letter}$")
        except PermissionError:
            return None
    
    def __print__(self):
        length = DIVIDER_LENGTH //3
        indent = " "*5
        used = '#'
        free = '-'
        usage = self.usage
        if not usage:
            return '- not available -'
        print(f"{indent}{self.letter}:")
        fraction = usage.used / usage.total
        print(f"{indent}[{used*round(fraction*length)}{free*round((1-fraction)*length)}] {usage.used/1e9:.1f} / {usage.total/1e9:.1f} GB")


if __name__ == "__main__":
    
    first_comp = "Acq"
    
    for comp,hostname in ALL_COMPS.items():
        
        if first_comp in comp:
            print(f"\n{comp.split('-')[0]}\n{'='*DIVIDER_LENGTH}")
            
        print(f"{comp.split('-')[1]}")
        
        if 'acq' in comp.lower():
            Drive("A", hostname).__print__()
            Drive("B", hostname).__print__()
        
        Drive("C", hostname).__print__()