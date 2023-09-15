import ipywidgets as ipw
import IPython
mouse_id = None

class MouseSelect(ipw.Text):
    def __init__(self, *args, **kwargs):
        
        super().__init__(
                         placeholder='Enter LabTracks mouse ID (e.g. 366122)',
                         continuous_update=False,
                         *args, **kwargs
                         )
        self.observe(self._on_change, names='value')
        self._on_change({'new': self.value})
        
    def _on_change(self, change):
        if change['new']:
            # IPython.display.clear_output()
            global mouse_id
            mouse_id = change['new']
            summary()
            print('\n', end='\r',flush=True)
            # super().__init__(value=change['new'])
        
import pathlib
import pprint
from data_validation import Session

INCOMING_ROOT = pathlib.Path("//allen/programs/braintv/production/incoming/neuralcoding")
def opt_root_from_mouse_id(mouse_id:str|int) -> pathlib.Path|None:
    openscope_root = pathlib.Path("//allen/programs/mindscope/workgroups/openscope")
    if (root := tuple(openscope_root.glob(f"*/AlignToPhysiology/{mouse_id}"))):
        return root[0]
    return None

def summary():
    mouse_dir = opt_root_from_mouse_id(mouse_id)
    if not mouse_dir:
        print(f"Mouse {mouse_id} not found in Openscope workgroup OPT folder")
        return
    else:
        src_paths = (
            list(mouse_dir.glob('images/*_probe*sortedccf_regions.csv'))
            + list(mouse_dir.glob('images/*_probe*sorted_ccf_regions.csv'))
        )
        if not src_paths:
            print(f"No csv files found for {mouse_id} in {mouse_dir}")
        
        elif len(src_paths) > 6:
            print(f"Too many csv files found for {mouse_id}: {[p.name for p in src_paths]}")
        else:
            print(f"Found {len(src_paths)} ccf_regions.csv files in OPT folder")
    session_id = src_paths[0].name.split('_')[0]
    
    try:
        session = Session(f"{session_id}_366122_12345678")
        lims_path = session.lims_path 
        # fill in dummy mouse_id/date - only need session id to be correct
        print(f"LIMS path found for session {session_id}")
    except:
        print(f"Could not find LIMS path for session {session_id}")
        return
    
    # lims_probe_folder = f'{session_id}_probe{probe_letter}'
    if lims_paths := list(lims_path.glob(f'*/*_probe*/continuous/Neuropix-PXI-100.0/ccf_regions.csv')):
        print(f"{mouse_id}: found {len(lims_paths)} CCF regions CSV files on lims for session {session_id}")       
        print(lims_paths[0].parent.parent.parent.parent)
        # IPython.display.HTML(
        #     f"<a href>{lims_paths[0].parent.parent.parent.parent.as_uri()} target='_blank'>Open LIMS folder</a>")
        # pprint.pprint(list(map(str,lims_paths)))
    else:
        print(f"No CCF regions files found on lims - upload needs to be made for {mouse_id} {session_id}.")
            
