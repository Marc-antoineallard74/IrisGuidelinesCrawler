# import zarr
# import threading
# import pandas as pd
# from tqdm.auto import tqdm
# from concurrent.futures import ThreadPoolExecutor
# from itertools import repeat
# from urllib.parse import quote, unquote
# from io import StringIO

# def parse_metadata(link_group):
#     """
#         Metadata parser, populate a metadata dict 
#     """
#     metadata = {}
#     try: metadata_str = pd.read_csv(StringIO(link_group['metadata'][:].tobytes().decode('utf-8')), na_filter=False)
#     except: return metadata
#     for _, row in metadata_str.iterrows():
#         key, value = row[metadata_str.columns[0]], row[metadata_str.columns[1]]
#         metadata[key] = metadata.get(key, []) + [value]
#     return metadata

# def process_link_group(link_group, link_pbar):
#     """
#         Single link group processer. Parse individually their metadata.
#     """
#     if not (metadata := parse_metadata(link_group)):
#         link_pbar.update(1)
#         return
#     idd = unquote(link_group.basename).split('/handle/')[1].split('?')[0]
#     with dc_lock:
#         dcs[idd] = {
#             'id': idd,
#             'language': metadata.get('dc.language.iso', ['']),
#             'file_link': metadata.get('dc.file-link', [''])
#         }
#     link_pbar.update(1)

# basemap = "https://iris.who.int/htmlmap"
# zstore = zarr.storage.DirectoryStore('who.iris')
# zroot = zarr.open_group(store=zstore, mode='r')
# dcs = {}
# dc_lock = threading.Lock()

# with tqdm(zroot.values(), desc=f"{f"Processing maps in {basemap.split('/')[-1]}":<39}", ncols=1000) as pbar:
#     for submap_group in pbar:
#         link_groups = list(submap_group.values())
#         with tqdm(total=len(link_groups), desc=f"{f"Processing links in {unquote(submap_group.basename).split('/')[-1]}":<40}", leave=False, ncols=1000) as link_pbar:
#             with ThreadPoolExecutor(max_workers=64) as executor:
#                 executor.map(process_link_group, link_groups, repeat(link_pbar))