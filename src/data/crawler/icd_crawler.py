from typing import get_args, Literal, Union, List
from types import SimpleNamespace

from tqdm import tqdm

import requests
import os

import pandas as pd

import argparse

from datasets import Dataset

import time

from dotenv import load_dotenv
load_dotenv()

class ICDWalker:
    # DB base path
    API_BASE_PATH = "https://id.who.int/"

    # Args options
    ICD_VERSIONS = Literal[10, 11]
    LINEARIZATIONS = Literal['mms']
    API_VERSIONS = Literal['v1', 'v2']

    def __init__(self, release='latest', icd_version: ICD_VERSIONS = 11, linearization: LINEARIZATIONS = 'mms', lang = 'en', api_version: API_VERSIONS ='v2'):
        self.token = self.setup_api()

        if release == 'latest':
            self.root_uri = self.get_latest_release(icd_version, linearization)
        else:
            self.root_uri = f"{self.API_BASE_PATH}/icd/release/{icd_version}/{release}/{linearization}"

        self.lang = lang
        self.api_version = api_version

        # Create data dicts
        self.category = {l: [] for l in self.available_languages}
        self.chapter = {l: [] for l in self.available_languages}
        self.postcoordination = {l: [] for l in self.available_languages}

    # ------------------------------- Query Helpers ------------------------------ #

    @staticmethod
    def setup_api(client_id=None, client_secret=None):
        # Setting
        token_endpoint = 'https://icdaccessmanagement.who.int/connect/token'
        client_id = client_id if client_id else os.getenv('ICD_CLIENT_ID')
        client_secret = client_secret if client_secret else os.getenv('ICD_CLIENT_SECRET')
        scope = 'icdapi_access'
        grant_type = 'client_credentials'

        # set data to post
        payload = {'client_id': client_id, 
                'client_secret': client_secret, 
                'scope': scope, 
                'grant_type': grant_type}
                
        # make request
        r = requests.post(token_endpoint, data=payload, verify=True).json()
        return r['access_token']

    @staticmethod
    def query_icd(uri, lang='en', version='v2', token=None):
        if token is None:
            token = ICDWalker.setup_api()

        # HTTP header fields to set
        headers = {'Authorization':  'Bearer '+token, 
                'Accept': 'application/json', 
                'Accept-Language': lang,
            'API-Version': version}

        # Make sure we run https requests
        sanitized_uri = uri.replace('http', 'https') if not 'https' in uri else uri

        response = requests.get(sanitized_uri, headers=headers, verify=True)  # Set verify=True for SSL verification
        response.raise_for_status()  # Raise an error for bad responses

        return response.json()
    
    @staticmethod
    def get_latest_release(icd_version, linearization=None):
        assert not (icd_version == 11 and not linearization)
        linearization = linearization if icd_version == 11 else ''
        return ICDWalker.query_icd(f"https://id.who.int/icd/release/{icd_version}/{linearization}")['latestRelease']
    
    @staticmethod
    def get_available_languages(uri):
        return ICDWalker.query_icd(uri)['availableLanguages']

    # -------------------------------- Properties -------------------------------- #
    
    def set_lang(self, lang):
        self.lang = lang
    
    def set_api_version(self, version):
        self.api_version = version

    @property
    def available_languages(self):
        return self.get_available_languages(self.root_uri)

    def get_dataframes(self, lang):
        """Convert the collected data into a pandas DataFrame."""
        return {
            'category': pd.DataFrame(self.category[lang]), 
            'chapter': pd.DataFrame(self.chapter[lang]), 
            'postcoordination': pd.DataFrame(self.postcoordination[lang])
        }

    # ---------------------------------------------------------------------------- #

    def _pause_crawl(self):
        time.sleep(60)
        self.walk_start_time = time.time()
        self.token = self.setup_api()

    def _extract_ids(self, data, key):
        return [os.path.basename(e) for e in data[key]] if key in data else None

    def _get_from_data(self, data, key, type='unique'):
        if type == 'unique':
            return data.get(key, {}).get('@value')
        elif type == 'list':
            return [d['label']['@value'] for d in data[key]] if key in data else None
        else:
            raise ValueError(f'Unrecognized type {type}.')

    def _get_foundation_data(self, uri):
        foundation_data = self.query_icd(uri, self.lang, self.api_version, self.token) if uri else {}
        return {
            'fully_specified_name': self._get_from_data(foundation_data, 'fullySpecifiedName'),
            'synonym': self._get_from_data(foundation_data, 'synonym', type='list'),
            'inclusion': self._get_from_data(foundation_data, 'inclusion', type='list'),
            'exclusion': self._get_from_data(foundation_data, 'exclusion', type='list'),
            'related_in_perinatal': foundation_data.get('relatedEntitiesInPerinatalChapter'),
            'f_child': foundation_data.get('child')
        }
    
    # def _get_codes(self, uris: list):
    #     codes = []
    #     for u in uris:
    #         data = self.query_icd(u, self.lang, self.api_version, self.token)
    #         if 'child' in data:
    #             codes.extend(self._get_codes(data['child']))
    #         if data.get('classKind') == 'category':
    #             codes.append(data.get('code'))
    #     return codes
    
    # TODO: do we retreive all? (tree paths)
    def _get_postcoordination(self, data: dict):
        return {
            'code': data.get('code'),
            **{os.path.basename(s['@id']): s.get('scaleEntity') for s in data.get('postcoordinationScale', {})}
        }
    
    def _get_chapter_data(self, data: dict) -> dict:
        return {
            'code': data.get('code'),
            'title': self._get_from_data(data, 'title'),
            'definition': self._get_from_data(data, 'definition'),
            'long_definition': self._get_from_data(data, 'longDefinition'),
            'parent': data.get('parent'),
            'index_terms': self._get_from_data(data, 'indexTerm', type='list'),
            'foundation_child_elsewhere': self._get_from_data(data, 'foundationChildElsewhere', type='list'),
            **self._get_foundation_data(data.get('source')),
            'f_uri': data.get('source'),
            'uri': data['@id'],
        }

    def _get_category_data(self, data: dict, chapter_data: dict) -> dict:
        return {
            'code': data.get('code'),
            'chapter': chapter_data['code'],
            'title': self._get_from_data(data, 'title'),
            'definition': self._get_from_data(data, 'definition'),
            'long_definition': self._get_from_data(data, 'longDefinition'),
            'parent': data.get('parent'),
            'index_terms': self._get_from_data(data, 'indexTerm', type='list'),
            'foundation_child_elsewhere': self._get_from_data(data, 'foundationChildElsewhere', type='list'),
            **self._get_foundation_data(data.get('source')),
            'f_uri': data.get('source'),
            'uri': data['@id'],
        }

    def _walk(self, uri, chapter_data=None, show_progress_bars=True, verbose=False, level=0):
        # Restart every ~3mins
        if time.time() - self.walk_start_time >= 180:
            self._pause_crawl()

        data = self.query_icd(uri, self.lang, self.api_version, self.token)
        indent = '  ' * level  # Create an indent based on the depth
        
        if data.get('classKind') == 'chapter':
            chapter_data = self._get_chapter_data(data)
            self.chapter[self.lang].append(chapter_data)

        elif data.get('classKind') == 'category':
            # Gather category's data
            category_data = self._get_category_data(data, chapter_data)
            self.category[self.lang].append(category_data)

            # Gather category's postcoordination
            postcoordination = self._get_postcoordination(data)
            self.postcoordination[self.lang].append(postcoordination)

            if verbose:
                self.print_data(data, chapter_data, indent)
        
        if verbose and 'title' in data:
            print(f"{indent}{data['title']['@value']}")  # Print the title entry if verbose
                
        # Assuming 'children' is a key that contains the URIs of child resources
        if 'child' in data:
            for child_uri in tqdm(data['child'], desc='Processing chapters...') if level == 0 and show_progress_bars else data['child']:
                self._walk(child_uri, chapter_data, show_progress_bars, verbose, level + 1)  # Increase level for indentation
                # TODO: remove, for debug purposes
                # if level != 0:
                #     break

    def walk(self, uri=None, show_progress_bars=True, verbose=False):
        if uri is None:
            uri = self.root_uri

        # Walk
        self.walk_start_time = time.time()
        self._walk(uri, show_progress_bars=show_progress_bars, verbose=verbose)

        return self.get_dataframes(self.lang)


    def print_data(self, data, chapter_data, indent=''):
        print(f"{indent} > Keys: {list(data.keys())}")
        print(f"{indent} - Code: {data.get('code')}")
        print(f"{indent} - Class Kind: {data.get('classKind')}")
        print(f"{indent} - Chapter: {chapter_data['title']}")
        print(f"{indent} - Chapter Code: {chapter_data['code']}")
        #print(f"{indent} - Parent: {data['parent']}")
        #print(f"{indent} - Source: {self.query_icd(data['source'], self.lang, self.api_version, self.token)}")
        print(f"{indent} - Description: {data.get('definition', {}).get('@value')}")
        print(f"{indent} - Index Terms: {[i['label']['@value'] for i in data.get('indexTerm', [])]}")
        print(f"{indent} - Inclusions: {[i['label']['@value'] for i in data.get('inclusion', [])]}")

def _args():
    parser = argparse.ArgumentParser(description="Crawl ICD to extract codes and their descriptions.")
    parser.add_argument('--lang', type=str, default='all')
    parser.add_argument('--release', type=str, default='latest')
    parser.add_argument('--linearization', type=str, choices=get_args(ICDWalker.LINEARIZATIONS), default='mms')
    parser.add_argument('--icd-version', type=int, choices=get_args(ICDWalker.ICD_VERSIONS), default=11)
    parser.add_argument('--api-version', type=str, choices=get_args(ICDWalker.API_VERSIONS), default='v2')

    parser.add_argument('--output-dir', type=str, default=None, help="In case you want to save locally.")
    parser.add_argument('--hf-repo', type=str, default=None, help="In case you want to push to HF hub.")

    return parser.parse_args()

if __name__ == "__main__":
    # Get arguments
    args = _args()

    # Instantiate the walker
    walker = ICDWalker(
        release=args.release, 
        icd_version=args.icd_version, 
        linearization=args.linearization,
        api_version=args.api_version
    )

    langs = walker.available_languages if args.lang == 'all' else args.lang.split(',')
    print(f'Crawled languages: {langs}')
    
    script_start = time.time()

    for l in langs:
        print(f"{'-'*30} Processing {l} {'-'*30}")
        walker.set_lang(l)

        # Walk the way
        dataframes_l = walker.walk()

        # Process each resulting df
        for cat, dfl in dataframes_l.items():
            dsl = Dataset.from_pandas(dfl)

            if args.output_dir:
                dsl.save_to_disk(os.path.join(args.output_dir, cat, l))
            if args.hf_repo:
                # TODO: What structure do we have: repo-dataset -> lang or repo -> lang-dataset?
                dsl.push_to_hub(args.hf_repo, f'{cat}-{l}', private=True, token=os.getenv('HF_TOKEN'))
            
            print(f'{cat}: {dsl}')
        print(f'{l} done!')

    script_end = time.time()

    print('All languages processed!')
    print(f'Time: {(script_end - script_start)/60} minutes')
