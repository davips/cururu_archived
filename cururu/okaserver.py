import json
from io import BytesIO

import requests

from cururu.persistence import Persistence
from pjdata.aux.compression import pack, unpack
from pjdata.types import Data


class OkaServer(Persistence):
    def __init__(self, token, storage_info=None, post=False, url="http://localhost:5000/api/cururu"):
        self.headers = {'Authorization': 'Bearer ' + token}
        self.storage_info = storage_info
        self.url = url
        self.post = post

    def store(self, data: Data, check_dup: bool = True):
        packed = pack(data)
        files = {
            'json': BytesIO(json.dumps({'create_post': self.post}).encode()),
            'file': BytesIO(packed)
        }
        r = requests.post(self.url, files=files, headers=self.headers)

    def _fetch_impl(self, data: Data, lock: bool = False) -> Data:
        response = requests.get(self.url + f"?uuid={data.id}", headers=self.headers)
        if b"errors" in response.content:
            raise Exception("Invalid token")
        return unpack(response.content)

    def fetch_matrix(self, id):
        pass

    def list_by_name(self, substring, only_historyless=True):
        pass

    def unlock(self, data, training_data_uuid=None):
        pass
