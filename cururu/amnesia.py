from cururu.persistence import Persistence
from pjdata.types import Data


class Amnesia(Persistence):
    def store(self, data: Data, check_dup: bool = True):
        pass

    def fetch_matrix(self, id):
        pass

    def unlock(self, data, training_data_uuid=None):
        pass

    def list_by_name(self, substring, only_historyless=True):
        return []

    def _fetch_impl(self, data: Data, lock: bool = False) -> Data:
        pass
