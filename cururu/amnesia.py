from cururu.persistence import Persistence


class Amnesia(Persistence):
    def list_by_name(self, substring, only_historyless=True):
        return []

    def _store_impl(self, data, fields, training_data_uuid, check_dup):
        return None

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        return None
