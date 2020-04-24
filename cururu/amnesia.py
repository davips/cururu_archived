from cururu.persistence import Persistence


class Amnesia(Persistence):
    def list_by_name(self, substring, only_historyless=True):
        return []

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        pass

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        return None
