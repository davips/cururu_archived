from cururu.persistence import Persistence


class Amnesia(Persistence):
    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        pass

    def fetch_matrix(self, name):
        pass

    def unlock(self, hollow_data, training_data_uuid=None):
        pass

    def list_by_name(self, substring, only_historyless=True):
        return []

    def fetch(self, hollow_data, fields, training_data_uuid='',
                    lock=False):
        pass
