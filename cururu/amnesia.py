from cururu.persistence import Persistence


class Amnesia(Persistence):
    def list_by_name(self, substring, only_historyless=True):
        return []

    def store(self, data, fields, check_dup=True):
        return None

    def fetch(self, data, fields, transformation=None, lock=False):
        return None
