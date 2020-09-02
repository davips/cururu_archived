from abc import ABC, abstractmethod

from pjdata.aux.uuid import UUID
from pjdata.content.specialdata import UUIDData
from pjdata.transformer.transformer import Transformer
from pjdata.types import Data


class Persistence(ABC):
    """
    This class stores and recovers results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """

    # @abstractmethod
    # def dump(self,):
    #     """Dump component"""

    @abstractmethod
    def store(self, data: Data, check_dup: bool = True):
        """
        Parameters
        ----------
        blocking
        training_data_uuid
        data
            Data object to store.
        fields
            List of names of the matrices to store (for performance reasons).
            When None, store them all.
        check_dup
            Whether to waste time checking duplicates

        Returns
        -------
        Data or None

        Exception
        ---------
        DuplicateEntryException
        """
        pass

    @abstractmethod
    def _fetch_impl(self, data: Data, lock: bool = False) -> Data:
        pass

    def fetch(self, data: Data, lock: bool = False) -> Data:
        """Fetch data from DB.

        Parameters
        ----------
        data
            Data object before being transformed by a pipeline.
        lock
            Whether to mark entry (input data and pipeline combination) as
            locked, when no data is found for the entry.

        Returns
        -------
        Data or None

        Exception
        ---------
        LockedEntryException, FailedEntryException
        """
        if not data.ishollow:
            raise Exception("Persistence expects a hollow Data object!")
        return self._fetch_impl(data, lock)

    @abstractmethod
    def fetch_matrix(self, id):
        pass

    @abstractmethod
    def list_by_name(self, substring, only_historyless=True):
        """Convenience method to retrieve a list of currently stored Data
        objects by name, ordered cronologically by insertion.

        They are PhantomData objects, i.e. empty ones.

        Parameters
        ----------
        substring
            part of the name to look for
        only_historyless
            When True, return only fresh datasets, i.e. Data objects never
            transformed before.

        Returns
        -------
        List of empty Data objects (PhantomData), i.e. without matrices.

        """
        pass

    @abstractmethod
    def unlock(self, data, training_data_uuid=None):
        pass

    def visual_history(self, id_, folder=None):
        """List with all steps/Data objects before the current one. The current avatar is also generated."""
        uuid = UUID()
        data = None
        lastuuid = UUID(id_)
        firstdata = self.fetch(UUIDData(lastuuid))
        # TODO: solve this check in pjdata
        if firstdata.history is None:
            firstdata.history = []
        history = (list(firstdata.history) == 0) or firstdata.historystr
        if folder:
            lastuuid.generate_avatar(f"{folder}/{f'{id_}.jpg'}")
        lst = []
        for transformer in history:
            if isinstance(transformer, Transformer):
                name = transformer.name
                transformeruuid = transformer.uuid
            else:
                name = transformer["name"]
                transformeruuid = transformer["uuid"]
            dic = {
                "label": uuid.id, "name": name, "help": str(transformer), "stored": data is not None
            }
            if folder:
                filename = f"{uuid}.jpg"
                dic["avatar"] = filename
                uuid.generate_avatar(f"{folder}/{filename}")
            lst.append(dic)
            uuid = uuid * transformeruuid
            data = self.fetch(UUIDData(uuid))

        return lst


class UnlockedEntryException(Exception):
    """No node locked entry for this input data and transformation
    combination."""


class LockedEntryException(Exception):
    """Another node is generating output data for this input data
    and transformation combination."""


class FailedEntryException(Exception):
    """This input data and transformation combination have already failed
    before."""


class DuplicateEntryException(Exception):
    """This input data and transformation combination have already been inserted
    before."""
