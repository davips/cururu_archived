from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import partial

from cururu.worker import Worker


@dataclass
class Persistence(ABC):
    """
    This class stores and recovers results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """
    blocking: bool = False
    worker = Worker()  # Global state !

    @abstractmethod
    def _store_impl(self, data, fields, training_data_uuid, check_dup):
        pass

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
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
        if not self.blocking:
            f = partial(
                self._store_impl,
                data, fields, training_data_uuid, check_dup
            )
            self.worker.put(f)
        else:
            self._store_impl(data, fields, training_data_uuid, check_dup)

    @abstractmethod
    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        """Fetch data from DB.

        Parameters
        ----------
        hollow_data
            Data object before being transformed by a pipeline.
        fields
            List of names of the matrices to fetch (for performance reasons).
            When None, fetch them all.
        lock
            Whether to mark entry (input data and pipeline combination) as
            locked, when no data is found for the entry.

        Returns
        -------
        Data or None

        Exception
        ---------
        LockedEntryException, FailedEntryException
        :param training_data_uuid:
        """
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
