from abc import ABC, abstractmethod


class Persistence(ABC):
    """
    This class stores and recovers results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """

    @abstractmethod
    def store(self, data_in, transformation, fields, data_out, check_dup=True):
        """
        Parameters
        ----------
        data_in
            Data object before transformation.
        transformation
            Transformation object containing the transformer and the stage of
            transformation (apply/use).
        fields
            List of names of the matrices to store.
        data_out
            Data object to recover.
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
    def fetch(self, data, transformation, fields, lock=False):
        """Fetch data from DB.

        Parameters
        ----------
        data
            Data object before being transformed by a Pipeline
        fields
            List of names of the matrices to fetch.
        transformation
            Transformation object containing the transformer and the stage of
            transformation (apply/use).
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
        pass

    @abstractmethod
    def list_by_name(self, substring):
        """
        Convenience method to retrieve a list of currently stored Data
        objects by name.
        :param substring: part of the name to look for
        :return: list of empty Data objects, i.e. without matrices
        """
        pass


class LockedEntryException(Exception):
    """Another node is generating output data for this input data
    and transformation combination."""


class FailedEntryException(Exception):
    """This input data and transformation combination have already failed
    before."""


class DuplicateEntryException(Exception):
    """This input data and transformation combination have already been inserted
    before."""
