from sqlalchemy import Column, Integer, LargeBinary, CHAR, \
    VARCHAR
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from cururu.persistence import Persistence


class SQL(Persistence):
    engine = None

    def __init__(self, blocking=False):
        super().__init__(blocking=blocking)
        Base = declarative_base(cls=CururuBase)

        class Data(Base):
            fields = VARCHAR(255)  # Up to 94 fields; 23*(M,Md,Mt,M_)=92.
            matrices = VARCHAR(2048)  # Up to 102 matrices.
            history = VARCHAR(65535)  # Up to 3277 transformations.

        class Dump(Base):  # Up to 4GiB per dump.
            value = Column(LargeBinary(length=(2 ** 32) - 1))

        Base.metadata.create_all(self.engine)

    def _store_impl(self, data, fields, training_data_uuid, check_dup):
        pass

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        pass

    def list_by_name(self, substring, only_historyless=True):
        pass


class CururuBase(object):
    n = Column(Integer, primary_key=True)
    id = Column(CHAR(19))

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


"""
Ex.:
Cache(Pipeline(
    Cache(File(...)),
    PCA(...),
    KNN()
))


Data
____________________________________________________________________________
id       | fields | matrices                   | history
----------------------------------------------------------------------------
ûçÍjfYOm   X,Y      éýáñdcÛz,ÐÜJNWÛrx            nkDovDÂa
eorêøhrð   X,Y,Z    éýáñdcÛz,ÐÜJNWÛrx,OopþoCêE   ýnMoÉáâä,0coÐRzx7,É27ÐBÉÁD


Dump
_________________________
id       | value
-------------------------
éýáñdcÛz   <blob nparray>
ÐÜJNWÛrx   <blob nparray>
nkDovDÂa   <blob text>
OopþoCêE   <blob nparray>
ýnMoÉáâä   <blob text>
0coÐRzx7   <blob text>
É27ÐBÉÁD   <blob text>
"""
