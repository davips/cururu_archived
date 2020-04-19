from pjml.tool.abc.mixin.timers import Timers
from sqlalchemy import Column, Integer, LargeBinary, CHAR, \
    VARCHAR
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

from cururu.persistence import Persistence
from pjdata.specialdata import NoData


class SQLA(Persistence):
    engine = None

    def __init__(self):
        st = Timers._clock()
        if not database_exists(self.engine.url):
            print('Creating database', self.engine.url)
            create_database(self.engine.url)
        print(Timers._clock() - st)

        st = Timers._clock()
        Base.metadata.create_all(self.engine)
        print(Timers._clock() - st)
        print('engine started,,,,,,,,,,,,,,,,,,,,,,,')
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        # TODO: verify if pure mysql is faster, or just profile to see the
        # weigth of sqlalchemy_utils

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        # TODO: merge pjdata.Data with sql.Data to have a single class and
        #  avoid having to copy the properties.
        da = Data(
            id=data.uuid00.pretty,
            names=data.matrix_names_str,
            matrices=data.uuids_str,
            history=data.history_str
        )
        self.session.add(da)

        # TODO: handle fields properly
        for matrix_name in data.matrix_names:
            du = Dump(
                id=data.uuids[matrix_name].pretty,
                value=data.field_dump(matrix_name)
            )
            self.session.add(du)

        self.session.commit()

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        Data(id=hollow_data.uuid00)
        d = self.session.query(Data).filter_by(
            id=hollow_data.uuid00.pretty
        ).first()
        if d is None:
            return None
        NoData.updated(d.history_str)

    def list_by_name(self, substring, only_historyless=True):
        pass


class CururuBase(object):
    n = Column(Integer, primary_key=True)
    id = Column(CHAR(19))

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


Base = declarative_base(cls=CururuBase)


class Data(Base):
    names = Column(VARCHAR(255))  # Up to 94 matrix names; 23*(M,Md,Mt,M_)=92
    matrices = Column(VARCHAR(2048))  # Up to 102 matrices.
    history = Column(VARCHAR(65535))  # Up to 3277 transformations.


class Dump(Base):  # Up to 4GiB per dump.
    value = Column(LargeBinary(length=(2 ** 32) - 1))


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
