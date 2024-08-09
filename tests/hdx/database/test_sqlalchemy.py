from datetime import datetime, timezone
from os import remove
from os.path import join
from shutil import copyfile

import pytest
from hdx.database.no_timezone import Base as NoTZBase
from sqlalchemy import NullPool, create_engine, select
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept

from .dbtestdate import DBTestDate


class TestDatabase:
    dbpath = join("tests", "test_database.db")
    testdb = join("tests", "fixtures", "test.db")

    @pytest.fixture(scope="function")
    def nodatabase(self):
        try:
            remove(self.dbpath)
        except OSError:
            pass
        return f"sqlite:///{self.dbpath}"

    @pytest.fixture(scope="function")
    def database_to_reflect(self):
        try:
            remove(self.dbpath)
            copyfile(self.testdb, self.dbpath)
        except OSError:
            pass
        return f"sqlite:///{self.dbpath}"

    def test_get_session(self, nodatabase):
        assert DBTestDate.__tablename__ == "db_test_date"

        dbengine = create_engine(nodatabase, poolclass=NullPool, echo=False)
        NoTZBase.metadata.create_all(dbengine)
        dbsession = Session(dbengine)
        assert str(dbsession.bind.engine.url) == nodatabase
        now = datetime(2022, 10, 20, 22, 35, 55, tzinfo=timezone.utc)
        input_dbtestdate = DBTestDate()
        input_dbtestdate.test_date = now
        dbsession.add(input_dbtestdate)
        dbsession.commit()
        dbtestdate = dbsession.execute(select(DBTestDate)).scalar_one()
        assert dbtestdate.test_date == now
        dbsession.close()
        dbengine.dispose()

    def test_get_reflect_session(self, database_to_reflect):
        dbengine = create_engine(
            database_to_reflect, poolclass=NullPool, echo=False
        )
        Base = automap_base(declarative_base=NoTZBase)
        Base.prepare(autoload_with=dbengine)
        assert str(dbengine.url) == database_to_reflect
        dbsession = Session(dbengine)
        assert isinstance(Base, DeclarativeAttributeIntercept)
        assert str(dbsession.bind.engine.url) == database_to_reflect
        Table1 = Base.classes.table1
        row = dbsession.execute(select(Table1)).scalar_one()
        assert row.id == "1"
        assert row.col1 == "wfrefds"
        # with reflection, type annotation maps do not work and hence
        # we don't have a timezone here
        assert row.date1 == datetime(1993, 9, 23, 14, 12, 56, 111000)
        dbsession.close()
        dbengine.dispose()
