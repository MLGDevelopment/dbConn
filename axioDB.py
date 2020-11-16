from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Table, Float, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


Base = declarative_base()


class ORM:

    @staticmethod
    def rows2dict(rows=[]):
        if not rows:
            return 0
        dicts = []
        for row in rows:
            d = {}
            for column in row.__table__.columns:
                d[column.name] = str(getattr(row, column.name))
            dicts.append(d)
        return dicts


class AxioProperty(Base, ORM):

    __tablename__ = "axio_properties"
    property_id = Column(
        String,
        primary_key=True
    )
    property_name = Column(String)
    property_address = Column(String)
    property_street = Column(String)
    property_city = Column(String)
    property_state = Column(String)
    property_zip = Column(String)
    property_website = Column(String)
    property_owner = Column(String)
    property_management = Column(String)
    property_asset_grade_market = Column(String)
    property_asset_grade_submarket = Column(String)
    year_built = Column(String)
    total_units = Column(Integer)
    total_square_feet = Column(Integer)
    status = Column(String)
    msa = Column(String)
    submarket_name = Column(String)
    survey_date = Column(String)
    last_sale_date = Column(Date)
    last_sale_price = Column(String)
    parcel_number = Column(String)
    levels = Column(Integer)
    latitude = Column(Numeric)
    longitude = Column(Numeric)

    rent_comp_data = relationship("RentComp")
    apo = relationship("AxioPropertyOccupancy", cascade="all,delete")

    @staticmethod
    def property_exists(axio_id):
        try:
            session.query(AxioProperty).filter(AxioProperty.property_id == axio_id).one()
            return 1
        except NoResultFound:
            return 0

    @staticmethod
    def fetch_property(axio_id):
        # TODO: ERROR HANDLING
        return session.query(AxioProperty).filter(AxioProperty.property_id == axio_id).one()

    @staticmethod
    def fetch_all_property_ids():
        return [i.property_id for i in session.query(AxioProperty).all()]
    
    @staticmethod
    def fetch_all_property_data():
        return session.query(AxioProperty).all()

    @staticmethod
    def fetch_all_property_data_by_state(state):
        return session.query(AxioProperty).filter(AxioProperty.property_state == state).all()


class RentComp(Base, ORM):

    __tablename__ = 'rent_comps'

    property_id = Column(
        String,
        ForeignKey('axio_properties.property_id'),
        primary_key=True
    )

    date_added = Column(
        Date,
        primary_key=True
    )

    unit_mix_id = Column(
        Integer,
        primary_key=True
    )

    type = Column(
        String,
        primary_key=True
    )

    area = Column(
        Integer,
        primary_key=True
    )

    quantity = Column(
        Integer,
        primary_key=True
    )

    avg_market_rent = Column(Integer)

    avg_effective_rent = Column(Integer)

    @staticmethod
    def fetch_rent_comp_as_of(axio_id, as_of_date):
        return session.query(RentComp).filter(
            RentComp.property_id.like(axio_id)).filter(RentComp.date_added == as_of_date).all()

    @staticmethod
    def fetch_all_rent_data():
        return session.query(RentComp).all()

    @staticmethod
    def fetch_unit_mix_for_ids(ids):
        # todo max date
        if not isinstance(ids, list):
            ids = [ids]
        return session.query(RentComp).filter(RentComp.property_id.in_(ids)).all()


class AxioPropertyOccupancy(Base, ORM):

    __tablename__ = 'axio_property_occupancy'

    property_id = Column(
        String,
        ForeignKey('axio_properties.property_id'),
        primary_key=True,
    )

    date = Column(
        Date,
        primary_key=True,
    )

    occupancy = Column(Numeric, nullable=False)

    @staticmethod
    def get_occupancy_as_of_date(axio_id, as_of_date):
        return session.query(AxioPropertyOccupancy).get((axio_id, as_of_date))

    @staticmethod
    def fetch_all_occ_data():
        return session.query(AxioPropertyOccupancy).all()

    @staticmethod
    def fetch_occupancies_for_ids(ids):
        if not isinstance(ids, list):
            ids = [ids]
        return session.query(AxioPropertyOccupancy).filter(AxioPropertyOccupancy.property_id.in_(ids)).all()



engine = create_engine('postgres://postgres:admin@localhost:5432/Acquisitions')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

if __name__ == "__main__":
    # r_c = RentComp(property_id=10001, type="2B/2B", area=1000, quantity=50, avg_market_rent=1200, avg_effective_rent=1000)
    # session.add(r_c)
    # session.commit()
    # r = AxioProperty()
    # ids = r.fetch_all_property_ids()
    pass
