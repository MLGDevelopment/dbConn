import datetime
import os
import sys
packages_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(packages_path)
from axioDB import *


def insert_axio_property():
    """
    test method to insert a record into table axio_properties
    :return:
    """
    axio_property = dict({
        "property_id": str(9999999),
        "property_address": "19000 Bluemound Rd, Brookfield, WI 32783",
        "property_street": "19000 Bluemound Rd",
        "property_city": "Brookfield",
        "property_state": "WI",
        "property_zip": "32783",
        "property_name": "MLG HQ",
        "property_owner": "MLG Companies",
        "property_management": "Point Real Estate",
        "year_built": 1990,
        "total_units": 100,
        "property_website": "mlgcapital.com",
        "property_asset_grade_market": "A+",
        "property_asset_grade_submarket": "A",
        "total_square_feet": 10000,
    })
    axp = AxioProperty(**axio_property)
    session.add(axp)
    session.commit()


def insert_rent_comp():
    """

    :return:
    """


def insert_property_occupancy():
    as_of_date = datetime.date.today()
    axio_occupancy = dict({
        "property_id": str(9999999),
        "date": as_of_date,
        "occupancy": 0.9
    })

    apo = AxioPropertyOccupancy(**axio_occupancy)
    session.add(apo)
    session.commit()


def main():
    """

    :return:
    """
    #insert_axio_property()
    #insert_rent_comp()
    #insert_property_occupancy()


if __name__ == "__main__":
    main()
