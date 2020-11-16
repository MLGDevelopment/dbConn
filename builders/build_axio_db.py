import os
import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderUnavailable
from sqlalchemy.exc import IntegrityError
from axioDB import *
curr_dir = os.path.dirname(os.path.realpath(__file__))
data_path = os.path.join(os.path.join(curr_dir, '..'), "data", "axioDB")



geolocator = Nominatim(user_agent="property-locator", timeout=10000)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

# TODO: add pre-check to improve performance


def get_lat_lon_from_address(address):
    """

    :return:
    """

    location = geolocator.geocode(address)
    if location:
        lat, long = location.latitude, location.longitude
    else:
        lat, long = None, None
    return lat, long


def axio_properties():
    csv_path = os.path.join(data_path, "axio_properties.csv")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict("records")
    count = 1
    for record in records:
        while count <= 3:
            try:
                lat, long = get_lat_lon_from_address(record['property_address'])
                break
            except GeocoderUnavailable:
                if count == 3:
                    lat, long = None
                time.sleep(2)
            count += 1

        record['latitude'], record['longitude'] = lat, long
        axp = AxioProperty(**record)
        try:
            session.add(axp)
            session.commit()
        except IntegrityError:
            session.rollback()
            session.flush()
        count = 1


def axio_property_occupancy():
    csv_path = os.path.join(data_path, "axio_property_occupancy.csv")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict("records")
    for record in records:
        axocc = AxioPropertyOccupancy(**record)
        try:
            session.add(axocc)
            session.commit()
        except IntegrityError:
            session.rollback()
            session.flush()


def rent_comps():
    csv_path = os.path.join(data_path, "rent_comps.csv")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict("records")
    for record in records:
        rc = RentComp(**record)
        try:
            session.add(rc)
            session.commit()
        except IntegrityError:
            session.rollback()
            session.flush()


if __name__ == '__main__':
    axio_properties()
    axio_property_occupancy()
    rent_comps()
