import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy.exc import IntegrityError
from axioDB import *
curr_dir = os.path.dirname(os.path.realpath(__file__))
data_path = os.path.join(os.path.join(curr_dir, '..'), "data", "axioDB")

# TODO: add pre-check to improve performance


def axio_properties():
    csv_path = os.path.join(data_path, "axio_properties.csv")
    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict("records")
    for record in records:
        axp = AxioProperty(**record)
        try:
            session.add(axp)
            session.commit()
        except IntegrityError:
            session.rollback()
            session.flush()


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
