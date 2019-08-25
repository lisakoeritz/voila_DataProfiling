#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""utils.py: Zusätzliche Funktionen notwendig zur korrekten Implementation von Datenimport und Datenprofil"""

__author__ = "Lisa Koeritz"

import pandas as pd
import logging

country_list = pd.read_csv('http://www.foodsecurityportal.org/api/countries.csv', index_col=0)

def datetimeIndex_parsing(index):
    """ turn given index into DateTimeIndex
    Args:
        index: index to be formatted
    Returns:
        DateTimeIndex or unformatted index
    """
    for fmt in ('%Y', '%b %y', '%Y-%m', '%m/%d/%Y'):
        try:
            return pd.to_datetime(index, format=fmt)
        except ValueError:
            pass
    return index


### source: https://hackersandslackers.com/extract-data-from-complex-json-python/ ###
### aufgerufen am 06.07.2019 ###
def extract_json_values(obj, key):
    """dissolve deeply nested json in readable json values
    Args:
        obj: JSON object to dissolve
        key: id to dissolve for
    Returns:
        JSON object dissolved for given key
    """
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    return results

def list_of_numbers_in_ds(ds: pd.DataFrame) -> list:
    """ turn given DataFrame into list with only the numeric(float or int) values present
    Args:
        ds: DataFrame to transform
    Returns:
        list of only numeric values or empty list if empty
    """
    try:
        ds_list = ds.values.flatten()
        filtered_list = filter(lambda i: isinstance(i, (int, float)), ds_list)
    except TypeError:
        filtered_list = ""
        logging.info("leerer Datensatz vorhanden")
    return list(filtered_list)

def check_country_fsp(not_a_country: str) -> str:
    """ uses given country list from Food Security Portal and returns Domain-Type place belongs to if possible
    Args:
        not_a_country: name of a place that was not identified as a country
    Returns:
        domain given place belongs to if available
    """
    if not_a_country in country_list.index and country_list.loc[not_a_country, "Type"] != "Country":
        return country_list.loc[not_a_country, "Type"]

