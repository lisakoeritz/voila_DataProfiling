#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""dataset_profile.py: Kalkulationen für das und Darstellung des Datenprofils auf Datensatzebene"""

__author__ = "Lisa Koeritz"

import logging
import pandas as pd
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_string_dtype
import numpy as np

from hdx.location.country import Country

import utils


TO_PERCENT = 100
KB = 1024.0

REGION_CODE = {
    2: "Africa",
    19: "Americas",
    142: "Asia",
    150: "Europe",
    9: "Oceania"
}

INTERVAL = {
    "AS-JAN": "Annual",
    "MS": "Monthly",
    "W-FRI": "Weekly"
}

MAX_UNITS = {
    'Kilacalories/capita/day': 6000,
    'People (thousands)': 8000000000,
    'Per 1,000 live births': 1000,
    'Percent': 100,
}

MIN_UNITS = {
    'Kilacalories/capita/day': 0,
    'Metric Tons': 0,
    'People (thousands)': 0,
    'Per 1,000 live births': 0,
    'Percent': 0,
    'Population per sq. km': 0,
    'U.S. Dollars/Kg': 0,
}


def describe_ds_as_dataframe(ds: pd.DataFrame, ds_md: dict) -> pd.DataFrame:
    """ describe the profile criteria for dataset
    Args:
        ds: the DataFrame to create Profile for
        ds_md: the Metadata dictionary of the DataFrame that is to be profiled
    Returns:
        A pandas DataFrame containing calculated description values.
    """
    no_country, iso = check_domain(ds)
    geo = check_regions(iso)

    data = [
        ["Domain", ds.columns.name],
        ["geographischer Geltungsbereich", geo],
        ["Zeitraum [von, bis]", get_time_range_ds(ds)],
        ["Zeitliche Granularität", check_interval(ds)],
        ["Anzahl der Zeilen", ds.index.size],
        ["Anzahl der Spalten", ds.columns.size],
        ["Datengröße in Kilobytes", get_memory_size(ds)],
        ["Distinkte Werte (Prozent)", get_unique_values_pct(ds)],
        ["Fehlende Werte (Prozent)", get_null_values_pct(ds)],
        ["Spalten ohne Werte (n)", ds.isna().all().sum()],
        ["Datentypen", get_data_types(ds).items()],
        ["Spalten mit exakt selben Werten", get_duplicated_columns(ds)],
        ["Überprüfung Aggregationsspalte", check_aggregation(ds, geo)],
        ["Open Data Schema", fivestar_opendata(ds_md)],
        ["Überprüfung des Wertebereichs", check_units(ds, ds_md)],
        ["Herausgeber-Kategorie", categorize_source(ds_md)],
        ["Domain-Check", no_country],
        #["Zeit seit Erstellung in Monaten", check_months_since_upload(ds_md)],
        ["Verzögerung Veröffentlichung in Monaten", check_delay_upload(ds, ds_md)]
    ]

    profile = pd.DataFrame(data=data, columns = ["Kriterien", "Ergebnis"])
    profile.set_index("Kriterien", inplace=True)

    return profile


def get_null_values_pct(ds: pd.DataFrame) -> float:
    """ checks the percentage of null values in dataset
    Args:
        ds: the DataFrame to calculate the operation on
    Returns:
       the percentage of null values
    """
    if not ds.empty:
        ds_list_filtered = utils.list_of_numbers_in_ds(ds)
        null_vals = np.isnan(ds_list_filtered).sum()
        null_vals_percent = null_vals / len(ds.values.flatten())
        if null_vals_percent:
            return null_vals_percent*TO_PERCENT
    else:
        logging.info("Fehlende Werte (Prozent) -> keine verarbeitbaren Daten (leerer Datensatz)")

def get_unique_values_pct(ds: pd.DataFrame) -> float:
    """ checks the percentage of unique values in dataset
    Args:
        ds: the DataFrame to calculate the operation on
    Returns:
       the percentage of unique values
    """
    # a list of all number values in dataframe
    ds_as_list_numeric = np.array(utils.list_of_numbers_in_ds(ds))
    #drop nan values from numpy array element
    ds_as_list_without_na = ds_as_list_numeric[~np.isnan(ds_as_list_numeric)]

    if len(ds_as_list_without_na) > 0:
        unique_vals = np.unique(ds_as_list_without_na)
        unique_vals_percent = (len(unique_vals) / len(ds_as_list_without_na))
        if unique_vals_percent:
            return unique_vals_percent*TO_PERCENT
    else:
        logging.info("Distinkte Werte (Prozent) -> leerer oder nicht-numerischer Datensatz")

def get_memory_size(ds: pd.DataFrame) -> float:
    """ retrieves memory size of dataframe and hence dataset
    Args:
        ds: the DataFrame to calculate the operation on
    Returns:
       the used memory size of the dataset
    """
    memory_size = ds.memory_usage(index=False).sum()
    if memory_size<KB:
        return memory_size
    else:
        return memory_size/KB

def get_duplicated_columns(ds: pd.DataFrame) -> list:
    """ checks whether there are columns of exact duplicates
        in dataframe and returns list of columns
    Args:
        ds: the DataFrame to calculate the operation on
    Returns:
        A list of duplicate columns
    """
    ds = ds.dropna(axis=1, how="all")
    if len(ds) > 0:
        duplicated_columns = ds.T.duplicated(keep=False)
        list_duplicate_columns = list(duplicated_columns[duplicated_columns].index)
        return list_duplicate_columns
    else:
        logging.info("Spalten mit exakt selben Werten -> keine überprüfbaren Daten in Datensatz")

def get_time_range_ds(ds: pd.DataFrame) -> list:
    """ check which timeframe (minimum - maximum) there is at least one value for in dataframe
    Args:
        ds: the DataFrame to calculate the operation on
    Returns:
        A list containing the minimum date and maximum date
    """
    ds_time_range = []
    if isinstance(ds.index, pd.core.indexes.datetimes.DatetimeIndex):
        min_time = ds.dropna(how='all').index.min().date()
        max_time = ds.dropna(how='all').index.max().date()
        ds_time_range = [min_time, max_time]
    else:
        logging.info("Zeitraum -> keine Zeitreihen in Datensatz")
    return ds_time_range

# problem: many nan types in series and is_numeric_type returns False
def get_data_types(ds: pd.DataFrame) -> dict:
    """ check which data types are in dataframe, options: numeric, string, constant values
    Args:
        ds: the DataFrame to calculate the operation on
    Returns:
        A dictionary with the amount of numeric, string and constant values
    """
    num = 0
    string = 0
    const = 0

    for column in ds.columns:
        if ds[column].nunique() == 0:
            const += 1
        elif is_numeric_dtype(ds[column]):
            num += 1
        elif is_string_dtype(ds[column]):
            string += 1

    dtypes = {
        "NUM": num,
        "STRING": string,
        "CONST": const
    }

    return dtypes


def categorize_source(ds_md: dict) -> str:
    """ categorize the given publishing entity of the original source
    Args:
        ds_md: A dictionary with the online provided metadata
    Returns:
        A str with the category of the publishing entity
    """
    igo = ["FAO", "World Bank", "ILO", "United Nations", "International Monetary Fund"]
    go = ["U.S. Energy Information Administration"]
    ngo = ["UNICEF", "IFPRI"]
    company = ["CBOT"]

    quelle = ds_md["Quelle"]

    try:
        if any(x in quelle for x in igo):
            gw_quelle = "Zwischenstaatliche Organisation"
        elif any(x in quelle for x in ngo):
            gw_quelle = "Nichtregierungsorganisation"
        elif any(x in quelle for x in go):
            gw_quelle = "Staatliche Organisation"
        elif any(x in quelle for x in company):
            gw_quelle = "Unternehmen"
        else:
            gw_quelle = "N/A"
    except TypeError:
        gw_quelle = "N/A"
    return gw_quelle

# hard-coded for now because all datasets of each plattform have same level; would need implementation further on
def fivestar_opendata(ds_md: dict) -> int:
    """ categorize the dataset in the five star open data schema
    Args:

    Returns:
        the Open Data category that the dataset belongs to
    """
    if "World Bank" in ds_md["Herausgeber"]:
        star = 4
    else:
        star = 3
    return star

def check_domain(ds: pd.DataFrame) -> [list, list]:
    """ if column domain is Country, check whether any column header does not belong to a country name
    Args:
        ds: the DataFrame to check the column headers on
    Returns:
        A list with countries that are not recognized as countries
    """
    not_a_country = []
    iso_list = []
    if 'country' in ds.columns.name.lower():
        for country in ds.columns:
            iso, fuzzy = Country.get_iso3_country_code_fuzzy(country, use_live=False)
            if iso is None:
                country_type = utils.check_country_fsp(country)
                if country_type is None:
                    not_a_country.append(country)
                else:
                    not_a_country.append((country + " (Domain: " + country_type + ")"))
            else:
                iso_list.append(iso)
    return not_a_country, iso_list

def check_regions(iso_list: list) -> list:
    """ check which regions are available in dataset or if it has data for all continents -> World
    Args:
        iso_list: list of available countries as iso-codes
    Returns:
        A set with regions available in the dataset
    """
    regions = set()
    if iso_list:
        for cc in REGION_CODE.keys():
            [regions.add(REGION_CODE[cc]) for iso in iso_list if iso in Country.get_countries_in_region(cc, use_live=False)]
        if regions == set(REGION_CODE.values()):
            return ["Welt"]
        if not regions:
            logging.info("geographischer Geltungsbereich -> keine Regionen erkannt")
            return ["N/A"]
    else:
        return ["N/A"]
    return list(sorted(regions))

def check_interval(ds: pd.DataFrame) -> str:
    """ check the periodicity of the dataframe when more than two rows in dataset
    Args:
        ds: the DataFrame to calculate the operation on
    Returns:
        A dict containing the given and the calculated periodicity
    """
    ds_interval_level = ""
    if isinstance(ds.index, pd.core.indexes.datetimes.DatetimeIndex):
        try:
            interval = pd.infer_freq(ds.index)
            if interval in INTERVAL.keys():
                ds_interval_level = INTERVAL[interval]
            else:
                logging.info("kein Intervall erkannt")
                ds_interval_level = "sporadisch"
        except ValueError:
            logging.info("Zeitliche Granularität -> zu wenige Datenpunkte")
            pass
    else:
        ds_interval_level = "N/A"
    return ds_interval_level

def check_aggregation(ds: pd.DataFrame, geo: list) -> list:
    """ if dataset contains Countries, has more than one column including a column named "World",
        check whether the value of the average or sum of the rest of the column adds up
    Args:
        ds: the DataFrame to check the values in
    Returns:
        A list of years in which the aggregation doesn't add up
    """
    agg_violation = []
    if "World" in ds.columns and len(ds.columns) > 1:
        if "Welt" in geo:
            for index, row in ds.iterrows():
                numeric_rows_filled_zeroes = pd.to_numeric(row, errors='coerce')
                all_countries_mean = (numeric_rows_filled_zeroes[ds.columns != "World"].mean())
                all_countries_sum = (numeric_rows_filled_zeroes[ds.columns != "World"].sum())
                world = numeric_rows_filled_zeroes[ds.columns == "World"].sum()
                if [all_countries_mean != world or all_countries_sum != world]:
                    agg_violation.append(index)
                if len(agg_violation) == len(ds.index):
                    agg_violation = ["alle Zeilen"]
        else:
            logging.info("Überprüfung Aggregationsspalte -> Aggregation nicht überprüfbar")
    else:
        logging.info("Überprüfung Aggregationsspalte -> keine Aggregationsspalte in Datensatz vorhanden")
    return agg_violation

# work-in-progress
def check_units(ds: pd.DataFrame, ds_md: dict) -> list:
    """ check whether any numeric values in DataFrame are out of range for the given unit
    Args:
        ds: the DataFrame to check the values for
        ds_md: A dictionary with the online provided metadata
    Returns:
        A list of columns where at least one value violates range of provided unit
    """
    violates_unit = []

    unit_to_max_num = MAX_UNITS.get(ds_md["Dateneinheit"])
    unit_to_min_num = MIN_UNITS.get(ds_md["Dateneinheit"])

    for column in ds.columns:
        test = pd.to_numeric(ds[column], errors='coerce').fillna(0)
        if str(unit_to_min_num) and (test < unit_to_min_num).any():
            violates_unit.append(column)
        if str(unit_to_max_num) and (test >= unit_to_max_num).any():
            violates_unit.append(column)
    return violates_unit

def check_months_since_upload(ds_md: dict) -> str:
    """ check the delay between the creation date given in metadata and present date in months
    Args:
        ds_md: A dictionary with the online provided metadata
    Returns:
        A dictionary of the provided creation date and the delay
    """
    if not ds_md["Erstellungsdatum"] or ds_md['Erstellungsdatum']=='N/A':
        delay_upload = "N/A"
    else:
        delay_upload = str(len(pd.date_range(
            start=pd.to_datetime(ds_md["Erstellungsdatum"], format='%Y'),
            end=pd.to_datetime("now"), freq='M')))

    #doc_create = ds_md["Erstellungsdatum"] if ds_md["Erstellungsdatum"] != "" else "N/A"
    #delay = {"doc_create_date": doc_create, "delay_upload_in_months": delay_upload}

    return delay_upload

def check_delay_upload(ds: pd.DataFrame, ds_md: dict) -> str:
    """ checks the delay between the creation date given in metadata and last referenced date in dataset in months
    Args:
        ds: DataFrame to check
        ds_md: A dictionary with the online provided metadata
    Returns:
        A dictionary of the provided creation date and the delay
    """
    if not ds_md["Erstellungsdatum"] or ds_md['Erstellungsdatum']=='N/A':
        delay = "N/A"
    else:
        upload_date = pd.to_datetime(ds_md["Erstellungsdatum"], format='%Y')
        clean_dataset = ds.dropna(axis=1, how='all')
        if len(clean_dataset)>0:
            last_date = clean_dataset.index.max()
            if last_date.year > upload_date.year:
                delay = "+" + str(len(pd.date_range(start= upload_date, end= last_date, freq='M')))
            elif upload_date.year == last_date.year:
                delay = str(len(pd.date_range(start=last_date, end= upload_date, freq='M')))
            else:
                delay = "-" + str(len(pd.date_range(start=last_date, end= upload_date, freq='M')))
        else:
            delay = "N/A"
            logging.info("Verzögerung Veröffentlichung -> Datensatz hat keine lesbaren Werte")
    return delay

