#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""column_profile.py: Kalkulationen für das und Darstellung des Datenprofils auf Spaltenebene"""

__author__ = "Lisa Koeritz"

import pandas as pd
import re
from dateutil.relativedelta import relativedelta

TO_PERCENT = 100

def describe_dc_as_dataframe(dc: pd.Series, ds_md: dict) -> pd.Series:
    """ describes the profile criteria for column
    Args:
        dc: the Series to create Profile for
        ds_md: the Metadata dictionary of the DataFrame that is to be profiled
    Returns:
        A Series containing calculated description values.
    """
    dc = pd.to_numeric(dc, errors='coerce')
    null_values = dc.isna().sum()
    unique_values = len(dc.dropna().unique()) / len(dc)
    constancy = dc.value_counts(normalize=True).max() #constancy defined as amount of most frequent value divided by amount of numbers in column

    dc_stats = [
        ["Metadaten spezifisch für Spalte", column_metadata(dc.name, ds_md)],
        ["Anzahl an Zeilen", len(dc)],
        ["Anzahl an fehlenden Werten", null_values],
        ["Fehlende Werte (Prozent)", (null_values / len(dc))*TO_PERCENT],
        ["Distinkte Werte (Prozent)", unique_values*TO_PERCENT],
        ["Konstanz (Prozent)", constancy*TO_PERCENT],
        ["Mittelwert", format(dc.mean(), 'f')],
        ["Minimumwert (Jahr, Wert)", ({dc.idxmin().date(): format(dc.min(), 'f')} if len(dc.dropna()) > 0 else "")],
        ["Maximumwert (Jahr, Wert)", ({dc.idxmax().date(): format(dc.max(), 'f')}  if len(dc.dropna()) > 0 else "")],
        ["Datenpunkte vorhanden für", check_is_consecutive(dc)]
    ]

    profile = pd.DataFrame(data=dc_stats, columns=["Kriterien", "Ergebnis"])
    profile.set_index("Kriterien", inplace=True)

    return profile

# prototypical solution
def column_metadata(dc_name: str, ds_md: dict) -> str:
    """ extracts metadata description given by database explicitly for parameter series
    Args:
        dc: the Series to check name for
        ds_md: the dict to extract metadata from
    Returns:
        descriptive meta-information given for series
    """
    if ds_md["Beschreibung"] and dc_name in ds_md["Beschreibung"]:
        result = re.search('%s([^\n]*)' % dc_name, ds_md["Beschreibung"])
        return result.group(0)
    else:
        return ""

def check_is_consecutive(dc: pd.Series) -> list:
    """ creates list of consecutive intervals for which datapoints are given in Series
    Args:
        dc: the Series to check datetimes in
    Returns:
        A list containing all intervals of consecutive dates in formatted output
    """
    interval_list = []
    col = list(dc.dropna().index)  # list of all dates with values
    try:
        start = col[-1]  # last list item
        end = col[0]
        if len(col) > 2:
            interval = pd.infer_freq(dc.index) # frequency can only be provided when more than two items
            if interval:
                delta = (relativedelta(years=1) if interval == "AS-JAN" else (
                    relativedelta(months=1) if interval == "MS" else relativedelta(weeks=1)))
                while len(col) > 1:
                    d2 = col.pop()
                    d3 = col[-1] + delta
                    if d3 != d2:
                        date_period = (str(d2.to_period('D')), str(start.to_period('D')))
                        interval_list.append(" - ".join(date_period))
                        start = col[-1]
        date_period = (str(end.to_period('D')), str(start.to_period('D')))
        interval_list.append(" - ".join(date_period))
        interval_list.reverse()
    except:
        pass
    return interval_list
