#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""import_data.py: Beschreibt den Import-Prozess der Datensätze und Metadaten aus dem Food Security Portal und Welt Bank Datenportal"""

__author__ = "Lisa Koeritz"

import logging
import pandas as pd
import requests
import json
import urllib
from bs4 import BeautifulSoup

import wbdata
import wbdata.fetcher
wbdata.fetcher.CACHE.sync = lambda : None

import utils

known_datasets_from_fsp = {}
known_datasets_from_wb = {}

metadata_list = {"Quelle","Erstellungsdatum","Kategorie","Titel","Beschreibung","Herausgeber","ID","Dateneinheit"} # basic eGMS attribute structure

ADDITIONAL_URLS = {
    'Commodities Futures Data': 'http://www.foodsecurityportal.org/api/commodities-futures-.csv',
    'Weekly Commodities Prices': 'http://www.foodsecurityportal.org/api/weekly-commodities-p.csv',
    'World Commodity Prices': 'http://www.foodsecurityportal.org/api/world-commodity-pric.csv'
}

# at start of program
def collect_all_dataset_links_fsp() -> dict:
    """ creates dictionary of all the dataset titles on www.foodsecurityportal.org/api and the download paths to easily access them
    Returns:
        A dict with titles as keys and link paths as values
    """
    try:
        base_url = 'http://www.foodsecurityportal.org/api/countries'
        response = requests.get(base_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        download_links = {}
        for link in soup.findAll(lambda opt: opt.name == 'option' and opt.parent.attrs.get('id') == 'edit-table-path'):
            if link.get('value') == '':
                continue
            download_link = base_url + link.get('value') + '.csv'
            title = link.getText()
            download_links[title] = download_link
        # because the api stores commodity data in an unreachable part of the api, it is manually added:
        download_links.update(ADDITIONAL_URLS)
        download_links.pop("Regions") #delete helper-dataset
        return download_links
    except TypeError:
        logging.critical("no dataset connection")



# at start of program
def collect_all_metadata_links_fsp() -> dict:
    """ creates dictionary of all the metadata titles on www.foodsecurityportal.org/api and the download paths to easily access them
    Returns:
        A dict with titles as keys and link paths as values
    """
    url = 'http://www.foodsecurityportal.org/api'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    metadataList = {}
    for link in soup.findAll('span', attrs={'class': 'field-content field-title'}):
        metadataLink = 'http://www.foodsecurityportal.org' + link.a['href']
        title = link.getText()
        if title not in ("Countries", "Regions"):
            metadataList[title] = metadataLink
    return metadataList


# check for all basic eGMS attributes and others needed (if applicable)
def get_metadata_attributes_fsp(url) -> dict:
    """ downloads the given metadata and fills metadata_list with all available metadata according to the given categories;
        hardcoded for now
    Args:
        url: url from which to retrieve metadata
    Returns:
        A dict with filled metadata
    """
    metadata_dict = dict.fromkeys(metadata_list, 'N/A')
    if not "N/A" in url:
        try:
            html = urllib.request.urlopen(url).read()
            soup = BeautifulSoup(html, 'lxml')
            title = soup.find('h6', attrs={'class': 'page-title'}).getText()
            metadata_dict["Titel"] = title
            for information in soup.find_all('div', attrs={'class': 'field-type-text'}):
                label = information.find('div', attrs={'class': 'field-label'}).getText().strip(":\xa0")
                attribute = information.find('div', attrs={'class': 'field-item'}).getText().strip()
                #if "Temporal Info" in label:
                #   metadata_dict["zeitl. Abdeckung"] = attribute
                if "Unit" in label:
                    metadata_dict["Dateneinheit"] = attribute
                elif "Source" in label:
                    metadata_dict["Herausgeber"] = [str(s) for s in str.split(attribute) if s.isalpha()][0]
                    metadata_dict["Erstellungsdatum"] = [int(s) for s in str.split(attribute) if s.isnumeric()][0]
                elif "Footnote" in label:
                    metadata_dict["Quelle"] = attribute
                #else:
                #    metadata_dict[label] = attribute
            description = soup.find('div', attrs={'class': 'filter-text'}).get_text().strip("\n")
            metadata_dict["Beschreibung"] = description
        except (urllib.error.HTTPError, KeyError):
            pass
        except TypeError:
            logging.info("URL Parameter ist notwendig")
        except AttributeError:
            logging.info("URL existiert nicht in API")
    return metadata_dict


def get_dataset_fsp(url) -> pd.DataFrame:
    """ downloads the given dataset and creates a dataframe with a datetime index if applicable
    Args:
        url: url to the dataset
    Returns:
        A pandas DataFrame of the dataset
    """
    if url in known_datasets_from_fsp:
        return known_datasets_from_fsp[url]
    else:
        try:
            dataset = pd.read_csv(url, index_col=0).T
            if dataset.empty:
                logging.info("given url doesn't exist in api")
                raise RuntimeWarning("Seems to have a broken link")
            if len(dataset.index) > 1:
                dataset.index = utils.datetimeIndex_parsing(dataset.index.str.strip())
            known_datasets_from_fsp.update({url: dataset})
            return dataset
        except ValueError:
            raise ValueError


def get_metadata_attributes_wb(ind) -> dict:
    """ downloads the given metadata for api.worldbank.org datasets and fills metadata_list with all available metadata
        fitting to the expected categories; hardcoded for now
    Args:
        ind: World Bank indicator for which to retrieve metadata
        metadata_list: raw metadata dict with basic eGMS attribute structure
    Returns:
        A dict with filled metadata dict
    """
    metadata_dict = dict.fromkeys(metadata_list, 'N/A')
    try:
        api_url = 'https://api.worldbank.org/v2/sources/2/series/' + ind + '/metadata?format=json'
        request = requests.get(api_url)
        values = utils.extract_json_values(request.json(), 'value')
        ids = utils.extract_json_values(request.json(), 'id')
        meta_schema_dict = dict(zip(ids[3:], values))

        metadata_dict["Herausgeber"] = "World Bank"
        #metadata_dict["zeitl. Abdeckung"] = (meta_schema_dict["Periodicity"] if ("Periodicity" in meta_schema_dict.keys()) else "N/A")
        metadata_dict["Quelle"] = (meta_schema_dict["Source"] if ("Source" in meta_schema_dict.keys()) else "N/A")
        metadata_dict["Kategorie"] = (meta_schema_dict["Topic"] if ("Topic" in meta_schema_dict.keys()) else "N/A")
        metadata_dict["Titel"] = (meta_schema_dict["IndicatorName"] if ("IndicatorName" in meta_schema_dict.keys()) else "N/A")
        metadata_dict["Beschreibung"] = (
            meta_schema_dict["Longdefinition"] if ("Longdefinition" in meta_schema_dict.keys()) else "N/A")
        metadata_dict["ID"] = ind
        # metadata_dict["Lizenz"] = meta_schema_dict["License_Type"]
        # metadata_dict["Methodik"] = meta_schema_dict["Statisicalconceptandmethodology"]
    except KeyError:
        logging.info("fehlender Indikator")
        pass
    except json.decoder.JSONDecodeError:
        logging.warning("keine Informationen für gegebenen Indikator vorhanden")
        pass
    return metadata_dict


def get_dataset_wb(indicator_id):
    """ downloads the given dataset and creates a dataframe
    Args:
        indicator_id: World Bank indicator_id for which to retrieve the dataset
    Returns:
        A pandas DataFrame of the dataset
    """
    if indicator_id in known_datasets_from_wb:
        return known_datasets_from_wb[indicator_id]
    else:
        dat = wbdata.get_data(indicator=indicator_id, convert_date=True, pandas=True)
        dat = dat.unstack(level=0)  # delete MultiIndex
        known_datasets_from_wb.update({indicator_id: dat})
        return dat
