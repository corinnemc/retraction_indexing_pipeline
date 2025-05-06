"""
This file contains methods to collect information about items that are indexed as retracted in Retraction Watch (but not
in PubMed) to see if PubMed covers these items.

Functions overview:
gather_pubmed_not_indexed(): gets items that are not indexed as retracted in PubMed but
 are indexed as retracted in Retraction Watch
batch_pmids(): divides a list of pmids into batches for processing.


"""
import pandas as pd
from datetime import date
import requests
import time


def read_in_unionlist(date: str):
    """
    Read unionlist into dataframe and correct datatype issues
    :param date: date of current unionlist
    :return:
    """
    unionlist = pd.read_csv(f'data/unionlist_{date}.csv')

    # Address possible pandas datatype issues
    unionlist['PubMedID'] = unionlist['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()
    unionlist['DOI'] = unionlist['DOI'].str.strip()

    return unionlist


def gather_pubmed_not_indexed(unionlist: pd.DataFrame) -> pd.DataFrame:
    """
    Uses regex to search unionlist for items not indexed as retracted in PubMed
    :param unionlist: unionlist of items indexed as retracted in all sources
    :return: dataframe with items that are not indexed as retracted in PubMed
    """
    pubmed_not_indexed = unionlist[~unionlist['Indexed_In'].str.contains(r'PubMed', regex=True, na=False)]
    print(pubmed_not_indexed)

    return pubmed_not_indexed


def batch_items(pmids: list, cut: int) -> list[list]:
    """
    It divides a list of pmids into batches for processing.
    :param pmids: list of pmids
    :param cut: maximum number of records to assign to a batch

    :return: nested list of batches (lists) of pmids
    """
    pmids_batches = []

    while len(pmids) >= cut:
        selected_pmids = pmids[:cut]
        pmids_batches.append(selected_pmids)
        #         print(selected_pmids)
        pmids = pmids[cut:]

    if pmids:
        pmids_batches.append(pmids)
        print(pmids)

    return pmids_batches


def check_pubmed_via_identifier(identifiers: list, email: str):
    """
    It will query PubMed for items in an identifier list

    :param identifiers: list of DOIs
    :param email: email to link to request
    """

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    params = {
        "db": "pubmed",
        "term": identifiers,
        "retmode": "json",
        "retmax": 10000,  # Maximum number of results per request
        "email": email,
    }

    response = requests.get(base_url, params=params)
    print(response)
    data = response.json()

    return data


def query_pubmed_with_doi(pubmed_not_indexed: pd.DataFrame, email: str):
    """
    Queries PubMed in batches using DOI as the identifier
    :param pubmed_not_indexed: dataframe with items that are not indexed as retracted in PubMed
    :param email: email to link to request
    :return: list of identifiers for items that are covered in PubMed but not indexed as retracted in PubMed
    """
    identifier_list = list(set(pubmed_not_indexed['DOI']))
    print(f"The total unique number of items to check coverage in PubMed is {len(identifier_list)}")

    identifier_batches = batch_items(identifier_list, 100)

    items_covered_not_indexed = []

    for batch in identifier_batches:
        dois = ','.join(batch)

        # Checking the items in PubMed
        result = check_pubmed_via_identifier(identifiers=dois, email=email)
        time.sleep(10)
        print(result)
        items_covered_not_indexed += result['esearchresult']['idlist']
        print(items_covered_not_indexed)

    return items_covered_not_indexed


def query_pubmed_with_pmid(pubmed_not_indexed: pd.DataFrame, email: str):
    """
    Queries PubMed in batches using PMID as the identifier.
    :param pubmed_not_indexed: dataframe with items that are not indexed as retracted in PubMed
    :param email: email to link to request
    :return: list of identifiers for items that are covered in PubMed but not indexed as retracted in PubMed
    """
    identifier_list = list(set(pubmed_not_indexed['PubMedID']))
    print(f"The total unique number of items to check coverage in PubMed is {len(identifier_list)}")

    identifier_batches = batch_items(identifier_list, 300)

    items_covered_not_indexed = []

    for batch in identifier_batches:
        pmids = ','.join(batch)

        # Checking the items in PubMed
        result = check_pubmed_via_identifier(identifiers=pmids, email=email)
        time.sleep(0.33)
        #       print(result)
        items_covered_not_indexed += result['esearchresult']['idlist']
        #       print(items_covered_not_indexed)

    return items_covered_not_indexed


def main():
    unionlist = read_in_unionlist(date='2025-05-04')
    pubmed_not_indexed = gather_pubmed_not_indexed(unionlist=unionlist)
    items_covered_not_indexed = query_pubmed_with_pmid(pubmed_not_indexed=pubmed_not_indexed,
                                                       email='corinne9@illinois.edu')

    pubmed_not_in_unionlist = unionlist[
                                unionlist['PubMedID'].isin(items_covered_not_indexed) &
                                (~unionlist['Indexed_In'].str.contains('PubMed'))
    ]

    pubmed_not_in_unionlist["Covered_In"] = "Retraction Watch; PubMed"

    pubmed_not_in_unionlist.to_csv(f'data/pubmed_coverednotindexed_{str(date.today())}.csv')


if __name__ == '__main__':
    main()
