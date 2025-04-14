"""
This file contains methods to combine data from pubmed_{date}.csv and retraction_watch_{date}.csv into one unionlist.

Functions overview:
convert_unicode: parses a string through various Unicode encoding options
read_csv_files_and_clean: read previously-created csv files, create pandas dataframes, input some missing values
check_individual_dataset: Clean and deduplicate records based on DOIs for Retraction Watch, PMIDs for PubMed.
count_DOI_and_PMID: count DOIs and PMIDs and duplicates in a given dataframe
"""
import numpy as np
import unicodedata
import pandas as pd


def convert_unicode(string: str) -> str:
    """
    It takes a string and passes it through different encoding parameter phases
    E.g. '10.\u200b1105/\u200btpc.\u200b010357' ->  '10.1105/tpc.010357'

    :param string: variable to be encoded
    :return: the actual string value devoided of encoded character
    """

    string = unicodedata.normalize('NFKD', string).encode('iso-8859-1', 'ignore').decode('iso-8859-1')
    string = unicodedata.normalize('NFKD', string).encode('latin1', 'ignore').decode('latin1')
    string = unicodedata.normalize('NFKD', string).encode('cp1252', 'ignore').decode('cp1252')
    return string


def read_csv_files_and_clean(pubmed_date: str, retraction_watch_date: str) -> pd.DataFrame:
    """
    Read in previously-gathered CSV files and return two pandas dataframes.

    :param pubmed_date: date information was gathered from PubMed
    :param retraction_watch_date: date information was gathered from Retraction Watch
    """
    pubmed = pd.read_csv(f"data/pubmed_{pubmed_date}.csv").rename(
        columns={'RetractionPubMedID': 'Retraction_Notice_PubMedID'})  # .drop(['Unnamed: 0'],axis=1

    pubmed['Indexed_In'] = 'PubMed'
    pubmed['Year'] = pubmed['Year'].str.split(':').str[0].astype(int)
    pubmed['DOI'] = pubmed['DOI'].str.lower().str.strip().astype(str).apply(convert_unicode)
    pubmed['PubMedID'] = pubmed['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()
    pubmed['Retraction_Notice_PubMedID'] = (pubmed['Retraction_Notice_PubMedID'].fillna(0).astype(int)
                                            .replace(0, '').astype(str))

    retractionwatch = pd.read_csv(
        f"data/retraction_watch_{retraction_watch_date}.csv", encoding='latin1'
    ).rename(
        columns={'OriginalPaperDOI': 'DOI',
                 'OriginalPaperPubMedID': 'PubMedID',
                 'OriginalPaperDate': 'Year',
                 'RetractionPubMedID': 'Retraction_Notice_PubMedID'}
    )

    retractionwatch['Indexed_In'] = 'Retraction Watch'
    retractionwatch['PubMedID'] = retractionwatch['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()
    retractionwatch['Retraction_Notice_PubMedID'] = retractionwatch['Retraction_Notice_PubMedID'].fillna(0).astype(int) \
        .replace(0, '').astype(str)

    retractionwatch.loc[retractionwatch['Year'] == '1/1/1753 12:00:00 AM', 'Year'] = '1/1/1753 00:00'
    # 'Year' column time information for publication from 1753 was entered differently, causing dt.year to fail
    retractionwatch['Year'] = pd.to_datetime(retractionwatch['Year']).dt.year

    # A row in Retraction Watch contains '|', causing later issues
    retractionwatch['DOI'] = retractionwatch['DOI'].str.replace(r'\|', '')  # "10.1038/embor.2009.88 |"
    retractionwatch['DOI'] = retractionwatch['DOI'].str.lower().fillna('').str.strip().apply(convert_unicode)


def check_individual_dataset(dataset) -> list:
    """
    Clean and deduplicate records based on DOIs for Retraction Watch, PMIDs for PubMed.
    After removing duplicates, we will return the count and the list of records with DOI,
    those without DOIs and duplicated records that will be dropped.

    :param dataset: dataset name as string, either "retractionwatch" or "pubmed"
    """

    # Fixing '0.1080/20430795.2021.1894544' DOI error instead of '10.1080/20430795.2021.1894544',
    # a peculiarity of Retraction Watch

    # Getting the DataFrame name
    df_name = [name for name, obj in globals().items() if obj is dataset][0]

    if df_name == 'retractionwatch':
        if '10.1038/embor.2009.88 |' in list(dataset['DOI']):
            dataset['DOI'] = dataset['DOI'].replace('10.1038/embor.2009.88 |', '10.1038/embor.2009.88')

    # Step 1: We identify the unique records of each dataset based on DOI.
    # 'records_with_DOI_has_dup': Identify records that have a valid DOI which should start with '10.'
    # 'records_with_DOI': Drop the duplicates from the previous line.

    records_with_DOI_has_dup = dataset.loc[dataset['DOI'].str.startswith('10.', na=False)]

    if df_name == 'pubmed':
        records_with_DOI = records_with_DOI_has_dup.drop_duplicates(subset=['DOI'], keep='last')
    else:
        records_with_DOI = records_with_DOI_has_dup.drop_duplicates(subset=['DOI'], keep='first')

    # Step 2: We create two duplicate lists.
    # 'duplicated_records_all': Identify ALL duplicated records for reference and download for checking manually.
    # 'duplicated_records': Identify duplicated records to drop but keep only
    # the first occurrence of each group of duplicates.
    duplicate_records_all = \
        records_with_DOI_has_dup.loc[records_with_DOI_has_dup.duplicated(subset=['DOI'], keep=False), :]

    # To check anamolies - to remove record with PubMedID 28202934 and leave that 26511294 with correct date
    if df_name == 'pubmed':
        duplicate_records_one_copy = records_with_DOI_has_dup.loc[
                            records_with_DOI_has_dup.duplicated(subset=['DOI'], keep='last'),
                            :]
    else:
        duplicate_records_one_copy = records_with_DOI_has_dup.loc[
                            records_with_DOI_has_dup.duplicated(subset=['DOI'], keep='first'),
                            :]

    # Step 3: We get the count of records without DOI. Duplicates may exist since we could not use DOI
    # to identify duplicate records
    records_without_DOI = dataset.loc[~dataset['DOI'].str.startswith('10.', na=False)]

    try:

        if len(dataset) == len(records_with_DOI) + len(records_without_DOI) + len(duplicate_records_one_copy):
            return [len(dataset),
                    len(records_with_DOI),
                    len(records_without_DOI),
                    len(duplicate_records_one_copy),
                    records_with_DOI,
                    records_without_DOI,
                    duplicate_records_one_copy,
                    duplicate_records_all]
            # return the count and items of each group

    except Exception:
        return ['ERROR']


def count_DOI_and_PubMedID(df, source) -> tuple:
    """
    :param df: DataFrame to work on
    :param source: source/database to look up to determine number of count

    :return: source, # DOI, # PubMedID, # Duplicated record -> list
    """

    df_DOI = df[(df['DOI'].str.startswith('10')) & (df['Indexed_In'].str.contains(source))]

    df_no_dup_DOI = df_DOI.drop_duplicates(subset=['DOI'], keep='first')  # DF that has no duplicated DOIs

    df_duplicated_DOI = df_DOI[df_DOI.duplicated(subset=['DOI'], keep='last')]  # DF containing only duplicated DOIs

    df_no_DOI = df[~(df['DOI'].str.startswith('10')) & (df['Indexed_In'].str.contains(source))]  # DF that has no DOI

    nDOI = len(df_no_dup_DOI)  # Number of items with unique DOI
    nDuplicatedDOI = len(df_duplicated_DOI)  # Number of items with duplicated DOIs
    nNoDOI = len(df_no_DOI)  # Number of items without DOI

    if 'PubMedID' in df.columns:

        df_PMID = df_DOI[
            ((df_DOI['PubMedID'] != "") | ~df_DOI['PubMedID'].isna()) & (df_DOI['source'].str.contains(source))]

        df_no_dup_PMID = df_PMID.drop_duplicates(subset=['DOI'], keep='first')  # DF that has no duplicated PMID

        df_duplicated_PMID = df_PMID[df_PMID.duplicated(subset=['DOI'], keep='last')]  # DF containing only duplicated PMIDs

        df_no_PMID = df[~(((df['PubMedID'] != "") | ~df['PubMedID'].isna()) & (df['source'].str.contains(source)))]

        nPMID = len(df_no_dup_PMID)  # Number of items with unique PMID
        nDuplicatedPMID = len(df_duplicated_PMID)  # Number of items that has duplicated PMID
        nNoPMID = len(df_no_PMID)  # Numbers of items with without PMID

    else:
        nPMID, nDuplicatedPMID, nNoPMID = 0, 0, 0

    Total = len(df)

    return source, Total, nDOI, nNoDOI, nDuplicatedDOI, nPMID, nNoPMID, nDuplicatedPMID
    # nDuplicateDOI,nPubMedID,nDuplicatePubMedID


def create_overview_table(pubmed: pd.DataFrame, retraction_watch: pd.DataFrame) -> pd.DataFrame:
    """
    Creates an overview table with duplicate tracking.
    :param pubmed: dataframe containing retracted publications from PubMed
    :param retraction_watch: dataframe containing retracted publications from Retraction Watch
    :return: Overview dataframe
    """
    nPubMed = count_DOI_and_PubMedID(pubmed, 'PubMed')
    nRW = count_DOI_and_PubMedID(retraction_watch, 'Retraction Watch')

    dbtable = []  # A nested list which stores the records of each group in each source
    ovtable = []  # Store the count of each group from each source and create a table for viewing

    dblist = [nPubMed, nRW]

    # Query results retrieved	Records with DOI	Records without DOI removed	Duplicate records removed
    # source,Total, nDOI,nNoDOI,nDuplicatedDOI,nPMID,nNoPMID,nDuplicatedPMID
    for result in dblist:
        dbtable.append(result)

        np_results = np.array(dbtable)

    # Create a table showing the count of each group
    overview = pd.DataFrame(np_results[:, [1, 2, 3, 4, 5]])
    overview.columns = ['Query_result', 'Records_withDOI', 'Records_withoutDOI', 'Duplicate_DOI_removed',
                        'DOI_records_withPubMedID']
    overview['Indexed_In'] = ['PubMed', 'Retraction Watch']

    # Re-order columns
    overview = overview[['Indexed_In', 'Query_result', 'Records_withDOI', 'Records_withoutDOI', 'Duplicate_DOI_removed',
                         'DOI_records_withPubMedID']]

    # Aggregating items in each column
    overview.loc[len(overview)] = ['Total', overview.Query_result.astype(int).sum(),
                                   overview.Records_withDOI.astype(int).sum(),
                                   overview.Records_withoutDOI.astype(int).sum(),
                                   overview.Duplicate_DOI_removed.astype(int).sum(),
                                   overview.DOI_records_withPubMedID.astype(int).sum(), ]

    overview.to_csv('data/datasources_overview.csv')
