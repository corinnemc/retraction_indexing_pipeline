"""
This file contains methods to combine data from pubmed_{date}.csv and retraction_watch_{date}.csv into one unionlist.

Functions overview:
convert_unicode: parses a string through various Unicode encoding options
clean_pubmed_data: read previously-created csv files, create pandas dataframes, standardize missing values
clean_retraction_watch_data: read previously-created csv file, create pandas dataframe, standardize missing values
check_individual_dataset_for_duplicates: Clean and deduplicate records based on DOIs for Retraction Watch, PMIDs for PubMed.
count_DOI_and_PMID: count DOIs, PMIDs, and duplicates in a given dataframe
create_overview_table: using counts above, creates an overview table of query totals and counts of interest
export_datasets_from_sources: partitions each source (Retraction Watch and PubMed) into items with DOI, without DOI, and with
duplicates
save_records_in_subgroups: uses check_individual_dataset_for_duplicates and export_datasets_from_sources to iterate through
sources and save separate csv files with items with DOI, without DOI, and with duplicates based on DOI
create_union_list: create union list and save to .csv file, matching on DOI values
"""
import numpy as np
import unicodedata
import pandas as pd
from datetime import date


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


def clean_pubmed_data(pubmed_date: str) -> pd.DataFrame:
    """
    Read in previously-gathered CSV file and return cleaned pandas dataframe for PubMed.

    :param pubmed_date: date information was gathered from PubMed
    :return: cleaned pandas dataframe
    """
    pubmed = pd.read_csv(f"data/{pubmed_date}_pubmed.csv").rename(
        columns={'RetractionPubMedID': 'Retraction_Notice_PubMedID'})  # .drop(['Unnamed: 0'],axis=1

    # Add column for target indicator
    pubmed['Indexed_In'] = 'PubMed'

    # Extract only Retraction Year and make an integer value
    pubmed['Year'] = pubmed['Year'].str.split(':').str[0].astype(int)

    # Convert DOI information from unicode
    pubmed['DOI'] = pubmed['DOI'].str.lower().str.strip().astype(str).apply(convert_unicode)

    # Fill NA PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    pubmed['PubMedID'] = pubmed['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()

    # Fill NA Retraction Notice PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    pubmed['Retraction_Notice_PubMedID'] = (pubmed['Retraction_Notice_PubMedID'].fillna(0).astype(int)
                                            .replace(0, '').astype(str))

    return pubmed

def clean_retraction_watch_data(retraction_watch_date: str) -> pd.DataFrame:
    """
    Read in previously-gathered CSV file and return cleaned pandas dataframe for Retraction Watch.

    :param retraction_watch_date: date information was gathered from Retraction Watch
    :return: cleaned pandas dataframe
    """

    retraction_watch = pd.read_csv(
        f"data/{retraction_watch_date}_retraction_watch.csv", encoding='latin1'
    ).rename(
        columns={'OriginalPaperDOI': 'DOI',
                 'OriginalPaperPubMedID': 'PubMedID',
                 'OriginalPaperDate': 'Year',
                 'RetractionPubMedID': 'Retraction_Notice_PubMedID'}
    )

    # Add column for target indicator
    retraction_watch['Indexed_In'] = 'Retraction Watch'

    # Fill NA PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    retraction_watch['PubMedID'] = retraction_watch['PubMedID'].fillna(0).astype(int).replace(0, '').astype(
        str).str.strip()

    # Fill NA Retraction Notice PubMed ID cells with 0, replace with empty string, make all PubMed IDs strings
    retraction_watch['Retraction_Notice_PubMedID'] = retraction_watch['Retraction_Notice_PubMedID'].fillna(0).astype(int) \
        .replace(0, '').astype(str)

    # Address single issue with date column causing dt.year to fail. Make Retraction Year an integer value
    retraction_watch.loc[retraction_watch['Year'] == '1/1/1753 12:00:00 AM', 'Year'] = '1/1/1753 00:00'
    retraction_watch['Year'] = pd.to_datetime(retraction_watch['Year']).dt.year

    # Address prior issue with | character listed in a DOI in Retraction Watch. Not currently an issue as of 5-1-2025.
    # Covert DOI values from unicode
    retraction_watch['DOI'] = retraction_watch['DOI'].str.replace(r'\|', '')  # "10.1038/embor.2009.88 |"
    retraction_watch['DOI'] = retraction_watch['DOI'].str.lower().fillna('').str.strip().apply(convert_unicode)

    return retraction_watch


def check_individual_dataset_for_duplicates(dataset: pd.DataFrame) -> list:
    """
    Deduplicate records based on DOIs for Retraction Watch and PubMed.
    After removing duplicates, we will return the count and the list of records with DOI,
    those without DOIs, and duplicated records that will be exported for later analysis.

    :param dataset: dataframe, either "retraction_watch" or "pubmed"
    """

    # Getting the DataFrame name
    df_name = [name for name, obj in globals().items() if obj is dataset]

    # Step 1: We identify the unique records of each dataset based on DOI.
    # 'records_with_DOI_has_dup': Identify records that have a valid DOI which should start with '10.'
    # 'records_with_DOI': Drop the duplicates from the previous line.

    records_with_DOI_has_dup = dataset.loc[dataset['DOI'].str.startswith('10.', na=False)]

    # To remove record with PubMed ID 26511294 (row ~6700 and date listed as 1900)
    # and leave record with PubMed ID 28202934 (row ~9675 and date listed as 2016) with correct date
    # Both records have DOI 10.1057/jphp.2015.37
    if df_name == 'pubmed':
        records_with_DOI = records_with_DOI_has_dup.drop_duplicates(subset=['DOI'], keep='last')
    else:
        records_with_DOI = records_with_DOI_has_dup.drop_duplicates(subset=['DOI'], keep='first')

    # Step 2: We create two duplicate lists.
    # 'duplicated_records_all': Identify ALL duplicated records for reference and download for checking manually.
    # 'duplicated_records_one_copy': Identify duplicated records to drop but keep only
    # the first occurrence of each group of duplicates.
    duplicate_records_all = \
        records_with_DOI_has_dup.loc[records_with_DOI_has_dup.duplicated(subset=['DOI'], keep=False), :]

    # To remove record with PubMed ID 26511294 (row ~6700 and date listed as 1900)
    # and leave record with PubMed ID 28202934 (row ~9675 and date listed as 2016) with correct date
    # Both records have DOI 10.1057/jphp.2015.37
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
    :param source: source/database to look up to determine number of count, e.g. "Retraction Watch" or "Pubmed"

    :return: source, # DOI, # PubMedID, # Duplicated record -> list
    """

    df_DOI = df[(df['DOI'].str.startswith('10')) & (df['Indexed_In'].str.contains(source))]

    # DF that has no duplicated DOIs
    df_no_dup_DOI = df_DOI.drop_duplicates(subset=['DOI'], keep='first')

    # DF containing only duplicated DOIs
    df_duplicated_DOI = df_DOI[df_DOI.duplicated(subset=['DOI'], keep='last')]

    # DF that has no DOI
    df_no_DOI = df[~(df['DOI'].str.startswith('10')) & (df['Indexed_In'].str.contains(source))]

    # Number of items with unique DOI
    nDOI = len(df_no_dup_DOI)

    # Number of items with duplicated DOIs
    nDuplicatedDOI = len(df_duplicated_DOI)

    # Number of items without DOI
    nNoDOI = len(df_no_DOI)

    if 'PubMedID' in df.columns:
        # DF of items with PMID
        df_PMID = df_DOI[
            ((df_DOI['PubMedID'] != "") | ~df_DOI['PubMedID'].isna()) & (df_DOI['Indexed_In'].str.contains(source))]

        # DF that has no duplicated PMID
        df_no_dup_PMID = df_PMID.drop_duplicates(subset=['DOI'], keep='first')

        # DF containing only duplicated PMIDs
        df_duplicated_PMID = df_PMID[df_PMID.duplicated(subset=['DOI'], keep='last')]

        # DF of items without PMID
        df_no_PMID = df[~(((df['PubMedID'] != "") | ~df['PubMedID'].isna()) & (df['Indexed_In'].str.contains(source)))]

        nPMID = len(df_no_dup_PMID)  # Number of items with unique PMID
        nDuplicatedPMID = len(df_duplicated_PMID)  # Number of items that has duplicated PMID
        nNoPMID = len(df_no_PMID)  # Numbers of items without PMID

    else:
        nPMID, nDuplicatedPMID, nNoPMID = 0, 0, 0

    Total = len(df)

    return source, Total, nDOI, nNoDOI, nDuplicatedDOI, nPMID, nNoPMID, nDuplicatedPMID
    # nDuplicateDOI,nPubMedID,nDuplicatePubMedID


def create_overview_table(pubmed: pd.DataFrame, retraction_watch: pd.DataFrame):
    """
    Creates an overview table with duplicate tracking.
    :param pubmed: dataframe containing retracted publications from PubMed
    :param retraction_watch: dataframe containing retracted publications from Retraction Watch
    :return: Overview dataframe
    """
    nPubMed = count_DOI_and_PubMedID(pubmed, 'PubMed')
    nRW = count_DOI_and_PubMedID(retraction_watch, 'Retraction Watch')

    dbtable = []  # A nested list which stores the records of each group in each source

    dblist = [nPubMed, nRW]

    # Order of returned values from count_DOI_and_PubMedID function
    # source, Total, nDOI, nNoDOI, nDuplicatedDOI, nPMID, nNoPMID, nDuplicatedPMID
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


def export_datasets_from_sources(source_list: list, dbtable: list, source_names: list):
    """
    Helper function to filter records with DOI, without DOI, and those with duplicated DOIs
    for each source and save each file. To be used in conjunction with check_individual_dataset_for_duplicates()

    :param source_list: Dataframes for each source
    :param dbtable: A nested list which stores the records of each group (DOI, no DOI, duplicated DOI) in each source
    :param source_names: list of source names in order
    :return: It will for each source return:
            - records with doi (saved as in this format: 'source_name_recordswithdoi_date.csv')
            - records without doi (saved as in this format: 'source_name_recordsnodoi_date.csv')
            - duplicated doi records (saved as in this format: 'source_name_duplicatedrecords_date.csv')
    """

    for i in range(len(source_list[:])):
        source_name = source_names[i]

        date_run = str(date.today())

        dbtable[i][4].sort_values(by=['DOI'], ascending=False).to_csv(
            f'data/{source_name}_recordswithdoi_{date_run}.csv'
        )
        dbtable[i][5].to_csv(
            f'data/{source_name}_recordsnodoi_{date_run}.csv'
        )
        dbtable[i][6].sort_values(by=['DOI'], ascending=False).to_csv(
            f'data/{source_name}_duplicatedrecords_{date_run}.csv'
        )


def save_records_in_subgroups(dataframe_list: list, source_names: list):
    """
    Iterates through dataframes to partition into groups of records with DOI, without DOI, and those with duplicated DOIs.
    :param dataframe_list: list of dataframe objects to iterate through
    :param source_names: list of string source names in same order as dataframe_list
    :return: exported csv files
    """
    nested_groups_list = []  # A nested list which stores the records of each group in each source

    for x in dataframe_list[:]:
        nested_groups_list.append(check_individual_dataset_for_duplicates(x))

    export_datasets_from_sources(dataframe_list[:], nested_groups_list, source_names)


def create_union_list():
    pubmed_retracted = pd.read_csv(f'data/pubmed_recordswithdoi_{str(date.today())}.csv')
    retraction_watch_retracted = pd.read_csv(f'data/retraction_watch_recordswithdoi_{str(date.today())}.csv')

    merged_df_with_conflicts = pd.merge(pubmed_retracted,
                                        retraction_watch_retracted,
                                        on='DOI',
                                        how='outer',
                                        indicator=True)
    merged_df_with_conflicts.rename(columns={'_merge': 'Merge'}, inplace=True)

    # Set up DataFrame
    fused_df = pd.DataFrame(columns=["DOI", "Author", "Title", "Year", "Journal", "PubMedID", "Indexed_In"])

    # Merge data sources. In cases where conflicts exist,
    # PubMed data was used (row.COLUMN_x, left DataFrame in original merge).
    for row in merged_df_with_conflicts.itertuples():
        if row.Merge == 'both':
            new_row = {'DOI': row.DOI,
                       'Author': row.Author_x,
                       'Title': row.Title_x,
                       'Year': row.Year_x,
                       'Journal': row.Journal_x,
                       'PubMedID': row.PubMedID_x,
                       'Indexed_In': "Retraction Watch; PubMed"}
        elif row.Merge == 'left_only':
            new_row = {'DOI': row.DOI,
                       'Author': row.Author_x,
                       'Title': row.Title_x,
                       'Year': row.Year_x,
                       'Journal': row.Journal_x,
                       'PubMedID': row.PubMedID_x,
                       'Indexed_In': "PubMed"}
        elif row.Merge == 'right_only':
            new_row = {'DOI': row.DOI,
                       'Author': row.Author_y,
                       'Title': row.Title_y,
                       'Year': row.Year_y,
                       'Journal': row.Journal_y,
                       'PubMedID': row.PubMedID_y,
                       'Indexed_In': "Retraction Watch"}
        fused_df.loc[len(fused_df)] = new_row

    # Confirm column "Indexed_In" is string and "Year" is int
    fused_df['Indexed_In'] = fused_df['Indexed_In'].astype(str)
    fused_df['Year'] = fused_df['Year'].astype(int)

    # Remove NA values from PubMedID column
    fused_df['PubMedID'] = fused_df['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str)

    fused_df.to_csv(f'data/unionlist_{str(date.today())}.csv')


def main():
    pubmed = clean_pubmed_data(pubmed_date='2025-04-13')
    retraction_watch = clean_retraction_watch_data(retraction_watch_date='2025-04-13')

    create_overview_table(pubmed=pubmed, retraction_watch=retraction_watch)

    save_records_in_subgroups(dataframe_list=[pubmed, retraction_watch], source_names=['pubmed', 'retraction_watch'])

    create_union_list()


if __name__ == '__main__':
    main()
