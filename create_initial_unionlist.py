"""
This file contains methods to combine data from pubmed_{date}.csv and retraction_watch_{date}.csv into one unionlist.

Functions overview:
convert_unicode: parses a string through various unicode encoding options
"""
import unicodedata


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


def check_individual_dataset(dataset) -> list:
    """
    Clean and deduplicate records based on DOIs.
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
        duplicate_records = records_with_DOI_has_dup.loc[
                            records_with_DOI_has_dup.duplicated(subset=['DOI'], keep='last'),
                            :]
    else:
        duplicate_records = records_with_DOI_has_dup.loc[
                            records_with_DOI_has_dup.duplicated(subset=['DOI'], keep='first'),
                            :]

    # Step 3: We get the count of records without DOI. Duplicates may exist since we could not use DOI
    # to identify duplicate records
    records_without_DOI = dataset.loc[~dataset['DOI'].str.startswith('10.', na=False)]

    try:

        if len(dataset) == len(records_with_DOI) + len(records_without_DOI) + len(duplicate_records):
            return [len(dataset),
                    len(records_with_DOI),
                    len(records_without_DOI),
                    len(duplicate_records),
                    records_with_DOI,
                    records_without_DOI,
                    duplicate_records,
                    duplicate_records_all]
            # return the count and items of each group

    except Exception:
        return ['ERROR']
