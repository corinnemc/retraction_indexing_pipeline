"""
This file contains methods to update the unionlist with a "Covered_In" column using csv files that note coverage.

Functions:
read_in_unionlist_without_coverage(): reads unionlist into DataFrame and corrects data type issues
read_in_covered_not_indexed(): reads pubmed_coverednotindexed into DataFrame and corrects datatype issues
merge_dataframes(): merges unionlist and pubmed_coverednotindexed dataframes to add coverage information to unionlist
"""
import pandas as pd


def read_in_unionlist_without_coverage(unionlist_date: str):
    """
    Read unionlist into dataframe and correct datatype issues
    :param unionlist_date: date of current unionlist
    :return: unionlist dataframe
    """
    unionlist = pd.read_csv(f'../data/{unionlist_date}_unionlist.csv')

    # Address possible pandas datatype issues
    unionlist['PubMedID'] = unionlist['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()
    unionlist['DOI'] = unionlist['DOI'].str.strip()

    return unionlist


def read_in_covered_not_indexed(pubmed_date: str):
    """
    Read pubmed_coverednotindexed into dataframe and correct datatype issues
    :param pubmed_date: date of current pubmed_coverednotindexed
    :return: pubmed_coverednotindexed dataframe
    """
    pubmed_covered_not_indexed = pd.read_csv(f'../data/pubmed_coverednotindexed_{pubmed_date}.csv')

    # Address possible pandas datatype issues
    pubmed_covered_not_indexed['PubMedID'] = (
        pubmed_covered_not_indexed['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip())
    pubmed_covered_not_indexed['DOI'] = pubmed_covered_not_indexed['DOI'].str.strip()

    return pubmed_covered_not_indexed


def merge_dataframes(unionlist: pd.DataFrame, pubmed_covered_not_indexed: pd.DataFrame, unionlist_date: str):
    """
    Merge unionlist and pubmed_coverednotindexed dataframes to add coverage information to unionlist
    :param unionlist: dataframe of unionlist with only indexing information
    :param pubmed_covered_not_indexed: dataframe of items PubMed covers but does not index as retracted
    :param unionlist_date: date of current unionlist to be used in saved csv file
    :return: updated unionlist dataframe with coverage information
    """
    merged_df = pd.merge(unionlist,
                         pubmed_covered_not_indexed,
                         on='DOI',
                         how='outer',
                         indicator=True)

    merged_df.rename(columns={'_merge': 'Merge'}, inplace=True)

    # Set up DataFrame
    fused_df = pd.DataFrame(columns=["DOI",
                                     "Author",
                                     "Title",
                                     "Year",
                                     "Journal",
                                     "PubMedID",
                                     "Indexed_In",
                                     "Covered_In"])

    for row in merged_df.itertuples():
        if row.Merge == 'both':  # In both unionlist (row.COLUMN_x dataframe or left dataframe)
                                 # and pubmed_covered_not_indexed
            new_row = {'DOI': row.DOI,
                       'Author': row.Author_x,
                       'Title': row.Title_x,
                       'Year': row.Year_x,
                       'Journal': row.Journal_x,
                       'PubMedID': row.PubMedID_x,
                       'Indexed_In': row.Indexed_In_x,
                       'Covered_In': row.Covered_In}  # This column only exists in pubmed_covered_not_indexed
        elif row.Merge == 'left_only':  # Only in unionlist (row.COLUMN_x dataframe or left dataframe)
            new_row = {'DOI': row.DOI,
                       'Author': row.Author_x,
                       'Title': row.Title_x,
                       'Year': row.Year_x,
                       'Journal': row.Journal_x,
                       'PubMedID': row.PubMedID_x,
                       'Indexed_In': row.Indexed_In_x,
                       'Covered_In': row.Indexed_In_x}  # Indexing implies coverage.

        fused_df.loc[len(fused_df)] = new_row

    fused_df.to_csv(f'../data/{unionlist_date}_unionlist_with_coverage.csv')


def main():
    print("Reading in data...")
    unionlist = read_in_unionlist_without_coverage(unionlist_date='2025-05-08')
    pubmed_covered_not_indexed = read_in_covered_not_indexed(pubmed_date='2025-05-08')

    print("Merging dataframes...")
    print("This can take some time as the program iterates over thousands of rows.")
    merge_dataframes(unionlist=unionlist,
                     pubmed_covered_not_indexed=pubmed_covered_not_indexed,
                     unionlist_date='2025-05-08')


if __name__ == '__main__':
    main()
