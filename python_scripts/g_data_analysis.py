"""
This file contains methods to complete data analysis to create visualizations and answer research questions.

Functions:
read_in_unionlist_with_coverage(): reads unionlist with coverage information into DataFrame and corrects data type issues
partition_by_source(): returns partitioned DataFrames containing items that a given source indexes, covers,
    covers but does not index, and does not cover
create_results_table(): aggregates result information using helper function partition_by_source()
calculate_pairwise_agreement(): calculates pairwise agreement
create_indexing_upset_plot(): creates UpSet plot for indexing status of unionlist items
create_coverage_upset_plot(): Creates UpSet plot for coverage status of unionlist items
main(): runs all data analysis with variable parameters
"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from upsetplot import from_contents, plot
from datetime import date


def read_in_unionlist_with_coverage(completed_unionlist_date: str):
    """
    Read unionlist into dataframe and correct datatype issues
    :param completed_unionlist_date: date of current unionlist
    :return: unionlist dataframe
    """
    unionlist = pd.read_csv(f'../data/{completed_unionlist_date}_unionlist_with_coverage.csv')

    # Address possible pandas datatype issues
    unionlist['PubMedID'] = unionlist['PubMedID'].fillna(0).astype(int).replace(0, '').astype(str).str.strip()
    unionlist['DOI'] = unionlist['DOI'].str.strip()

    return unionlist


def partition_by_source(unionlist: pd.DataFrame, source_name: str):
    """
    Count total items that a given source indexes and covers
    :param unionlist: DataFrame of completed unionlist
    :param source_name: source to look up to determine count

    :return: items_indexed, items_covered, items_covered_not_indexed
    """

    items_indexed = unionlist[(unionlist['Indexed_In'].str.contains(source_name, na=False))]

    items_covered = unionlist[(unionlist['Covered_In'].str.contains(source_name, na=False))]

    items_covered_not_indexed = unionlist[
                                    (unionlist['Covered_In'].str.contains(source_name, na=False)) &
                                    (~unionlist['Indexed_In'].str.contains(source_name, na=False))
    ]

    items_not_covered = unionlist[~(unionlist['Covered_In'].str.contains(source_name, na=False))]

    return items_indexed, items_covered, items_covered_not_indexed, items_not_covered


def create_results_table(unionlist: pd.DataFrame):
    """
    Aggregates result information using helper function partition_by_source
    :param unionlist: dataframe of completed unionlist
    :return: variables with items indexed, covered, covered and not indexed, and not covered for both Retraction Watch
    and PubMed. Additionally, aggregate counts saved into csv file.
    """
    (pubmed_indexed,
     pubmed_covered,
     pubmed_covered_not_indexed,
     pubmed_not_covered) = (
        partition_by_source(unionlist, 'PubMed')
    )

    (retraction_watch_indexed,
     retraction_watch_covered,
     retraction_watch_covered_not_indexed,
     retraction_watch_not_covered) = (
        partition_by_source(unionlist, 'Retraction Watch')
    )

    # Print statements left below for troubleshooting
    # print('Pubmed Indexed:\n', pubmed_indexed.head(), len(pubmed_indexed))
    # print('PubMed All Covered:\n', pubmed_covered.head(), len(pubmed_covered))
    # print('PubMed Covered Not Indexed:\n', pubmed_covered_not_indexed.head(), len(pubmed_covered_not_indexed))
    # print('Pubmed Not Covered:\n', pubmed_not_covered.head(), len(pubmed_not_covered))
    #
    # print('Retraction Watch Indexed:\n', retraction_watch_indexed.head(), len(retraction_watch_indexed))
    # print('Retraction Watch All Covered:\n', retraction_watch_covered.head(), len(retraction_watch_covered))
    # print('Retraction Watch Covered Not Indexed:\n', retraction_watch_covered_not_indexed.head(),
    #       len(retraction_watch_covered_not_indexed))
    # print('Retraction Watch Not Covered:\n', retraction_watch_not_covered.head(), len(retraction_watch_not_covered))

    results_df = pd.DataFrame(columns=["Source",
                                       "Items indexed as retracted",
                                       "Items covered \n that are indexed as retracted in 1+ sources",
                                       "Items covered but not indexed as retracted",
                                       "Items not covered"])

    pubmed_row = {
        'Source': 'PubMed',
        'Items indexed as retracted': len(pubmed_indexed),
        'Items covered \n that are indexed as retracted in 1+ sources': len(pubmed_covered),
        'Items covered but not indexed as retracted': len(pubmed_covered_not_indexed),
        'Items not covered': len(pubmed_not_covered),
    }

    retraction_watch_row = {
        'Source': 'Retraction Watch',
        'Items indexed as retracted': len(retraction_watch_indexed),
        'Items covered \n that are indexed as retracted in 1+ sources': len(retraction_watch_covered),
        'Items covered but not indexed as retracted': len(retraction_watch_covered_not_indexed),
        'Items not covered': len(retraction_watch_not_covered),
    }

    results_df.loc[len(results_df)] = pubmed_row
    results_df.loc[len(results_df)] = retraction_watch_row
    results_df.to_csv(f'../data/{str(date.today())}_aggregate_results.csv')

    return (pubmed_indexed,
            pubmed_covered,
            pubmed_covered_not_indexed,
            pubmed_not_covered,
            retraction_watch_indexed,
            retraction_watch_covered,
            retraction_watch_covered_not_indexed,
            retraction_watch_not_covered)


def calculate_pairwise_agreement(unionlist: pd.DataFrame):
    """
    Calculates pairwise agreement for two sources, that is
    (all items both sources index) / (all items both sources cover)
    :param unionlist: dataframe of completed unionlist
    :return: pairwise agreement as percentage value
    """
    both_index = unionlist[(unionlist['Indexed_In'].str.contains('Retraction Watch; PubMed', na=False))]
    both_cover = unionlist[(unionlist['Covered_In'].str.contains('Retraction Watch; PubMed', na=False))]

    pairwise_agreement = (len(both_index) / len(both_cover)) * 100

    return pairwise_agreement


def create_indexing_upset_plot(pubmed_indexed: pd.DataFrame, retraction_watch_indexed: pd.DataFrame):
    """
    Creates UpSet plot for indexing status of unionlist items
    :param pubmed_indexed: dataframe of all items indexed in PubMed
    :param retraction_watch_indexed: dataframe of all items indexed in Retraction Watch
    :return: Indexing UpSet plot
    """
    sns.set_theme()
    upset_plot_indexing = from_contents({'PubMed': pubmed_indexed['DOI'],
                                         'Retraction Watch': retraction_watch_indexed['DOI'], })

    fig = plt.figure(figsize=(10, 6))
    plot(upset_plot_indexing,
         fig=fig,
         subset_size='count',
         show_counts=True,
         facecolor="#5D3A9B",
         sort_categories_by='-input',
         element_size=None)

    plt.suptitle('Indexing status overlap of reportedly-retracted items across PubMed and Retraction Watch', size=13)
    plt.ylim(0, 35000)
    #  plt.show()
    plt.savefig('../data/indexing_status_upset_plot.png')


def create_coverage_upset_plot(pubmed_covered: pd.DataFrame, retraction_watch_covered: pd.DataFrame):
    """
    Creates UpSet plot for coverage status of unionlist items
    :param pubmed_covered: dataframe of all items covered by PubMed
    :param retraction_watch_covered: dataframe of all items covered by Retraction Watch
    :return: Coverage UpSet plot
    """
    sns.set_theme()
    upset_plot_coverage = from_contents({'PubMed': pubmed_covered['DOI'],
                                         'Retraction Watch': retraction_watch_covered['DOI'], })

    fig = plt.figure(figsize=(10, 6))
    plot(upset_plot_coverage,
         fig=fig,
         subset_size='count',
         show_counts=True,
         facecolor="#E66100",
         sort_categories_by='-input',
         element_size=None)

    plt.suptitle('Coverage status overlap of reportedly-retracted items across PubMed and Retraction Watch', size=13)
    plt.ylim(0, 35000)
    #  plt.show()
    plt.savefig('../data/coverage_status_upset_plot.png')


def main():
    print("Several future warnings will be displayed. These can be ignored.")
    unionlist = read_in_unionlist_with_coverage(completed_unionlist_date='2025-05-08')
    (pubmed_indexed,
     pubmed_covered,
     pubmed_covered_not_indexed,
     pubmed_not_covered,
     retraction_watch_indexed,
     retraction_watch_covered,
     retraction_watch_covered_not_indexed,
     retraction_watch_not_covered) = (
        create_results_table(unionlist)
    )

    create_indexing_upset_plot(pubmed_indexed=pubmed_indexed, retraction_watch_indexed=retraction_watch_indexed)

    create_coverage_upset_plot(pubmed_covered=pubmed_covered, retraction_watch_covered=retraction_watch_covered)

    pairwise_agreement = calculate_pairwise_agreement(unionlist)
    print(f'Pairwise agreement is {pairwise_agreement}')


if __name__ == '__main__':
    main()
