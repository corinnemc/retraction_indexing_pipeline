"""
This file contains methods to completely run the retraction indexing pipeline.
"""
from a_retraction_watch_data_collection import *
from b_pubmed_data_collection import *
from c_profile_collected_data import *
from d_create_initial_unionlist import *
from e_pubmed_coverage_query import *
from f_update_unionlist_with_coverage import *
from g_data_analysis import *

# Input your email.
EMAIL = 'corinne9@illinois.edu'
# Get most recent commit SHA from https://gitlab.com/crossref/retraction-watch-data
RETRACTION_WATCH_COMMIT_ID = '28e4670f328f4879ebfbbe2ae66e5f50b0e02184'


def main():
    print("All information is saved to the /data/ folder.")
    print("1. Gathering Retraction Watch data...")
    data_path = f"../data"
    commit_id = RETRACTION_WATCH_COMMIT_ID
    get_gitlab_file_and_sha(data_path, commit_id)

    print("2. Gathering PubMed data...")
    get_pubmed_data(
        start_year=1950,  # Via the PubMed interface, retracted publications start in 1951
                          # https://pubmed.ncbi.nlm.nih.gov/?term="Retracted+Publication"[pt]
        end_year=date.today().year,
        interval_year=2,  # Chose a year interval where there are not more than 10,000 results returned;
                          # PubMed can only return 10,000 results per request.
                          # In 2022 there were over 6,000 retractions, so current best practice is year interval of 2.
        term="'Retracted Publication'[PT]",
        email=EMAIL,
        no_records=300
    )

    print("3. Data profiling")
    print("Profiling PubMed data...")
    pubmed_file = f"{str(date.today())}_pubmed.csv"
    profile_retrieved_data(pubmed_file)
    print("Profiling Retraction Watch data...")
    retraction_watch_file = f"{str(date.today())}_retraction_watch.csv"
    profile_retrieved_data(retraction_watch_file)

    print("4. Creating initial unionlist...")
    print("Reading in datasets...")
    pubmed = clean_pubmed_data(pubmed_date=f'{str(date.today())}')
    retraction_watch = clean_retraction_watch_data(retraction_watch_date=f'{str(date.today())}')
    print("Creating overview table...")
    create_overview_table(pubmed=pubmed, retraction_watch=retraction_watch)
    print("Saving records with DOI, without DOI, and duplicates in separate files...")
    save_records_in_subgroups(dataframe_list=[pubmed, retraction_watch], source_names=['pubmed', 'retraction_watch'])
    print("Merging records with DOI into union list...")
    print("This can take some time as the program iterates over thousands of rows.")
    create_union_list()

    print("5. Querying PubMed for items it does not index as retracted")
    unionlist = read_in_unionlist_without_coverage(unionlist_date=f'{str(date.today())}')
    pubmed_not_indexed = gather_pubmed_not_indexed(unionlist=unionlist)
    items_covered_not_indexed = query_pubmed_with_pmid(pubmed_not_indexed=pubmed_not_indexed,
                                                       email=EMAIL)
    pubmed_not_in_unionlist = unionlist[
                                unionlist['PubMedID'].isin(items_covered_not_indexed) &
                                (~unionlist['Indexed_In'].str.contains('PubMed'))
    ]
    print("A SettingWithCopyWarning will be displayed. This can be ignored.")
    pubmed_not_in_unionlist["Covered_In"] = "Retraction Watch; PubMed"
    pubmed_not_in_unionlist.to_csv(f'../data/pubmed_coverednotindexed_{str(date.today())}.csv')

    print("6. Updating unionlist with coverage information...")
    print("Reading in data...")
    unionlist = read_in_unionlist_without_coverage(unionlist_date=f'{str(date.today())}')
    pubmed_covered_not_indexed = read_in_covered_not_indexed(pubmed_date=f'{str(date.today())}')
    print("Merging dataframes...")
    print("This can take some time as the program iterates over thousands of rows.")
    merge_dataframes(unionlist=unionlist,
                     pubmed_covered_not_indexed=pubmed_covered_not_indexed,
                     unionlist_date=f'{str(date.today())}')

    print("7. Completing data analysis...")
    print("Several FutureWarnings will be displayed. These can be ignored.")
    unionlist = read_in_unionlist_with_coverage(unionlist_date=f'{str(date.today())}')
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

    print("Pipeline is complete. Yay!")


if __name__ == '__main__':
    main()
