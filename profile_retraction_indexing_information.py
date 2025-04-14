"""
This file contains methods for profiling incoming information.

Functions overview:

"""
import pandas as pd
from ydata_profiling import ProfileReport


def profile_retrieved_data(filename):
    df = pd.read_csv(f"data/{filename}")
    ydata_profile = ProfileReport(df, title=f"{filename} ydata profiling report")
    ydata_profile.to_file(f"data/{filename[:-4]}_profiling_report.html")


def main():
    file = "2025-04-13_pubmed.csv"
    profile_retrieved_data(file)


if __name__ == "__main__":
    main()
