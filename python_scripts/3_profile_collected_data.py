"""
This file contains methods for profiling incoming data.

Functions overview:
profile_retrieved_data(filename): creates ydata_profiling report for listed csv file.
main: runs profile_retrieved_data with varied parameters
"""
import pandas as pd
from ydata_profiling import ProfileReport


def profile_retrieved_data(filename):
    """
    Creates ydata_profiling report for listed csv file.
    :param filename: name of file in /data/ folder to profile
    :return: html ydata_profiling report
    """
    df = pd.read_csv(f"../data/{filename}")
    ydata_profile = ProfileReport(df, title=f"{filename} ydata profiling report")
    ydata_profile.to_file(f"../data/{filename[:-4]}_profiling_report.html")


def main():
    file = "2025-05-08_retraction_watch.csv"
    profile_retrieved_data(file)


if __name__ == "__main__":
    main()
