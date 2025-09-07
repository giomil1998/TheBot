import os

import pandas as pd

from src.wrds_api.WRDSCredentialsLoader import EnvironmentLoader
from src.wrds_api.WRDSConnection import WRDSConnection


class DataHandler:
    @staticmethod
    def fetch_or_read_data(get_new_data, start_date, end_date):
        if get_new_data:
            funda, crsp = DataHandler.download_data(start_date, end_date)
            funda = DataHandler.add_piotroski_column_to_funda(funda)
            DataHandler.save_file_to_directory(funda, "../input_data", "funda.csv")
            DataHandler.save_file_to_directory(crsp, "../input_data", "crsp.csv")
            return funda, crsp
        else:
            return DataHandler.read_data('../input_data/funda.csv', '../input_data/crsp.csv')

    @staticmethod
    def download_data(start_date, end_date):
        print("Downloading Data")
        wrds_credentials = EnvironmentLoader.load_wrds_credentials()
        wrds_connection = WRDSConnection(wrds_credentials['wrds_username'], wrds_credentials['wrds_password'])
        funda = wrds_connection.download_fundamental_data(start_date, end_date)
        crsp = wrds_connection.download_crsp_data(start_date, end_date)
        wrds_connection.close()
        return funda, crsp

    @staticmethod
    def save_file_to_directory(funda, directory, file_name):
        if not os.path.exists(directory):
            os.makedirs(directory)
        print("Saving data")
        funda.to_csv(os.path.join(directory, file_name))

    @staticmethod
    def add_piotroski_column_to_funda(df):
        print("Calculating Piotroski scores")
        return df.groupby('cusip').apply(DataHandler.calculate_piotroski).reset_index(drop=True)

    # Process Fundamental Data to Calculate Piotroski Score
    @staticmethod
    def calculate_piotroski(df):
        df['roa'] = df['ni'] / df['at'].shift(1)
        df['cfo'] = df['oancf'] / df['at'].shift(1)
        df['delta_roa'] = df['roa'] - df['roa'].shift(1)
        df['accrual'] = df['cfo'] - df['roa']
        df['delta_liquid'] = (df['aco']/df['lco']) - (df['aco'].shift(1)/df['lco'].shift(1))
        df['delta_offering'] = df['csho'] - df['csho'].shift(1)
        df['delta_leverage'] = df['dltt'] / df['at'].shift(1) - df['dltt'].shift(1) / df['at'].shift(2)
        df['delta_margin'] = (df['sale'] - df['cogs']) / df['sale'] - (df['sale'].shift(1) - df['cogs'].shift(1)) / df['sale'].shift(1)
        df['delta_turn'] = df['sale'] / df['at'] - df['sale'].shift(1) / df['at'].shift(1)

        # Initialize the Piotroski column with zeros
        df['Score'] = 0

        # Add 1 for each criterion satisfied
        df['Score'] += (df['roa'] > 0).astype(int)
        df['Score'] += (df['cfo'] > 0).astype(int)
        df['Score'] += (df['delta_roa'] > 0).astype(int)
        df['Score'] += (df['accrual'] > 0).astype(int)
        df['Score'] += (df['delta_offering'] <= 0).astype(int)
        df['Score'] += (df['delta_liquid'] > 0).astype(int)
        df['Score'] += (df['delta_leverage'] <= 0).astype(int)
        df['Score'] += (df['delta_margin'] > 0).astype(int)
        df['Score'] += (df['delta_turn'] > 0).astype(int)

        return df

    @staticmethod
    def read_data(funda_file_path, crsp_file_path):
        """Read the fundamental and CRSP data from CSV files."""
        print("Loading data")
        funda = pd.read_csv(funda_file_path)
        crsp = pd.read_csv(crsp_file_path)
        return funda, crsp

    @staticmethod
    def clean_funda(funda, start_date, end_date, market_cap_threshold, crsp):
        """Clean the funda DataFrame by removing duplicates, filtering missing years, and cleaning CUSIP."""
        print("Cleaning funda dataframe")
        funda = DataHandler.standardize_date(funda, 'datadate')
        funda = DataHandler.drop_first_year_of_each_ticker(funda)
        funda = DataHandler.filter_time_range(funda, "datadate", start_date, end_date)
        funda = DataHandler.filter_duplicates(funda)
        funda = DataHandler.filter_missing_years(funda)
        funda = DataHandler.standardize_cusips(funda, 'cusip')
        funda = DataHandler.filter_funda_by_market_cap(funda, market_cap_threshold, crsp)
        funda = funda.sort_values('datadate')
        return funda

    @staticmethod
    def clean_crsp(crsp, start_date, end_date):
        """Clean the crsp DataFrame by ensuring CUSIPs are 8 character long strings."""
        print("Cleaning crsp dataframe")
        crsp = DataHandler.standardize_date(crsp, 'date')
        crsp = DataHandler.filter_time_range(crsp, "date", start_date, end_date)
        crsp = DataHandler.standardize_cusips(crsp, 'cusip')
        return crsp

    @staticmethod
    def filter_duplicates(df):
        """Remove duplicate rows with identical 'cusip', 'datadate'"""
        return df.drop_duplicates(subset=['cusip', 'datadate']).reset_index(drop=True)

    @staticmethod
    def filter_missing_years(df):
        """Filters out rows with missing values in columns used to calculate score."""
        return df.dropna(subset=['roa', 'cfo', 'delta_leverage', 'delta_margin', 'delta_turn'])

    @staticmethod
    def standardize_cusips(df, cusip_column):
        """Ensure that the CUSIP column contains strings of 8 characters, trimming and formatting as necessary."""
        df[cusip_column] = df[cusip_column].astype(str).str.strip()  # Converts CUSIP codes into strings and trims spaces
        df[cusip_column] = df[cusip_column].str[:8]  # Keeps first 8 characters
        #TODO: Check matching with cusip codes longer than 8 digits.
        return df

    @staticmethod
    def standardize_date(df, date_column):
        """Ensure that the date column contains datetime objects."""
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')  # Coerce invalid dates to NaT
        return df

    @staticmethod
    def drop_first_year_of_each_ticker(funda):
        """Drop the earliest row for each ticker (tic) in the funda DataFrame."""
        # Sort by 'datadate' to ensure the earliest dates are at the top for each 'tic'
        funda = funda.sort_values(by=['tic', 'datadate'])
        # Drop the first occurrence of each 'tic' and keep the rest
        funda = funda.groupby('tic').apply(lambda x: x.iloc[1:]).reset_index(drop=True)
        return funda

    @staticmethod
    def filter_time_range(funda, column_name, start_date, end_date):
        return funda[(funda[column_name] >= start_date) & (funda[column_name] <= end_date)].copy()

    @staticmethod
    def filter_funda_by_market_cap(funda, market_cap_threshold, crsp):
        """Filter funda based on market cap threshold using values from crsp on the closest available date."""
        # Calculate market cap in crsp data
        crsp = DataHandler.calculate_market_cap(crsp)
        funda = DataHandler.merge_funda_with_crsp(funda, crsp)
        filtered_funda = DataHandler.apply_market_cap_threshold(funda, market_cap_threshold)
        return filtered_funda

    @staticmethod
    def calculate_market_cap(crsp):
        """Calculate market cap in the crsp DataFrame."""
        crsp['market_cap'] = crsp['prc'] * crsp['shrout'] * 1000  # shrout is in thousands
        return crsp

    @staticmethod
    def merge_funda_with_crsp(funda, crsp):
        """Merge funda with crsp to get market cap on the closest date for each datadate in funda."""
        funda['datadate'] = pd.to_datetime(funda['datadate'])
        crsp['date'] = pd.to_datetime(crsp['date'])

        # Merge on 'cusip' and closest date prior to or on 'datadate'.
        # Sometimes 'datadate' is on the weekend so this is necessary.
        merged_funda = pd.merge_asof(
            funda.sort_values('datadate'),
            crsp[['cusip', 'date', 'market_cap']].sort_values('date'),
            left_on='datadate',
            right_on='date',
            by='cusip',
            direction='backward'
        )
        return merged_funda

    @staticmethod
    def apply_market_cap_threshold(funda, market_cap_threshold):
        """Filter rows in funda where market cap meets or exceeds the threshold."""
        return funda[funda['market_cap'] >= market_cap_threshold].drop(columns=['date'])