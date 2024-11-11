import os
import pandas as pd
from EnvironmentLoader import EnvironmentLoader
from WRDSConnection import WRDSConnection
from datetime import datetime

class DataHandler:
    @staticmethod
    def fetch_and_save_new_data(start_date, end_date):
        print("Downloading Data")
        wrds_credentials = EnvironmentLoader.load_wrds_credentials()
        wrds_connection = WRDSConnection(wrds_credentials['wrds_username'], wrds_credentials['wrds_password'])
        funda = wrds_connection.fetch_fundamental_data(start_date, end_date)
        crsp = wrds_connection.fetch_crsp_data(start_date, end_date)
        wrds_connection.close()

        print("Calculating Piotroski Scores")
        DataHandler.add_piotroski_column_to_funda(funda)
        # Ensure the 'data' directory exists, create it if not
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        print("Saving data")
        funda.to_csv(os.path.join(data_dir, "funda.csv"))
        crsp.to_csv(os.path.join(data_dir, "crsp.csv"))
        return funda, crsp

    @staticmethod
    def add_piotroski_column_to_funda(df):
        return df.groupby('cusip').apply(DataHandler.calculate_piotroski).reset_index(drop=True)

    # Process Fundamental Data to Calculate Piotroski Score
    @staticmethod
    def calculate_piotroski(df):
        df['roa'] = df['ni'] / df['at'].shift(1)
        df['cfo'] = df['oancf'] / df['at'].shift(1)
        df['delta_roa'] = df['roa'] - df['roa'].shift(1)
        df['accrual'] = df['cfo'] - df['roa']
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
    def clean_funda(funda, start_date, end_date):
        """Clean the funda DataFrame by removing duplicates, filtering missing years, and cleaning CUSIP."""
        print("Cleaning funda dataframe")
        funda = DataHandler.standardize_date(funda, 'datadate')
        funda = DataHandler.drop_first_year_of_each_ticker(funda)
        funda = DataHandler.add_piotroski_column_to_funda(funda)
        funda = filter_time_range(funda, "datadate", start_date, end_date)
        funda = DataHandler.filter_duplicates(funda)
        funda = DataHandler.filter_missing_years(funda)
        funda = DataHandler.standardize_cusips(funda, 'cusip')
        funda = funda.sort_values('datadate')
        return funda

    @staticmethod
    def clean_crsp(crsp, start_date, end_date, market_cap_threshold=1_000_000_000, inactivity_threshold=30):
        """Clean the crsp DataFrame by ensuring CUSIPs are standardized and filtering by market cap."""
        print("Cleaning crsp dataframe")

        # Filter data within the time range
        crsp = filter_time_range(crsp, "date", pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date())

        # Standardize CUSIPs and dates
        crsp = DataHandler.standardize_cusips(crsp, 'cusip')
        crsp = DataHandler.standardize_date(crsp, 'date')

        # Calculate market capitalization
        crsp['market_cap'] = crsp['prc'] * crsp['shrout'] * 1000  # shrout is in thousands

        # Filter companies with market cap below threshold for too many consecutive days
        def filter_below_threshold(group):
            # Boolean series: True if below threshold
            below_threshold = group['market_cap'] < market_cap_threshold

            # Identify streaks of consecutive days below threshold
            streak_groups = (below_threshold != below_threshold.shift()).cumsum()
            max_streak = below_threshold.groupby(streak_groups).transform('sum').max()

            # Drop group if max streak exceeds inactivity threshold
            if max_streak > inactivity_threshold:
                return None
            return group

        # Apply the filter by group
        crsp = crsp.groupby('cusip').apply(filter_below_threshold)

        # Drop invalid groups and reset the index
        crsp = crsp.dropna(subset=['cusip']).reset_index(drop=True)

        return crsp

    @staticmethod
    def filter_duplicates(df):
        """Remove duplicate rows with identical 'cusip', 'datadate', and 'Score'"""
        return df.drop_duplicates(subset=['cusip', 'datadate', 'Score']).reset_index(drop=True)

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

def filter_time_range(funda, column_name, start_date, end_date):
    return funda[(funda[column_name] >= start_date) & (funda[column_name] <= end_date)].copy()