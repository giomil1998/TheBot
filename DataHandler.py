import pandas as pd

from EnvironmentLoader import EnvironmentLoader
from WRDSConnection import WRDSConnection

class DataHandler:
    @staticmethod
    def fetch_and_save_new_data(start_date, end_date):
        wrds_credentials = EnvironmentLoader.load_wrds_credentials()
        wrds_connection = WRDSConnection(wrds_credentials['wrds_username'], wrds_credentials['wrds_password'])
        funda = wrds_connection.fetch_fundamental_data(start_date, end_date)
        crsp = wrds_connection.fetch_crsp_data(start_date, end_date)
        wrds_connection.close()

        DataHandler.add_piotroski_column_to_funda(funda)

        funda.to_csv("data/funda.csv")
        crsp.to_csv("data/crsp.csv")
        return funda, crsp

    @staticmethod
    def add_piotroski_column_to_funda(df):
        return df.groupby('cusip').apply(DataHandler.calculate_piotroski(df)).reset_index(drop=True)

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
        funda = pd.read_csv(funda_file_path)
        crsp = pd.read_csv(crsp_file_path)
        return funda, crsp

    @staticmethod
    def clean_funda(funda):
        """Clean the funda DataFrame by removing duplicates, filtering missing years, and cleaning CUSIP."""
        funda = DataHandler.filter_duplicates(funda)
        funda = DataHandler.filter_missing_years(funda)
        funda = DataHandler.standardize_cusips(funda, 'cusip')
        funda = DataHandler.standardize_date(funda, 'datadate')
        funda = funda.sort_values('datadate')
        return funda

    @staticmethod
    def clean_crsp(crsp):
        """Clean the crsp DataFrame by ensuring CUSIPs are 8 characters long and strings."""
        crsp = DataHandler.standardize_cusips(crsp, 'cusip')
        crsp = DataHandler.standardize_date(crsp, 'date')
        return crsp

    @staticmethod
    def filter_duplicates(df):
        """Remove duplicate rows with identical 'cusip', 'datadate', and 'Score'"""
        return df.drop_duplicates(subset=['cusip', 'datadate', 'Score']).reset_index(drop=True)

    @staticmethod
    def filter_missing_years(df):
        """Filters out rows with missing data."""
        return df[df['at'].notna()]

    @staticmethod
    def standardize_cusips(df, cusip_column):
        """Ensure that the CUSIP column contains strings of 8 characters, trimming and formatting as necessary."""
        df[cusip_column] = df[cusip_column].astype(str).str.strip()
        df[cusip_column] = df[cusip_column].str[:8]
        return df

    @staticmethod
    def standardize_date(df, date_column):
        """Ensure that the date column contains datetime objects."""
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')  # Coerce invalid dates to NaT
        return df