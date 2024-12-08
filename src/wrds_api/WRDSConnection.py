import wrds


# Fetch Piotroski F-scores and stock prices
# Assume Compustat data for Piotroski scores and CRSP data for stock prices

# Piotroski F-scores (Fundamental Data)
#lt - liabilities
#comp.fundq name of compustat database

class WRDSConnection:
    def __init__(self, username, password):
        self.db = wrds.Connection(wrds_username=username, wrds_password=password)

    def fetch_annual_fundamental_data(self, start_date, end_date):
        print('Fetching fundamental annual data')
        funda_query = f"""
            SELECT gvkey, datadate, fyear, fic, tic,
                   at, lt, pstkl, txditc, pstkrv,
                   txdb, pstk, ni, oancf, dltt, mkvalt, ebit, epspx,
                   dlc, ivst, che, re,
                   sale, cogs, xsga, xint, xrd, dp, cusip
            FROM comp.funda
            WHERE indfmt = 'INDL'
            AND datafmt = 'STD'
            AND popsrc = 'D'
            AND consol = 'C'
            AND datadate BETWEEN '{start_date}' AND '{end_date}'
        """
        return self.db.raw_sql(funda_query)

    def fetch_quarterly_fundamental_data(self, start_date, end_date):
        print('Fetching fundamental quarterly data')
        funda_query = f"""
            SELECT gvkey, datadate, fyearq, fic, tic, fqtr,
                   atq, ltq, /*pstklq,*/ txditcq, pstkrq,
                   txdbq, pstkq, niq, oancfy, dlttq, mkvaltq, epspxq,
                   dlcq, ivstq, cheq, req,
                   saleq, cogsq, xsgaq, xintq, xrdq, dpq, cusip
            FROM comp.fundq
            WHERE indfmt = 'INDL'
            AND datafmt = 'STD'
            AND popsrc = 'D'
            AND consol = 'C'
            AND datadate BETWEEN '{start_date}' AND '{end_date}'
        """
        return self.db.raw_sql(funda_query)

    def fetch_crsp_data(self, start_date, end_date):
        print('Fetching crsp data')
        crsp_query = f"""
            SELECT permno, date, ret, prc, shrout, cusip
            FROM crsp.dsf
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """
        return self.db.raw_sql(crsp_query)

    def close(self):
        self.db.close()