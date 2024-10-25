import pandas as pd

class PortfolioManager:
    def __init__(self, inactivity_threshold, portfolio_size):
        self.inactivity_threshold = inactivity_threshold
        self.portfolio_size = portfolio_size
        self.long_portfolio = {}   # {cusip: {'score': Score, 'entry_date': date, 'last_traded_date': date}}
        self.short_portfolio = {}
        self.company_scores = pd.DataFrame(columns=['cusip', 'score', 'datadate'])
        self.company_scores.set_index('cusip', inplace=True)

    def remove_inactive_holdings(self, portfolio, current_date):
        to_remove = []
        for cusip, info in portfolio.items():
            days_inactive = (current_date - info['last_traded_date']).days
            if days_inactive > self.inactivity_threshold:
                to_remove.append(cusip)
        for cusip in to_remove:
            del portfolio[cusip]

    def update_company_scores(self, new_reports):
        for idx, row in new_reports.iterrows():
            cusip = row['cusip']
            score = row['Score']
            datadate = row['datadate']
            self.company_scores.loc[cusip] = {'score': score, 'datadate': datadate}

    def build_portfolios(self, current_date):
        if not self.company_scores.empty:
            # Get the top and bottom companies by score
            sorted_scores = self.company_scores.sort_values(by=['score', 'datadate'], ascending=[False, True])
            top_companies = sorted_scores.head(self.portfolio_size)
            bottom_companies = sorted_scores.tail(self.portfolio_size)

            # Update or add companies to long portfolio
            self._update_portfolio(self.long_portfolio, top_companies, current_date)

            # Update or add companies to short portfolio
            self._update_portfolio(self.short_portfolio, bottom_companies, current_date)
        else:
            self.long_portfolio = {}
            self.short_portfolio = {}

    def _update_portfolio(self, portfolio, companies, current_date):
        for cusip in companies.index:
            score = companies.loc[cusip, 'score']
            datadate = companies.loc[cusip, 'datadate']
            if cusip not in portfolio:
                portfolio[cusip] = {
                    'score': score,
                    'entry_date': current_date,
                    'last_traded_date': current_date  # Initialize with current date
                }
            else:
                portfolio[cusip]['score'] = score  # Update score if it has changed

    def remove_inactive_holdings_from_portfolios(self, current_date):
        self.remove_inactive_holdings(self.long_portfolio, current_date)
        self.remove_inactive_holdings(self.short_portfolio, current_date)

    def update_last_traded_date(self, traded_cusips, current_date):
        for cusip in self.long_portfolio.keys():
            if cusip in traded_cusips:
                self.long_portfolio[cusip]['last_traded_date'] = current_date

        for cusip in self.short_portfolio.keys():
            if cusip in traded_cusips:
                self.short_portfolio[cusip]['last_traded_date'] = current_date

    def get_current_portfolios(self):
        long_cusips = list(self.long_portfolio.keys())
        short_cusips = list(self.short_portfolio.keys())
        return long_cusips, short_cusips