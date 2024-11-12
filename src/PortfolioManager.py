import pandas as pd


class PortfolioManager:
    def __init__(self, inactivity_threshold, long_portfolio_size, short_portfolio_size):
        self.inactivity_threshold = inactivity_threshold
        self.long_portfolio_size = long_portfolio_size
        self.short_portfolio_size = short_portfolio_size
        self.long_portfolio = {}  # {cusip: {'score': Score, 'entry_date': date, 'last_traded_date': date}}
        self.short_portfolio = {}
        self.company_scores = pd.DataFrame(columns=['cusip', 'score', 'datadate'])
        self.company_scores.set_index('cusip', inplace=True)

    def purge_inactive_from_company_scores(self, current_date):
        """Purge companies from company_scores if they haven't published reports for over a year."""
        # Calculate the date exactly one year before the current date
        yearly_inactivity_cutoff = current_date - pd.DateOffset(years=1)

        # Identify companies in company_scores that haven't published a report for over a year
        inactive_companies = self.company_scores[self.company_scores['datadate'] < yearly_inactivity_cutoff].index
        self.company_scores.drop(inactive_companies, inplace=True)

    def remove_inactive_holdings(self, portfolio, current_date):
        """Remove inactive holdings from a given portfolio and company_scores."""
        to_remove = []
        for cusip, info in portfolio.items():
            days_inactive = (current_date - info['last_traded_date']).days
            if days_inactive > self.inactivity_threshold:
                to_remove.append(cusip)
        for cusip in to_remove:
            del portfolio[cusip]
            if cusip in self.company_scores.index:
                self.company_scores.drop(cusip, inplace=True)

    def update_company_scores(self, new_reports, current_date):
        self.purge_inactive_from_company_scores(current_date)
        for idx, row in new_reports.iterrows():
            cusip = row['cusip']
            score = row['Score']
            # Check if the cusip already exists in company_scores
            if cusip in self.company_scores.index:
                # Update only if the new datadate is more recent than the existing one
                if current_date > self.company_scores.loc[cusip, 'datadate']:
                    self.company_scores.loc[cusip] = {'score': score, 'datadate': current_date}
            else:
                # Add new entry if the cusip does not exist
                self.company_scores.loc[cusip] = {'score': score, 'datadate': current_date}

    def build_portfolios(self, current_date, long_portfolio_size, short_portfolio_size):
        if not self.company_scores.empty:
            # Get the top companies by score
            sorted_scores = self.company_scores.sort_values(by=['score', 'datadate'], ascending=[False, True])
            top_companies = sorted_scores.head(long_portfolio_size)
            bottom_companies = sorted_scores.tail(short_portfolio_size)

            self.long_portfolio = self._update_portfolio(self.long_portfolio, top_companies, current_date)
            self.short_portfolio = self._update_portfolio(self.short_portfolio, bottom_companies, current_date)
        else:
            self.long_portfolio = {}
            self.short_portfolio = {}

    def _update_portfolio(self, existing_portfolio, companies, current_date):
        """Create a new portfolio based on the given companies, retaining last_traded_date for existing entries."""
        new_portfolio = {}

        for cusip in companies.index:
            score = companies.loc[cusip, 'score']

            # Retain the last_traded_date if the company is already in the existing portfolio
            if cusip in existing_portfolio:
                last_traded_date = existing_portfolio[cusip]['last_traded_date']
            else:
                # Initialize last_traded_date to current_date if it's a new entry
                last_traded_date = current_date

            new_portfolio[cusip] = {
                'score': score,
                'entry_date': current_date,
                'last_traded_date': last_traded_date
            }
        return new_portfolio


    def remove_inactive_holdings_from_portfolios(self, current_date):
        """Remove inactive holdings from both long and short portfolios and company scores."""
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
