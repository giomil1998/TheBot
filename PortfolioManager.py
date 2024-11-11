import pandas as pd
from scipy.optimize import minimize


class PortfolioManager:
    def __init__(self, inactivity_threshold, long_portfolio_size, short_portfolio_size):
        self.inactivity_threshold = inactivity_threshold
        self.long_portfolio_size = long_portfolio_size
        self.short_portfolio_size = short_portfolio_size
        self.long_portfolio = {}  # {cusip: {'score': Score, 'entry_date': date, 'last_traded_date': date, 'weight': weight}}
        self.short_portfolio = {}
        self.company_scores = pd.DataFrame(columns=['cusip', 'score', 'datadate'])
        self.company_scores.set_index('cusip', inplace=True)

    def purge_inactive_from_company_scores(self, current_date):
        """Purge companies from company_scores if they haven't published reports for over a year."""
        yearly_inactivity_cutoff = current_date - pd.DateOffset(years=1)
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
            if cusip in self.company_scores.index:
                if current_date > self.company_scores.loc[cusip, 'datadate']:
                    self.company_scores.loc[cusip] = {'score': score, 'datadate': current_date}
            else:
                self.company_scores.loc[cusip] = {'score': score, 'datadate': current_date}

    def build_portfolios(self, current_date, long_portfolio_size, short_portfolio_size, crsp, lookback_days=60, method="inverse_volatility"):
        if not self.company_scores.empty:
            sorted_scores = self.company_scores.sort_values(by=['score', 'datadate'], ascending=[False, True])
            top_companies = sorted_scores.head(long_portfolio_size)
            bottom_companies = sorted_scores.tail(short_portfolio_size)

            volatilities = self.calculate_historical_volatility(crsp, lookback_days, current_date)

            if method == "inverse_volatility":
                long_weights = self.calculate_inverse_volatility_weights(volatilities, top_companies.index)
                short_weights = self.calculate_inverse_volatility_weights(volatilities, bottom_companies.index)
            elif method == "minimum_variance":
                cov_matrix_long = self.calculate_covariance_matrix(crsp, top_companies.index, lookback_days, current_date)
                cov_matrix_short = self.calculate_covariance_matrix(crsp, bottom_companies.index, lookback_days, current_date)
                long_weights = self.calculate_minimum_variance_weights(cov_matrix_long)
                short_weights = self.calculate_minimum_variance_weights(cov_matrix_short)

            self.long_portfolio = self._update_portfolio(self.long_portfolio, top_companies, long_weights, current_date)
            self.short_portfolio = self._update_portfolio(self.short_portfolio, bottom_companies, short_weights, current_date)
        else:
            self.long_portfolio = {}
            self.short_portfolio = {}

    def calculate_historical_volatility(self, crsp, lookback_days, current_date):
        """Calculate historical volatility for companies."""
        lookback_start = current_date - pd.Timedelta(days=lookback_days)
        historical_data = crsp[(crsp['date'] >= lookback_start) & (crsp['date'] < current_date)]
        volatilities = historical_data.groupby('cusip')['ret'].std()
        return volatilities

    def calculate_inverse_volatility_weights(self, volatilities, cusips):
        """Assign weights inversely proportional to volatilities."""
        filtered_volatilities = volatilities[volatilities.index.isin(cusips)]
        weights = 1 / filtered_volatilities
        return weights / weights.sum()

    def calculate_covariance_matrix(self, crsp, cusips, lookback_days, current_date):
        """Calculate covariance matrix for selected stocks."""
        lookback_start = current_date - pd.Timedelta(days=lookback_days)
        historical_data = crsp[(crsp['date'] >= lookback_start) & (crsp['date'] < current_date)]
        filtered_data = historical_data[historical_data['cusip'].isin(cusips)]
        pivot_data = filtered_data.pivot(index='date', columns='cusip', values='ret').fillna(0)
        return pivot_data.cov()

    def calculate_minimum_variance_weights(self, cov_matrix):
        """Solve for minimum variance portfolio weights."""
        n = len(cov_matrix)
        initial_weights = np.ones(n) / n
        constraints = ({'type': 'eq', 'fun': lambda w: np.sum(w) - 1})
        bounds = [(0, 1) for _ in range(n)]

        def portfolio_variance(w):
            return np.dot(w.T, np.dot(cov_matrix, w))

        result = minimize(portfolio_variance, initial_weights, bounds=bounds, constraints=constraints)
        return result.x if result.success else None

    def _update_portfolio(self, existing_portfolio, companies, weights, current_date):
        """Update portfolio with weights."""
        new_portfolio = {}
        for cusip, weight in zip(companies.index, weights):
            score = companies.loc[cusip, 'score']
            last_traded_date = existing_portfolio.get(cusip, {}).get('last_traded_date', current_date)
            new_portfolio[cusip] = {
                'score': score,
                'weight': weight,
                'entry_date': current_date,
                'last_traded_date': last_traded_date
            }
        return new_portfolio

    def remove_inactive_holdings_from_portfolios(self, current_date):
        """Remove inactive holdings from both portfolios."""
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
        long_cusips = {cusip: data['weight'] for cusip, data in self.long_portfolio.items()}
        short_cusips = {cusip: data['weight'] for cusip, data in self.short_portfolio.items()}
        return long_cusips, short_cusips
