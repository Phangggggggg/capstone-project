import finnhub

class FinnhubClient:
    def __init__(self, api_key):
        """
        Initialize the FinnhubClient with the given API key.
        
        Parameters:
        - api_key: Your Finnhub API key.
        """
        self.client = finnhub.Client(api_key=api_key)

    def get_visa_data(self, symbol, start_date, end_date):
        """
        Fetch stock data for a given symbol and date range.

        Parameters:
        - symbol: Stock symbol (e.g., "AAPL").
        - start_date: Start date in "YYYY-MM-DD" format.
        - end_date: End date in "YYYY-MM-DD" format.

        Returns:
        - Data fetched from the API, or an error message.
        """
        try:
            # Fetching stock data (this is just an example method call; adjust based on the actual method you need)
            data = self.client.stock_visa_application(symbol, start_date, end_date)
            return data
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        
    def get_stock_data(self,symbol):
        try:
            # Fetching stock data (this is just an example method call; adjust based on the actual method you need)
            data = self.client.quote(symbol)
            return data
        except Exception as e:
            print(f"An error occurred: {e}")
            return None