from dotenv import load_dotenv
import os

class EnvironmentLoader:
    @staticmethod
    def load_wrds_credentials():
        # Load environment variables from the .env file
        load_dotenv()
        return {
            "wrds_username": os.getenv('WRDS_USERNAME'),
            "wrds_password": os.getenv('WRDS_PASSWORD')
        }