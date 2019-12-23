"""
Module to download all latest survey_monkey data
into gcs for analysis and dashboard updating
---------------------
@Author: Gabriel Yin
"""
import os
import pandas as pd
from google.cloud import storage, bigquery
import warnings
warnings.filterwarnings("ignore")

class SurveyMonkeyDataLoader():
    """
    Class definition for surveymonkey data loader 
    """
    def __init__(self):
        """
        class initialization
        """
        self.PROJECT_ID = 'hawkfish-prod-0c4ce6d0'
        client = storage.Client()
    
    def download_from_big_query(self):
        """
        Function to download from big query table
        """
        client = bigquery.Client()
        query_sm = """
        SELECT 
         *
        FROM survey_monkey.survey_monkey;
        """
        query_job = client.query(
            query_sm,
            location="US"
        )
        print("-" * 20)
        print("Start downloading survey monkey data..")
        survey_monkey = query_job.to_dataframe()
        print("-" * 20)
        print("Download has finished..")
        
        storage_client = storage.Client(project=self.PROJECT_ID)
        bucket = storage_client.get_bucket("gabriel_bucket_test")
        blob = bucket.blob("agg_surveymonkey_data.csv")
        blob.upload_from_string(survey_monkey.to_csv(index=False,
                                                    encoding="utf-8")) 
        
    
