"""
Module to download all latest survey data
from survey_monkey and bluelabs then upload
to gcs in order to upload the dashboard
---------------------
@Author: Gabriel Yin 
"""
import os
import pandas as pd
from google.cloud import storage, bigquery
import warnings
warnings.filterwarnings("ignore")

class BluelabsDataLoader():
    """
    Class to download all bluelabs data
    with functions to clean column and 
    aggregate data
    """
    def __init__(self):
        """
        class initialization
        """
        self.PROJECT_ID = 'hawkfish-prod-0c4ce6d0'
        client = storage.Client()
        self.bucket = client.get_bucket('user_ground_truth')
        self.file_names = list(key.name for key in self.bucket.list_blobs() 
                               if key.name.startswith('bluelabs_raw_survey_returns/12')
                               or key.name.startswith('bluelabs_raw_survey_returns/2019'))

    def download(self):
        """
        Function to download all files up to date 
        from google bucket
        """
        path = 'survey_raw_data/'
        print('-' * 20)
        print("Stage 1/3: Start downloading all bluelabs data..")
        for file_name in self.file_names:
            blob = storage.Blob(file_name, self.bucket)
            blob.download_to_filename(path + file_name.split('/')[1])

        print('-' * 20)
        print("All data has been downloaded.")
        
        return self
    
    def clean_agg(self):
        """
        Function to read in file and do some cleaning and write to bucket
        """
        print('-' * 20)
        print("Stage 2/3: generating agg data..")
        raw_path = 'survey_raw_data/'
        filtered_dfs = []
        for file_name in self.file_names:
            raw_data = pd.read_csv(raw_path + file_name.split('/')[1])
            if raw_data.shape[1] == 45 or raw_data.shape[1] == 47:
                raw_data.columns = [col.lower() for col in raw_data.columns]
                raw_data = raw_data.rename(
                    columns={'qrate_mbpost':'qratepost',
                             'qturnout':'qturnoutprimary',
                             'qpostrate_text':'qratepost_text'
                            })
                filtered_dfs.append(raw_data)
            else:
                pass

        self.agg_df = filtered_dfs[0].append(filtered_dfs[1])
        for filtered_df in filtered_dfs[2:]:
            self.agg_df = self.agg_df.append(filtered_df)
            
        print('-' * 20)
        print("Agg data has been generated.")
        print(self.agg_df.shape)
        return self

    def save(self):
        """
        Function to save the intermediate cleaned results to bucket 
        """
        print('-' * 20)
        print("Stage 3/3: Saving agg data to hdfs..")
        storage_client = storage.Client(project=self.PROJECT_ID)
        bucket = storage_client.get_bucket("gabriel_bucket_test")
        blob = bucket.blob("agg_bluelabs_data.csv")
        blob.upload_from_string(self.agg_df.to_csv(index=False)) 
        
        print('-' * 20)
        print("agg_survey_data.csv has been successfully saved.")
        
        return self
    
    def download_from_gcs(self):
        """
        Driver function to download all data from gcs 
        """
        self.download().clean_agg().save()
    
    def download_from_big_query(self):
        """
        Driver function to download all data from 
        big query table
        """
        print('-' * 20)
        print("Start downloading from big query table...")
        client = bigquery.Client()
        query = """
        SELECT 
         *
        FROM bluelabs_survey_results.all_survey_results;
        """
        query_job = client.query(
            query,
            location="US"
         )
        bluelabs_agg = query_job.to_dataframe()
        
        print("-" * 20)
        print("Downloading finished.")
        
        storage_client = storage.Client(project=self.PROJECT_ID)
        bucket = storage_client.get_bucket("gabriel_bucket_test")
        blob = bucket.blob("agg_bluelabs_data.csv")
        blob.upload_from_string(bluelabs_agg.to_csv(index=False)) 
        
        print('-' * 20)
        print("Bluelabs data has been saved.")
        
