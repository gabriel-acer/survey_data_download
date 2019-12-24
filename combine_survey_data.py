# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from google.cloud import storage, bigquery
from bluelabs import BluelabsDataLoader, BluelabsDataAggregator
from survey_monkey import SurveyMonkeyDataLoader
import warnings
warnings.filterwarnings("ignore")

class SurveyDataCombiner():
    """
    Class to combine survey data files
    """
    def __init__(self):

        target_cols = ['date', 'respondents_id', 'state', 'zipcode', 'gender',
       'religion', 'hispanic', 'turnout', 'race',
       'education', 'age', 'name_first_choice_candidates',
       'rate_klobuchar', 'rate_yang', 'rate_sanders', 'rate_booker',
       'rate_warren', 'rate_biden', 'rate_castro',
       'rate_bloomberg', 'rate_bennet', 'rate_buttigieg',
        'rate_gabbard', 'rate_steyer', 'bloomberg_support', 'age_bin', 'response_status', 'source_id']

        self.bl_data = pd.read_csv("gcs://gabriel_bucket_test/bluelabs_superset.csv")
        self.sm_data = pd.read_csv("gcs://gabriel_bucket_test/agg_surveymonkey_data.csv")
        self.combined_data = self.bl_data[target_cols].append(self.sm_data[target_cols])
        
        PROJECT_ID = 'hawkfish-prod-0c4ce6d0'
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.get_bucket("gabriel_bucket_test")
        print('-' * 20)
        print('Saving combined dataset..')
        blob = bucket.blob("bluelabs_surveymonkey_agg.csv")
        blob.upload_from_string(self.combined_data.to_csv(index=False))
        print('-' * 20)
        print('Saving complete.')
