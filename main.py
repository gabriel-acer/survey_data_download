import os
import pandas as pd
from google.cloud import storage, bigquery
import warnings
from bluelabs import BluelabsDataLoader
from survey_monkey import SurveyMonkeyDataLoader
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    SurveyMonkeyDataLoader().download_from_big_query()
    BluelabsDataLoader().download_from_big_query()
    BluelabsDataAggregator().clean().voter_age_zip().decode_cols().save()
