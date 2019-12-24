# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from google.cloud import storage, bigquery
from bluelabs import BluelabsDataLoader, BluelabsDataAggregator
from survey_monkey import SurveyMonkeyDataLoader
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    #SurveyMonkeyDataLoader().download_from_big_query()
    #BluelabsDataLoader().download_from_big_query()
    bl = BluelabsDataAggregator().clean().voter_age_zip().decode_cols().save()
    sm = SurveyMonkeyDataLoader().download_from_big_query().clean().decode().save()
