# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from google.cloud import storage, bigquery
from bluelabs import BluelabsDataLoader, BluelabsDataAggregator
from survey_monkey import SurveyMonkeyDataLoader
from combine_survey_data import SurveyDataCombiner
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    #SurveyMonkeyDataLoader().download_from_big_query()
    #BluelabsDataLoader().download_from_gcs()
    #bl = BluelabsDataAggregator().run()
    sm = SurveyMonkeyDataLoader().run()
    final = SurveyDataCombiner().update_misc_graphs()
