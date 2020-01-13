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
        'rate_gabbard', 'rate_steyer', 'bloomberg_support', 'age_bin', 'response_status', 'source_id',
                      'employement', 'income']

        self.bl_data = pd.read_csv("gcs://gabriel_bucket_test/bluelabs_superset.csv")
        self.sm_data = pd.read_csv("gcs://gabriel_bucket_test/agg_surveymonkey_data.csv")
        self.sm_data = self.sm_data.rename(columns={'employment_status':'employement'})
        self.combined_data = self.bl_data[target_cols].append(self.sm_data[target_cols])
        
        self.combined_data.respondents_id = self.combined_data.respondents_id.astype('str')
        self.combined_data['date'] = self.combined_data['date'].apply(lambda x: '-'.join(['20'+ x.split('/')[2], 
                                           x.split('/')[0],
                                           x.split('/')[1]]) if '/' in x else x)
        self.combined_data['date'] = pd.to_datetime(self.combined_data['date'])
        
        PROJECT_ID = 'hawkfish-prod-0c4ce6d0'
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.get_bucket("gabriel_bucket_test")
        print('-' * 20)
        print('Saving combined dataset..')
        blob = bucket.blob("bluelabs_surveymonkey_agg.csv")
        blob.upload_from_string(self.combined_data.to_csv(index=False))
        print('-' * 20)
        print('Saving complete. Start uploading to big query..')
        
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = bigquery_client.dataset('bluelabs_survey_monkey_combined')
        trf = bigquery.Table(dataset_ref.table('bl_sm_support'))
        bigquery_client.delete_table(trf)
        
        trf = bigquery.Table(dataset_ref.table('bl_sm_support'))
        new_table = bigquery_client.create_table(trf)
        
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        
        table_ref = dataset_ref.table('bl_sm_support')

        job = bigquery_client.load_table_from_dataframe(
        self.combined_data, table_ref, job_config=job_config)

    def update_misc_graphs(self):
        """
        Function to update misc graphs on dashboard
        """
        
        def load_from_gcs(BUCKET_NAME, filename):
            return pd.read_csv("gcs://" + BUCKET_NAME + '/' + filename)
        PROJECT_ID = 'hawkfish-prod-0c4ce6d0'
        bluelabs, survey_monkey = pd.read_csv("gcs://gabriel_bucket_test/bluelabs_superset.csv"), \
                         pd.read_csv("gcs://gabriel_bucket_test/agg_surveymonkey_data.csv")
        
        survey_monkey['candidates'] = survey_monkey['name_first_choice_candidates']
        survey_monkey.loc[survey_monkey.name_first_choice_candidates == 'None of the above',  'candidates'] = 'Other'
        survey_monkey.loc[survey_monkey.name_first_choice_candidates == 'No Answer',  'candidates']= 'Undecided'
        
        answered = survey_monkey[['date','source_id']].append(bluelabs[['date', 'source_id']])
        completed = survey_monkey[survey_monkey.response_status=='completed'][['date',
                    'source_id']].append(bluelabs[bluelabs.response_status=='completed'][['date', 'source_id']])

        temp = answered.groupby(['date'])['source_id'].value_counts().to_frame()
        temp = temp.rename(columns={'source_id':'answered_counts'}).reset_index()

        survey_counts = completed.groupby(['date'])['source_id'].value_counts().to_frame()

        survey_counts = survey_counts.rename(columns={'source_id':'completed_counts'}).reset_index()
        survey_counts = survey_counts.merge(temp, on=['date', 'source_id'], how='outer')


        # calculate totals for both dashboard
        temp = answered.date.value_counts().to_frame().reset_index()
        temp = temp.rename(columns={'date':'answered_counts', 
                                   'index':'date'})
        temp_2 = completed.date.value_counts().to_frame().reset_index()
        temp_2 = temp_2.rename(columns={'date':'completed_counts', 
                                   'index':'date'})
        temp_2 = temp_2.merge(temp, on=['date'], how='outer')
        temp_2['source_id'] = 'totals'

        survey_counts = survey_counts.append(temp_2)
        
        bluelabs['candidates'] = bluelabs['name_first_choice_candidates']
        combo_df = survey_monkey[['candidates', 'date','source_id']].append(bluelabs[['candidates','date', 'source_id']])
        
        support_df = combo_df[combo_df.candidates.notna()]
        candidates = list(support_df.candidates.unique())

        final_dict = {}

        for x in candidates:
            temp = support_df[support_df.candidates==x]
            temp_2 = temp.groupby(['date', 
                                   'source_id'])['candidates'].count()/support_df.groupby(['date', 
                                                                                   'source_id'])['candidates'].count()
            final_dict[x] = temp_2

        final_df = pd.DataFrame(final_dict).reset_index()
        final_df = final_df.fillna(0)
        
        totals_df = pd.DataFrame()
        for x in candidates:
            temp = support_df[support_df.candidates==x]
            temp_2 = temp.groupby(['date'])['source_id'].count()/support_df.groupby(['date'])['source_id'].count()
            totals_df[x] = temp_2 
        totals_df = totals_df.fillna(0)
        totals_df = totals_df.reset_index()
        totals_df['source_id'] ='totals'
        final_df = final_df.append(totals_df)
        
        final_df = final_df.merge(survey_counts, on=['date', 'source_id'], how='outer')
        final_df = final_df.fillna(0)
        
        turnout_df = survey_monkey[['turnout', 
                          'date','source_id']].append(bluelabs[['turnout','date', 'source_id']])
        
        turnout_df.turnout = turnout_df.turnout.fillna('No answer')
        responses = list(turnout_df.turnout.unique())

        x = 'No answer'
        temp = turnout_df.groupby(['date', 
                             'source_id'])['turnout'].value_counts()/turnout_df.groupby(['date', 
                             'source_id'])['turnout'].count()
        temp = temp.to_frame()
        temp = temp.rename(columns={'turnout':'turnout_percentage'})
        temp = temp.reset_index()

        totals_turnout = turnout_df.groupby(['date'])['turnout'].value_counts()/\
                        turnout_df.groupby(['date'])['turnout'].count()
        totals_turnout = totals_turnout.to_frame()
        totals_turnout = totals_turnout.rename(columns={'turnout':'turnout_percentage'})
        totals_turnout = totals_turnout.reset_index()
        totals_turnout['source_id'] = 'totals'

        totals_turnout = totals_turnout.append(temp)
        totals_turnout['turnout_percentage'] = totals_turnout['turnout_percentage'] * 100

        final_df = final_df.merge(totals_turnout, on=['date','source_id'], how='outer')
        
        final_df[candidates] = final_df[candidates].astype(float)
        final_df.loc[final_df.source_id=='bluelabs', 'source_id']='BLUELABS'
        final_df.loc[final_df.source_id=='survey_monkey', 'source_id']='SURVEY MONKEY'
        final_df['date'] = final_df['date'].apply(lambda x: '-'.join(['20'+ x.split('/')[2], 
                                           x.split('/')[0],
                                           x.split('/')[1]]) if '/' in x else x)
        final_df['date'] = pd.to_datetime(final_df['date'])

        final_df['test'] = 'test'
        final_df['qturnout'] = final_df['turnout']
        tf = pd.read_csv("gcs://togzhan_bucket/survey_dashboard/combined_support_test.csv")
        final_df = final_df[list(tf.columns)]

        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.get_bucket("gabriel_bucket_test")
        print('-' * 20)
        print("Saving combined support dataset..")
        blob = bucket.blob("combined_support_test2.csv")
        blob.upload_from_string(final_df.to_csv(index=False))
        
        phone_types = load_from_gcs('user_ground_truth',
                         'additional_data/prim_march_20191130_supplement_sample_for_vendor_phonetype.csv')

        phone_types_2 = load_from_gcs('user_ground_truth',
                                 'additional_data/prim_march_20191130_sample_for_vendor_bilingual_phonetype.csv')

        phone_types_3 = load_from_gcs('user_ground_truth',
                                 'additional_data/prim_march_20191130_sample_for_vendor_english_phonetype.csv')


        phone_types = phone_types.append(phone_types_2).append(phone_types_3)

        # changing the labels, so it's more understandable
        phone_types.loc[phone_types.phone_type=='L', 'phone_type'] = "Landline"
        phone_types.loc[phone_types.phone_type=='C', 'phone_type'] = "Cell"

        #merge back with bluelabs data
        bluelabs = bluelabs.merge(phone_types, left_on='respondents_id', 
                                  right_on='voterbase_id', how='left')
        
        
        # count support for all candidates for all phone types
        bl_support_df = bluelabs[bluelabs.candidates.notna()]
        bl_final_dict = {}
        for x in candidates:
            # subset for this candidate only
            temp = bl_support_df[bl_support_df.candidates==x]
            temp_2 = temp.groupby(['date', 'source_id'])['candidates'].count()/\
                    bl_support_df.groupby(['date', 'source_id'])['candidates'].count()
            bl_final_dict[x] = temp_2

        bl_final_df = pd.DataFrame(bl_final_dict).reset_index()
        bl_final_df = bl_final_df.drop(columns=['source_id'])
        bl_final_df['phone_type'] ='totals' 
        
        # count support by candidates but now for phone type
        bl_phone_dict = {}
        for x in candidates:
            temp = bl_support_df[bl_support_df.candidates==x]
            temp_2 = temp.groupby(['date', 'phone_type'])['candidates'].count()/\
                            bl_support_df.groupby(['date', 'phone_type'])['candidates'].count()
            bl_phone_dict[x] = temp_2

        bl_phone_df = pd.DataFrame(bl_phone_dict).reset_index()
        
        bl_final_df = bl_final_df.append(bl_phone_df)
        bl_final_df = bl_final_df.sort_values(by='date')
        bl_final_df[candidates]= bl_final_df[candidates]*100
        bl_final_df = bl_final_df.fillna(0)
        
        def saving_to_gcs(filepath, df, bucket_name):
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(filepath)
            blob.upload_from_string(df.to_csv(index=False))
            
        bucket_name = 'gabriel_bucket_test'
        filepath = "bluelabs_support2.csv"
        print('-' * 20)
        print("Saving bl support dataset..")
        saving_to_gcs(filepath, bl_final_df, bucket_name)
        
        sm_support_df = survey_monkey

        sm_final_dict = {}
        for x in candidates:
            temp = sm_support_df[sm_support_df.candidates==x]
            temp_2 = temp.groupby(['date', 'source_id'])['candidates'].count()/\
                    sm_support_df.groupby(['date', 'source_id'])['candidates'].count()
            sm_final_dict[x]=temp_2

        sm_final_df = pd.DataFrame(sm_final_dict).reset_index()
        sm_final_df = sm_final_df.sort_values(by=['date'])
        sm_final_df = sm_final_df.drop(columns=['source_id'])
        # sm_final_df['qturnout'] = 'totals'
        sm_final_df = sm_final_df.fillna(0)
        
        sm_final_df[candidates] = sm_final_df[candidates]*100
        bucket_name = 'gabriel_bucket_test'
        print('-' * 20)
        print("Saving sm support dataset..")
        filepath = "sm_support2.csv"
        saving_to_gcs(filepath, sm_final_df, bucket_name)
