# -*- coding: utf-8 -*-
"""
Module to download all latest survey data
from survey_monkey and bluelabs then upload
to gcs in order to upload the dashboard
---------------------
@Author: Gabriel Yin 
"""
import os
import pandas as pd
import numpy as np
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
        
class BluelabsDataAggregator():
    """
    Class to clean/aggregate bluelabs 
    data to update dashboard
    """
    def __init__(self):
        """
        Class initiator
        """
        self.PROJECT_ID = 'hawkfish-prod-0c4ce6d0'
        self.BUCKET_NAME = 'gabriel_bucket_test'
        storage_client = storage.Client(project=self.PROJECT_ID)
        bucket = storage_client.get_bucket(self.BUCKET_NAME)
        blob = bucket.get_blob("agg_bluelabs_data.csv")
        data = blob.download_to_filename("survey_raw_data/agg_bluelabs_data.csv")
        self.bluelabs_data = pd.read_csv("survey_raw_data/agg_bluelabs_data.csv")
    
    def clean(self):
        """
        Function to conduct cleaning on bluelabs data
        NOTE: This part needs to be broken into smaller functions
        """
        self.bluelabs_data = self.bluelabs_data[
            (self.bluelabs_data.duration_call > 0) & (self.bluelabs_data.qturnoutprimary.notna())
        ]
        # states 
        split_df = self.bluelabs_data['voterbase_id'].str.split('-', expand=True)
        self.bluelabs_data['state'] = split_df[0]
        # candidates
        candidates = ['Joe Biden',
                     'Bernie Sanders',
                     'Elizabeth Warren',
                     'Michael Bloomberg',
                     '[LEAVE BLANK]',
                     'Cory Booker',
                     'Pete Buttigieg',
                     '[LEAVE BLANK]',
                     '[LEAVE BLANK]',
                     'Steve Bullock',
                     'Kamala Harris',
                     '[LEAVE BLANK]',
                     'Julian Castro',
                     'Amy Klobuchar',
                     'Andrew Yang',
                     'Tulsi Gabbard',
                     '[LEAVE BLANK]',
                     '[LEAVE BLANK]',
                     '[LEAVE BLANK]',
                     'Other',
                     'Undecided',
                     'Tom Steyer',
                     'Michael Bennet']

        order = [i for i in range(1, len(candidates)+1)]
        candidate_dict = dict(zip(order, candidates))
        
        self.bluelabs_data['qsupport'] = self.bluelabs_data['qsupport'].apply(
                lambda x : candidate_dict[x] if not np.isnan(x) else np.nan)
        
        return self
    
    def voter_age_zip(self):
        """
        Function to query voter_base_id file to 
        get zipcode and year born for each voter 
        """
        print('-' * 20)
        print("Start downloading voter_id file")
        
        query = """
        SELECT 
         bl.voterbase_id,
         bl.qbyear, 
         bl.qbyearbucket, 
         bl.qsex, 
         pii.vb_vf_reg_zip AS zipcode,
         pii.vb_voterbase_gender, 
         pii.vb_voterbase_dob
        FROM bluelabs_survey_results.all_survey_results AS bl
        INNER JOIN civis_national_raw.ts_analytics_trimmed AS pii
        ON bl.voterbase_id = pii.vb_voterbase_id
      """
        client = bigquery.Client(location="US")
        query_job = client.query(
            query,
            location="US",
        )  

        df = query_job.to_dataframe()
        
        print('-' * 20)
        print("Voter file has been successfully downloaded. ")
        df['year'] = [str(x)[:4] for x in df.vb_voterbase_dob]
        
        non_zero = df[df.qbyear.notna()]
        non_zero.qbyear = non_zero.qbyear.astype(int)
        non_zero.qbyear = non_zero.qbyear.astype(str)
        non_zero['match'] = np.where(non_zero.qbyear==non_zero.year, 1, 0)
        
        self.bluelabs_data = self.bluelabs_data.merge(df[
            ['voterbase_id','vb_voterbase_gender', 'year', 'zipcode']], 
            on=['voterbase_id'], how='left')
        
        self.bluelabs_data['age'] = 2019 - self.bluelabs_data['year'].astype(float)
        
        self.bluelabs_data['source_id'] = 'bluelabs'
        
        # generate age bin 
        # generate age bins
        self.bluelabs_data = self.bluelabs_data.rename(columns={'date_called' : 'date'})
        self.bluelabs_data = self.bluelabs_data[
            (self.bluelabs_data.age > 18) & 
            (self.bluelabs_data.age < 100)
        ]
        
        self.bluelabs_data.loc[(self.bluelabs_data['age']>=18) & (self.bluelabs_data['age'] <=38), 'age_bin'] = 'Millenials'
        self.bluelabs_data.loc[(self.bluelabs_data['age']>=39) & (self.bluelabs_data['age'] <=54), 'age_bin'] = 'Gen_X'
        self.bluelabs_data.loc[(self.bluelabs_data['age']>=55) & (self.bluelabs_data['age'] <=73), 'age_bin'] = 'Boomer'
        self.bluelabs_data.loc[(self.bluelabs_data['age']>=74),'age_bin'] = 'Silent_Generation'
        
        return self
    
    def decode_cols(self):
        """
        Function to decode columns into real values 
        NOTE: Probably this code can be further modualized 
        """
        # race 
        self.bluelabs_data.loc[self.bluelabs_data.qrace==1,'race']='White'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==2,'race']='Black or AfricanAmerican'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==3,'race']='Hispanic, Latino or Latin-American'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==4,'race']='Asian, Asian-American or Pacific Islander'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==5,'race']='Other'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==6,'race']='Other'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==7,'race']='Other'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==8,'race']='Other'
        self.bluelabs_data.loc[self.bluelabs_data.qrace==9,'race']='No answer'
        # education 
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==1, 'education']= 'Did Not Complete High School'
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==2, 'education']= 'Graduated High School'
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==3, 
                  'education']= 'Some College, No Degree'
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==4, 'education']= 'Associates degree'
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==5, 'education']= 'Bachelor’s degree'
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==6, 'education']= 'Masters, PhD'
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==7, 'education']= 'No answer'
        self.bluelabs_data.loc[self.bluelabs_data.qeducation==8, 'education']= 'No answer'
        # turnout 
        self.bluelabs_data['turnout_response'] = ''
        self.bluelabs_data.loc[self.bluelabs_data['qturnoutprimary']==1,'turnout_response'] = 'Definitely Vote'
        self.bluelabs_data.loc[self.bluelabs_data['qturnoutprimary']==2,'turnout_response'] = 'Probably Vote'
        self.bluelabs_data.loc[self.bluelabs_data['qturnoutprimary']==3,'turnout_response'] = '50/50 to vote'
        self.bluelabs_data.loc[self.bluelabs_data['qturnoutprimary']==4,'turnout_response'] = 'Probably not vote'
        self.bluelabs_data.loc[self.bluelabs_data['qturnoutprimary']==5,'turnout_response'] = 'Definitely Not Vote'
        self.bluelabs_data.loc[self.bluelabs_data['qturnoutprimary']==6,'turnout_response'] = 'Don’t know/not sure'
        self.bluelabs_data.loc[self.bluelabs_data['qturnoutprimary']==7,'turnout_response'] = 'Refused'

        self.bluelabs_data['qturnout'] = ''
        self.bluelabs_data.loc[self.bluelabs_data.turnout_response.isin(['Definitely Vote', 'Probably Vote' ]), 
             'qturnout']= 'Very likely I will vote'

        self.bluelabs_data.loc[self.bluelabs_data.turnout_response.isin(['50/50 to vote', 'Don’t know/not sure']), 
             'qturnout']= '50-50 chance I will vote'

        self.bluelabs_data.loc[self.bluelabs_data.turnout_response.isin(['Definitely Not Vote', 'Probably not vote']), 
             'qturnout']= 'Very likely I will NOT vote' 

        self.bluelabs_data.loc[self.bluelabs_data.turnout_response.isin(['Refused']), 
             'qturnout']= 'No answer'
        # qratepast
        past_rate = {
            1: "Bernie Sanders",
            2: "Hillary Clinton",
            3: "Other",
            4: "Not vote",
            5: "Don't know",
            6: "Refused"
        }
        
        emp_dict = {
            1: "Full-time",
            2: "Part-time",
            3: "Self-employed",
            4: "Retired",
            5: "A student",
            6: "Disabled",
            7: "Unemployed",
            8: "Other",
            9: "Refused"
        }
        
        religion_dict = {
            1: "Protestant",
            2: "Roman Catholic",
            3: "Mormon",
            4: "Orthodox",
            5: "Jewish",
            6: "Muslim",
            7: "Buddhist",
            8: "Hindu",
            9: "Atheist",
            10: "Agnostic",
            11: "Nothing in particular",
            12: "Just Christian",
            13: "Unitarian",
            14: "Something else",
            15: "Dont't know"
        }
        
        income_dict = {
            1: "Less than $30,000",
            2: "$30,000 to less than $40,000",
            3: "$40,000 to less than $50,000",
            4: "$50,000 to less than $75,000",
            5: "$75,000 to less than $100,000",
            6: "$100,000 to less than $150,000",
            7: "$150,000 or more",
            8: "Refused",
            12: "Other"
        }
        
        race_his = {
            1: "Yes",
            2: "No",
            3: "Refused"
        }

        self.bluelabs_data['past_vote'] = self.bluelabs_data['qpastvote'].apply(
                lambda x : past_rate[x] if not np.isnan(x) else np.nan)
        self.bluelabs_data['employement'] = self.bluelabs_data['qemployed'].apply(
                lambda x : emp_dict[x] if not np.isnan(x) else np.nan)
        self.bluelabs_data['religion'] = self.bluelabs_data['qreligion'].apply(
                lambda x : religion_dict[x] if not np.isnan(x) else np.nan)
            
            
        self.bluelabs_data['income'] = self.bluelabs_data['qincome'].apply(
                lambda x : income_dict[x] if not np.isnan(x) else np.nan)
            
            
        self.bluelabs_data['racehisp'] = self.bluelabs_data['qracehisp'].apply(
                lambda x : race_his[x] if not np.isnan(x) else np.nan)
        
        self.bluelabs_data['gender'] = self.bluelabs_data.vb_voterbase_gender
        
        return self
        
    
    def save(self):
        """
        Function to save the results
        """
        key_cols = [
            'date',
            'voterbase_id',
            'state',
            'zipcode',
            'vb_voterbase_gender',
            'employement',
            'religion',
            'income',
            'racehisp',
            'turnout_response',
            'qturnout',
            'race',
            'education',
            'past_vote'
            ]
        rate_cols = list(col for col in self.bluelabs_data.columns
                        if col.startswith('qrate'))
        key_cols.extend(rate_cols)
        storage_client = storage.Client(project=self.PROJECT_ID)
        bucket = storage_client.get_bucket(self.BUCKET_NAME)
        blob = bucket.blob("bluelabs_superset.csv")
        blob.upload_from_string(self.bluelabs_data[key_cols].to_csv(index=False))
        
        return self
