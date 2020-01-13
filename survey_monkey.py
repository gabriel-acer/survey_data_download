"""
Module to download all latest survey_monkey data
into gcs for analysis and dashboard updating
---------------------
@Author: Gabriel Yin
"""
import os
import numpy as np
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
        self.survey_monkey = None
    
    def download_from_big_query(self):
        """
        Function to download from big query table
        """
        client = bigquery.Client()
        query_sm = """
        SELECT 
         *
        FROM survey_monkey.survey_monkey_curr;
        """
        query_job = client.query(
            query_sm,
            location="US"
        )
        print("-" * 20)
        print("Downloading survey monkey data..")
        self.survey_monkey = query_job.to_dataframe()
        print("-" * 20)
        print("Download has finished..")
        
        return self
    
    def clean(self):
        """
        Function to clean the data
        """
        split_df = self.survey_monkey.end_time.astype(str).str.split(' ', expand=True)
        self.survey_monkey['date']= split_df[0]
        # candidates decode 
        candidates = ['Michael Bennet','Joe Biden','Michael Bloomberg',
        'Cory Booker', 'Pete Buttigieg', 'Julian Castro', 'Tulsi Gabbard', 'Amy Klobuchar',
        'Bernie Sanders', 'Tom Steyer','Elizabeth Warren', 'Andrew Yang', 'None of the above',
        'No answer'
        ]
        
        gender_dict_sm = {
        1:"Male",
        2:"Female",
        3:"Not listed",
        4:"No answer"
        }

        education_dict_sm = {
        1:"Did Not Complete High School",
        2:"Graduated High School",
        3:"Attended college no degree",
        4:"Associates degree",
        5:"Bachelors degree",
        6:"Master, PhD",
        7:"No answer"
        }

        race_dict_sm = {
        1:"White",
        2:"Black or African American",
        3:"Hispanic, Latino or Latin-American",
        4:"Asian, Asian-American or Pacific Islander",
        5:"Other",
        6:"Other",
        7:"Other",
        8:"No answer"
        }

        states = ["Alabama",
          "Alaska",
          "Arizona",
          "Arkansas",
          "California",
          "Colorado",
          "Connecticut",
          "Delaware",
          "District of Columbia",
          "Florida",
          "Georgia",
          "Hawaii",
          "Idaho",
          "Illinois",
          "Indiana",
          "Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland",
          "Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
          "Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
          "North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
          "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
          "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming", "No answer"]
        
        candidates = ['Michael Bennet','Joe Biden','Michael Bloomberg',
                'Cory Booker', 'Pete Buttigieg', 'Julian Castro', 'Tulsi Gabbard', 'Amy Klobuchar',
                    'Bernie Sanders', 'Tom Steyer','Elizabeth Warren', 'Andrew Yang', 'None of the above',
        'No answer']

        numbers = [i for i in range(1, len(candidates) + 1)]

        candidates_df = pd.DataFrame({'name_first_choice_candidates':candidates, 
                             'candidate_first_choice':numbers})

        state_dict_sm = dict(zip(range(0, 52), states))
        
        df = self.survey_monkey;
    
        df.candidate_first_choice = df.candidate_first_choice.astype(float)
        df.candidate_second_choice = df.candidate_second_choice.astype(float)
        
        df = df.merge(candidates_df, on='candidate_first_choice', how='left')
        
        candidates_df = pd.DataFrame({'name_second_choice_candidates':candidates, 
                             'candidate_second_choice':numbers})

        df = df.merge(candidates_df, on='candidate_second_choice',how='left')
        
        df = df[df.age >= 18]

        df.loc[(df['age'] >= 18) & (df['age'] <= 38), 'age_bin'] = 'Millenials'
        df.loc[(df['age'] >= 39) & (df['age'] <= 54), 'age_bin'] = 'Gen_X'
        df.loc[(df['age'] >= 55) & (df['age'] <= 73), 'age_bin'] = 'Boomer'
        df.loc[(df['age'] >= 74),'age_bin'] = 'Silent_Generation'
        
        df.loc[df.partyid==1, 'party']='Republican'
        df.loc[df.partyid==2, 'party']='Democrat'
        df.loc[df.partyid==3, 'party']='Independent'
        df.loc[df.partyid==4, 'party']='No party choice'
        df.loc[df.partyid==5, 'party']='No answer'
        
        df['gender'] = df['gender'].map(lambda x: gender_dict_sm[x])
        df['education'] = df['education'].map(lambda x: education_dict_sm[x])
        df['state'] = df['state'].map(lambda x: state_dict_sm[x])
        df['race'] = df['race'].map(lambda x: race_dict_sm[x])
        
        df.qturnout = df.likely_vote_primary_dem
        df.loc[df.qturnout==1, 'qturnout'] = 'Very likely I will vote'
        df.loc[df.qturnout==2, 'qturnout'] = '50-50 chance I will vote'
        df.loc[df.qturnout==3, 'qturnout'] = 'Very likely I will NOT vote'
        df.loc[df.qturnout==4, 'qturnout'] = 'No answer'
        # fix date issue
        df.loc[df.date=='12/11/19','date'] = '2019-12-11'
        df.loc[df.date=='12/12/19','date'] = '2019-12-12'
        df.loc[df.date=='12/13/19','date'] = '2019-12-13'
        df.loc[df.date=='12/14/19','date'] = '2019-12-14'
        df.loc[df.date=='12/15/19','date'] = '2019-12-15'
        df.loc[df.date=='12/16/19','date'] = '2019-12-16'
        df.loc[df.date=='12/17/19','date'] = '2019-12-17'
        
        self.survey_monkey = df
        
        
        return self
    
    def decode(self):
        """
        Function to decode data
        """
        sm_religion = {
            1:"No religious group",
            2:"Protestant",
            3:"Catholic",
            4:"Mormon",
            5:"Orthodox",
            6:"Jewish",
            7:"Muslim",
            8:"Buddhist",
            9:"Hindu",
            10:"Other",
            11:"Other"
        }

        sm_income = {
            1:"Under $15,000",
            2:"Between $15,000 and $29,999",
            3:"Between $30,000 and $49,999",
            4:"Between $50,000 and $74,999",
            5:"Between $75,000 and $99,999",
            6:"Between $100,000 and $150,000",
            7:"Over $150,000",
            8:"No answer"
        }

        sm_emp = {
            1:"Full-time",
            2:"Part-time",
            3:"Self-employed",
            4:"Retired",
            5:"Student",
            6:"Disabled",
            7:"Unemployed",
            8:"No answer"
    
        }

        sm_en = {
            1:"Yes",
            2:"No",
            3:"No answer"
        }

        sm_hisp = {
            1:"Yes",
            2:"No",
            3:"No answer"
        }


        try:
            self.survey_monkey['religion'] = self.survey_monkey['religion'].apply(lambda x: sm_religion[x] )
            self.survey_monkey['income'] = self.survey_monkey['income'].apply(lambda x: sm_income[x] )
            self.survey_monkey['employment_status'] = self.survey_monkey['employment_status'].apply(lambda x: sm_emp[x] )
            self.survey_monkey['evangelical'] = self.survey_monkey['evangelical'].apply(lambda x: sm_en[x] if not np.isnan(x)
                                                     else np.nan)
            self.survey_monkey['hispanic'] = self.survey_monkey['hispanic'].apply(lambda x: sm_hisp[x] if not np.isnan(x)
                                                     else np.nan)
        except TypeError:
            pass
        
        self.survey_monkey['source_id'] = 'survey_monkey'
        
        return self
    
    def save(self):
        """
        Function to save the aggregated data into gcs
        """
        self.survey_monkey = self.survey_monkey.rename(
            columns={'response_id':'respondents_id', 
                     'qturnout':'turnout'})
        storage_client = storage.Client(project=self.PROJECT_ID)
        bucket = storage_client.get_bucket("gabriel_bucket_test")
        blob = bucket.blob("agg_surveymonkey_data.csv")
        blob.upload_from_string(self.survey_monkey.to_csv(index=False,
                                                    encoding="utf-8")) 
        
        print('-' * 20)
        print("Survey monkey data has been saved.")
        
        return self
    
    def run(self):
        """
        Driver function to run everything
        """
        self.download_from_big_query()
        self.clean()
        self.decode()
        self.save()
