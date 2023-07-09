import os
import sys

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from commons.generic_constants import GenericConstants
from scripts.mongo_db_connection import MongoDBConnection


class MongoDbCreation:
    def __init__(self, cmd_list):
        url = None
        if cmd_list:
            url = cmd_list[0]
        self.mongo_db_conn_obj = MongoDBConnection(url)
        self.mongo_db_conn_obj.drop_collections()

    def create_mongo_db(self):
        input_data = self.read_input_files(GenericConstants.INPUT_FILES_DIRS)
        artist_data = list(self.create_artist_data(input_data).values())
        self.mongo_db_conn_obj.collection_insert_many(
            self.mongo_db_conn_obj.create_collection(GenericConstants.ARTIST), artist_data
        )
        artwork_data = list(self.create_artist_work_data(input_data).values())
        self.mongo_db_conn_obj.collection_insert_many(
            self.mongo_db_conn_obj.create_collection(GenericConstants.ARTWORK), artwork_data
        )

    @staticmethod
    def read_input_files(file_details):
        input_data = {}
        for file_category, file_name in file_details.items():
            input_data[file_category] = pd.read_csv(file_name)

        return input_data

    @staticmethod
    def create_artist_data(input_data):
        artist_data = dict()
        artist_df = input_data[GenericConstants.ARTIST]
        artist_df['stateAb'] = artist_df['stateAb'].str.upper()
        artwork_df = input_data[GenericConstants.ARTWORK]
        state_df = input_data[GenericConstants.STATE]
        state_df['stateAb'] = state_df['stateAb'].str.upper()
        temp_df = pd.merge(artist_df, artwork_df, how='left', left_on=['aID'], right_on=['aID'])
        df = pd.merge(temp_df, state_df, left_on=['stateAb'], right_on=['stateAb']).set_index('aID')
        for index, row in df.iterrows():
            if index not in artist_data:
                artist_data[index] = {
                    'aID': index,
                    'artistName': row['name'],
                    'birthDate': row['birthDate'],
                    'stateName': row['stateName'],
                    'artwork': [
                        {
                            'title': row['title'],
                            'price': row['price'],
                            'form': row['form']
                        }
                    ]
                }
            else:
                art_work = {
                    'title': row['title'],
                    'price': row['price'],
                    'form': row['form']
                }
                artist_data[index]['artwork'].append(art_work)
        return artist_data

    @staticmethod
    def create_artist_work_data(input_data):
        artwork_data = dict()
        artist_df = input_data[GenericConstants.ARTIST]
        artist_df = artist_df[['aID', 'name']]
        artist_df.rename(columns={'name': 'artistName'}, inplace=True)
        artwork_df = input_data[GenericConstants.ARTWORK]
        bought_df = input_data[GenericConstants.BOUGHT]
        customer_df = input_data[GenericConstants.CUSTOMER]
        customer_df['stateAb'] = customer_df['stateAb'].str.upper()
        state_df = input_data[GenericConstants.STATE]
        state_df['stateAb'] = state_df['stateAb'].str.upper()

        art_artwork_df = pd.merge(artist_df, artwork_df, how='left', left_on=['aID'], right_on=['aID'])
        artwork_bought_df = pd.merge(art_artwork_df, bought_df, left_on=['artID'], right_on=['artID'], how='left')
        customer_df = pd.merge(artwork_bought_df, customer_df, left_on=['cID'], right_on=['cID'], how='left')
        df = pd.merge(customer_df, state_df, left_on=['stateAb'], right_on=['stateAb'], how='left').set_index('artID')

        for index, row in df.iterrows():

            if index not in artwork_data:
                artwork_data[index] = {
                    'title': row['title'],
                    'creationDate': row['creationDate'],
                    'price': row['price'],
                    'form': row['form'],
                    'artistName': row['artistName'],
                    'customers': list()
                }
            customer = {}
            if type(row['name']) == str:
                customer = {
                    'customerName': row['name'],
                    'city': row['city'],
                    'stateName': row['stateName']
                }
            if customer:
                artwork_data[index]['customers'].append(customer)

        return artwork_data


if __name__ == '__main__':
    cmd_args = sys.argv
    MongoDbCreation(cmd_args[1:]).create_mongo_db()
