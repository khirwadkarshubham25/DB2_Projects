import json
import os
import sys

import pymongo
from bson import json_util

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.mongo_db_connection import MongoDBConnection


class GetDBData:
    def __init__(self, query_option_list):
        if len(query_option_list) == 1:
            self.mongo_db_obj = MongoDBConnection(query_option_list[0])
        else:
            self.mongo_db_obj = MongoDBConnection()

    def get_db_data(self):
        query = {}
        query_1_statement, query_1 = self.get_artist_data()
        query[query_1_statement] = query_1
        query_2_statement, query_2 = self.get_artwork_data()
        query[query_2_statement] = query_2
        query_3_statement, query_3 = self.get_artwork_data_filter_price()
        query[query_3_statement] = query_3
        query_4_statement, query_4 = self.get_customer_count()
        query[query_4_statement] = query_4
        query_5_statement, query_5 = self.get_painting_sculpture_artwork_details()
        query[query_5_statement] = query_5
        query_6_statement, query_6 = self.get_total_price_for_art_form()
        query[query_6_statement] = query_6
        self.write_query_outputs(query)

    def get_artist_data(self):
        query_1_statement = "Get Data of all Artist"
        artist_collection = self.mongo_db_obj.db['Artist']
        data = list(artist_collection.find({}).sort("aID", pymongo.ASCENDING))
        return query_1_statement, data

    def get_artwork_data(self):
        query_2_statement = "Get Data of all Artwork"
        artwork_collection = self.mongo_db_obj.db['Artwork']
        data = list(artwork_collection.find({}))
        return query_2_statement, data

    def get_artwork_data_filter_price(self):
        query_3_statement = "Get all artists Name, Birth Date and State whose artworks price is greater than equal to 20000"
        artist_collection = self.mongo_db_obj.db['Artist']
        data = list(artist_collection.aggregate([
            {
                "$match": {
                    'artwork.price': {
                        '$gte': 20000
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'artistName': 1,
                    'birthDate': 1,
                    'stateName': 1
                }
            }
        ]))
        return query_3_statement, data

    def get_customer_count(self):
        query_4_statement = "Get name of artwork and total customers who bought painting"
        artwork_collection = self.mongo_db_obj.db['Artwork']
        data = artwork_collection.aggregate([
            {
                '$project': {
                    '_id': 0,
                    'artistName': 1,
                    'customerCount': {
                        "$size": "$customers"
                    }
                }
            },
            {
                '$sort': {
                    'customerCount': -1
                }
            }
        ])

        return query_4_statement, data

    def get_painting_sculpture_artwork_details(self):
        query_5_statement = "Get all artwork details where artwork type is painting or sculpture"
        artist_collection = self.mongo_db_obj.db['Artwork']
        data = list(artist_collection.aggregate([
            {"$match":
                {
                    'form': {
                        '$in': ["painting", "sculpture"]
                    }
                }
             }
        ]))
        return query_5_statement, data

    def get_total_price_for_art_form(self):
        query_6_statement = "Get total price for each form of artwork"
        artist_collection = self.mongo_db_obj.db['Artwork']
        data = list(artist_collection.aggregate([
            {
                "$group": {
                    "_id": "$form",
                    "totalPrice": {
                        "$sum": "$price"
                    }
                }
            },
            {
                "$project": {
                    "form": "$_id",
                    "totalPrice": "$totalPrice",
                    "_id": 0
                }
            }
        ]))
        return query_6_statement, data

    def get_artist_customer_details(self):
        query_7 = "Get all the details of the artist and details of the customer who bought his paintings"
        artwork_collection = self.mongo_db_obj.db['Artwork']
        data = list(artwork_collection.aggregate([
            {
                '$lookup': {
                    'from': 'Artist',
                    'localField': 'artistName',
                    'foreignField': 'artistName',
                    'as': 'artistDetails'
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': {
                        '$mergeObjects': [
                            {
                                '$arrayElemAt': ["$artistDetails", 0]
                            },
                            '$$ROOT'
                        ]
                    },
                }
            },
            {
                '$project': {
                    'artistDetails': 0
                }
            }
        ]))

        for d in data:
            print(d)

    @staticmethod
    def write_query_outputs(query):
        i = 1
        for query_statement, query_data in query.items():
            with open(f'output_files/query_{i}', 'w') as f:
                out = f'Query {i}: {query_statement}\n\n'
                for data in query_data:
                    out = f'{out}\n{str(json.dumps(json.loads(json_util.dumps(data)), indent=4))}'
                f.write(out)
            i += 1


if __name__ == '__main__':
    cmd_args = sys.argv
    GetDBData(cmd_args[1:]).get_db_data()
