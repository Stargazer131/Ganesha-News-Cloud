from server import data
import numpy as np
from server.updater import combined_distance
from pynndescent import NNDescent
import os
from bson import json_util


def init_database(articles=1000):
    with data.connect_to_mongo(cloud=False) as client:
        articles_per_category = articles // 10
        db = client['Ganesha_News']
        collection_name = 'newspaper'
        collection = db[collection_name]

        categories = ['xe', 'the-gioi', 'giai-tri', 'khoa-hoc-cong-nghe', 'suc-khoe', 'du-lich', 'giao-duc', 'kinh-doanh', 'thoi-su', 'the-thao']
        query = {}
        sort_criteria = {"published_date": -1}
        articles = []
        for category in categories:
            print(f'Get articles for {category}')
            query['category'] = category
            articles.extend(collection.find(query).sort(sort_criteria).limit(articles_per_category))
        
        articles.sort(key=lambda x: x['index'])
        article_indices = {doc['index'] for doc in articles}
        for index, doc in enumerate(articles):
            doc['index'] = index

        print('Update neighbor graph')
        loaded_topic_distributions = data.load_topic_distributions('D:/Project/VSC/be/data/ann_model/topic_distributions.npy')
        topic_distributions = np.array([row for index, row in enumerate(loaded_topic_distributions) if index in article_indices])
        nndescent = NNDescent(topic_distributions, metric=combined_distance)
        data.save_neighbor_graph(nndescent.neighbor_graph[0])

        data_to_json(articles)

    # with data.connect_to_mongo(cloud=True) as client:
    #     db = client['Ganesha_News']
    #     collection_name = 'newspaper'
    #     collection = db[collection_name]
        
    #     print('Delete old database')
    #     collection.delete_many({})

    #     print('Update new database')
    #     collection.insert_many(articles)


def init_database_only_vnexpress():
    with data.connect_to_mongo(cloud=False) as client:
        db = client['Ganesha_News']
        collection_name = 'newspaper'
        collection = db[collection_name]

        query = {'web': 'vnexpress'}
        sort_criteria = {"published_date": -1}
        articles = list(collection.find(query).sort(sort_criteria))
        
        articles.sort(key=lambda x: x['index'])
        article_indices = {doc['index'] for doc in articles}
        for index, doc in enumerate(articles):
            doc['index'] = index

        print('Update neighbor graph')
        loaded_topic_distributions = data.load_topic_distributions('D:/Project/VSC/be/data/ann_model/topic_distributions.npy')
        topic_distributions = np.array([row for index, row in enumerate(loaded_topic_distributions) if index in article_indices])
        nndescent = NNDescent(topic_distributions, metric=combined_distance)
        data.save_neighbor_graph(nndescent.neighbor_graph[0])

        data_to_json(articles)

    # with data.connect_to_mongo(cloud=True) as client:
    #     db = client['Ganesha_News']
    #     collection_name = 'newspaper'
    #     collection = db[collection_name]
        
    #     print('Delete old database')
    #     collection.delete_many({})

    #     print('Update new database')
    #     collection.insert_many(articles)


def try_connect_to_cloud():
    client = data.connect_to_mongo()
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)


def data_to_json(data: list):
    output_dir = f'data/Ganesha_News'
    os.makedirs(output_dir, exist_ok=True)
    serialized_data = json_util.dumps(data, indent=4, ensure_ascii=False)
    file_path = os.path.join(output_dir, 'newspaper.json')

    with open(file_path, "w", encoding="utf-8") as file:
        file.write(serialized_data)


if __name__ == '__main__':
    init_database_only_vnexpress()
    # init_database(50000)
    # # try_connect_to_cloud()

    