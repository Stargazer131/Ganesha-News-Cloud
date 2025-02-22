from server import data
import numpy as np
from server.updater import combined_distance
from pynndescent import NNDescent


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
            query['category'] = category
            articles.extend(collection.find(query).sort(sort_criteria).limit(articles_per_category))
        
        articles.sort(key=lambda x: x['index'])
        article_indices = {doc['index'] for doc in articles}
        for index, doc in enumerate(articles):
            doc['index'] = index

        loaded_topic_distributions = data.load_topic_distributions('D:/Project/VSC/be/data/ann_model/topic_distributions.npy')
        topic_distributions = np.array([row for index, row in enumerate(loaded_topic_distributions) if index in article_indices])
        nndescent = NNDescent(topic_distributions, metric=combined_distance)
        data.save_neighbor_graph(nndescent.neighbor_graph[0])


    with data.connect_to_mongo(cloud=True) as client:
        db = client['Ganesha_News']
        collection_name = 'newspaper'
        collection = db[collection_name]
        collection.delete_many({})
        collection.insert_many(articles)


if __name__ == '__main__':
    init_database(100000)

    