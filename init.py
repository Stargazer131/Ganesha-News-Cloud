from server import data
from gensim.models import LdaModel
from gensim.corpora import Dictionary
from gensim.matutils import sparse2full
import random
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
        
        random.shuffle(articles)
        lda_model = LdaModel.load('data/lda_model/lda_model')
        dictionary = Dictionary.load('data/lda_model/dictionary')

        processed_documents = []
        for index, doc in enumerate(articles):
            title = data.process_sentence(doc['title'])
            description = data.process_paragraph(doc['description'])
            content = data.process_content(doc['content'])
            processed_documents.append(title + description + content)
            doc['index'] = index
        
        corpus = [dictionary.doc2bow(doc) for doc in processed_documents]
        lda_corpus = lda_model[corpus]
        topic_distributions = np.array([sparse2full(vec, lda_model.num_topics) for vec in lda_corpus])
        nndescent = NNDescent(topic_distributions, metric=combined_distance)
        data.save_neighbor_graph(nndescent.neighbor_graph[0])


    with data.connect_to_mongo(cloud=True) as client:
        db = client['Ganesha_News']
        collection_name = 'newspaper'
        collection = db[collection_name]
        collection.delete_many({})
        collection.insert_many(articles)


if __name__ == '__main__':
    init_database(50000)

    