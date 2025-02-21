import numpy as np
from pymongo import UpdateOne
from crawler.database.dantri import DantriCrawler
from crawler.database.vietnamnet import VietnamnetCrawler
from crawler.database.vnexpress import VnexpressCrawler
from crawler.database.vtcnews import VtcnewsCrawler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from server import data
import random
from gensim.models import LdaModel
from gensim.corpora import Dictionary
from gensim.matutils import sparse2full
import requests
import numba
from pynndescent import NNDescent
 

FLOAT32_EPS = np.finfo(np.float32).eps
FLOAT32_MAX = np.finfo(np.float32).max

@numba.njit(fastmath=True)
def combined_distance(x, y):
    # prepare
    dim = x.shape[0]
    norm_x = 0.0
    norm_y = 0.0
    l1_norm_x = 0.0
    l1_norm_y = 0.0
    
    for i in range(dim):
        l1_norm_x += x[i]
        l1_norm_y += y[i]
        norm_x += x[i] ** 2
        norm_y += y[i] ** 2

    # cosine
    if norm_x == 0.0 and norm_y == 0.0:
        result_cos = 0.0
    elif norm_x == 0.0 or norm_y == 0.0:
        result_cos = 1.0
    else:
        result_cos = 0.0
        for i in range(dim):
            result_cos += x[i] * y[i]
        result_cos = 1.0 - (result_cos / np.sqrt(norm_x * norm_y))
        
    # jensen shannon
    result_jen = 0.0
    l1_norm_x_jen = l1_norm_x + FLOAT32_EPS * dim
    l1_norm_y_jen = l1_norm_y + FLOAT32_EPS * dim

    pdf_x = (x + FLOAT32_EPS) / l1_norm_x_jen
    pdf_y = (y + FLOAT32_EPS) / l1_norm_y_jen
    m = 0.5 * (pdf_x + pdf_y)

    for i in range(dim):
        result_jen += 0.5 * (
            pdf_x[i] * np.log(pdf_x[i] / m[i]) + pdf_y[i] * np.log(pdf_y[i] / m[i])
        )
        
    # hellinger
    if l1_norm_x == 0 and l1_norm_y == 0:
        result_hel = 0.0
    elif l1_norm_x == 0 or l1_norm_y == 0:
        result_hel = 1.0
    else:
        result_hel = 0.0
        for i in range(dim):
            result_hel += np.sqrt(x[i] * y[i])
        result_hel = np.sqrt(1 - result_hel / np.sqrt(l1_norm_x * l1_norm_y))
        
    # jaccard
    if l1_norm_x == 0 and l1_norm_y == 0:
        result_jac = 0.0
    elif l1_norm_x == 0 or l1_norm_y == 0:
        result_jac = 1.0
    else:
        intersection = 0.0
        union = 0.0
        for i in range(dim):
            if x[i] <= y[i]:
                intersection += x[i]
                union += y[i]
            else:
                intersection += y[i]
                union += x[i]
        result_jac = 1 - intersection / union
    
    # combined
    return (result_cos + result_jen + result_hel + result_jac) / 4


def crawl_new_articles(vnexpress: bool, dantri: bool, vietnamnet: bool, vtcnews: bool, limit: int):    
    articles = []
    black_list = []
                
    if vnexpress:
        for category in VnexpressCrawler.categories:
            temp_articles, temp_black_list = VnexpressCrawler.crawl_articles(category, limit)
            articles.extend(temp_articles)
            black_list.extend(
                [{"link": link, "web": VnexpressCrawler.web_name} for link in temp_black_list]
            )

    if dantri:
        for category in DantriCrawler.categories:
            temp_articles, temp_black_list = DantriCrawler.crawl_articles(category, limit)
            articles.extend(temp_articles)
            black_list.extend(
                [{"link": link, "web": DantriCrawler.web_name} for link in temp_black_list]
            )

    if vietnamnet:
        for category in VietnamnetCrawler.categories:
            temp_articles, temp_black_list = VietnamnetCrawler.crawl_articles(category, limit)
            articles.extend(temp_articles)
            black_list.extend(
                [{"link": link, "web": VietnamnetCrawler.web_name} for link in temp_black_list]
            )

    if vtcnews:
        for category in VtcnewsCrawler.categories:
            temp_articles, temp_black_list = VtcnewsCrawler.crawl_articles(category, limit)
            articles.extend(temp_articles)
            black_list.extend(
                [{"link": link, "web": VtcnewsCrawler.web_name} for link in temp_black_list]
            )

    with data.connect_to_mongo() as client:
        db = client['Ganesha_News']

        if len(articles) > 0:
            random.shuffle(articles)
            collection = db['temporary_newspaper']
            collection.insert_many(articles)
            
        if len(black_list) > 0:
            black_collection = db['black_list']
            black_collection.insert_many(black_list)

    print(f"\nCrawl {data.total_documents('temporary_newspaper')} new articles!\n")


def check_duplicated_titles(similarity_threshold=0.75, time_threshold_in_days=1.5):
    # load database (old) and newly crawled articles
    old_articles = data.get_titles('newspaper')
    new_articles = data.get_titles('temporary_newspaper')
    articles = old_articles + new_articles
    last_database_index = len(old_articles)
    if last_database_index == 0:
        last_database_index = -1
    
    print('Preprocessing titles')
    old_titles = data.load_processed_titles()    
    new_titles = [data.process_title(doc['title']) for doc in new_articles]
    titles = old_titles + new_titles
    vectorizer = TfidfVectorizer(lowercase=False)
    tfidf_matrix = vectorizer.fit_transform(titles)
    dup_index = set()
    
    print('Check among articles')
    cosine_sim_matrix = cosine_similarity(tfidf_matrix, dense_output=False)
    rows, cols = cosine_sim_matrix.nonzero()
    values = cosine_sim_matrix.data
    filter_index = np.where(values >= similarity_threshold)[0]
    result = [(rows[i], cols[i]) for i in filter_index if rows[i] < cols[i]]

    for i1, i2 in result:
        date1 = articles[i1]['published_date']
        web1 = articles[i1]['web']
        date2 = articles[i2]['published_date']
        web2 = articles[i2]['web']
        time_diff_in_days = abs((date1 - date2).total_seconds()) / (3600 * 24)

        if web1 in ['dantri', 'vnexpress'] and web2 in ['vietnamnet', 'vtcnews']:
            dup_index.add(i2)

        elif web2 in ['dantri', 'vnexpress'] and web1 in ['vietnamnet', 'vtcnews']:
            dup_index.add(i1)

        elif web1 in ['dantri', 'vnexpress'] and web2 in ['dantri', 'vnexpress']:
            if web1 != web2 and time_diff_in_days <= time_threshold_in_days:
                if date1 >= date2:
                    dup_index.add(i2)
                else:
                    dup_index.add(i1)

        elif web1 in ['vietnamnet', 'vtcnews'] and web2 in ['vietnamnet', 'vtcnews']:
            if date1 >= date2:
                dup_index.add(i2)
            else:
                dup_index.add(i1)

    # delete duplicated articles
    old_dup_index = [int(id) for id in dup_index if id <= last_database_index]
    new_dup_id = [articles[id]['_id'] for id in dup_index if id > last_database_index]
    black_list = [
        {"link": articles[id]['link'], "web": articles[id]['web']} for id in dup_index
    ]

    with data.connect_to_mongo() as client:
        db = client['Ganesha_News']
        collection = db['newspaper']
        temp_collection = db['temporary_newspaper']
        b_collection = db['black_list']
    
        if len(old_dup_index) > 0:
            result = collection.delete_many({'index': {'$in': old_dup_index}})
            print(f'Deleted {result.deleted_count} duplicated documents from database')
            
            bulk_updates = []
            for i, doc in enumerate(list(collection.find({}, {"_id": 1}))):
                bulk_updates.append(
                    UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {"index": i}}
                    )
                )
            result = collection.bulk_write(bulk_updates)
            print(f'Reset index')

        if len(new_dup_id) > 0:
            result = temp_collection.delete_many({'_id': {'$in': new_dup_id}})
            print(f'Deleted {result.deleted_count} crawled duplicated documents')

        if len(black_list) > 0:
            result = b_collection.insert_many(black_list)
            print(f'Added {len(result.inserted_ids)} black list document')
            
    # Update processed titles list
    titles = [title for i, title in enumerate(titles) if i not in dup_index]
    data.save_processed_titles(titles)

    # Update saved topic distributions
    topic_distributions = data.load_topic_distributions()
    topic_distributions = np.array([row for i, row in enumerate(topic_distributions) if i not in old_dup_index])
    data.save_topic_distributions(topic_distributions)


def update_nndescent_index():
    print('Load LDA model')
    lda_model = LdaModel.load('data/lda_model/lda_model')
    dictionary = Dictionary.load('data/lda_model/dictionary')
    
    print('Processing document content')
    processed_documents = []
    article_content = data.get_content('temporary_newspaper')
    for doc in article_content:
        title = data.process_sentence(doc['title'])
        description = data.process_paragraph(doc['description'])
        content = data.process_content(doc['content'])
        processed_documents.append(title + description + content)
        
    print('Predicting topic distributions')
    corpus = [dictionary.doc2bow(doc) for doc in processed_documents]
    lda_corpus = lda_model[corpus]
    new_topic_distributions = np.array([sparse2full(vec, lda_model.num_topics) for vec in lda_corpus])
    old_topic_distributions = data.load_topic_distributions()
    if old_topic_distributions.shape[0] == 0:
        topic_distributions = new_topic_distributions
    else:
        topic_distributions = np.vstack((old_topic_distributions, new_topic_distributions))
    data.save_topic_distributions(topic_distributions)

    print('Updating nndescent index')
    nndescent = NNDescent(topic_distributions, metric=combined_distance)
    data.save_neighbor_graph(nndescent.neighbor_graph[0])


def update_database():
    with data.connect_to_mongo() as client:
        db = client['Ganesha_News']
        collection = db['newspaper']
        temp_collection = db['temporary_newspaper']
        articles = list(temp_collection.find({}, {"_id": 0}))

        index = data.total_documents('newspaper')
        for article in articles:
            article['index'] = index
            index += 1

        result = collection.insert_many(articles)
        print(f'Copy {len(result.inserted_ids)} articles to original database')
        temp_collection.drop()


def update_new_articles(vnexpress=True, dantri=True, vietnamnet=True, vtcnews=True, limit=10 ** 9):
    print('\nStep 1: Crawl new articles')
    crawl_new_articles(vnexpress, dantri, vietnamnet, vtcnews, limit)

    if data.is_collection_empty_or_not_exist('temporary_newspaper'):
        print('No new articles have been found')
    else:
        print('\nStep 2: Check for duplicated titles')
        check_duplicated_titles()

        print('\nStep 3: Update ANN model')
        update_nndescent_index()
        
        print('\nStep 4: Update database')
        update_database()

    return data.load_neighbor_graph()

