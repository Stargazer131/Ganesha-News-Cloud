from time import time
from bson import json_util
import os
from underthesea import sent_tokenize, word_tokenize
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import unicodedata
import pickle
from pynndescent import NNDescent
import numpy as np
from dotenv import load_dotenv


load_dotenv()

def caculate_time(func: callable):
    start_time = time()
    func()
    end_time = time()
    executed_time = end_time - start_time
    print(f'Executed time: {executed_time:.3f}s')


def connect_to_mongo(cloud=True):
    if cloud:
        username = os.getenv("USERNAME")
        password = os.getenv("PASSWORD")
        connect_str = f"mongodb+srv://{username}:{password}@ganeshanews.ygqol.mongodb.net/?retryWrites=true&w=majority&appName=GaneshaNews"
        return MongoClient(connect_str, server_api=ServerApi('1'))
    else:
        host = 'localhost'
        port = 27017
        connect_str = f"mongodb://{host}:{port}"
        return MongoClient(connect_str)


def load_nndescent() -> NNDescent:
    with open('data/ann_model/nndescent.pkl', "rb") as f:
        return pickle.load(f)
    
    
def save_nndescent(nndescent: NNDescent):
    with open('data/ann_model/nndescent.pkl', "wb") as f:
        pickle.dump(nndescent, f)


def save_neighbor_graph(graph: np.ndarray):
  np.save('data/ann_model/neighbor_graph.npy', graph)


def load_neighbor_graph() -> np.ndarray:
    try:
        return np.load('data/ann_model/neighbor_graph.npy')
    except:
        return np.array([])


def save_topic_distributions(matrix: np.ndarray):
  np.save('data/ann_model/topic_distributions.npy', matrix)


def load_topic_distributions(filepath='data/ann_model/topic_distributions.npy') -> np.ndarray:
    try:
        return np.load(filepath)
    except:
        return np.array([])


def load_processed_titles() -> list[str]:
    try:
        with open('data/preprocess/processed_titles.pkl', "rb") as f:
            return pickle.load(f)
    except:
        return []
    
    
def save_processed_titles(processed_titles: list[str]):
    with open('data/preprocess/processed_titles.pkl', "wb") as f:
        pickle.dump(processed_titles, f)


def load_stop_words():
    with open('data/preprocess/vietnamese-stopwords.txt', 'r', encoding='utf-8') as file:
        data = file.readlines()
        return set([word.strip() for word in data])
    

def load_fixed_words():
    with open('data/preprocess/fixed-words.txt', 'r', encoding='utf-8') as file:
        data = file.readlines()
        return set([word.strip() for word in data])


def create_punctuations_string():
    punctuations = ''.join(
        chr(i) for i in range(0x110000)
        if unicodedata.category(chr(i)).startswith('P') or
        unicodedata.category(chr(i)).startswith('S') or
        unicodedata.category(chr(i)).startswith('N')
    )

    remove_digits = str.maketrans('', '', '0123456789')
    punctuations = punctuations.translate(remove_digits)
    return punctuations


stop_words = load_stop_words()
fixed_words = load_fixed_words()
translator = str.maketrans('', '', create_punctuations_string())


def process_sentence(sent: str):
    sent = sent.translate(translator)
    tokens = word_tokenize(sent, fixed_words=fixed_words)
    result = []
    for token in tokens:
        if token in fixed_words:
            result.append(token.replace(' ', '_'))
        else:
            token = token.lower()
            if not token.isnumeric() and token not in stop_words:
                result.append(token.replace(' ', '_'))
                
    return result


def process_paragraph(text: str):
    res = []
    texts = sent_tokenize(text)
    for text in texts:
        res.extend(process_sentence(text))
    return res


def process_content(content: list):
    res = []
    for element in content:
        if isinstance(element, str) and not element.startswith('IMAGECONTENT'):
            res.extend(process_paragraph(element))
    return res


def process_title(title: str):
    return ' '.join(process_sentence(title))


def get_titles(collection_name: str):
    with connect_to_mongo() as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        projection = {"published_date": 1, "link": 1, "web": 1, "title": 1}
        return list(collection.find({}, projection))
    
    
def get_content(collection_name: str):
    with connect_to_mongo() as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        projection = {"title": 1, "description": 1, "content": 1}
        return list(collection.find({}, projection))


def total_documents(collection_name: str):
    with connect_to_mongo() as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        return collection.count_documents({})
    

def is_collection_empty_or_not_exist(collection_name: str):
    with connect_to_mongo() as client:
        db = client['Ganesha_News']

        if collection_name not in db.list_collection_names():
            return True

        if db[collection_name].count_documents == 0:
            return True
        
        return False


def backup_data(collection_name='newspaper'):
    with connect_to_mongo('localhost') as client:
        db = client['Ganesha_News']
        output_dir = f'data/Ganesha_News'
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f'{collection_name}.json')

        collection = db[collection_name]
        data = list(collection.find())
        serialized_data = json_util.dumps(data, indent=4, ensure_ascii=False)

        with open(file_path, "w", encoding="utf-8") as file:
            file.write(serialized_data)


def get_category_list(collection_name: str):
    with connect_to_mongo() as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        projection = {"category": 1}
        return list(collection.find({}, projection))
    

def test_accuracy(top_n=10):
    top_recommendations = load_neighbor_graph()
    data = get_category_list('newspaper')

    correct_recommendation = 0
    for recommendations in top_recommendations:
        main_category = data[int(recommendations[0])]['category']
        
        for index in recommendations[1 : top_n + 1]:
            category = data[int(index)]['category']
            if category == main_category:
                correct_recommendation += 1
            
    print(f'Total correct recommendation: {correct_recommendation} / {len(top_recommendations) * top_n}')    
    print(f'Accuracy: {correct_recommendation / (len(top_recommendations) * float(top_n)) * 100 : .2f} %')

