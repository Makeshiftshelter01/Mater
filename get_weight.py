from ruri_dao import CrwalingDAO
from ckonlpy.tag import Twitter
import re
import configparser
from pymongo import MongoClient
import mongo_proxy
import os

# 사전 초기화 및 단어 추가
twitter = Twitter()

nick_list = ['문재앙', '문제인', '문죄인', '문재해', '문재지변', '화재인', '문근혜', '문구라',
             '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥',
             '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리', '문어벙', '문오다리',
             '문고구마', '먼저인', '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지',  '재앙 ',
             '문재인', '문대통령', '문정부', '문프', '문통', '문재앙', '문제인', '문죄인', '문재해', '문재지변', '화재인', '문근혜', '문구라',
             '문벌구', '문구라', '문찐따', '문재인조', '쇼통령', '곡재인', '쩝쩝이', '문쩝쩝', '문보궐', '문변태', '문치매', '문치맥',
             '문등쉰', '문각기동대', '문혼밥', '문틀딱', '문정은', '문북한', '문쪼다', '문저리', '문어벙', '문오다리',
             '문고구마', '먼저인', '문틀러', '문주주의', '문벌레',  '문노스',  '미세문지', '재앙이', '문제아', '문세먼지', '재앙 '
             '젠틀재인', '문바마', '문깨끗', '파파미', '왕수석', 'negotiator', '달님', '문프', '명왕', '재인리', '금괴왕']

for nick in nick_list:
    twitter.add_dictionary(nick, 'Noun')

config = configparser.ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)


def get_random_samples(collection, sample_count, title_lists, content_lists, doc_with_moon_count):

    dao = CrwalingDAO()
    conn = dao.setdbinfo(collection)

    cursor = conn.m_collection.aggregate([
        {
            '$sample': {'size': sample_count}
        }
    ])
    conn.m_client.close()
    for doc in cursor:

        # check if this doc contains any of Moon's nickname
        for nick in nick_list:
            if (nick in doc['ctitle'] or nick in doc['content']['ccontent']):
                doc_with_moon_count.append(doc)

        title_lists.append(doc['ctitle'])
        content_lists.append(doc['content']['ccontent'])


def get_sentence_avg_length(sample_count, title_lists, moon_token_lists):
    sentence_lengths = 0
    for i in range(len(title_lists)):
        text = title_lists[i]
        # 전처리
        text = re.sub('[0-9A-Za-zㅋㅎㄷㅡ]+', ' ', text)
        text = re.sub(
            '[\[\]\.\!\?\/\.\:\-\>\~\@\·\"\"\%\,\(\)\& ]+', ' ', text)
        text = re.sub('[\n\xa0\r]+', ' ', text)
        text = re.sub('[^\x00-\x7F\uAC00-\uD7AF]+', ' ', text)
        text = re.sub('&nbsp;| |\t|\r|\n', ' ', text)

        # 토큰화
        tokens = twitter.nouns(text)  # only nouns

        sentence_lengths += len(tokens)

        for token in tokens:
            if token in nick_list:
                moon_token_lists.append(token)

    avg_sentence_length = sentence_lengths / sample_count

    return avg_sentence_length


def get_avg_sentence_count_from_content(sample_count, content_lists, avg_sentence_length, moon_token_lists):

    content_lengths = 0
    for i in range(len(content_lists)):
        text = content_lists[i]

        if (isinstance(text, list)):
            text = ' '.join(text)

        # 전처리
        text = re.sub('[0-9A-Za-zㅋㅎㄷㅡ]+', ' ', text)
        text = re.sub(
            '[\[\]\.\!\?\/\.\:\-\>\~\@\·\"\"\%\,\(\)\& ]+', ' ', text)
        text = re.sub('[\n\xa0\r]+', ' ', text)
        text = re.sub('[^\x00-\x7F\uAC00-\uD7AF]+', ' ', text)
        text = re.sub('&nbsp;| |\t|\r|\n', ' ', text)

        # 토큰화
        tokens = twitter.nouns(text)

        # this content contains N sentences
        content_lengths += len(tokens) / avg_sentence_length

        for token in tokens:
            if token in nick_list:
                moon_token_lists.append(token)

    avg_sentence_count_in_content = content_lengths / sample_count

    return avg_sentence_count_in_content


def do_main_process(collection, iteration, sample_count):
    total_sentence_length = 0
    total_content_length = 0
    total_moon_mentioning = 0

    for i in range(iteration):
        title_lists = []
        content_lists = []
        doc_with_moon_count = []
        moon_token_lists = []

        get_random_samples(collection, sample_count, title_lists,
                           content_lists, doc_with_moon_count)

        avg_sentence_length = get_sentence_avg_length(
            sample_count, title_lists, moon_token_lists)

        avg_sentence_count_in_content = get_avg_sentence_count_from_content(
            sample_count, content_lists, avg_sentence_length, moon_token_lists)

        total_sentence_length += avg_sentence_length
        total_content_length += avg_sentence_count_in_content

        print('**Routine : %s  \nDocuments with Moon mentioning : %s \nMoon Tokens : %s' % (
            i+1, len(doc_with_moon_count), len(moon_token_lists)))

        try:
            total_moon_mentioning += (len(moon_token_lists) /
                                      len(doc_with_moon_count))
        except ZeroDivisionError as e:
            total_moon_mentioning += 0

    final_sentence_length = total_sentence_length / iteration
    final_content_length = total_content_length / iteration
    final_moon_mention = total_moon_mentioning / iteration
    print()

    print('avg sentence length', final_sentence_length)
    print('avg content_length', final_content_length)
    print('avg moon mentioning', final_moon_mention)

    final_value = final_moon_mention / \
        (final_sentence_length * final_content_length)
    print('final value', final_value)
    return final_value


def db_upload(comm_name, value):
    mongodb1_info = config['mongoDB']
    client = mongo_proxy.MongoProxy(
        MongoClient(('mongodb://%s:%s@%s:%s' % (mongodb1_info['username'], mongodb1_info['password'], mongodb1_info['host'], mongodb1_info['port']))))

    db = client[mongodb1_info['database']]

    # 컬렉션 객체 가져오기
    comm = db['weights']
    comm.insert_one({'name': comm_name, 'value': value})
    client.close()


target_list = [
    {'coll': 'ilbe', 'name': 'ilbe'},
    {'coll': 'realclien', 'name': 'clien'},
    {'coll': 'realmlbpark', 'name': 'mlbpark'},
    {'coll': 'realcook', 'name': 'cook'},
    {'coll': 'ygosu2', 'name': 'ygosu'},
    {'coll': 'ruri', 'name': 'ruli'},

]


for target in target_list:
    value = do_main_process(target['coll'], 30, 200)

    db_upload(target['name'], value)
    print(target['name'], 'finished and uploaded')

print('done')
