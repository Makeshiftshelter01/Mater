import re
# 중복을 체크하기 위한 글 번호 추출 함수들
import requests
import lxml.html


def extract_clien_number(link, idate):
    post_number = re.sub('.*park/', '', link)
    post_number = re.sub(r'\?.*', '', post_number)
    return post_number


# document_srl을 쓰는 게시물들은 순서가 안맞을 때가 있음.. 고로 날짜+시간을 임시로 번호로 설정할 것
def extract_ilbe_number(link, idate):
    post_number = re.sub(r'\(.*', '', idate)
    post_number = re.sub('[^0-9]', '', post_number)
    return post_number


def extract_cook_number(link, idate):
    post_number = re.sub('.*num=', '', link)
    post_number = re.sub(r'&.*', '', post_number)
    return post_number


def extract_MPark_number(link, idate):
    post_number = re.sub(r'.*?id=', '', link)
    post_number = re.sub(r'&.*', '', post_number)
    return post_number


def extract_ygosu_number(link, idate):
    post_number = re.sub(r'.*?issue/', '', link)
    post_number = re.sub(r'/\?.*', '', post_number)
    return post_number


def extract_ygosu2_number(link, idate):
    post_number = re.sub(r'.*?yeobgi/', '', link)
    post_number = re.sub(r'/\?.*', '', post_number)
    return post_number

# document_srl을 쓰는 게시물들은 순서가 안맞을 때가 있음.. 고로 날짜+시간을 임시로 번호로 설정할 것


def extract_theqoo_number(link, idate):
    post_number = re.sub('[^0-9]', '', idate)
    return post_number


def extract_ruri_number(link, idate):
    post_number = re.sub(r'.*read/', '', link)
    post_number = re.sub(r'\?.*', '', post_number)
    return post_number


def extract_inven_number(link, idate):
    post_number = re.sub(r'.*762/', '', link)
    post_number = re.sub(r'\?.*', '', post_number)
    return post_number


def extract_ppomppu_number(link, idate):
    post_number = re.sub('.*no=', '', link)

    return post_number


# 링크에서 글 번호 추출


def extract_numbers_from_link(target, link, idate='0'):
    methods = {
        'clien': extract_clien_number,
        'ilbe': extract_ilbe_number,
        'cook': extract_cook_number,
        'MPark': extract_MPark_number,
        'ygosu': extract_ygosu_number,
        'ygosu2': extract_ygosu2_number,
        'theqoo': extract_theqoo_number,
        'ruriweb': extract_ruri_number,
        'inven': extract_inven_number,
        'theqoo2': extract_theqoo_number,
        'ppomppu': extract_ppomppu_number
    }

    target_function = methods[target]

    post_number = target_function(link, idate)

    try:
        return int(post_number)
    except:
        return 'Error'
