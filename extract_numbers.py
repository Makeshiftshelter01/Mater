import re
# 중복을 체크하기 위한 글 번호 추출 함수들 


def extract_clien_number(link):
    post_number = re.sub('.*park/','',link)
    post_number = re.sub(r'\?.*','',post_number)
    return post_number

def extract_ilbe_number(link):
    post_number = re.sub('.*document_srl=','',link)
    post_number = re.sub(r'&.*','',post_number)
    return post_number

def extract_cook_number(link):
    post_number = re.sub('.*num=','',link)
    post_number = re.sub(r'&.*','',post_number)
    return post_number

def extract_MPark_number(link):
    post_number = re.sub(r'.*?id=','',link)
    post_number = re.sub(r'&.*','',post_number)
    return post_number

def extract_ygosu_number(link):
    post_number = re.sub(r'.*?issue/','',link)
    post_number = re.sub(r'/\?.*','',post_number)
    return post_number

def extract_theqoo_number(link):
    post_number = re.sub(r'.*document_srl=','',link)
    post_number = re.sub(r'&.*','',post_number)
    return post_number

def extract_ruri_number(link):
    post_number = re.sub(r'.*read/','',link)
    post_number = re.sub(r'\?.*','',post_number)
    return post_number

def extract_inven_number(link):
    post_number = re.sub(r'.*762/','',link)
    post_number = re.sub(r'\?.*','',post_number)
    return post_number

# 링크에서 글 번호 추출 
def extract_numbers_from_link(target, link):
    methods = {
        'clien' : extract_clien_number,
        'ilbe' : extract_ilbe_number,
        'cook': extract_cook_number,
        'MPark': extract_MPark_number,
        'ygosu' : extract_ygosu_number,
        'theqoo': extract_theqoo_number,
        'ruri': extract_ruri_number,
        'inven':extract_inven_number
    }

    target_function = methods[target]

    post_number = target_function(link)

    try:
        return int(post_number)
    except:
        return 'Error'
