import re
import json
import requests
import lxml.html


# 댓글 리스트가 비어있으면 '', 리스트가 1개만 담겨있다면 스트링으로 변환, 아니라면 리스트를 그대로 돌려주는 함수
def check_replies_list(replies_list):
    if replies_list == []:
        replies_list = ''

    elif len(replies_list) == 1:
        replies_list = replies_list[0]

    return replies_list


def get_replies_from_parsed_html(parsed_html):  # lxml 파싱으로 가져오는 방식은 동일하게 적용
    replies_list = []

    for comment in parsed_html:
        replies_list.append(comment.text_content())

    replies_list = check_replies_list(replies_list)

    return replies_list


def get_ygosu_replies(link):
    try:
        # 리플 정보를 가지고 있는 스크립트를 가져오기 위해 get 리퀘스트부터 먼저 실행
        res = requests.get(link)
        html = res.text
        root = lxml.html.fromstring(html)
        reply_info = ''

        for p in root.cssselect('script'):  # 여기에 리플 정보가 있음
            if 'var reply_info_str' in p.text_content():
                reply_info = p.text_content()

        # 잘라서 적절한 형태로 변환
        reply_info = re.findall("reply_info_str=.*\'", reply_info)[0]
        reply_info = re.sub('reply_info_str=', '', reply_info)

        ygosu_headers = {
            'Origin': 'https://www.ygosu.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Connection': 'keep-alive',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8,la;q=0.7'
        }

        data = {'reply_info': reply_info}

        # post방식으로 가져오기
        res = requests.post('https://www.ygosu.com/reply/reply_list.yg',
                            headers=ygosu_headers, data=data)

        received_response = res.text

        # json 형식으로 파싱
        parsed_json = json.loads(received_response)

        # json 내부에 다시 html 형식을 가져올 수 있으므로 가져온 후 html 파싱
        parsed_html = lxml.html.fromstring(parsed_json['html'])

        # 리플은 함수로 보내서 처리
        ygosu_replies = get_replies_from_parsed_html(
            parsed_html.cssselect('td.comment'))

    except:
        # 에러가 뜨면 요청이 비어있어서 그런 것(리플이 없는 것)이므로 빈 것으로 반환
        ygosu_replies = ''

    return ygosu_replies


def get_clien_replies(link):
    try:
        board_number = re.sub(r'\?.*', '', link)
        board_number = re.sub('.*park/', '', board_number)

        comment_link = 'https://www.clien.net/service/board/park/'+board_number + '/comment'

        # 클리앙은 겟 방식으로
        res = requests.get(comment_link)
        html = res.text
        parsed_html = lxml.html.fromstring(html)

        # 파싱한 것은 리플 가져오는 함수로 보내기
        clien_replies = get_replies_from_parsed_html(
            parsed_html.cssselect('div.comment_view'))

    except:
        # 에러가 뜨면 요청이 비어있어서 그런 것(리플이 없는 것)이므로 빈 것으로 반환
        clien_replies = ''

    return clien_replies

def get_ilbe_replies(link):
    try:
        board_number = re.sub('.*view/', '', link)
        board_number = re.sub(r'\?page.*', '', board_number)


        comment_link = 'http://www.ilbe.com/commentlist/'+board_number +'?page=0' 

        # 일베는 겟 방식으로
        res = requests.get(comment_link)
        html = res.text
        parsed_html = lxml.html.fromstring(html)

        # 파싱한 것은 리플 가져오는 함수로 보내기
        ilbe_replies = get_replies_from_parsed_html(
            parsed_html.cssselect('span.cmt'))

    except:
        # 에러가 뜨면 요청이 비어있어서 그런 것(리플이 없는 것)이므로 빈 것으로 반환
        ilbe_replies = ''

    return ilbe_replies

def get_theqoo_replies(link):
    try:
        document_srl = re.sub('.*document_srl=', '', link)

        headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                   'Content-Type': 'application/json; charset=UTF-8',
                   'Origin': 'https://theqoo.net',
                   'Referer': link,
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}

        data = 'act=dispBoardContentCommentListTheqoo&document_srl='+document_srl+'&cpage=0'

        res = requests.post('https://theqoo.net/index.php',
                            headers=headers, data=data)

        received_response = res.text

        # 숨어있는 BOM 문자열 때문에 일반적으로 파싱이 안되므로 첫번째 문자를 자르고 파싱
        parsed_json = json.loads(received_response[1:])

        theqoo_replies = []
        for comment in parsed_json['comment_list']:
            rep = comment['content']
            theqoo_replies.append(rep)

        # 리스트 체크 함수를 다이렉트로 씀
        theqoo_replies = check_replies_list(theqoo_replies)

    except:
        # 에러가 뜨면 요청이 비어있어서 그런 것(리플이 없는 것)이므로 빈 것으로 반환
        theqoo_replies = ''

    return theqoo_replies


def get_inven_replies(link):
    try:
        board_index = re.sub(r'.*wow/', '', link)
        board_index = re.sub(r'/.*', '', board_index)

        article_code = re.sub(r'.*/'+board_index, '', link)
        article_code = re.sub(r'/', '', article_code)
        article_code = re.sub(r'\?.*', '', article_code)

        headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                   'Accept-Encoding': 'gzip, deflate',
                   'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8,la;q=0.7',
                   'Connection': 'keep-alive',
                   'Content-Length': '111',
                   'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                   'Host': 'www.inven.co.kr',
                   'Origin': 'http://www.inven.co.kr',
                   'Referer': link,
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
                   'X-Requested-With': 'XMLHttpRequest'}

        data = {'comeidx': board_index,  # 게시판 번호(최논게:762)
                'articlecode': article_code,  # 글 번호
                'sortorder': 'date',
                'act': 'list',
                'out': 'json'}

        res = requests.post(
            'http://www.inven.co.kr/common/board/comment.json.php', headers=headers, data=data)
        received_response = res.text

        parsed_json = json.loads(received_response)

        inven_replies = []
        for comment in parsed_json['commentlist'][0]['list']:
            rep = comment['o_comment']
            inven_replies.append(rep)

        # 리스트 체크 함수를 다이렉트로 씀
        inven_replies = check_replies_list(inven_replies)

    except:
        # 에러가 뜨면 요청이 비어있어서 그런 것(리플이 없는 것)이므로 빈 것으로 반환
        inven_replies = ''

    return inven_replies


def get_special_replies(target, link):
    if target == 'ygosu':
        return get_ygosu_replies(link)
    elif target == 'ygosu2':
        return get_ygosu_replies(link)
    elif target == 'clien':
        return get_clien_replies(link)
    elif target == 'theqoo':
        return get_theqoo_replies(link)
    elif target == 'theqoo2':
        return get_theqoo_replies(link)
    elif target == 'inven':
        return get_inven_replies(link)
    elif target == 'ilbe':
        return get_ilbe_replies(link)