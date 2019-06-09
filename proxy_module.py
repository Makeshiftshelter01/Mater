import requests
import lxml.html
import re
import random


def get_proxy_address():
    res = requests.get(
        'https://free-proxy-list.net/anonymous-proxy.html')  # 프록시 사이트
    html = res.text
    root = lxml.html.fromstring(html)
    proxies_list = []
    for i in range(20):
        temp = []
        for j, part_html in enumerate(root.cssselect('tbody tr td')[(0 + i) * 8:(1+i)*8]):
            content = part_html.text_content()
            temp.append(content)

        # 프록시 서버 목록 중 https 허용 여부가 yes고, 종류가 elite proxy인 경우
        if temp[6] == 'yes' and temp[4] == 'elite proxy':
            real_proxy = ':'.join(temp[0:2])  # : 로 아이피와 포트를 합침
            proxies_list.append(real_proxy)
    return proxies_list  # 결과 리스트를 반환


def get_html_from_proxy(link, succeededIp, params={}):

    proxy_success = False  # 프록시 접속 성공 여부
    # user-agent 리스트
    user_agent_list = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
                       'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
                       'Mozilla/5.0 (X11; U; Linux Core i7-4980HQ; de; rv:32.0; compatible; JobboerseBot; https://www.jobboerse.com/bot.htm) Gecko/20100101 Firefox/38.0',
                       'Mozilla/5.0 (X11; U; Linux Core i7-4980HQ; de; rv:32.0; compatible; JobboerseBot; http://www.jobboerse.com/bot.htm) Gecko/20100101 Firefox/38.0',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
                       'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)',
                       'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1'
                       ]

    if succeededIp != 'none':
        print('검증된 아이피로 접속')
        try:
            proxies = {
                "http": succeededIp,
                "https": succeededIp
            }

            # 헤더의 user-agent도 랜덤 선택
            headers = {'User-Agent': random.choice(user_agent_list)}

            res = requests.get(link, headers=headers,
                               proxies=proxies, params=params, timeout=5)  # 타임아웃을 5초로 설정해서 지나친 딜레이를 막음

            if res.encoding == 'UTF-8':  # 현재 프록시 서버를 써야하는 사이트의 기준은 인코딩이 utf-8로 반환되었을 때에만 성공으로 인정

                html = res.text

                return [html, succeededIp]

            else:

                raise ConnectionError

        except:
            print('검증된 아이피 실패, 새로운 아이피 접속')

            while proxy_success == False:  # 접속이 성공할 때까지
                print('시도...')
                proxies_list = get_proxy_address()  # 프록시 사이트에서 가져온다
                try:
                    # 가져온 프록시 리스트 중에 하나를 랜덤 선택
                    proxy = random.choice(proxies_list)

                    proxies = {
                        "http": proxy,
                        "https": proxy
                    }

                    # 헤더의 user-agent도 랜덤 선택
                    headers = {'User-Agent': random.choice(user_agent_list)}

                    res = requests.get(link, headers=headers,
                                       proxies=proxies, params=params, timeout=5)  # 타임아웃을 5초로 설정해서 지나친 딜레이를 막음

                    html = res.text

                    if res.encoding == 'UTF-8':  # 현재 프록시 서버를 써야하는 사이트의 기준은 인코딩이 utf-8로 반환되었을 때에만 성공으로 인정
                        proxy_success = True
                        succeededIp = proxy

                except:
                    pass

            return [html, succeededIp]  # 결과 반환
    else:

        while proxy_success == False:  # 접속이 성공할 때까지
            print('시도...')
            proxies_list = get_proxy_address()  # 프록시 사이트에서 가져온다
            try:
                # 가져온 프록시 리스트 중에 하나를 랜덤 선택
                proxy = random.choice(proxies_list)

                proxies = {
                    "http": proxy,
                    "https": proxy
                }

                # 헤더의 user-agent도 랜덤 선택
                headers = {'User-Agent': random.choice(user_agent_list)}

                res = requests.get(link, headers=headers,
                                   proxies=proxies, params=params, timeout=5)  # 타임아웃을 5초로 설정해서 지나친 딜레이를 막음

                html = res.text

                if res.encoding == 'UTF-8':  # 현재 프록시 서버를 써야하는 사이트의 기준은 인코딩이 utf-8로 반환되었을 때에만 성공으로 인정
                    proxy_success = True
                    succeededIp = proxy

            except:
                pass

        return [html, succeededIp]  # 결과 반환
