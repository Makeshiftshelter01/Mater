import requests
import lxml.html
import cssselect
import time
from time import sleep
import re


title =[]

url = 'http://factcheck.snu.ac.kr/v2/facts'
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}

#pagination > div > a.last
res = requests.get(url, headers=headers)
html = res.text
root = lxml.html.fromstring(html)

part_html = root.cssselect('#pagination > div > a.last')[0]

tmp = part_html.get('href')
last = tmp[-3:]
print(last)


sleep(0.1)
#container > div > div.left_article > div > ul > li:nth-child(1) > div > div.fcItem_top.clearfix > div.prg.fcItem_li > p > a
for i in range(1,int(last)+1):
    params = {'page': i}
    res = requests.get(url, headers=headers,params=params)
    html = res.text
    root = lxml.html.fromstring(html)

    

    for part_html in root.cssselect('div.fcItem_li p a'):
        F_title =[]
        F_title.append(part_html.text_content())
        F_title[0] = re.sub('[\!\?\/\★\♥\$\&\@\%\~\[\]\(\)\{\}\.\,\=/+\-\_\:\;\*\^\'\"\<\>]*','', F_title[0])
        F_title[0] = re.sub('[\t\r\n\xa0]*','', F_title[0])
        
        title.append(F_title)
    sleep(0.2)

print(title)





