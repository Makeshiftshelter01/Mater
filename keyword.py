import requests
import lxml.html
import cssselect
import time
from time import sleep
import re
from selenium import webdriver

keyword = []

url = 'http://factcheck.snu.ac.kr/v2/facts'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'}

# pagination > div > a.last
res = requests.get(url, headers=headers)
html = res.text
root = lxml.html.fromstring(html)

part_html = root.cssselect('#pagination > div > a.last')[0]

tmp = part_html.get('href')
last = tmp[-3:]
print(last)

wd = webdriver.Chrome(executable_path=r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe')

sleep(0.1)
# container > div > div.left_article > div > ul > li:nth-child(1) > div > div.fcItem_top.clearfix > div.prg.fcItem_li > p > a
for i in range(1, int(last) + 1):
    params = {'page': i}
    res = requests.get(url, headers=headers, params=params)
    html = res.text
    root = lxml.html.fromstring(html)

    titles = []
    for part_html in root.cssselect('div.fcItem_li p a'):

        tit = part_html.text_content()
        tit = re.sub('[\!\?\/\★\♥\$\&\@\%\~\[\]\(\)\{\}\.\,\=/+\-\_\:\;\*\^\'\"\<\>]*', '',
                                  tit)
        tit = re.sub('[\t\r\n\xa0]*', '', tit)
        titles.append(tit)

        sleep(0.2)

    inurl = url + '?page=' + str(i)
    wd.get(inurl)

    meters = []
    for part_html in wd.find_elements_by_css_selector('div.meter-label'):
        met = part_html.text
        meters.append(met)

    for i in range(len(titles)):
        contents = {}
        contents['title'] = titles[i]
        contents['meters'] = meters[i]
        keyword.append(contents)


wd.quit()
print(keyword)

    
print(len(keyword))