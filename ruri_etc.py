import requests
import sys
from time import sleep

class CrException:
    # def exceptlower(self, link, inner_res):
    def exceptlpage(self, innerlink, headers):
        inner_res = requests.get(innerlink, headers = headers)
        # inner_html = None
        # inner_root = None

        #100번 접속
        i = 0
        while i < 100:
            if inner_res.status_code != 200:
                sleep(0.1)
                inner_res = requests.get(innerlink, headers=headers)
                print('재접속 시도중 : %d 회' % i)
                i += 1
            else:
                i = 100
        # inner_html = inner_res.text
        # inner_root = lxml.html.frmstring(inner_html)
        return inner_res

class CrStatus:
    def progressBar(self, value, endvalue, msg, bar_length=50):
        percent = float(value) / (endvalue)
        arrow = '=' * int(round(percent * bar_length)-1) + '>'
        spaces = ' ' * (bar_length - len(arrow))

        # sys.stdout.write("\r ---------- {0} ----------".format(msg))
        sys.stdout.write("\rstatus : [{0}] {1}% [{2} / {3}] - {4}".format(arrow + spaces, int(round(percent * 100)), value, endvalue, msg))
        if value == (endvalue):
            print('\n')
        sys.stdout.flush()