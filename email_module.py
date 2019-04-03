import smtplib
from configparser import ConfigParser
import datetime


def send_email(email_subject, email_body):
    config = ConfigParser()
    config.read('email.ini')

    today = datetime.date.today()
    today = datetime.datetime.strftime(today, '%Y-%m-%d')

    subject = email_subject + ' ' + today
    body = email_body

    email_server = smtplib.SMTP('smtp.gmail.com', 587)
    email_server.ehlo()
    email_server.starttls()
    email_server.login(config['email']['address'], config['email']['password'])
    message = 'Subject:{} \n\n {}'.format(subject, body)
    email_server.sendmail(config['email']['address'], config['email']['address'],message)
    email_server.quit()

    print('Email Sent Successfully')