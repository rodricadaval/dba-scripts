#!/usr/bin/env python3

import smtplib
import os
import sys
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
import json
from lib import dynamo

def get_secret(email="rodri.cadaval@gmail.com"):
    if email == "dba@acompanydomain.com" or email == "rodri.cadaval@gmail.com":
        try:
            i_dynamo = dynamo.DynamoClient('dev')
            var = json.loads(i_dynamo.get_vars('dba_gmail_app_test_secret'))
            return var['value']
        except Exception as e:
            print("No se encontraron variables del servicio en dynamo dev, busco en prod")
            i_dynamo = dynamo.DynamoClient('xx')
            var = json.loads(i_dynamo.get_vars('dba_gmail_app_test_secret'))
            return var['value']

class Gmail(object):
    def __init__(self, email):
        self.email = email
        self.password = get_secret(email)
        self.server = 'smtp.gmail.com'
        self.port = 587
        session = smtplib.SMTP(self.server, self.port)
        session.ehlo()
        session.starttls()
        session.ehlo
        session.login(self.email, self.password)
        self.session = session

    def send_message(self, to, subject, body):
        headers = [
            "From: " + self.email,
            "Subject: " + subject,
            "To: " + to,
            "MIME-Version: 1.0",
            "Content-Type: text/html"]
        headers = "\r\n".join(headers)
        self.session.sendmail(
            self.email,
            to,
            headers + "\r\n\r\n" + body)
