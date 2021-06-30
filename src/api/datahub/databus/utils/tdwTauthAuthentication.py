# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import base64
import json
import random
import socket
import sys
import time
import uuid
from datetime import datetime as dt

import pyDes
import requests
from common.log import logger
from datahub.databus.settings import AUTH_TDW_URL, DNS_SERVICE


class TdwTauthAuthentication:
    def __init__(self, userName, cmk, target, proxyUser=None):
        self.userName = userName
        self.cmk = cmk
        self.expire = True
        self.expireTimeStamp = int(time.mktime(dt.now().timetuple())) * 1000
        self.proxyUser = proxyUser
        self.ip = self.get_host_ip()
        self.sequence = random.randint(0, 999)
        self.identifier = {
            "user": self.userName,
            "host": self.ip,
            "target": target,
            "lifetime": 7200000,
        }

        self.ClientAuthenticator = {"principle": self.userName, "host": self.ip}

    def get_host_ip(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((DNS_SERVICE, 80))
            ip = s.getsockname()[0]
        finally:
            s.close()
        return ip

    def getSessionTicket(self):
        requestUrl = AUTH_TDW_URL

        self.identifier["timestamp"] = int(time.mktime(dt.now().timetuple()))
        identifierBody = base64.b64encode(bytes(json.dumps(self.identifier)))
        response = requests.get(requestUrl, params={"ident": identifierBody})
        self.sessionTicket = response.text

    def decryptClientTicket(self):
        try:
            sessionTicket = json.loads(self.sessionTicket)
        except Exception:
            logger.error("There is no such User.")
            sys.exit(1)

        self.serviceTicket = sessionTicket["st"]
        clientTicket = sessionTicket["ct"]
        clientTicket = base64.b64decode(clientTicket)

        try:
            cmk = base64.b64decode(self.cmk).decode("utf8").decode("hex")
            DESede = pyDes.triple_des(cmk, pyDes.ECB, padmode=pyDes.PAD_PKCS5)
            clientTicket = json.loads(str(DESede.decrypt(clientTicket)))
            self.expireTimeStamp = clientTicket["timestamp"] + clientTicket["lifetime"]
            self.sessionKey = base64.b64decode(clientTicket["sessionKey"])

        except Exception:
            logger.error("There is not proper master key.")
            sys.exit(1)

    def constructAuthentication(self):
        self.ClientAuthenticator["timestamp"] = int(time.mktime(dt.now().timetuple())) * 1000
        self.ClientAuthenticator["nonce"] = uuid.uuid1().hex
        self.ClientAuthenticator["sequence"] = self.sequence
        self.sequence += 1
        if self.proxyUser:
            self.ClientAuthenticator["proxyUser"] = self.proxyUser
        ClientAuthenticator = bytes(json.dumps(self.ClientAuthenticator))

        DESede = pyDes.triple_des(self.sessionKey, pyDes.ECB, padmode=pyDes.PAD_PKCS5)
        ClientAuthenticator = DESede.encrypt(ClientAuthenticator)
        authentication = "tauth." + self.serviceTicket + "." + str(base64.b64encode(ClientAuthenticator))
        return {"secure-authentication": authentication}

    def isExpire(self):
        return self.expireTimeStamp <= int(time.mktime(dt.now().timetuple())) * 1000

    def getAuthentication(self):
        if self.isExpire():
            self.getSessionTicket()
            self.decryptClientTicket()
        return self.constructAuthentication()
