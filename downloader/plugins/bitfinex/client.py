#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import logging

# Homebrew
from plugins.IsinToCurrencyPair.currency import Currency

# Init Logging Facilities
log = logging.getLogger(__name__)

"""
The bitfinex class support two types to get market data. The first type is RESTful. The second one is websocket.
"""
class Bitfinex:
    def __init__(self, api_version='v1', url='https://api.bitfinex.com', log_level=None):
        self.url = url
        self.api_version = api_version
        self.log = logging.getLogger(self.__module__)
        self.log.setLevel(level=log_level if log_level else logging.INFO)

    def ticker(self, isin):
        # get the base and quote from a dictionary inside the application.
        currency_client = Currency()
        c = currency_client.getCurrencyPair(isin)
        base = c['base']
        quote = c['quote']

        # build the url
        url = self.url + '/' + self.api_version + '/pubticker/' + base + quote
        r = requests.get(url)

        if r.status_code == 200:
            return r.status_code, r.json()
        else:
            return r.status_code, 0
