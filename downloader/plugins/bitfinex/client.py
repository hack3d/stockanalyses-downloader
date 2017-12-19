#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
from downloader.plugins.IsinToCurrencyPair.currency import Currency


class Bitfinex:
    def __init__(self, api_version='v1', url='https://api.bitfinex.com'):
        self.url = url
        self.api_version = api_version

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
