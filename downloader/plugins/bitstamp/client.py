#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
from plugins.IsinToCurrencyPair.currency import Currency


class BaseClient(object):
    api_url = "https://www.bitstamp.net/api/v2/"

    def _construct_url(self, url, base, quote):
        """
        Build url
        :param url: 
        :param base: 
        :param quote: 
        :return: 
        """

        if not base and not quote:
            return url
        else:
            url = url + base.lower() + quote.lower()
            return url

    def _request(self, func, url):
        """
        Handle the request
        :param func: 
        :param url: 
        :return: 
        """

        # construct url
        url = self.api_url + url

        if func == 'GET':
            r = requests.get(url)

            if r.status_code == 200:
                return r.status_code, r.json()
            else:
                return r.status_code, 0


class Public(BaseClient):
    """
    Public API
    """

    def ticker(self, isin):
        """
        Returns dictionary
        :param isin:  
        :return: 
        """

        # get the base and quote from a dictionary inside the application.
        currency_client = Currency()
        c = currency_client.getCurrencyPair(isin)
        base = c['base']
        quote = c['quote']

        # build url
        url = self._construct_url("ticker/", base, quote)
        return self._request("GET", url)
