#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Build a dictionary of ISIN to CurrencyPair for downloading them.
class Currency:
    def __init__(self):
        self.currencypair = {'XFC000000001': {'base': 'btc', 'quote': 'eur'},
                             'XFC000000002': {'base': 'btc', 'quote': 'usd'}
                             }

    def getCurrencyPair(self, isin):
        return self.currencypair[isin]
