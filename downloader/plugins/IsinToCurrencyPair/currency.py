#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Build a dictionary of ISIN to CurrencyPair for downloading them.
class Currency:
    def __init__(self):
        self.currencypair = {'XFC000000001': {'base': 'btc', 'quote': 'eur', 'pair': 'btceur', 'isin': 'XFC000000001'},
                             'XFC000000002': {'base': 'btc', 'quote': 'usd', 'pair': 'btcusd', 'isin': 'XFC000000002'},
                             'XFC000000003': {'base': 'ltc', 'quote': 'usd', 'pair': 'ltcusd', 'isin': 'XFC000000003'}
                             }

    def getCurrencyPair(self, isin):
        return self.currencypair[isin]

    def getIsin(self, pair):
        for key1 in self.currencypair.keys():
            if pair == self.currencypair[key1]['pair']:
                return self.currencypair[key1]['isin']
