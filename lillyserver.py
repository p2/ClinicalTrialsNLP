#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.dirname(__file__))

import requests
import trialserver
from trial import Trial


class LillyV2Server(trialserver.TrialServer):
	""" Trial server as provided by LillyCOI's v2 API on
	https://developer.lillycoi.com/.
	"""
	def __init__(self):
		super().__init__("https://data.lillycoi.com/")
	
	def search_prepare_parts(self, url, params):
		par = []
		for key, val in params.items():
			par.append("{}={}".format(key, val.replace(' ', '+')))
		
		url = "{}?size=50&{}".format(url, '&'.join(par))
		return url, None
	
	def search_process_response(self, response):
		trials = []
		meta = {}
		results = response.get('results') or []
		for result in results:
			id_info = result.get('id_info') or {}
			trial = Trial(id_info.get('nct_id'), result)
			trials.append(trial)
		
		more = response.get('_links', {}).get('next', {}).get('href')
		
		return trials, meta, more
	
	def target_profiles_request(self):
		headers = self.headers
		headers.update(self.trial_headers)
		
		return requests.Request('GET', '{}{}'.format(self.base, 'target-profiles'), data=None, headers=headers)
		
