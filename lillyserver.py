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
	
	def __init__(self, key_secret):
		if key_secret is None:
			raise Exception("You must provide the base64-encoded {key}:{secret} combination")
		
		super().__init__("https://data.lillycoi.com/")
		self.headers = {
			'Authorization': 'Basic {}'.format(key_secret)
		}
		self.trial_headers = self.search_headers = {'Accept': 'application/json'}
	
	
	def search_prepare_parts(self, url, finder, params):
		par = []
		if params is not None:
			for key, val in params.items():
				par.append("{}={}".format(key, val.replace(' ', '+')))
		
		# respect options set on the finder instance
		if finder is not None:
			if finder.limit_countries is not None:
				i = 1
				for ctry in finder.limit_countries:
					par.append("country{}={}".format(i, ctry.replace(' ', '+')))
					i += 1
			
			if finder.limit_only_recruiting:
				par.insert(0, "overall_status=Open+Studies")
		
		url = "{}?size=25&{}".format(url, '&'.join(par))
		return url, None
	
	def search_process_response(self, response):
		trials = []
		meta = {
			'total': response.get('total_count') or 0,
		}
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
		
