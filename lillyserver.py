#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.dirname(__file__))

import requests
import trialserver
import trial
from jsondocument import jsondocument


class LillyV2Server(trialserver.TrialServer):
	""" Trial server as provided by LillyCOI's v2 API on
	https://developer.lillycoi.com/.
	"""
	
	def __init__(self, key_secret):
		if key_secret is None:
			raise Exception("You must provide the base64-encoded {key}:{secret} combination")
		
		super().__init__("https://data.lillycoi.com/")
		self.batch_size = 50
		self.headers = {
			'Authorization': 'Basic {}'.format(key_secret)
		}
		self.trial_headers = self.search_headers = {'Accept': 'application/json'}
	
	
	def search_prepare_parts(self, path, params):
		if params is None:
			raise Exception("Must provide search parameters")
		
		par = []
		prms = params.copy()
		
		# process special search parameters
		if prms.get('countries') is not None:
			i = 1
			for ctry in prms['countries']:
				par.append("country{}={}".format(i, ctry.replace(' ', '+')))
				i += 1
			del prms['countries']
		
		if prms.get('recruiting', False):
			par.insert(0, "overall_status=Open+Studies")
			del prms['recruiting']
		
		# create URL
		for key, val in prms.items():
			par.append("{}={}".format(key, val.replace(' ', '+')))
		
		path = "{}?size={}&{}".format(path, self.batch_size, '&'.join(par))
		return path, None
	
	def search_process_response(self, response):
		trials = []
		meta = {
			'total': response.get('total_count') or 0,
		}
		results = response.get('results') or []
		for result in results:
			id_info = result.get('id_info') or {}
			trial = LillyTrial(id_info.get('nct_id'), result)
			trial.retrieve_profile(self)
			trials.append(trial)
		
		more = response.get('_links', {}).get('next', {}).get('href')
		
		return trials, meta, more


class LillyTrial(trial.Trial):
	""" Extend the CTG base trial by what Lilly's API is providing.
	"""
	
	def __init__(self, nct=None, json=None):
		super().__init__(nct, json)
		self.profile = None
	
	def retrieve_profile(self, server):
		if self._links is not None:
			profiles = self._links.get('target-profile')
			if profiles is not None and len(profiles) > 0:
				href = profiles[0].get('href')
				if href is not None:
					sess = requests.Session()
					req = server.base_request('GET', None, href)
					res = sess.send(sess.prepare_request(req))
					if res.ok:
						self.profile = LillyTargetProfile(self, res.json())
	

class LillyTargetProfile(jsondocument.JSONDocument):
	""" Represent a target profile.
	"""
	
	def __init__(self, trial, json):
		super().__init__('tp-{}'.format(trial.nct), 'target-profile', json)
		self.trial = trial
	
	
