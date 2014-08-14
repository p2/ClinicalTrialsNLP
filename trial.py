#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#	Representing a ClinicalTrials.gov trial
#
#	2012-12-13	Created by Pascal Pfiffner
#	2014-07-29	Migrated to Python 3 and JSONDocument
#

import sys
import os.path
sys.path.insert(0, os.path.dirname(__file__))

import datetime
import logging
import re

from jsondocument import jsondocument
from analyzable import Analyzable
from eligibilitycriteria import EligibilityCriteria
from geo import km_distance_between
# from paper import Paper		# needs refactoring


class Trial(jsondocument.JSONDocument):
	""" Describes a trial found on ClinicalTrials.gov.
	"""
	
	def __init__(self, nct=None, json=None):
		super().__init__(nct, 'trial', json)
		self._papers = None
		
		# eligibility & analyzables
		self._eligibility = None
		self._analyze_keypaths = None
		self._analyzables = None
	
	
	# -------------------------------------------------------------------------- Properties
	@property
	def nct(self):
		return self.id
	
	@property
	def title(self):
		""" Construct the best title possible.
		"""
		if not self.__dict__.get('title'):
			title = self.official_title
			if not title:
				title = self.brief_title
			acronym = self.acronym
			if acronym:
				if title:
					title = "%s: %s" % (acronym, title)
				else:
					title = acronym
			self.__dict__['title'] = title
		
		return self.__dict__.get('title')
	
	@title.setter
	def title(self, value):
		self.__dict__['title'] = value
	
	@property
	def entered(self):
		""" How many years ago was the trial entered into ClinicalTrials.gov.
		"""
		now = datetime.datetime.now()
		first = self.date('firstreceived_date')
		return round((now - first[1]).days / 365.25 * 10) / 10 if first[1] else None
		
	@property
	def last_updated(self):
		""" How many years ago was the trial last updated.
		"""
		now = datetime.datetime.now()
		last = self.date('lastchanged_date')
		return round((now - last[1]).days / 365.25 * 10) / 10 if last[1] else None
	
	@property
	def interventions(self):
		""" Returns a set of intervention types of the receiver.
		"""
		if self.__dict__.get('interventions') is None:
			types = set()
			if self.intervention is not None:
				for intervent in self.intervention:
					inter_type = intervent.get('intervention_type')
					if inter_type:
						types.add(inter_type)
			
			if 0 == len(types):
				types.add('Observational')
			
			self.__dict__['interventions'] = list(types)
		
		return self.__dict__.get('interventions')
	
	@property
	def phases(self):
		""" Returns a set of phases in drug trials.
		Non-drug trials might still declare trial phases, we don't filter those.
		"""
		if self.__dict__.get('phases') is None:
			my_phases = self.phase
			if my_phases and 'N/A' != my_phases:
				phases = set(my_phases.split('/'))
			else:
				phases = set(['N/A'])
			self.__dict__['phases'] = list(phases)
		
		return self.__dict__.get('phases')
	
	def date(self, dt):
		""" Returns a tuple of the string date and the parsed Date object for
		the requested JSON object. """
		dateval = None
		parsed = None
		
		if dt is not None:
			date_dict = getattr(self, dt)
			if date_dict is not None and type(date_dict) is dict:
				dateval = date_dict.get('value')
				
				# got it, parse
				if dateval:
					dateregex = re.compile('(\w+)\s+((\d+),\s+)?(\d+)')
					searched = dateregex.search(dateval)
					match = searched.groups() if searched is not None else []
					
					# convert it to ISO-8601. If day is missing use 28 to not crash the parser for February
					dt = "%s-%s-%s" % (match[3], str(match[0])[0:3], str('00' + match[2])[-2:] if match[2] else 28)
					parsed = datetime.datetime.strptime(dt, "%Y-%m-%d")
		
		return (dateval, parsed)
	
	
	# -------------------------------------------------------------------------- API
	@property
	def js(self):
		""" The JSON to return for a JSON API call.
		"""
		js = {}
		api = self.api
		# print(api.keys())
		for key in [
				"_id",
				"brief_summary",
				"overall_contact", "overall_contact_backup"
				"condition", "primary_outcome",
			]:
			val = api.get(key)
			if val:
				js[key] = val
		
		js['title'] = self.title
		js['interventions'] = self.interventions
		js['phases'] = self.phases
		
		return js
	
	
	# -------------------------------------------------------------------------- Trial Locations
	
	def locations_closest_to(self, lat, lng, limit=0, open_only=True):
		""" Returns a list of tuples, containing the trial location and their
		distance to the provided latitude and longitude.
		If limit is > 0 then only the closest x locations are being returned.
		If open_only is True, only (not yet) recruiting locations are
		considered.
		"""
		closest = []
		
		# get all distances (must be instantiated, are not being cached)
		if self.location is not None:
			for loc_json in self.location:
				loc = TrialLocation(self, loc_json)
				
				if not open_only \
					or 'Recruiting' == loc.status \
					or 'Not yet recruiting' == loc.status \
					or 'Enrolling by invitation' == loc.status:
					
					closest.append((loc, loc.km_distance_from(lat, lng)))
		
		# sort and truncate
		closest.sort(key=lambda tup: tup[1])
		
		if limit > 0 and len(closest) > limit:
			closest = closest[0:limit]
		
		return closest
	
	
	# -------------------------------------------------------------------------- Keywords
	
	def cleanup_keywords(self, keywords):
		""" Cleanup keywords. """
		better = []
		re_split = re.compile(r';\s+')		# would be nice to also split on comma, but some ppl use it
											# intentionally in tags (like "arthritis, rheumatoid")
		re_sub = re.compile(r'[,\.]+\s*$')
		for keyword in keywords:
			for kw in re_split.split(keyword):
				if kw and len(kw) > 0:
					kw = re_sub.sub('', kw)
					better.append(kw)
		
		return better


class TrialLocation(object):
	""" An object representing a trial location.
	"""
	trial = None
	status = None
	contact = None
	contact_backup = None
	facility = None
	pi = None
	geo = None
	
	def __init__(self, trial, json_loc=None):
		self.trial = trial
		
		if json_loc is not None:
			self.status = json_loc.get('status')
			self.contact = json_loc.get('contact')
			self.contact_backup = json_loc.get('contact_backup')
			self.facility = json_loc.get('facility')
			self.pi = json_loc.get('investigator')
			self.geo = json_loc.get('geodata')
	
	
	# -------------------------------------------------------------------------- Properties
	@property
	def address_parts(self):
		if self.contact is not None:
			return trial_contact_parts(self.contact)
		if self.contact_backup is not None:
			return trial_contact_parts(self.contact_backup)
		return None
	
	@property
	def city(self):
		return self.geo.get('formatted')
	
	@property
	def best_contact(self):
		""" Tries to find the best contact data for this location, starting
		with "contact", then "contact_backup", then the trial's
		"overall_contact". """
		loc_contact = self.contact
		
		if loc_contact is None \
			or (loc_contact.get('email') is None and loc_contact.get('phone') is None):
			loc_contact = self.contact_backup
		
		if loc_contact is None \
			or (loc_contact.get('email') is None and loc_contact.get('phone') is None):
			loc_contact = getattr(self.trial, 'overall_contact')
		
		return loc_contact
	
	
	# -------------------------------------------------------------------------- Geodata
	def km_distance_from(self, lat, lng):
		""" Calculates the distance in kilometers between the location and the
		given lat/long pair using the Haversine formula. """
		lat2 = self.geo.get('latitude') if self.geo else 0
		lng2 = self.geo.get('longitude') if self.geo else 0
		
		return km_distance_between(lat, lng, lat2, lng2)
	
	
	# -------------------------------------------------------------------------- Serialization
	def json(self):
		return {
			'status': self.status,
			'facility': self.facility,
			'investigator': self.pi,
			'contact': self.best_contact,
			'geodata': self.geo
		}


def trial_contact_parts(contact):
	""" Returns a list with name, email, phone composed from the given
	contact dictionary. """
	if not contact:
		return ['No contact']
	
	# name and degree
	nameparts = []
	if 'first_name' in contact and contact['first_name']:
		nameparts.append(contact['first_name'])
	if 'middle_name' in contact and contact['middle_name']:
		nameparts.append(contact['middle_name'])
	if 'last_name' in contact and contact['last_name']:
		nameparts.append(contact['last_name'])
	name = ' '.join(nameparts) if len(nameparts) > 0 else 'Unknown contact'
	
	if 'degrees' in contact and contact['degrees']:
		name = '%s, %s' % (name, contact['degrees'])
	
	parts = [name]
	
	# email
	if 'email' in contact and contact['email']:
		parts.append(contact['email'])
	
	# phone
	if 'phone' in contact and contact['phone']:
		fon = contact['phone']
		if 'phone_ext' in contact and contact['phone_ext']:
			fon = '%s (%s)' % (fon, contact['phone_ext'])
		
		parts.append(fon)
	
	return parts
	
