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
		if not self.__dict__['title']:
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
		
		return self.__dict__['title']
	
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
	def intervention_types(self):
		""" Returns a set of intervention types of the receiver.
		"""
		types = set()
		for intervent in self.intervention:
			inter_type = intervent.get('intervention_type')
			if inter_type:
				types.add(inter_type)
		
		if 0 == len(types):
			types.add('Observational')
		
		return types
	
	@property
	def trial_phases(self):
		""" Returns a set of phases in drug trials.
		Non-drug trials might still declare trial phases, we don't filter those.
		"""
		my_phases = self.phase
		if my_phases and 'N/A' != my_phases:
			phases = set(my_phases.split('/'))
		else:
			phases = set(['N/A'])
		
		return phases
	
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
	
	
	# -------------------------------------------------------------------------- NLP
	
	def codify_analyzable(self, keypath, nlp_pipelines, force=False):
		""" Take care of codifying the given keypath using an analyzable.
		This method will be called before the NLP pipeline(s) are being run and
		might be run again afterwards, if trials have been waiting for the NLP
		pipeline to complete. """
		
		# make sure we know about this keypath
		if self._analyze_keypaths is None:
			self._analyze_keypaths = [keypath]
		elif keypath not in self._analyze_keypaths:
			self._analyze_keypaths.append(keypath)
		
		self._codify_analyzable(keypath, nlp_pipelines, force)
	
	def _codify_analyzable(self, keypath, nlp_pipelines, force=False):
		""" Use internally. """
		if keypath is None:
			raise Exception("You must provide a keypath to 'codify_analyzable'")
		
		# get Analyzable object
		if self._analyzables is None:
			self._analyzables = {}
		
		if keypath not in self._analyzables:
			analyzable = Analyzable(self, keypath)
			self._analyzables[keypath] = analyzable
			
			# load from db
			stored = self.load_codified_property(keypath)
			if stored is not None:
				analyzable.codified = stored
		else:
			analyzable = self._analyzables[keypath]
		
		# codify (if needed) and store
		newly_stored = analyzable.codify(nlp_pipelines, force)
		if newly_stored:
			for nlp, content in newly_stored.iteritems():
				self.store_codified_property(keypath, content, nlp)
	
	def codify_analyzables(self, nlp_pipelines, force=False):
		""" Codifies all analyzables that the receiver knows about. """
		if self._analyze_keypaths is None:
			return
		
		for keypath in self._analyze_keypaths:
			self._codify_analyzable(keypath, nlp_pipelines, force)
	
	def analyzable_results(self):
		""" Returns codified results for our analyzables, with the following
		hierarchy:
		{ property: { nlp_name: { date: <date>, codes: { type: [#, #, ...] } } } }
		"""
		if not self._analyzables:
			return None
		
		d = {}
		for prop, analyzable in self._analyzables.iteritems():
			d[prop] = analyzable.codified
		return d
	
	
	def filter_snomed(self, exclusion_codes):
		""" Returns the SNOMED code if the trial should be filtered, None
		otherwise. """
		
		if self.eligibility is None:
			return None
		
		return self.eligibility.exclude_by_snomed(exclusion_codes)
	
	
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
	
