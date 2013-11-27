#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing a ClinicalTrials.gov trial
#
#	2012-12-13	Created by Pascal Pfiffner
#

import datetime
import dateutil.parser
import logging
import re

from mngobject import MNGObject
from analyzable import Analyzable
from eligibilitycriteria import EligibilityCriteria
# from paper import Paper		# needs refactoring
from geo import km_distance_between


class Trial (MNGObject):
	""" Describes a trial found on ClinicalTrials.gov.
	"""
	
	collection_name = 'studies'
	
	def __init__(self, nct=None):
		super(Trial, self).__init__(nct)
		self._title = None
		self.papers = None
		
		# eligibility & analyzables
		self._eligibility = None
		self.analyze_keypaths = None
		self._analyzables = None
		
		# NLP
		self.nlp = None
		self.waiting_for_ctakes_pmc = False
	
	
	# -------------------------------------------------------------------------- Properties
	@property
	def nct(self):
		return self.id
	
	@property
	def title(self):
		""" Construct the best title possible. """
		if not self._title:
			if not self.loaded:
				self.load()
			
			if self.doc is None:
				return 'Unknown Title'
			
			# we have a document, create the title
			title = self.doc.get('official_title')
			if not title:
				title = self.doc.get('brief_title')
			acronym = self.doc.get('acronym')
			if acronym:
				if title:
					title = "%s: %s" % (acronym, title)
				else:
					title = acronym
			self._title = title
		
		return self._title
			
	@property
	def entered(self):
		""" How many years ago was the trial entered into ClinicalTrials.gov. """
		now = datetime.datetime.now()
		first = self.date('firstreceived_date')
		return round((now - first[1]).days / 365.25 * 10) / 10 if first[1] else None
		
	@property
	def last_updated(self):
		""" How many years ago was the trial last updated. """
		now = datetime.datetime.now()
		last = self.date('lastchanged_date')
		return round((now - last[1]).days / 365.25 * 10) / 10 if last[1] else None
	
	@property
	def eligibility_inclusion(self):
		return self.eligibility.inclusion_text
	
	@property
	def eligibility_exclusion(self):
		return self.eligibility.exclusion_text
	
	@property
	def intervention_types(self):
		""" Returns a set of intervention types of the receiver. """
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

	def __getattr__(self, name):
		""" As last resort, we forward calls to non-existing properties to our
		document. """
		
		if not self.loaded:
			self.load()
		
		if self.doc:
			return self.doc.get(name)
		raise AttributeError
	
	
	def date(self, dt):
		""" Returns a tuple of the string date and the parsed Date object for
		the requested JSON object. """
		dateval = None
		parsed = None
		
		if dt is not None:
			date_dict = self.doc.get(dt) if self.doc else None
			if type(date_dict) is dict:
				dateval = date_dict.get('value')
				
				# got it, parse
				if dateval:
					dateregex = re.compile('(\w+)\s+((\d+),\s+)?(\d+)')
					searched = dateregex.search(dateval)
					match = searched.groups() if searched is not None else []
					
					# convert it to almost-ISO-8601. If day is missing use 28 to not crash the parser for February
					fmt = "%s-%s-%s" % (match[3], str(match[0])[0:3], str('00' + match[2])[-2:] if match[2] else 28)
					parsed = dateutil.parser.parse(fmt)
		
		return (dateval, parsed)
	
	
	def update_from_lilly(self, json):
		""" Incoming JSON from Lilly; for efficiency we drop all content
		except keys starting with an underscore. Faster than deepUpdate, which
		usually just replaces everything from Lilly's JSON anyway. """
		
		if json is None:
			return
		
		if self.id is None:
			self.id = json.get('id')
		
		if not self.loaded:
			self.load()
		
		if self.doc is not None:
			for key, val in self.doc.iteritems():
				if '_' == key[:1]:
					json[key] = val
		
		self.replace_with(json)
	
	
	def did_update_doc(self):
		""" We may need to fix some keywords. """
		if 'keyword' in self.doc:
			self.doc['keyword'] = self.cleanup_keywords(self.doc['keyword'])
	
	
	def json(self, extra_fields=['brief_summary']):
		""" Returns a JSON-ready representation.
		There is a standard set of fields and the fields stated in
		"extra_fields" will be appended.
		"""
		
		# main dict
		d = {
			'nct': self.id,
			'title': self.title,
			'eligibility': self.eligibility.json()
		}
		
		# add extra fields
		if self.doc is not None:
			for fld in extra_fields:
				d[fld] = getattr(self, fld)
		elif extra_fields is not None and len(extra_fields) > 0:
			logging.debug("Requesting extra fields %s but don't have a document" % extra_fields)
		
		return d
	
	def report_row(self):
		""" Generates an HTML row for the report_row document.
		"""
		return self.eligibility.report_row()
	
	
	# -------------------------------------------------------------------------- PubMed
	def run_pmc(self, run_dir):
		""" Finds, downloads, extracts and parses PMC-indexed publications for
		the trial. """
		self.find_pmc_packages()
		self.download_pmc_packages(run_dir)
		self.parse_pmc_packages(run_dir)
	
	
	def find_pmc_packages(self):
		""" Determine whether there was a PMC-indexed publication for the trial.
		"""
		if self.nct is None:
			logging.warning("Need an NCT before trying to find publications")
			return
		
		# find paper details
		self.papers = Paper.find_by_nct(self.nct)
		for paper in self.papers:
			paper.fetch_pmc_ids()
	
	
	def download_pmc_packages(self, run_dir):
		""" Downloads the PubMed Central packages for our papers. """
		
		if self.papers is not None:
			for paper in self.papers:
				paper.download_pmc_packages(run_dir)
	
	
	def parse_pmc_packages(self, run_dir):
		""" Looks for downloaded packages in the given run directory and
		extracts the paper text from the XML in the .nxml file.
		"""
		if self.papers is None:
			return
		
		import os.path
		if not os.path.exists(run_dir):
			raise Exception("The run directory %s doesn't exist" % run_dir)
		
		import codecs
		ct_in_dir = os.path.join(Trial.ctakes.get('root', run_dir), 'ctakes_input')
		for paper in self.papers:
			paper.parse_pmc_packages(run_dir, ct_in_dir)
			
			# also dump CT criteria if the paper has methods
			if paper.has_methods:
				plaintextpath = os.path.join(ct_in_dir, "%s-%s-CT.txt" % (self.nct, paper.pmid))
				with codecs.open(plaintextpath, 'w', 'utf-8') as handle:
					handle.write(self.eligibility.formatted())
				
				self.waiting_for_ctakes_pmc = True
	
	
	# -------------------------------------------------------------------------- Persistence
	def codified_properties(self):
		""" Returns all codified properties. """
		return self.doc.get('_codified') if self.doc else None
	
	def load_codified_property(self, prop, nlp_name=None):
		""" Checks if the given property has been codified by the given NLP
		engine and loads the codes if so.
		If no NLP name is given returns all existing ones. """
		if not self.loaded:
			self.load()
		
		codifieds = self.doc.get('_codified')
		cod_all = codifieds.get(prop) if codifieds else None
		if nlp_name is None:
			return cod_all
		
		return cod_all.get(nlp_name) if cod_all else None
	
	def store_codified_property(self, prop, codes, nlp_name):
		""" Stores the codes generated by the named NLP pipeline for the given
		property. """
		
		# store partial
		if codes and len(codes) > 0:
			key = '_codified.%s.%s' % (prop, nlp_name)
			self.store({key: codes})
	
	
	# -------------------------------------------------------------------------- Eligibility Criteria
	@property
	def eligibility(self):
		if self._eligibility is None:
			elig_obj = self.doc.get('_eligibility_obj')
			self._eligibility = EligibilityCriteria(elig_obj)
			
			# no object yet, parse from JSON
			if elig_obj is None and self.doc:
				self._eligibility.load_lilly_json(self.doc.get('eligibility'))
				self.doc['_eligibility_obj'] = self._eligibility.doc
				self.store({'_eligibility_obj': self._eligibility.doc})
		
		return self._eligibility
	
	
	# -------------------------------------------------------------------------- NLP
	def codify_analyzable(self, keypath, nlp_pipelines, force=False):
		""" Take care of codifying the given keypath using an analyzable.
		This method will be called before the NLP pipeline(s) are being run and
		might be run again afterwards, if trials have been waiting for the NLP
		pipeline to complete. """
		
		# make sure we know about this keypath
		if self.analyze_keypaths is None:
			self.analyze_keypaths = [keypath]
		elif keypath not in self.analyze_keypaths:
			self.analyze_keypaths.append(keypath)
		
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
		if self.analyze_keypaths is None:
			return
		
		for keypath in self.analyze_keypaths:
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
	
	
	def waiting_for_nlp(self, check_pipelines):
		""" Returns a set of NLP names if any of our criteria needs to run
		through that NLP pipeline.
		"""
		s = set()
		
		for n in check_pipelines:
			if 'ctakes' == n.name and self.waiting_for_ctakes_pmc:
				s.add(n.name)
			elif self._analyzables:
				for prop, analyzable in self._analyzables.iteritems():
					if analyzable.waiting_for_nlp(n.name):
						s.add(n.name)
						break
		
		return s
	
	
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
		re_split = re.compile(r';\s*')		# would be nice to also split on comma, but some ppl use it
											# intentionally in tags (like "arthritis, rheumatoid")
		re_sub = re.compile(r'[,\.]+\s*$')
		for keyword in keywords:
			for kw in re_split.split(keyword):
				if kw and len(kw) > 0:
					kw = re_sub.sub('', kw)
					better.append(kw)
		
		return better
	
	
	# -------------------------------------------------------------------------- Utilities
	def __unicode__(self):
		return '<trial.Trial %s>' % (self.id)
	
	def __str__(self):
		return unicode(self).encode('utf-8')
	
	def __repr__(self):
		return str(self)


class TrialLocation (object):
	""" An object representing a trial location. """
	
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



# if '__main__' == __name__:
	# trial = Trial.retrieve(['NCT01299818'])[0]
	# trial.store_codified_property('test', ['a', 'bcde'], 'foobar')
	
