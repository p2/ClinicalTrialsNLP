#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing a ClinicalTrials.gov study
#
#	2012-12-13	Created by Pascal Pfiffner
#

import datetime
import dateutil.parser
import os
import logging
import codecs
import json
import re

import requests
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.WARNING)

from dbobject import DBObject
from nlp import split_inclusion_exclusion, list_to_sentences
from umls import UMLS, UMLSLookup, SNOMEDLookup, RxNormLookup
from paper import Paper
from ctakes import cTAKES
from metamap import MetaMap


class Study (DBObject):
	""" Describes a study found on ClinicalTrials.gov.
	"""
	
	ctakes = None
	metamap = None
	
	def __init__(self, nct=0):
		super(Study, self).__init__()
		self.nct = nct
		self.papers = None
		self.updated = None
		self.dir = {}
		
		self.gender = 0
		self.min_age = 0
		self.max_age = 200
		self.population = None
		self.healthy_volunteers = False
		self.sampling_method = None
		self.criteria_text = None
		self.criteria = []
		
		self.nlp = []
		if Study.ctakes is not None:
			self.nlp.append(cTAKES(Study.ctakes))
		if Study.metamap is not None:
			self.nlp.append(MetaMap(Study.metamap))
		
		self.waiting_for_ctakes_pmc = False
	
	
	def from_dict(self, d):
		""" Set properties from Lilly's dictionary.
		"""
		
		# study properties
		if d.get('id'):
			self.nct = d.get('id')
			del d['id']
		
		# hydrate if not already done
		if not self.hydrated:
			self.load()
		
		# eligibility
		e = d.get('eligibility')
		if e is not None:
			
			# gender
			gender = e.get('gender')
			if 'Both' == gender:
				self.gender = 0
			elif 'Female' == gender:
				self.gender = 2
			else:
				self.gender = 1
			
			# age
			a_max = e.get('maximum_age')
			if a_max and 'N/A' != a_max:
				self.max_age = [int(y) for y in a_max.split() if y.isdigit()][0]
			a_min = e.get('minimum_age')
			if a_min and 'N/A' != a_min:
				self.min_age = [int(y) for y in a_min.split() if y.isdigit()][0]
			
			# population and sampling
			pop = e.get('study_pop')
			self.population = pop.get('textblock') if pop else None
			self.sampling_method = e.get('sampling_method')
			self.healthy_volunteers = ('Yes' == e.get('healthy_volunteers'))
			
			# criteria
			crit = e.get('criteria')
			self.criteria_text = crit.get('textblock') if crit else None
			del d['eligibility']
		
		# all the rest
		self.dir = d
	
	
	def date(self, dt):
		""" Returns a tuple of the string date and the parsed Date object for
		the requested JSON object. """
		dateval = None
		parsed = None
		
		if dt is not None:
			date_dict = self.dir.get(dt) if self.dir else None
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
	
	def json(self, extra_fields=['brief_summary']):
		""" Returns a JSON-ready representation.
		The fields stated in "extra_fields" will be pulled out from the
		trial's "dir" property.
		"""
		
		# best title
		title = self.dir.get('brief_title')
		if not title:
			title = self.dir.get('official_title')
		acronym = self.dir.get('acronym')
		if acronym:
			if title:
				title = "%s: %s" % (acronym, title)
			else:
				title = acronym
		
		# criteria
		c = {
			'gender': self.gender,
			'min_age': self.min_age,
			'max_age': self.max_age,
			'healthy_volunteers': self.healthy_volunteers,
			'formatted': self.eligibility_formatted
		}
		
		# main dict
		d = {
			'nct': self.nct,
			'title': title,
			'criteria': c
		}
		
		# add extra fields
		for fld in extra_fields:
			d[fld] = self.dir.get(fld)
		
		return d
	
	@property
	def eligibility_formatted(self):
		""" Puts the criteria in a human-readable format
		"""
		
		# gender
		gen = 'Both'
		if self.gender > 0:
			gen = 'Male' if 1 == self.gender else 'Female'
		
		# the main criteria
		main = self.criteria_text
		if self.criteria and len(self.criteria) > 0:
			inc = ['Inclusion Criteria:']
			exc = ['Exclusion Criteria:']
			for crit in self.criteria:
				if crit.is_inclusion:
					inc.append(crit.text)
				else:
					exc.append(crit.text)
			
			t_inc = "\n\t".join(inc)
			t_exc = "\n\t".join(exc)
			main = "%s\n\n%s" % (t_inc, t_exc)
		
		# additional bits
		return "Gender: %s\nAge: %d - %d\nHealthy: %s\n\n%s" % (
			gen, self.min_age, self.max_age,
			'Yes' if self.healthy_volunteers else 'No',
			main
		)
	
	
	def report_row(self):
		""" Generates an HTML row for the report_row document.
		"""
		if self.criteria is None or len(self.criteria) < 1:
			return ''
		
		# collect criteria
		rows = []
		snomed = SNOMEDLookup()
		rxnorm = RxNormLookup()
		umls = UMLSLookup()
		is_first = True
		for crit in self.criteria:
			css_class = '' if is_first else 'crit_first'
			in_ex = 'in' if crit.is_inclusion else 'ex'
			
			# this criterium has been codified
			rspan = max(len(crit.snomed), len(crit.rxnorm_ctakes), len(crit.cui_metamap))
			if rspan > 0:
				
				c_html = """<td class="%s" rowspan="%d">%s</td>
				<td class="%s" rowspan="%d">%s</td>""" % (css_class, rspan, crit.text, css_class, rspan, in_ex)
				
				# create cells
				for i in xrange(0, rspan):
					sno = crit.snomed[i] if len(crit.snomed) > i else ''
					rx = crit.rxnorm_ctakes[i] if len(crit.rxnorm_ctakes) > i else ''
					cui = crit.cui_metamap[i] if len(crit.cui_metamap) > i else ''
					
					if 0 == i:
						rows.append(c_html + """<td class="%s">%s</td>
						<td class="%s">%s</td>
						<td class="%s">%s</td>
						<td class="%s">%s</td>
						<td class="%s">%s</td>
						<td class="%s">%s</td>""" % (css_class, sno, css_class, snomed.lookup_code_meaning(sno), css_class, rx, css_class, rxnorm.lookup_code_meaning(rx, True), css_class, cui, css_class, umls.lookup_code_meaning(cui, True)))
					else:
						rows.append("""<td>%s</td>
						<td>%s</td>
						<td>%s</td>
						<td>%s</td>
						<td>%s</td>
						<td>%s</td>""" % (sno, snomed.lookup_code_meaning(sno), rx, rxnorm.lookup_code_meaning(rx, True), cui, umls.lookup_code_meaning(cui, True)))
			
			# no codes for this criterium
			else:
				rows.append("""<td class="%s">%s</td>
					<td class="%s">%s</td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>""" % (css_class, crit.text, css_class, in_ex, css_class, css_class, css_class, css_class, css_class, css_class, css_class))
			
			is_first = False
		
		if len(rows) < 1:
			return ''
		
		# compose HTML
		html = """<tr class="trial_first">
		<td rowspan="%d">
			<a href="http://clinicaltrials.gov/ct2/show/%s" target="_blank">%s</a>
		</td>
		<td rowspan="%d" onclick="toggle(this)">
			<div style="display:none;">%s</div>
		</td>
		%s</tr>""" % (len(rows), self.nct, self.nct, len(rows), self.eligibility_formatted, rows[0])
		
		rows.pop(0)
		for row in rows:
			html += "<tr>%s</tr>" % row
		
		return html
	
	
	# extract single criteria from plain text eligibility criteria
	def process_eligibility_from_text(self):
		""" Does nothing if the "criteria" property already holds at least one
		StudyEligibility object, otherwise parses "criteria_text" into such
		objects.
		"""
		if self.criteria and len(self.criteria) > 0:
			return
		
		crit = []
		
		# split into inclusion and exclusion
		(inclusion, exclusion) = split_inclusion_exclusion(self.criteria_text)
		
		# parsed by bulleted list, produce one criterion per item; we also could
		# concatenate them into one file each.
		for txt in inclusion:
			obj = StudyEligibility(self)
			obj.is_inclusion = True
			obj.text = txt
			crit.append(obj)
		
		for txt in exclusion:
			obj = StudyEligibility(self)
			obj.is_inclusion = False
			obj.text = txt
			crit.append(obj)
		
		self.criteria = crit
		self.store_criteria()
	
	
	# assigns codes to all eligibility criteria
	def codify_eligibility(self):
		""" Retrieves the codes from SQLite or, if there are none, passes the
		text criteria to NLP.
		"""
		if self.criteria is not None:
			for criterium in self.criteria:
				criterium.codify()
	
	
	def waiting_for_nlp(self, nlp_name):
		""" Returns True if any of our criteria needs to run through NLP.
		"""
		if 'ctakes' == nlp_name and self.waiting_for_ctakes_pmc:
			return True
		
		if self.criteria and len(self.criteria) > 0:
			for criterium in self.criteria:
				if nlp_name in criterium.waiting_for_nlp:
					return True
		
		return False
	
	
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
		
		if not os.path.exists(run_dir):
			raise Exception("The run directory %s doesn't exist" % run_dir)
		
		ct_in_dir = os.path.join(Study.ctakes.get('root', run_dir), 'ctakes_input')
		for paper in self.papers:
			paper.parse_pmc_packages(run_dir, ct_in_dir)
			
			# also dump CT criteria if the paper has methods
			if paper.has_methods:
				plaintextpath = os.path.join(ct_in_dir, "%s-%s-CT.txt" % (self.nct, paper.pmid))
				with codecs.open(plaintextpath, 'w', 'utf-8') as handle:
					handle.write(self.eligibility_formatted)
				
				self.waiting_for_ctakes_pmc = True
	
	
	
	# -------------------------------------------------------------------------- Database Storage
	
	def should_insert(self):
		""" We use REPLACE INTO, so we always insert. """
		return True
	
	def should_update(self):
		return False
	
	def will_insert(self):
		if self.nct is None:
			raise Exception('NCT is not set')
	
	def insert_tuple(self):
		sql = '''REPLACE INTO studies
			(nct, updated, elig_gender, elig_min_age, elig_max_age, elig_population, elig_sampling, elig_accept_healthy, elig_criteria, dict)
			VALUES
			(?, datetime(), ?, ?, ?, ?, ?, ?, ?, ?)'''
		params = (
			self.nct,
			self.gender,
			self.min_age,
			self.max_age,
			self.population,
			self.sampling_method,
			self.healthy_volunteers,
			self.criteria_text,
			json.dumps(self.dir) if self.dir else ''
		)
		
		return sql, params
	
	def did_store(self):
		self.store_criteria()
	
	def store_criteria(self):
		""" Stores our criteria to SQLite.
		"""
		if self.criteria and len(self.criteria) > 0:
			for criterium in self.criteria:
				criterium.store()
	
	
	def load(self, force=False):
		""" Load from SQLite.
		Will fill all stored properties and load all StudyEligibility belonging
		to this study into the "criteria" property.
		"""
		if self.hydrated and not force:
			return
		
		if self.nct is None:
			raise Exception('NCT is not set')
		
		# get from SQLite
		sql = 'SELECT * FROM studies WHERE nct = ?'
		data = Study.sqlite_select_one(sql, (self.nct,))
		
		# populate ivars
		if data is not None:
			self.updated = dateutil.parser.parse(data[1])
			self.gender = data[2]
			self.min_age = data[3]
			self.max_age = data[4]
			self.population = data[5]
			self.sampling_method = data[6]
			self.healthy_volunteers = data[7]
			self.criteria_text = data[8]
			self.dir = json.loads(data[9]) if data[9] else None
			
			self.hydrated = True
			
			# populate parsed eligibility criteria
			self.criteria = StudyEligibility.load_for_study(self)
	
	
	# -------------------------------------------------------------------------- Class Methods
	table_name = 'studies'
	
	@classmethod
	def table_structure(cls):
		return '''(
			nct UNIQUE,
			updated TIMESTAMP,
			elig_gender INTEGER,
			elig_min_age INTEGER,
			elig_max_age INTEGER,
			elig_population TEXT,
			elig_sampling TEXT,
			elig_accept_healthy INTEGER DEFAULT 0,
			elig_criteria TEXT,
			dict TEXT
		)'''
	
	@classmethod
	def did_setup_tables(cls, db_path):
		StudyEligibility.setup_tables(db_path)
	
	
	@classmethod
	def setup_ctakes(cls, setting):
		cls.ctakes = setting
	
	@classmethod
	def setup_metamap(cls, setting):
		cls.metamap = setting
	
	@classmethod
	def sqlite_release_handle(cls):
		cls.sqlite_handle = None
		StudyEligibility.sqlite_release_handle()
	
	
	# -------------------------------------------------------------------------- Utilities
	def __unicode__(self):
		return '<study.Study %s>' % (self.nct)
	
	def __str__(self):
		return unicode(self).encode('utf-8')
	
	def __repr__(self):
		return str(self)
	



# Study eligibility criteria management
class StudyEligibility (DBObject):
	""" Holds one part of a study's eligibility criteria.
	Studies can have a lot of them.
	"""
	
	def __init__(self, study):
		super(StudyEligibility, self).__init__()
		self.study = study
		self.updated = None
		self.is_inclusion = False
		self.text = None
		self.snomed = None			# these are None if unprocessed, lists (empty or not) otherwise
		self.cui_ctakes = None
		self.rxnorm_ctakes = None
		self.cui_metamap = None
		self.waiting_for_nlp = []
	
	
	@classmethod
	def load_for_study(cls, study):
		""" Finds all stored criteria belonging to one study
		"""
		if study is None or study.nct is None:
			raise Exception('Study NCT is not set')
		
		found = []
		
		# find all
		sql = 'SELECT * FROM criteria WHERE study = ?'
		for rslt in cls.sqlite_select(sql, (study.nct,)):
			elig = StudyEligibility(study)
			elig.from_db(rslt)
			elig.hydrated = True
			found.append(elig)
		
		return found
	
	
	def from_db(self, data):
		""" Fill from an SQLite-retrieved list.
		"""
		self.id = data[0]
		self.updated = dateutil.parser.parse(data[2]) if data[2] else None
		self.is_inclusion = True if 1 == data[3] else False
		self.text = data[4]
		
		# the codes; if it has been NLP-processed but no code was found, the
		# string stored will be "|" to indicate that very fact, hence we filter
		# empty strings here.
		self.snomed = filter(None, data[5].split('|')) if data[5] and len(data[5]) > 0 else None
		self.cui_ctakes = filter(None, data[6].split('|')) if data[6] and len(data[6]) > 0 else None
		self.rxnorm_ctakes = filter(None, data[7].split('|')) if data[7] and len(data[7]) > 0 else None
		self.cui_metamap = filter(None, data[8].split('|')) if data[8] and len(data[8]) > 0 else None
	
	
	# -------------------------------------------------------------------------- Codification
	def codify(self):
		""" Three stages:
		      1. Reads the codes from SQLite, if they are there
		      2. Reads and stores the codes from the NLP output dir(s)
		      3. Writes the criteria to the NLP input directories and fills the
		         "waiting_for_nlp" list
		"""
		# not hydrated, fetch from SQLite (must be done manually)
		if not self.hydrated:
			raise Exception('must hydrate first (not yet implemented)')
		
		if self.study is None or self.study.nlp is None:
			return False
		
		for nlp in self.study.nlp:
			if not self.parse_nlp(nlp):
				self.write_nlp(nlp)
				
	
	def write_nlp(self, nlp):
		""" Writes the NLP engine input file and sets the waiting flag.
		It also sets the waiting flag if the file hasn't been written but there
		is yet no output. """
		waiting = False
		
		if nlp.write_input(self.text, '%d.txt' % self.id):
			waiting = True
		else:
			arr = self.cui_ctakes if 'ctakes' == nlp.name else self.cui_metamap
			if not arr or len(arr) < 1:
				waiting = True
		
		# waiting for NLP processing?
		if waiting:
			if self.waiting_for_nlp is None:
				self.waiting_for_nlp = [nlp.name]
			else:
				self.waiting_for_nlp.append(nlp.name)
	
	def parse_nlp(self, nlp, force=False):
		""" Parses the NLP output file (currently understands cTAKES and MetaMap
		output) and stores the codes in the database. """
		
		# skip parsing if we already did parse before
		if 'ctakes' == nlp.name:
			if self.snomed is not None:
				return True
		elif 'metamap' == nlp.name:
			if self.cui_metamap is not None:
				return True
		
		# parse our file; if it doesn't return a result we'll return False which
		# will cause us to write to the NLP engine's input
		filename = '%d.txt' % self.id
		ret = nlp.parse_output(filename, filter_sources=True)
		if ret is None:
			return False
		
		# got cTAKES data
		if 'ctakes' == nlp.name:
			if 'snomed' in ret:
				self.snomed = ret.get('snomed', [])
			if 'cui' in ret:
				self.cui_ctakes = ret.get('cui', [])
			if 'rxnorm' in ret:
				self.rxnorm_ctakes = ret.get('rxnorm', [])
		
		# got MetaMap data
		elif 'metamap' == nlp.name:
			if 'cui' in ret:
				self.cui_metamap = ret.get('cui', [])
		
		# no longer waiting
		if self.waiting_for_nlp is not None \
			and nlp.name in self.waiting_for_nlp:
			self.waiting_for_nlp.remove(nlp.name)
		
		return True
	
	
	# -------------------------------------------------------------------------- SQLite Handling
	def should_insert(self):
		return self.id is None
	
	def will_insert(self):
		if self.study is None or self.study.nct is None:
			raise Exception('Study NCT is not set')
	
	def insert_tuple(self):
		sql = '''INSERT OR IGNORE INTO criteria
				(criterium_id, study) VALUES (?, ?)'''
		params = (
			self.id,
			self.study.nct
		)
		
		return sql, params
	
	def update_tuple(self):
		""" returns the sql and parameters needed for an update.
		For the NLP-parsed fields, we enter an empty string if we hadn't parsed
		before but if we parsed and no codes were returned, we write a pipe
		symbol. Upon hydration this will be parsed to a list with two empty
		strings, which will be filtered to an empty list. Any list object
		indicates that the criterium has been parsed before. An empty string
		however will be changed to a None, indicating a need for NLP processing.
		"""
		sql = '''UPDATE criteria SET
			updated = datetime(), is_inclusion = ?, text = ?,
			snomed = ?, cui_ctakes = ?, rxnorm_ctakes = ?,
			cui_metamap = ?
			WHERE criterium_id = ?'''
		
		# process the codes
		if self.snomed is not None:
			snomed_ctakes = '|'.join(self.snomed) if len(self.snomed) > 0 else '|'
		else:
			snomed_ctakes = ''
		
		if self.cui_ctakes is not None:
			cui_ctakes = '|'.join(self.cui_ctakes) if len(self.cui_ctakes) > 0 else '|'
		else:
			cui_ctakes = ''

		if self.rxnorm_ctakes is not None:
			rxnorm_ctakes = '|'.join(self.rxnorm_ctakes) if len(self.rxnorm_ctakes) > 0 else '|'
		else:
			rxnorm_ctakes = ''

		if self.cui_metamap is not None:
			cui_metamap = '|'.join(self.cui_metamap) if len(self.cui_metamap) > 0 else '|'
		else:
			cui_metamap = ''
		
		params = (
			1 if self.is_inclusion else 0,
			self.text,
			snomed_ctakes, cui_ctakes, rxnorm_ctakes,
			cui_metamap,
			self.id
		)
		
		return sql, params
	
	
	# -------------------------------------------------------------------------- Class Methods
	table_name = 'criteria'
	
	@classmethod
	def table_structure(cls):
		return '''(
			criterium_id INTEGER PRIMARY KEY AUTOINCREMENT,
			study TEXT,
			updated TIMESTAMP,
			is_inclusion INTEGER,
			text TEXT,
			snomed TEXT,
			cui_ctakes TEXT,
			rxnorm_ctakes TEXT,
			cui_metamap TEXT
		)'''
	
	
	# -------------------------------------------------------------------------- Utilities
	def __unicode__(self):
		return '<study.StudyEligibility %s (%s)>' % (self.study.nct, 'inclusion' if self.is_inclusion else 'exclusion')
	
	def __str__(self):
		return unicode(self).encode('utf-8')
	
	def __repr__(self):
		return str(self)

