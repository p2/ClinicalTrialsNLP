#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing a trial's eligibility criteria
#
#	2013-08-27	Created by Pascal Pfiffner
#

import datetime
import uuid
import logging

from mngobject import MNGObject
from umls import UMLS, UMLSLookup, SNOMEDLookup, RxNormLookup
from nlp import split_inclusion_exclusion, list_to_sentences


class EligibilityCriteria (MNGObject):
	""" Representing a trial's eligibility criteria. """
	
	def __init__(self):
		super(EligibilityCriteria, self).__init__()
		self.last_processed = None
		self.text = None
		self.min_age = None
		self.max_age = None
		self.gender = None
		self.text = None
		self.criteria = None
	
	
	def load_lilly_json(self, elig):
		""" Loads instance variables from Lilly's JSON dictionary. """
		if elig is None:
			return
		
		if self.doc is None:
			self.doc = {}
		
		# gender
		gender = elig.get('gender')
		if 'Both' == gender:
			self.doc['gender'] = 0
		elif 'Female' == gender:
			self.doc['gender'] = 2
		else:
			self.doc['gender'] = 1
		
		# age
		a_max = elig.get('maximum_age')
		if a_max and 'N/A' != a_max:
			self.doc['max_age'] = [int(y) for y in a_max.split() if y.isdigit()][0]
		a_min = elig.get('minimum_age')
		if a_min and 'N/A' != a_min:
			self.doc['min_age'] = [int(y) for y in a_min.split() if y.isdigit()][0]
		
		# textual criteria
		elig_txt = elig.get('criteria', {}).get('textblock')
		if elig_txt:
			self.doc['text'] = elig_txt
		
		self.did_update_doc()
	
	
	def did_update_doc(self):
		if self.doc:
			self.last_processed = self.doc.get('last_processed')
			self.min_age = self.doc.get('min_age')
			self.max_age = self.doc.get('max_age')
			self.gender = self.doc.get('gender')
			self.text = self.doc.get('text')
			self.criteria = self.doc.get('criteria')
	
	
	def _process(self):
		""" Parses gender/age into document variables and then parses the
		textual inclusion/exclusion format into dictionaries stored in a
		"criteria" property.
		"""
		if self.text is None:
			logging.info("No eligibility criteria text found")
			return
		
		# split criteria text into inclusion and exclusion
		crit = []
		(inclusion, exclusion) = split_inclusion_exclusion(self.text)
		
		# parsed by bulleted list, produce one criterion per item; we also could
		# concatenate them into one file each.
		for txt in inclusion:
			obj = {'id': str(uuid.uuid4()), 'is_inclusion': True, 'text': txt}
			crit.append(obj)
		
		for txt in exclusion:
			obj = {'id': str(uuid.uuid4()), 'is_inclusion': False, 'text': txt}
			crit.append(obj)
		
		self.criteria = crit
	
	
	def codify(self, nlp_engines):
		""" Retrieves the codes from the database or, if there are none, tries
		to parse NLP output or passes the text criteria to NLP.
		"""
		if self.criteria is None:
			self._process()
		
		if self.criteria is not None:
			for criterion in self.criteria:
				self.criterion_codify(criterion, nlp_engines)
		
		self.last_processed = datetime.datetime.now()
		self.update_doc()
	
	
		
	def waiting_for_nlp(self, nlp_name):
		""" Returns True if any of our criteria needs to run through NLP.
		"""
		if self.criteria is not None and len(self.criteria) > 0:
			for criterion in self.criteria:
				if nlp_name in criterion.get('waiting_for_nlp', []):
					return True
		
		return False

	
	def criterion_codify(self, criterion, nlp_engines):
		""" Three stages:
		      1. Reads the codes from SQLite, if they are there
		      2. Reads and stores the codes from the NLP output dir(s)
		      3. Writes the criteria to the NLP input directories and fills the
		         "waiting_for_nlp" list
		"""
		if nlp_engines is None or 0 == len(nlp_engines):
			return False
		
		for nlp in nlp_engines:
			if not self.criterion_parse_nlp_output(criterion, nlp):
				self.criterion_write_nlp_input(criterion, nlp)
	
	def criterion_write_nlp_input(self, criterion, nlp):
		""" Writes the NLP engine input file and sets the waiting flag.
		It also sets the waiting flag if the file hasn't been written but there
		is yet no output. """
		wait = False
		
		if nlp.write_input(criterion.get('text'), '%s.txt' % str(criterion.get('id'))):
			wait = True
		else:
			arr = criterion.get('cui_ctakes') if 'ctakes' == nlp.name else criterion.get('cui_metamap')
			if not arr or len(arr) < 1:
				wait = True
		
		# waiting for NLP processing?
		if wait:
			waiting = criterion.get('waiting_for_nlp', [])
			waiting.append(nlp.name)
			criterion['waiting_for_nlp'] = waiting
	
	def criterion_parse_nlp_output(self, criterion, nlp, force=False):
		""" Parses the NLP output file (currently understands cTAKES and MetaMap
		output) and stores the codes in the database. """
		
		# skip parsing if we already did parse before
		if 'ctakes' == nlp.name:
			if criterion.get('snomed') is not None:
				return True
		elif 'metamap' == nlp.name:
			if criterion.get('cui_metamap') is not None:
				return True
		
		# parse our file; if it doesn't return a result we'll return False which
		# will cause us to write to the NLP engine's input
		filename = '%s.txt' % str(criterion.get('id'))
		ret = nlp.parse_output(filename, filter_sources=True)
		if ret is None:
			return False
		
		# got cTAKES data
		if 'ctakes' == nlp.name:
			if 'snomed' in ret:
				criterion['snomed'] = ret.get('snomed', [])
			if 'cui' in ret:
				criterion['cui_ctakes'] = ret.get('cui', [])
			if 'rxnorm' in ret:
				criterion['rxnorm_ctakes'] = ret.get('rxnorm', [])
		
		# got MetaMap data
		elif 'metamap' == nlp.name:
			if 'cui' in ret:
				criterion['cui_metamap'] = ret.get('cui', [])
		
		# no longer waiting
		wait = criterion.get('waiting_for_nlp')
		if wait is not None and nlp.name in wait:
			wait.remove(nlp.name)
			criterion['waiting_for_nlp'] = wait
		
		return True
	
	
	def exclude_by_snomed(self, exclusion_codes):
		""" Returns the SNOMED code that would exclude the trial, or None. """
		if self.criteria is None or 0 == len(self.criteria):
			return None
		
		for crit in self.criteria:
			
			# check exclusion criteria
			if not crit.get('is_inclusion') and crit.get('snomed') is not None:
				match = None
				for snomed_c in crit.get('snomed'):
					if '-' != snomed_c[0:1]:		# SNOMED codes starting with a minus were negated
						if snomed_c in exclusion_codes:
							return snomed_c
		
		return None
	
	
	def json(self):
		return {
			'min_age': self.min_age,
			'max_age': self.max_age,
			'gender': self.gender,
			'text': self.text
		}
	
	@property
	def formatted(self):
		""" Puts the criteria in a human-readable format
		"""
		if self.doc is None:
			return "No eligibility data"
		
		# the main criteria
		elig = self.doc.get('eligibility')
		main = elig.get('criteria', {}).get('textblock')
		if self.criteria is not None and len(self.criteria) > 0:
			inc = ['Inclusion Criteria:']
			exc = ['Exclusion Criteria:']
			for crit in self.criteria:
				if crit.get('is_inclusion', False):
					inc.append(crit.get('text'))
				else:
					exc.append(crit.get('text'))
			
			t_inc = "\n\t".join(inc)
			t_exc = "\n\t".join(exc)
			main = "%s\n\n%s" % (t_inc, t_exc)
		
		# additional bits
		return "Gender: %s\nAge: %s - %s\nHealthy: %s\n\n%s" % (
			self.gender,
			self.min_age,
			self.max_age,
			elig.get('healthy_volunteers'),
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
			in_ex = 'in' if crit.get('is_inclusion', False) else 'ex'
			
			# this criterion has been codified
			c_snomed_ct = crit.get('snomed', [])
			c_rx_ct = crit.get('rxnorm_ctakes', [])
			c_cui_mm = crit.get('cui_metamap', [])
			rspan = max(len(c_snomed_ct), len(c_rx_ct), len(c_cui_mm))
			if rspan > 0:
				
				c_html = """<td class="%s" rowspan="%d">%s</td>
				<td class="%s" rowspan="%d">%s</td>""" % (css_class, rspan, crit.get('text'), css_class, rspan, in_ex)
				
				# create cells
				for i in xrange(0, rspan):
					sno = c_snomed_ct[i] if len(c_snomed_ct) > i else ''
					rx = c_rx_ct[i] if len(c_rx_ct) > i else ''
					cui = c_cui_mm[i] if len(c_cui_mm) > i else ''
					
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
			
			# no codes for this criterion
			else:
				rows.append("""<td class="%s">%s</td>
					<td class="%s">%s</td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>
					<td class="%s"></td>""" % (css_class, crit.get('text'), css_class, in_ex, css_class, css_class, css_class, css_class, css_class, css_class, css_class))
			
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
