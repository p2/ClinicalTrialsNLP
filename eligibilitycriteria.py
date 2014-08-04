#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing a trial's eligibility criteria
#
#	2013-08-27	Created by Pascal Pfiffner
#

import sys
import os.path
sys.path.insert(0, os.path.dirname(__file__))

import uuid
import logging
from datetime import datetime
import re

from UMLS.umls import UMLS, UMLSLookup
from UMLS.snomed import SNOMEDLookup
from UMLS.rxnorm import RxNormLookup
from nlp import split_inclusion_exclusion, list_to_sentences


class EligibilityCriteria (object):
	""" Representing a trial's eligibility criteria. """
	
	def __init__(self, doc=None):
		super(EligibilityCriteria, self).__init__()
		self.text = doc.get('text') if doc else None
		self.gender = doc.get('gender') if doc else None
		self.min_age = doc.get('min_age') if doc else None
		self.max_age = doc.get('max_age') if doc else None
		self.inclusion_text = doc.get('inclusion_text') if doc else None
		self.exclusion_text = doc.get('exclusion_text') if doc else None
		self.criteria = doc.get('criteria') if doc else None


	@property
	def formatted_html(self):
		""" Formats inclusion/exclusion criteria as HTML.
		Simply runs the plain text through a Markdown parser after removing
		too much leading whitespace and angle brackets.
		This method imports the markdown module, we only rarely use this method
		and importing markdown takes a quarter second or so.
		"""
		if self.text is None:
			return None
		
		# this takes VERY LONG, figure out how to do it while idle
		import markdown
		txt = re.sub(r'^ +', r' ', self.text, flags=re.MULTILINE)
		txt = txt.replace('>', '&gt;')
		txt = txt.replace('<', '&lt;')
		txt = markdown.markdown(txt)
		txt = re.sub(r'(</?li>)\s*</?p>', r'\1', txt)
		
		return txt
	
	
	def load_lilly_json(self, elig):
		""" Loads instance variables from Lilly's JSON dictionary. """
		if elig is None:
			return
		
		# gender
		gender = elig.get('gender')
		if 'Both' == gender:
			self.gender = 0
		elif 'Female' == gender:
			self.gender = 2
		else:
			self.gender = 1
		
		# age
		a_max = elig.get('maximum_age')
		if a_max and 'N/A' != a_max:
			self.max_age = [int(y) for y in a_max.split() if y.isdigit()][0]
		a_min = elig.get('minimum_age')
		if a_min and 'N/A' != a_min:
			self.min_age = [int(y) for y in a_min.split() if y.isdigit()][0]
		
		# textual criteria
		elig_txt = elig.get('criteria', {}).get('textblock')
		if elig_txt:
			self.text = elig_txt
			self._split_inclusion_exclusion()
	
	def _split_inclusion_exclusion(self):
		""" Parses gender/age into document variables and then parses the
		textual inclusion/exclusion format into dictionaries stored in a
		"criteria" property.
		"""
		if not self.text:
			logging.info("No eligibility criteria text found")
			return
		
		# split criteria text into inclusion and exclusion
		crit = []
		(inclusion, exclusion) = split_inclusion_exclusion(self.text)
		
		self.inclusion_text = '{SEPARATOR}'.join(inclusion) if inclusion else ''
		if len(self.inclusion_text):
			self.inclusion_text = re.sub(r'\.?{SEPARATOR}\s*', '. ', self.inclusion_text)
		self.exclusion_text = '{SEPARATOR}'.join(exclusion) if exclusion else ''
		if len(self.exclusion_text):
			self.exclusion_text = re.sub(r'\.?{SEPARATOR}\s*', '. ', self.exclusion_text)
		
		# parsed by bulleted list, produce one criterion per item; we also could
		# concatenate them into one file each.
		for txt in inclusion:
			obj = {'id': str(uuid.uuid4()), 'is_inclusion': True, 'text': txt}
			crit.append(obj)
		
		for txt in exclusion:
			obj = {'id': str(uuid.uuid4()), 'is_inclusion': False, 'text': txt}
			crit.append(obj)
		
		self.criteria = crit
	
	
	@property
	def doc(self):
		""" A JSON representation of ourselves that can be stored. """
		doc = {}
		for key, val in vars(self).iteritems():
			doc[key] = val
		
		return doc
	
	
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
			'text': self.text,
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
