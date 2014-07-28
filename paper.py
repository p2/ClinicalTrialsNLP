#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing a ClinicalTrials.gov study
#
#	2013-04-24	Created by Pascal Pfiffner
#

import logging
import requests
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.WARNING)
from urllib2 import urlopen
from xml.dom.minidom import parse, parseString
import os.path
import shutil
import tarfile
import codecs

from dbobject import DBObject


class Paper (DBObject):
	""" Representing one paper, per PMID.
	
	For now a paper can only relate to one trial, should probably improve that
	at one point.
	"""
	
	table_name = 'papers'
	
	
	def __init__(self, nct, pmid):
		if pmid is None:
			logging.error("Instantiating a paper without PMID")
		
		super(Paper, self).__init__()
		self.nct = nct
		self.pmid = pmid
		self.pmcids = None
		self.paper_methods = None
	
	
	def archive_name(self, pmc_id):
		""" The name of the archive for the given PMC-package.
		"""
		return "%s-%s-%s.tgz" % (self.nct, self.pmid, pmc_id)
	
	def methods_name(self, pmc_id):
		""" The filename for the methods parsed from the OA package. """
		return "%s-%s-%s.xml" % (self.nct, self.pmid, pmc_id)
	
	@property
	def has_methods(self):
		return self.paper_methods is not None and len(self.paper_methods) > 0
	
	
	# -------------------------------------------------------------------------- PubMed Central Ids
	def fetch_pmc_ids(self):
		""" Downloads the paper's XML from eutils and parses the interesting
		parts to get ahold of the PubMed Central Ids. """
		
		if self.pmcids is not None:
			return
		
		if self.pmid is None:
			logging.error("Whoa, have a paper without PMID, cannot fetch details")
			return
		
		pmcids = []
		
		# eutils URL
		url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=%s&retmode=xml" % self.pmid
		res = requests.get(url)
		if not res.ok:
			logging.warning("%d -- failed to get %s: %s" % (res.status_code, url, res.error))
		else:
			root = parseString(res.content).documentElement
			try:
				# try to find the <OtherId> node and extract its data if the source is NLM
				article = root.getElementsByTagName('PubmedArticle')[0]
				citation = article.getElementsByTagName('MedlineCitation')[0]
				others = citation.getElementsByTagName('OtherID')
				for other in others:
					if 'NLM' == other.getAttribute('Source'):
						pmcids.append(other.firstChild.data)
			except Exception as e:
				logging.warning("Error when parsing eutils XML %s: %s" % (url, e))
		
		# remember the pmcids
		self.pmcids = pmcids
		self.store()
		
		# warn if we have a PMID but no PMC-id
		if self.pmid is not None and len(pmcids) < 1:
			logging.info("No PMCID found for %s despite PMID: %s", self.nct, self.pmid)
	
	
	def download_pmc_packages(self, run_dir):
		""" Downloads the PubMed Central package if there is one.
		"""
		if self.pmcids is None or len(self.pmcids) < 1:
			return
		
		if not os.path.exists(run_dir):
			raise Exception("The run directory %s does not exist" % run_dir)
		
		# loop all PubMed Central ids
		for pmc_id in self.pmcids:
			filename = self.archive_name(pmc_id)
			filepath = os.path.join(run_dir, filename)
			
			# we don't yet have the archive, download the XML to get to the links
			if not os.path.exists(filepath):
				links = []
				
				url = "http://www.pubmedcentral.nih.gov/utils/oa/oa.fcgi?id=%s" % pmc_id
				res = requests.get(url)
				if not res.ok:
					logging.warning("%d -- failed to get %s: %s" % (res.status_code, url, res.error))
				else:
					root = parseString(res.content).documentElement
					try:
						# find the link to the package
						records_parent = root.getElementsByTagName('records')[0]
						records = records_parent.getElementsByTagName('record')
						for record in records:
							n_links = record.getElementsByTagName('link')
							for link in n_links:
								if 'tgz' == link.getAttribute('format'):
									links.append(link.getAttribute('href'))
					except Exception as e:
						logging.warning("Error when parsing %s [PMID %s]: %s" % (url, self.pmid, e))
				
				if len(links) > 1:
					logging.warning("xxx>  We got more than 1 link, can not currently handle this, help!")
				
				# download package to file
				for link in links:
					req = urlopen(link)
					with open(filepath, 'wb') as handle:
						shutil.copyfileobj(req, handle)
	
	
	def parse_pmc_packages(self, run_dir, ctakes_in_dir):
		""" Parses the XML found in our OA package. """
		
		if self.pmcids is None or len(self.pmcids) < 1:
			return
		
		if not os.path.exists(run_dir):
			raise Exception("The run directory %s does not exist" % run_dir)
		
		# loop all PubMed Central ids
		for pmc_id in self.pmcids:
			arname = self.archive_name(pmc_id)
			arpath = os.path.join(run_dir, arname)
			if os.path.exists(arpath):
				
				# do we already have these methods?
				methodsname = self.methods_name(pmc_id)
				methodspath = os.path.join(ctakes_in_dir, methodsname)
				if os.path.exists(methodspath):
					continue
				
				# unziptar (if necessary)
				tar = tarfile.open(arpath)
				names = tar.getnames()
				dirname = names[0] if len(names) > 0 else None
				if dirname is None:
					logging.warning("The archive %s is not readable" % arpath)
					return
				
				dirpath = os.path.join(run_dir, dirname)
				if not os.path.exists(dirpath):
					tar.extractall(path=run_dir)
				tar.close()
				
				if not os.path.exists(dirpath):
					logging.warning("Apparently failed to extract %s" % arpath)
					return
				
				methods = []
				
				# get .nxml from the extracted directory
				for filename in os.listdir(dirpath):
					if len(os.path.basename(filename)) > 4 and ".nxml" == os.path.basename(filename)[-5:]:
						
						# parse .nxml
						root = parse(os.path.join(dirpath, filename)).documentElement
						try:
							body = root.getElementsByTagName('body')[0]
							sections = body.getElementsByTagName('sec')
							for section in sections:
								if 'methods' in section.getAttribute('sec-type'):
									methods.append(section.toprettyxml())
						except Exception as e:
							logging.debug("Error when parsing .nxml named %s in %s" % (filename, dirpath))
				
				self.paper_methods = methods
				
				# so we got methods
				if len(methods) > 0:
					with codecs.open(methodspath, 'w', 'utf-8') as handle:
						handle.write("<root>%s</root>\n" % "\n".join(methods))
				else:
					logging.info("No methods found in package %s" % dirpath)
	
	
	@classmethod
	def find_by_nct(cls, nct):
		""" Finds papers published for a given NCT id.
		Uses eutils to find papers with the given NCT in the abstract, could
		probably be more sophisticated.
		Returns an array. """
		
		papers = []
		
		if nct is None:
			logging.error("Need an NCT to find papers")
			return papers
		
		# use eutils to find PMIDs
		url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=(%s%%5BTitle%%2FAbstract%%5D)" % nct
		res = requests.get(url)
		if not res.ok:
			logging.warning("%d -- failed to get %s: %s" % (res.status_code, url, res.error))
		else:
			
			# we are looking for: <IdList><Id>22563743</Id></IdList>
			root = parseString(res.content).documentElement
			id_list = root.getElementsByTagName('IdList')
			if id_list is not None and len(id_list) > 0:
				id_nodes = id_list[0].getElementsByTagName('Id')
				
				# find pmids in <Id/> nodes
				if len(id_nodes) > 0:
					for node in id_nodes:
						if node.firstChild:
							paper = Paper(nct, node.firstChild.data)
							paper.load()
							papers.append(paper)
		
		return papers
	
	
	# -------------------------------------------------------------------------- Storage
	def should_insert(self):
		return self.id is None
	
	def will_insert(self):
		if self.nct is None:
			raise Exception('NCT is not set')
	
	def insert_tuple(self):
		sql = '''INSERT INTO papers (nct, pmid) VALUES (?, ?)'''
		params = (self.nct, self.pmid)
		
		return sql, params
	
	def update_tuple(self):
		sql = '''UPDATE papers SET
			updated = datetime(), pmcids = ?
			WHERE paper_id = ?'''
		params = (
			'|'.join(self.pmcids),
			self.id
		)
		
		return sql, params
	
	
	def load(self):
		if self.id is None and self.pmid is None and self.nct is None:
			return
		
		data = None
		
		if self.id is not None:
			sql = '''SELECT * FROM papers WHERE paper_id = ?'''
			data = Paper.sqlite_select_one(sql, (self.id,))
		
		elif self.pmid is not None:
			sql = '''SELECT * FROM papers WHERE pmid = ?'''
			data = Paper.sqlite_select_one(sql, (self.pmid,))
		
		elif self.nct is not None:
			sql = '''SELECT * FROM papers WHERE nct = ?'''
			data = Paper.sqlite_select_one(sql, (self.nct,))
		
		# fill ivars
		if data is not None:
			self.id = data[0]
			self.nct = data[1]
			self.pmid = data[2]
			self.pmcids = data[3].split('|') if data[3] else []
	
	
	@classmethod
	def table_structure(cls):
		return '''(
			paper_id INTEGER PRIMARY KEY AUTOINCREMENT,
			nct VARCHAR,
			pmid INT,
			pmcids TEXT,
			updated TIMESTAMP
		)'''
	
	@classmethod
	def did_setup_tables(cls, db_path):
		cls.add_index('nct')
		cls.add_index('pmid')
	
	
	# -------------------------------------------------------------------------- Utilities
	def __unicode__(self):
		return '<paper.Paper %s>' % (self.pmid)
	
	def __str__(self):
		return unicode(self).encode('utf-8')
	
	def __repr__(self):
		return str(self)

