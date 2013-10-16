#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	utilities to handle UMLS
#
#	2013-01-01	Created by Pascal Pfiffner
#


import csv
import sys
import os.path
import logging

from sqlite import SQLite


class UMLS (object):
	""" A class for importing UMLS terminologies into an SQLite database.
	"""
	
	@classmethod
	def check_databases(cls, be_gentle=False):
		""" Check if our databases are in place and if not, import them.
		if "be_gentle" is True, the method will not exit on missing databases.
		
		UMLS: (umls.db)
		If missing prompt to use the `umls.sh` script
		
		SNOMED: (snomed.db)
		Read SNOMED CT from tab-separated files and create an SQLite database.
		"""
		
		# UMLS
		umls_db = os.path.join('databases', 'umls.db')
		if not os.path.exists(umls_db):
			if be_gentle:
				logging.warning("The UMLS database at %s does not exist. Run the import script `databases/umls.sh`." % umls_db)
			else:
				logging.error("The UMLS database at %s does not exist. Run the import script `databases/umls.sh`." % umls_db)
				sys.exit(1)
		
		# SNOMED
		SNOMED.sqlite_handle = None
		try:
			SNOMED.setup_tables()
		except Exception, e:
			if be_gentle:
				logging.warning("SNOMED setup failed: %s" % e)
			else:
				logging.error("SNOMED setup failed: %s" % e)
				sys.exit(1)
		
		# RxNorm
		rxnorm_db = os.path.join('databases', 'rxnorm.db')
		if not os.path.exists(rxnorm_db):
			if be_gentle:
				logging.warning("The RxNorm database at %s does not exist. Run the import script `databases/rxnorm.sh`." % rxnorm_db)
			else:
				logging.error("The RxNorm database at %s does not exist. Run the import script `databases/rxnorm.sh`." % rxnorm_db)
				sys.exit(1)
		
		else:
			rx_map = {
				'descriptions': 'snomed_desc.csv',
				'relationships': 'snomed_rel.csv'
			}
			
			# need to import?
			for table, filename in rx_map.iteritems():
				num_query = 'SELECT COUNT(*) FROM %s' % table
				num_existing = SNOMED.sqlite_handle.executeOne(num_query, ())[0]
				if num_existing > 0:
					continue
				
				snomed_file = os.path.join('databases', filename)
				if not os.path.exists(snomed_file):
					logging.warning("Need to import SNOMED, but the file %s is not present. Download SNOMED from http://www.nlm.nih.gov/research/umls/licensedcontent/snomedctfiles.html" % filename)
					continue
				
				SNOMED.import_csv_into_table(snomed_file, table)



class UMLSLookup (object):
	""" UMLS lookup """
	
	sqlite_handle = None
	preferred_sources = ['"SNOMEDCT"', '"MTH"']	
	
	def __init__(self):
		self.sqlite = SQLite.get('databases/umls.db')
	
	def lookup_code(self, cui, preferred=True):
		""" Return a list with triples that contain:
		- name
		- source
		- semantic type
		by looking it up in our "descriptions" database.
		The "preferred" settings has the effect that only names from SNOMED
		(SNOMEDCD) and the Metathesaurus (MTH) will be reported. A lookup in
		our "descriptions" table is much faster than combing through the full
		MRCONSO table.
		"""
		if cui is None or len(cui) < 1:
			return []
		
		# STR: Name
		# SAB: Abbreviated Source Name
		# STY: Semantic Type
		if preferred:
			sql = 'SELECT STR, SAB, STY FROM descriptions WHERE CUI = ? AND SAB IN (%s)' % ", ".join(UMLSLookup.preferred_sources)
		else:
			sql = 'SELECT STR, SAB, STY FROM descriptions WHERE CUI = ?'
		
		# return as list
		arr = []
		for res in self.sqlite.execute(sql, (cui,)):
			arr.append(res)
		
		return arr
		
	
	def lookup_code_meaning(self, cui, preferred=True, no_html=True):
		""" Return a string (an empty string if the cui is null or not found)
		by looking it up in our "descriptions" database.
		The "preferred" settings has the effect that only names from SNOMED
		(SNOMEDCD) and the Metathesaurus (MTH) will be reported. A lookup in
		our "descriptions" table is much faster than combing through the full
		MRCONSO table.
		"""
		names = []
		for res in self.lookup_code(cui, preferred):
			if no_html:
				names.append("%s (%s)  [%s]" % (res[0], res[1], res[2]))
			else:
				names.append("%s (<span style=\"color:#090;\">%s</span>: %s)" % (res[0], res[1], res[2]))
		
		comp = ", " if no_html else "<br/>\n"
		return comp.join(names) if len(names) > 0 else ''

	

class SNOMED (object):
	sqlite_handle = None
	
	# -------------------------------------------------------------------------- Database Setup
	@classmethod
	def import_csv_into_table(cls, snomed_file, table_name):
		""" Import SNOMED CSV into our SQLite database.
		The SNOMED CSV files can be parsed by Python's CSV parser with the
		"excel-tab" flavor.
		"""
		
		logging.debug('..>  Importing SNOMED %s into snomed.db...' % table_name)
		
		# not yet imported, parse tab-separated file and import
		with open(snomed_file, 'rb') as csv_handle:
			cls.sqlite_handle.isolation_level = 'EXCLUSIVE'
			sql = cls.insert_query_for(table_name)
			reader = unicode_csv_reader(csv_handle, dialect='excel-tab')
			i = 0
			try:
				for row in reader:
					if i > 0:			# first row is the header row
						
						# execute SQL (we just ignore duplicates)
						params = cls.insert_tuple_from_csv_row_for(table_name, row)
						try:
							cls.sqlite_handle.execute(sql, params)
						except Exception as e:
							sys.exit(u'Cannot insert %s: %s' % (params, e))
					i += 1
				
				# commit to file
				cls.sqlite_handle.commit()
				cls.did_import(table_name)
				cls.sqlite_handle.isolation_level = None
			
			except csv.Error as e:
				sys.exit('CSV error on line %d: %s' % (reader.line_num, e))

		logging.debug('..>  %d concepts parsed' % (i-1))


	@classmethod
	def setup_tables(cls):
		""" Creates the SQLite tables we need, not the tables we deserve.
		"""
		if cls.sqlite_handle is None:
			cls.sqlite_handle = SQLite.get('databases/snomed.db')
		
		# descriptions
		cls.sqlite_handle.create('descriptions', '''(
				concept_id INTEGER PRIMARY KEY,
				lang TEXT,
				term TEXT,
				isa VARCHAR,
				active INT
			)''')
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS isa_index ON descriptions (isa)")
		
		# relationships
		cls.sqlite_handle.create('relationships', '''(
				relationship_id INTEGER PRIMARY KEY,
				source_id INT,
				destination_id INT,
				rel_type INT,
				rel_text VARCHAR,
				active INT
			)''')
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS source_index ON relationships (source_id)")
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS destination_index ON relationships (destination_id)")
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS rel_type_index ON relationships (rel_type)")
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS rel_text_index ON relationships (rel_text)")
		
	
	@classmethod
	def insert_query_for(cls, table_name):
		""" Returns the insert query needed for the given table
		"""
		if 'descriptions' == table_name:
			return '''INSERT OR IGNORE INTO descriptions
						(concept_id, lang, term, isa, active)
						VALUES
						(?, ?, ?, ?, ?)'''
		if 'relationships' == table_name:
			return '''INSERT OR IGNORE INTO relationships
						(relationship_id, source_id, destination_id, rel_type, active)
						VALUES
						(?, ?, ?, ?, ?)'''
		return None
	
	
	@classmethod
	def insert_tuple_from_csv_row_for(cls, table_name, row):
		if 'descriptions' == table_name:
			isa = ''
			if len(row) > 6:
				if '900000000000013009' == row[6]:
					isa = 'synonym'
				elif '900000000000003001' == row[6]:
					isa = 'full'
			return (int(row[4]), row[5], row[7], isa, int(row[2]))
		if 'relationships' == table_name:
			return (int(row[0]), int(row[4]), int(row[5]), int(row[7]), int(row[2]))
		return None
	
	
	@classmethod
	def did_import(cls, table_name):
		""" Allows us to set hooks after tables have been imported
		"""
		if 'relationships' == table_name:
			cls.sqlite_handle.execute('''
				UPDATE relationships SET rel_text = 'isa' WHERE rel_type = 116680003
			''')
			cls.sqlite_handle.execute('''
				UPDATE relationships SET rel_text = 'finding_site' WHERE rel_type = 363698007
			''')



class SNOMEDLookup (object):
	""" SNOMED lookup """
	
	sqlite_handle = None
	
	
	def __init__(self):
		self.sqlite = SQLite.get('databases/snomed.db')
	
	def lookup_code_meaning(self, snomed_id, preferred=True, no_html=True):
		""" Returns HTML for all matches of the given SNOMED id.
		The "preferred" flag here currently has no function.
		"""
		if snomed_id is None or len(snomed_id) < 1:
			return ''
		
		sql = 'SELECT term, isa, active FROM descriptions WHERE concept_id = ?'
		names = []
		
		# loop over results
		for res in self.sqlite.execute(sql, (snomed_id,)):
			if not no_html and ('synonym' == res[1] or 0 == res[2]):
				names.append("<span style=\"color:#888;\">%s</span>" % res[0])
			else:
				names.append(res[0])
		
		if no_html:
			return ", ".join(names) if len(names) > 0 else ''
		return "<br/>\n".join(names) if len(names) > 0 else ''



class RxNormLookup (object):
	""" RxNorm lookup """
	
	sqlite_handle = None
	
	
	def __init__(self):
		self.sqlite = SQLite.get('databases/rxnorm.db')
	
	def lookup_code_meaning(self, rx_id, preferred=True, no_html=True):
		""" Return HTML for the meaning of the given code.
		If preferred is True (the default), only one match will be returned,
		looking for specific TTY and using the "best" one. """
		if rx_id is None or len(rx_id) < 1:
			return ''
		
		# retrieve all matches
		sql = 'SELECT STR, TTY, RXAUI FROM RXNCONSO WHERE RXCUI = ? AND LAT = "ENG"'
		found = []
		names = []
		format_str = "<span title=\"RXAUI: %s\">%s <span style=\"color:#888;\">[%s]</span></span>"
		
		# loop over them
		for res in self.sqlite.execute(sql, (rx_id,)):
			found.append(res)
		
		if len(found) > 0:
			
			# preferred name only
			if preferred:
				for tty in ['BN', 'IN', 'PIN', 'SBDC', 'SCDC', 'SBD', 'SCD', 'MIN']:
					for res in found:
						if tty == res[1]:
							names.append(format_str % (res[2], res[0], res[1]))
							break
					else:
						continue
					break
				
				if len(names) < 1:
					res = found[0]
					names.append(format_str % (res[2], res[0], res[1]))
			
			# return a list of all names
			else:
				for res in found:
					names.append(format_str % (res[2], res[0], res[1]))
		
		return "<br/>\n".join(names) if len(names) > 0 else ''
	


# the standard Python CSV reader can't do unicode, here's the workaround
def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
	csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
	for row in csv_reader:
		yield [unicode(cell, 'utf-8') for cell in row]

