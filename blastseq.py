import gzip, json, codecs, sys, os
from xml.etree import ElementTree as ET
from operator import itemgetter
import time
import shutil
from shutil import copytree, copyfile, rmtree


class BlastSeq(object):

	def __init__(self, temp_location, blast_location, database_location, query_location, min_length):
		self.location = temp_location
		self.blast_loc = blast_location
		self.query_loc = query_location
		self.temp_query_loc = self.location + "temp_query.fsa"
		self.database_loc = database_location
		self.xml_loc = self.location + "temp_query.xml"
		self.res_loc = self.location + "results/"
		self.data = {}
		self.min_length = min_length

	def run(self):
		print("Running..")
		print("Querying file: %s" % self.query_loc)
		queries_done = 0
		with codecs.open(self.query_loc, "r") as fasta_file:
			while True:
				identifier = fasta_file.readline()
				data = fasta_file.readline()
				if not data: break

				with codecs.open(self.temp_query_loc, "w") as fast_file:
					fast_file.write(identifier)
					fast_file.write(data)

				self.blast()
				self.read_xml()
				queries_done += 1
				print("Done %d queries" % queries_done)

		self.save_data()

	def blast(self):
		os.system(self.blast_loc + " -query " + self.temp_query_loc + " -db " + self.database_loc + " -outfmt 5 -matrix BLOSUM62 -evalue 0.00000000001 -max_target_seqs 1000 -num_threads 2 -gapopen 3 -gapextend 11 -word_size 6 -threshold 40 -out " + self.xml_loc)

	def read_xml(self):
		for event, elem in ET.iterparse(self.xml_loc, events=("start", "end")):
			if event == "end":
				if elem.tag == "Iteration":
					self.process_element(elem)
					elem.clear()

	def process_element(self, elem):
		query_def = elem.find("Iteration_query-def").text.split(" ")[1]
		#query_mag = query_def.split("/")[3].split("_")[0] ##
		hsps = []
		for hit in elem.find("Iteration_hits").findall("Hit"):
			hit_def = hit.find("Hit_def").text
			#hit_mag = hit_def.split("/")[3].split("_")[0] 
			#if query_mag == hit_mag:
			#	continue

			for hsp in hit.find("Hit_hsps").findall("Hsp"):
				hsp_len = int(hsp.find("Hsp_align-len").text)
				hsp_iden = int(hsp.find("Hsp_identity").text)
				if hsp_len < self.min_length or hsp_iden < self.min_length-100:
					continue

				q_s = int(hsp.find("Hsp_query-from").text)
				q_e = int(hsp.find("Hsp_query-to").text)
				h_s = int(hsp.find("Hsp_hit-from").text)
				h_e = int(hsp.find("Hsp_hit-to").text)

				hsps.append([q_s, q_e, h_s, h_e, hsp_len, hit_def, hsp_iden])
		hsps.sort(key=itemgetter(4), reverse=True)
		self.data[query_def] = hsps


	def save_data(self):
		if not os.path.exists(self.res_loc):
			os.makedirs(self.res_loc)
		with gzip.open(self.res_loc + "/finished.gz", "wb") as gzip_file:
			gzip_file.write(bytes(json.dumps(self.data), "utf-8"))
			self.data.clear()

