import argparse, os, sys, gzip, json
from tqdm import tqdm
from natsort import natsorted
import Levenshtein
import shutil
from operator import itemgetter
from text_encoder import TextEncoder

class ClusterSeperator:

	''' Code that seperates cluster hits. This is used in case there's a sliding window effect going on, so clusters have texts inside them that have
		nothing to do with each other '''


	def __init__(self, filled_clusters, save_folder, files_to_read, language, min_count_for_seperating, max_count_for_seperating, max_distance):
		self.filled_clusters = filled_clusters
		self.save_folder = save_folder
		self.files_to_read = files_to_read.split()
		self.language = language
		self.min_count_for_seperating = min_count_for_seperating
		self.max_count_for_seperating = max_count_for_seperating
		self.max_distance = max_distance


	''' Basic generator to read the given files '''
	def read_clusters(self):
		if len(self.files_to_read) != 0:
			for f in self.files_to_read:
				with gzip.open(self.filled_clusters + "/" + f, "rt") as gzf:
					yield json.loads(gzf.read()), f
		else:
			files = natsorted(os.listdir(self.filled_clusters))
			for f in files:
				if not os.path.exists(self.save_folder + "/" + f):
					with gzip.open(self.filled_clusters + "/" + f, "rt") as gzf:
						yield json.loads(gzf.read()), f


	''' Helper method for tqdm, so we know how many clusters we have to read '''
	def calculate_total_clusters_to_read(self):
		if len(self.files_to_read) != 0:
			return len(self.files_to_read)
		else:
			return len(os.listdir(self.filled_clusters))


	''' This method is called to begin the sepeartion process '''
	def seperate_clusters(self):
		total_files = self.calculate_total_clusters_to_read()
		for cluster_data, filename in tqdm(self.read_clusters(), total=total_files):
			new_clusters = {}
			for key, value in cluster_data.items():
				new_c = self.seperate(key, value, filename)
				new_clusters.update(new_c)

			self.save_new_clusters(new_clusters, filename)


	''' Checks if a cluster needs seperating. i.e, if it has enough hits '''
	def needs_seperating(self, value):
		if len(value["hits"]) < self.min_count_for_seperating or len(value["hits"]) > self.max_count_for_seperating:
			return False
		else:
			return True

	''' Define which seperation method to use '''
	def seperate(self, key, value, filename):
		if not self.needs_seperating(value):
			return {key: value}

		return self.seperate_blast(key, value, filename)

	''' Deprecated method, works, but is _very_ computationally expensive if hit count is long. Uses Levenshtein distance as the measure '''
	def seperate_levenshtein(self, key, value):
		encoder = TextEncoder(self.language)
		new_clusters = []

		## Get texts, sort, encode
		texts = sorted(value["hits"], key=len, reverse=True)
		encoded = [encoder.encode_text(t["text"]) for t in texts]
		dones = set()

		## Go through texts, starting for longest to shortest. Add indexes to own clusters if l distance low enough
		for start_text_index in range(len(texts)):
			if start_text_index in dones:
				continue
			new_cluster = [start_text_index]
			dones.add(start_text_index)
			for comp_text_index in range(start_text_index + 1, len(texts)):
				if comp_text_index in dones or len(texts[start_text_index]["text"]) * self.max_distance > len(texts[comp_text_index]["text"]):
					continue

				distance = Levenshtein.distance(encoded[start_text_index], encoded[comp_text_index])
				if (len(encoded[start_text_index]) - distance) / len(encoded[start_text_index]) >= self.max_distance:
					new_cluster.append(comp_text_index)
					dones.add(comp_text_index)
			new_clusters.append(new_cluster)

		## Find single clusters
		single_clusters = []
		single_clusters = [index for index, hits in enumerate(new_clusters) if len(hits) == 1]
		single_clusters = [new_clusters.pop(index) for index in reversed(single_clusters)]


		## Combine single clusters to some other cluster
		for single_cluster in single_clusters:
			## Only comparing against one node in each cluster
			current_to_add = 0
			current_distance = 0
			for cluster_i, cluster in enumerate(new_clusters):
				hit_to_compare = cluster[0]
				distance = Levenshtein.distance(encoded[single_cluster[0]], encoded[hit_to_compare])
				if distance > current_distance:
					current_to_add = cluster_i
					current_distance = distance
			new_clusters[current_to_add].append(single_cluster[0])

		clusters = {}
		for cluster_i, cluster in enumerate(new_clusters):
			new_key = "{}_{}".format(key, cluster_i)
			clusters[new_key] = {}
			clusters[new_key]["length"] = 0
			clusters[new_key]["hits"] = []
			for text_index in cluster:
				clusters[new_key]["hits"].append(texts[text_index])

		return clusters

	''' Makes sure there are no remnants from previous clusters in the temp BLAST folder '''
	def clean_blast_folder(self):
		if not os.path.exists(self.blast_folder):
			os.makedirs(self.blast_folder)
		else:
			shutil.rmtree(self.blast_folder)
			os.makedirs(self.blast_folder)


	''' Seperate a cluster using BLAST. return a dictionary with new clusters '''
	def seperate_blast(self, key, value, filename):
		print()
		print("Filename: {}\tCluster size: {}".format(filename, len(value["hits"])))

		self.blast_folder = self.save_folder + "/blast"
		self.clean_blast_folder()
		encoder = TextEncoder(self.language)

		hits = value["hits"]
		hits.sort(key=lambda k: len(k["text"]), reverse=False)

		texts = [v["text"] for v in value["hits"]]
		encoded = [encoder.encode_text(t) for t in texts]

		self.make_db(encoded)
		results = self.blast_data()
		hit_results = self.extract_hit_results(results)
		clusters = []
		cluster_map = {}
		done_i = set()

		for i in range(len(hit_results)):
			if i in done_i: continue
			curr = hit_results[i]
			hit_length = curr[0][2]
			cluster = [i]
			done_i.add(i)
			for hsp in curr:
				align_text_i = hsp[0]
				align_length = hsp[1]
				align_text_full_length = len(encoded[align_text_i])
				if align_text_i in done_i: continue
				if hit_length > align_text_full_length:
					longer = hit_length
				else:
					longer = align_text_full_length
				if longer * self.max_distance < align_length:
					cluster.append(align_text_i)
					done_i.add(align_text_i)
					cluster_map[align_text_i] = len(clusters)

			cluster_map[i] = len(clusters)
			clusters.append(cluster)

		## FIND LEN == 1:
		top = []
		for cluster_i, cluster in enumerate(clusters):
			if len(cluster) == 1:
				hit_index = cluster[0]
				res = hit_results[hit_index]
				best = (None, 10000)
				for v in res:
					align_text_i = v[0]
					align_length = v[1]
					hit_length = v[2]
					align_text_full_length = len(encoded[align_text_i])
					diff = abs(hit_length - align_text_full_length)
					if diff < best[1]:
						best = (align_text_i, diff)
				res.sort(key=itemgetter(1), reverse=True)
				#print(res[0][0], cluster_map[res[0][0]])
				try:
					clusters[cluster_map[res[0][0]]].append(hit_index)
				except KeyError:
					pass
				top.append(cluster_i)

		top.sort(reverse=True)
		for i in top:
			clusters.pop(i)

		new_clusters = {}
		for cluster_i, cluster in enumerate(clusters):
			l = 0
			cluster_hits = []
			for hit_index in cluster:
				cluster_hits.append(hits[hit_index])
				l += len(texts[hit_index])
			d = {"hits": cluster_hits, "length": int(l/len(cluster_hits))}
			new_clusters[key + "_" + str(cluster_i)] = d
		return new_clusters

	''' Extract BLAST hits from the BLAST output file '''
	def extract_hit_results(self, results):
		hits = results.split("# BLASTP 2.4.0+")
		hit_results = []
		for hit_i, hit in enumerate(hits):
			hit_r = []
			hit_length = 0
			if not hit: continue
			for line in hit.split("\n"):
				if line.startswith("#") or not line: continue
				if line.startswith(str(hit_i)):
					hit_length = int(line.split()[5])
					continue
				splits = line.split()
				align_text_i = int(splits[0]) - 1
				align_length = int(splits[5])
				hit_r.append((align_text_i, align_length, hit_length))
			if not hit_r:
				continue
			hit_r.sort(key=itemgetter(1), reverse=True)
			hit_results.append(hit_r)

		return hit_results


	''' Make BLAST DB '''
	def make_db(self, encoded):
		gi = 1
		with open(self.blast_folder + "/database.fsa", "w") as dbf:
			for text in encoded:
				dbf.write(">gi|{} {}\n{}\n".format(gi, gi, text))
				gi += 1
		print()
		os.system("makeblastdb -in {}/database.fsa -out {}/textdb -dbtype prot -title textdb -parse_seqids -hash_index".format(self.blast_folder, self.blast_folder))


	''' Compares the texts using BLAST '''
	def blast_data(self):
		os.system("blastp -db {}/textdb -query {}/database.fsa -word_size 4 -threshold 400 -gapopen 3 -gapextend 11 -matrix BLOSUM62 -evalue 1e-5 -outfmt \"7 stitle qstart qend sstart send length ppos\" -max_target_seqs 10000 -num_threads 24 > {}/results.tsv".format(self.blast_folder, self.blast_folder, self.blast_folder))
		with open("{}/results.tsv".format(self.blast_folder), "r") as tsvf:
			tsvfile = tsvf.read()
		return tsvfile

	''' Saves new clusters '''
	def save_new_clusters(self, new_clusters, filename):
		if not os.path.exists(self.save_folder):
			os.makedirs(self.save_folder)

		with gzip.open(self.save_folder + "/" + filename, "wt") as gzf:
			gzf.write(json.dumps(new_clusters))

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Cluster seperator")
	parser.add_argument("--filled_clusters", help="Location of the filled clusters that are to be seperated.", required=True)
	parser.add_argument("--save_folder", help="Folder where to save the seperated clusters." required=True)
	parser.add_argument("--language", help="Which language to use for protein encoding.", default="eng")
	parser.add_argument("--min_count", help="Minimum hit count to start seperating.", default=100, type=int)
	parser.add_argument("--max_count", help="Maximum hit count to start seperating.", default=20000, type=int)
	parser.add_argument("--max_distance", help="Maximum length distance. Default 0.75" default=0.75, type=float)


	args = parser.parse_args()
	print(args)

	seperator = ClusterSeperator(args.filled_clusters, args.save_folder, args.files_to_read,
								args.seperate_style, args.language, args.min_count, args.max_count, args.max_distance)
	seperator.seperate_clusters()
