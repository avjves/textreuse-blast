import argparse, os, gzip, json, sys, pickle, zlib, logging
from tqdm import tqdm
from operator import itemgetter
from community import CommunityDetector
logging.basicConfig(level=logging.INFO)

class Combiner:

	def __init__(self, old_file_folder, new_file_folder, link_file, link_folder, save_folder):
		self.old_file_folder = old_file_folder
		self.new_file_folder = new_file_folder
		self.link_folder = link_folder
		self.save_folder = save_folder
		self.link_file = link_file
		self.max_count = max_count
		self.min_count = min_count
		self.community = CommunityDetector()
		self.node_similarity = 0.90

	def combine_clusters(self):
		''' Calculate links and combine clusters.
			Will also save the links if link file was specified for future use '''

		links = self.gather_links()
		if self.link_file:
			with gzip.open(self.link_file, "wt") as gzf:
				gzf.write(json.dumps(links))

		self.combine_links_clusters(links)

	def gather_links(self):
		''' Gather all links, by loading both datas in and the calculating links '''
		data = []
		for folder in [self.old_file_folder, self.new_file_folder]:
			logging.info("Processing folder: {}".format(folder))
			cluster_data = self.read_files(folder)
			ids, cluster_map = self.seperate_per_id(cluster_data, compress=False)
			data.append([ids, cluster_map])
			cluster_data = ""

		old_data, new_data = data

		links = self.calculate_cluster_links(old_data, new_data)
		for key, value in links.items():
			links[key] = list(value)

		return links



	def calculate_cluster_links(self, old_data, new_data):
		''' Calculate cluster links between old data and new data '''
		logging.info("Calculating cluster links...")
		links = {}
		tot = len(new_data[0])
		for key, value in tqdm(new_data[0].items(), total=tot):
			base = key.split("___")[0]
			new_indexes = value
			old_indexes = old_data[0].get(base, None)
			if old_indexes == None:
				continue

			old_indexes.sort(key=itemgetter(0))
			new_indexes.sort(key=itemgetter(0))


			for i in range(0, len(new_indexes)):
				new_index_pair = new_indexes[i]
				match = []

				for j in range(0, len(old_indexes)):
					old_index_pair = old_indexes[j]

					resp = self.similarity(new_index_pair, old_index_pair)
					if resp == -1:
						break
					elif resp == 1:
						match.append(old_index_pair)
					else:
						continue

				for matching_hit_pair in match:
					old_cluster = self.get_cid(matching_hit_pair, old_data[1], key)
					new_cluster = self.get_cid(new_index_pair, new_data[1], key)
					links[new_cluster] = links.get(new_cluster, set())
					links[new_cluster].add(old_cluster)
	return links



	def get_cid(self, index_pair, mapping, base):
		doc_id = "{}___{}_{}".format(base, index_pair[0], index_pair[1])
		return mapping[base][doc_id]

	def similarity(self, n1, n2):
		''' Calculate similarity between node1 and node2 '''
		n1_l, n2_l = n1[1] - n1[0], n2[1] - n2[0]

		n1_s = set(list(range(n1[0], n1[1])))
		n2_s = set(list(range(n2[0], n2[1])))
		if n1_l > n2_l:
			intersect_l = len(n1_s.intersection(n2_s))
			to_compare = len(n1_s)
		else:
			intersect_l = len(n2_s.intersection(n1_s))
			to_compare = len(n2_s)

		if intersect_l > self.node_similarity*to_compare:
			return 1
		else:
			return 0


	def read_files(self, folder):
		''' Read files in '''
		logging.info("Reading files...")
		files = os.listdir(folder)
		clusters = {}
		for filename in tqdm(files):
			with gzip.open(folder + "/" + filename, "rt") as gzf:
				data = json.loads(gzf.read())

			else:
				newd = {}
				for key, value in data.items():
					hits = []
					for hit in value["hits"]:
						enc = hit["node"]
						hits.append(enc)
					newd[key] = [hits, 0]
				clusters.update(newd)

		return clusters

	def read_new_clusters(self):
		''' Read new clusters in '''
		clusters = {}
		for filename in tqdm(os.listdir(self.new_file_folder)):
			with gzip.open(self.new_file_folder + "/" + filename, "rt") as gzf:
				gd = json.loads(gzf.read())
			clusters.update(gd)
		return clusters

	def seperate_per_id(self, data, compress):
		''' Seperate hits per ID and also create a mapping of them back to their cluster '''
		logging.info("Seperating data into maps...")
		ids = {}
		cluster_map = {}
		for key, value in tqdm(data.items()):
			for hit_id in value[0]:
				base, indexes = hit_id.split("___")
				if base.count("_") == 2:
					base, s, e = base.split("_")
					base = "{}__{}_{}".format(base, s, e)
				if "ext" in hit_id:
					base = base + "_ext"
					indexes = indexes.split("_ext")[0]
				indexes = [int(val) for val in indexes.split("_")]
				hit_id = "{}___{}_{}".format(base, indexes[0], indexes[1])
				ids[base] = ids.get(base, [])
				ids[base].append(indexes)
				cluster_map[base] = cluster_map.get(base, {})
				cluster_map[base][hit_id] = key

		if compress:
			for key, value in ids.items():
				ids[key] = zlib.compress(pickle.dumps(value))
			for key, value in cluster_map.items():
				cluster_map[key] = zlib.compress(pickle.dumps(value))

		return ids, cluster_map

	def read_links(self):
		''' Read link file '''
		d = {}
		for filename in tqdm(os.listdir(self.link_folder)):
			with gzip.open(self.link_folder + "/" + filename, "rt") as gzf:
				gd = json.loads(gzf.read())
				for key, value in gd.items():
					if key in d:
						d[key] = d[key] + value
					else:
						d[key] = value
		return d

	def combine_links_clusters(self, link_data=None):
		''' Combine clusters based on the links '''
		logging.info("Reading links...")
		if not link_data:
			link_data = self.read_links()
		actual = {}
		for key, value in link_data.items():
			v = value[0]
			actual[v] = actual.get(v, [])
			actual[v].append(key)

		self.combine_clusters(actual)


	def extract_new_hits(self, cluster_data): ## Change this to match what your new data is
		''' Extract hits from the new clusters '''
		hits = []
		for hit in cluster_data["hits"]:
			base = hit["node"].split("_")[0]
			if not base.isdigit():
				hits.append(hit)
		return hits

	def get_good_keys(self, keys, clusters):
		''' Get keys that have at least one new data hit '''
		good_keys = []
		for key in keys:
			clus = clusters[key]
			hits = clus["hits"]
			crr = 0
			for hit in hits:
				if not hit["node"].split("_")[0].isdigit():
					crr += 1
			if crr > 0:
				good_keys.append(key)
		return good_keys

	def combine_clusters(self, actual):
		''' Perform the actual combining '''
		new_clusters = self.read_new_clusters()
		logging.info("Limiting clusters...")
		old_files = os.listdir(self.old_file_folder)
		logging.info("Adding new stuff into clusters...")
		done_news = set()
		for old_file in tqdm(old_files):
			with gzip.open(self.old_file_folder + "/" + old_file, "rt") as old_gz:
				gd = json.loads(old_gz.read())
			for key in list(gd.keys()):
				value = gd[key]
				if key in actual:
					value["new_hits"] = []
					new_cluster_keys = actual[key]
					for key in new_cluster_keys:
						done_news.add(key)
					good_keys = self.get_good_keys(new_cluster_keys, new_clusters)
					for key in good_keys:
						done_news.add(key)
						new_hits = self.extract_new_hits(new_clusters[key])
						value["new_hits"].append(new_hits)
					##TODO Include duplicate deletion here already

			with gzip.open(self.save_folder + "/" + old_file, "wt") as new_gz:
				new_gz.write(json.dumps(gd))

		self.save_seperate_new_clusters(new_clusters, done_news)

	def save_seperate_new_clusters(self, new_clusters, done_news):
		''' Save new clusters that weren't combined into old clusters into one pickle file. '''
		for done in done_news:
			del new_clusters[done]
		with open(self.save_folder + "/completely_new_clusters.pkl", "wb") as pf:
			pickle.dump(new_clusters, pf)

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Add new clusters / new info to old clusters based on new runs.")
	parser.add_argument("--old_file_folder", help="Folder where old clusters are.")
	parser.add_argument("--new_file_folder", help="Folder where new clusters are.")
	parser.add_argument("--link_file", help="Link file. Will be gz file.")
	parser.add_argument("--link_folder", help="Location of the link folder. Use this if linking is done and want to combine them.")
	parser.add_argument("--save_folder", help="Location of the save folder, where new combined clusters will be saved.")

	args = parser.parse_args()
	print(args)
	c = Combiner(args.old_file_folder, args.new_file_folder, args.link_file, args.link_folder, args.save_folder, args.min_count, args.max_count, args.discard_olds)
	c.combine_clusters()
