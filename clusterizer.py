import os, sys, json, gzip, logging, tarfile, argparse, shelve
import networkx as nx
from operator import itemgetter
from natsort import natsorted
from joblib import Parallel, delayed
from community import CommunityDetector
logging.basicConfig(level=0)

class ParallelJobRunner:

	def __init__(self, output_folder, min_length, max_length, node_similarity, compress):
		self.output_folder = output_folder
		self.min_length = min_length
		self.max_length = max_length
		self.node_similarity = node_similarity

		## Read data, can be either tsv files or gzipped tar files
	def read_data_parallel(self, filename, file_index, min_alignment_score):
		file_loc = self.output_folder + "/batches/" + filename
		data = {}
		if filename.endswith("tar.gz"): ## TAR compressed
			tarf = tarfile.open(file_loc)
			members = tarf.getmembers()
			for member in members:
				if not member.name.endswith(".tsv"): continue
				memberfile = tarf.extractfile(member)
				tsv_data = self.process_tsv(memberfile.read().decode(), min_alignment_score)
				data.update(tsv_data)
		elif filename.endswith(".gz"):
			with gzip.open(file_loc, "rt") as gzf:
				gd = json.loads(gzf.read())
			for key, value in gd.items():
				tsv_data = self.process_tsv(value, min_alignment_score)
				data.update(tsv_data)
		elif filename.endswith(".tsv"):
			with open(file_loc, "r") as tsv_file:
				tsv_data = self.process_tsv(tsv_file.read(), min_alignment_score)
				data.update(tsv_data)
		else:
			raise TypeError("Wrong file format.")
		return data

		## Read the actual TSV file
	def process_tsv(self, data, min_alignment_score):
		tsv_data = {}
		for line in data.split("\n"):
			if line.startswith("# Query:"): ## Starting a new query
				key = line.split(" ", 3)[3].strip()
				continue
			elif not line or line.startswith("#"): # other #'s are just comments
				continue
			tabs = line.split("\t")
			other_key = tabs[0]
			q_start, q_end, h_start, h_end, length = [int(val) for val in tabs[1:-1]]
			alignment = float(tabs[-1])
			if key == other_key or not self.min_length <= length <= self.max_length or alignment < 100*min_alignment_score:
				continue
			tsv_data.setdefault(key, [])
			tsv_data[key].append([q_start, q_end, h_start, h_end, length, other_key])
		return tsv_data

		## Flattens data, returns all hsps from the subkeys under the main key (e.g. x_1_2, x_3_4 --> x)
	def flatten_data_parallel(self, key, value):
		flattened_data = {}
		real_hsps = []
		for sub_key_data in value:
			sub_key = sub_key_data[0]
			query_index_start = int(sub_key.split("__")[1].split("_")[0])
			query_extra = 0
			if query_index_start != 0: ## To get actual offset values
				query_extra = query_index_start

			for hsp in sub_key_data[1]:
				q_start, q_end, h_start, h_end, length, other_key = hsp
				hit_index_start = int(other_key.split("__")[1].split("_")[0])
				hit_extra = 0
				if hit_index_start != 0: ## Same here
					hit_extra = hit_ind

				## Adding the offsets to the values
				q_start += query_extra
				q_end += query_extra
				h_start += hit_extra
				h_end += hit_extra
				other_key = other_key.split("__")[0]
				real_hsps.append([q_start, q_end, h_start, h_end, other_key])
		flattened_data[key] = real_hsps
		return flattened_data

	def find_nodes_parallel(self, key, value):
		nodes = {}
		for hsp in value:
			begin_node = hsp[0:2]
			end_node = hsp[2:4]
			other_key = hsp[5]
			nodes.setdefault(key, []).append(begin_node)
			nodes.setdefault(other_key, []).append(end_node)
		return nodes

	def stringify(self, key, node):
		return "{}___{}_{}".format(key, node[0], node[1])

	def stringify_data_parallel(self, key, value):
		data = {}
		data[key] = []
		for hsp in value:
			begin = self.stringify(key, hsp[0:2])
			end = self.stringify(hsp[5], hsp[2:4])
			data[key].append([begin, end])
		return data

	def calculate_node_similarities_parallel(self, key, nodes):
		nodes.sort(key=itemgetter(0)) ## Sort by starting offset
		new_nodes = []
		mapping = {}
		used = set()
		for i in range(0, len(nodes)):
			if i in used:
				continue
			curr_node = nodes[i]
			new_node_nodes = [curr_node] ## add all nodes here that are to be considered as one, then calc centroid
			for j in range(i+1, len(nodes)):
				if j in used:
					continue
				comp_node = nodes[j]
				sim = self.similarity(curr_node, comp_node)
				if sim == 1:
					new_node_nodes.append(comp_node)
					used.add(j)
				elif sim == -1:
					break
				else:
					continue

			new_node = self.stringify(key, self.calculate_new_node(new_node_nodes)) ## already stringified
		#	if len(new_node_nodes) > 1:
				#print(new_node_nodes)
			#new_nodes.append(new_node)

			## TODO after testing, only add nodes that are centroid of more than 1 to mapping, save space
			#
			for node in new_node_nodes:
				mapping[self.stringify(key, node)] = new_node

		return mapping



	def similarity(self, n1, n2):
		#print(n1, n2)
		lengths = n1[1] - n1[0], n2[1] - n2[0]

		extra = min(lengths) * (1-self.node_similarity)
		if n2[0]-extra > n1[0]:
			return -1
		else:
			overlap = n1[1] - n2[0]
			if n2[1] < n1[1]:
				overlap - n1[1]-n2[1]
			if overlap/max(lengths) > self.node_similarity:
			#	print(n1, n2, "Woo")
				return 1
			else:
				return 0
			#overlap = n2[0]-n1[0]
			#overlap = max(n1[0], n2[0]) + min(n1[1], n2[1])
			#if overlap < extra:
			#	return 0
			#else:
			#	print(n1, n2, "WOO")
			#	return 1

	def calculate_new_node(self, new_nodes):
		starts = []
		ends = []
		for node in new_nodes:
			starts.append(node[0])
			ends.append(node[1])
		return [int(sum(starts) / len(starts)), int(sum(ends) / len(ends))]


class Clusterizer:

	def __init__(self, output_folder, min_length, max_length, threads, node_similarity, pre_split, compress=False):
		self.output_folder = output_folder
		self.min_length = min_length
		self.max_length = max_length
		self.threads = threads
		self.pre_split = pre_split
		self.parallelizer = ParallelJobRunner(output_folder, min_length, max_length, node_similarity, compress)
		self.community = CommunityDetector()
		self.clusters_per_file = 1000
		self.min_alignment_score = 0.65

	def clusterize(self):
		logging.info("Starting clusterizing, using {} cores...".format(self.threads))
		data = self.read_data()
		data = self.flatten_data(data)
		nodes = self.find_nodes(data)
		data = self.stringify_data(data)
		mapping = self.calculate_node_similarities(nodes)
		graph = self.fill_graph(data, mapping)
		self.extract_clusters(graph, 0)

		## Read the data in parallel, combine results into one dictionary, data = dictionary, key = id (file1), value = list of hsps
	def read_data(self):
		logging.info("Reading data...")
		files = os.listdir(self.output_folder + "/batches")
		datas = Parallel(n_jobs=self.threads)(delayed(self.parallelizer.read_data_parallel)(filename, file_index, self.min_alignment_score) for file_index, filename in enumerate(files))
		data = {key: value for data_dictionary in datas for key, value in data_dictionary.items()}
		return data

		## Flatten the data in case keys were pre split before for BLASTing, i.e, file1 split into file1__0_1000, file1__1000_2000...
	def flatten_data(self, data):
		if not self.pre_split:
			return data
		else:
			logging.info("Flattening data...")
			## First gather all subkey datas
			temp_data = self.gather_sub_key_data(data)
			## Parallelize flattening
			flattened_datas = Parallel(n_jobs=self.threads)(delayed(self.parallelizer.flatten_data_parallel)(key, value) for key, value in temp_data.items())
			data = {key: value for data_dictionary in flattened_datas for key, value in data_dictionary.items()}
			return data

	def gather_sub_key_data(self, data):
		keys = {}
		temp_data = {}
		for key in data:
			q_key = key.split("__")[0]
			keys.setdefault(q_key, [])
			keys[q_key].append(key)

		for key, value in keys.items():
			temp_data[key] = []
			for sub_key in value:
				temp_data[key].append([sub_key, data[sub_key]])
		return temp_data

		## Finds all nodes (offset_start, offset_end) for every key
	def find_nodes(self, data):
		logging.info("Finding nodes...")

		node_dicts = Parallel(n_jobs=self.threads)(delayed(self.parallelizer.find_nodes_parallel)(key, value) for key, value in data.items())
		nodes = {}
		for node_dict in node_dicts:
			for key, value in node_dict.items():
				if key in nodes:
					nodes[key] += value
				else:
					nodes[key] = value

		return nodes


		## Make strings from the hsps values
	def stringify_data(self, data):
		logging.info("Stringifying data...")

		stringified_dicts = Parallel(n_jobs=self.threads)(delayed(self.parallelizer.stringify_data_parallel)(key, value) for key, value in data.items())
		data = {key: value for data_dictionary in stringified_dicts for key, value in data_dictionary.items()}

		return data


		## Calculate mean / centroid nodes, so two nodes that are almost same will be considered one
		## TODO, on disk
	def calculate_node_similarities(self, nodes):
		logging.info("Calculating node similarities...")

		maps = Parallel(n_jobs=self.threads)(delayed(self.parallelizer.calculate_node_similarities_parallel)(key, value) for key, value in nodes.items())
		mapping = {}
		##TODO, if nodes unncessary, remove
		for data_map in maps:
			mapping.update(data_map)

		return mapping


	def fill_graph(self, data, mapping):
		logging.info("Filling graph...")
		graph = nx.Graph()
		for key, pairs in data.items():
			for edgepair in pairs:
				graph.add_edge(mapping[edgepair[0]], mapping[edgepair[1]])
		return graph

	def extract_clusters(self, graph, iteration):
		subgraphs = nx.connected_component_subgraphs(graph)
		cluster_index = 0
		save_index = 0
		clusters = {}
		for subgraph_index, subgraph in enumerate(subgraphs):
			nodes = subgraph.nodes()
			edges = subgraph.edges()
			new_clusters = self.community.detect(nodes, edges)
			for new_cluster in new_clusters:
				clusters["cluster_{}".format(cluster_index)] = new_cluster
				cluster_index += 1
			if len(clusters) >= self.clusters_per_file:
				self.save_clusters(clusters, save_index, iteration)
				save_index += 1
				clusters.clear()
		self.save_clusters(clusters, save_index, iteration)

	def save_clusters(self, clusters, save_index, iteration):
		if not os.path.exists("{}/clusters/unfilled/iteration_{}".format(self.output_folder, iteration)):
			os.makedirs("{}/clusters/unfilled/iteration_{}".format(self.output_folder, iteration))
		with gzip.open("{}/clusters/unfilled/iteration_{}/clusters_{}.gz".format(self.output_folder, iteration, save_index), "wt") as gzf:
			gzf.write(json.dumps(clusters))

class ClusterizerVol2(Clusterizer):

	def __init__(self, output_folder, min_length, max_length, threads, node_similarity, pre_split, compress=False):
		self.output_folder = output_folder
		self.min_length = min_length
		self.max_length = max_length
		self.threads = threads
		self.pre_split = pre_split
		self.node_similarity = node_similarity
		self.parallelizer = ParallelJobRunner(output_folder, min_length, max_length, node_similarity, compress)
		self.community = CommunityDetector()
		self.clusters_per_file = 1000
		self.files_per_iteration = 50

	def clusterize(self):
		logging.info("Starting clusterizing, using {} cores...".format(self.threads))
		current_iteration = 0
		file_count = int(self.get_file_counts())
		file_count = 150
		for i in range(0, file_count, self.files_per_iteration):
			logging.info("Clusterizing {}/{} files, iteration {}, {} per iteration...".format(i, file_count, current_iteration, self.files_per_iteration))
			data = self.read_data(i)
			data = self.flatten_data(data)
			nodes = self.find_nodes(data)
			data = self.stringify_data(data)
			mapping = self.calculate_node_similarities(nodes)
			graph = self.fill_graph(data, mapping)
			self.extract_clusters(graph, i)
			current_iteration += 1
		logging.info("Clusterized all files...")
		logging.info("Starting combining clusters...")
		self.combine_clusters(current_iteration)

	def get_file_counts(self):
		files = os.listdir(self.output_folder + "/batches/")
		return len(files)

	def read_data(self, iteration):
		logging.info("Reading data...")
		files = natsorted(os.listdir(self.output_folder + "/batches"))
		files = files[iteration*self.files_per_iteration:(iteration+1)*self.files_per_iteration]
		datas = Parallel(n_jobs=self.threads)(delayed(self.parallelizer.read_data_parallel)(filename, file_index) for file_index, filename in enumerate(files))
		data = {key: value for data_dictionary in datas for key, value in data_dictionary.items()}
		return data

	def gload(self, where):
		with gzip.open(where, "rt") as gzf:
			return json.loads(gzf.read())


	def compare(self, c1, c2):
		lengths = [c1[1]-c1[0], c2[1]-c2[0]]
		maxlen = max(lengths)
		extra = maxlen*0.1
		if c2[1] > c1[1] + extra:
				return -1
		else:
				overlap = set(range(c1[0], c1[1])).intersection(set(range(c2[0], c2[1])))
				if len(overlap)/maxlen >= self.node_similarity:
						return 1
				else:
						return 0


		##TODO FIX! n2 is the one changing, not n1
	def compare_v2(self, n1, n2):
		lengths = [n1[1]-n1[0], n2[1]-n2[0]]
		maxlen = max(lengths)
		extra_per_side = maxlen*(1-self.node_similarity)/2
		if n1[0] > n2[1]:
			return -1
		else:
			## too small, too, big, too little in the middle ~~
			if n1[0] < n2[0]-extra_per_side or n1[1] > n2[1]+extra_per_side or n2[0] > n1[0]+extra_per_side or n2[1] < n1[1]-extra_per_side:
				return 0
			else:
				return 1


	def create_mapping(self, clusters, identifier):
		node_map = {} ## SHELVE? or w/e
		doc_map = {}
		for cluster_id, cluster_data in clusters.items():
			nodes = cluster_data[0]
			for node in nodes:
				node_map[node] = identifier + "_" + cluster_id
				node_doc, indexes = node.split("___")
				indexes = [int(val) for val in indexes.split("_")]
				doc_map[node_doc] = doc_map.get(node_doc, [])
				doc_map[node_doc].append(indexes)
		return node_map, doc_map


	def calculate_matching_nodes(self, start_map, iter_map):
		matches={}
		for iter_id, iter_data in iter_map.items():
			doc_matches = []
			if iter_id not in start_map: continue
			else:
				start_data = start_map[iter_id]
				start_data.sort(key=itemgetter(0))
				iter_data.sort(key=itemgetter(0))

				for i in range(0, len(iter_data)):
					iter_node = iter_data[i]
					for j in range(0, len(start_data)):
						start_node = start_data[j]
						comparison = self.compare_v2(iter_node, start_node)
						if comparison == 1:
							doc_matches.append([iter_node, start_node])
						elif comparison == 0:
							continue
						else:
							break
			matches[iter_id] = doc_matches

		return matches

	def process_new_nodes(self, nodes):
		## combine nodes from same doc :)
		print(len(nodes))
		return nodes

	def combine_matches(self, matchings, start_node_map, start_doc, start_clusters, iter_node_map, iter_map, iter_clusters, iteration):
		node_graph = nx.Graph()
		cluster_graph = nx.Graph()
		new_cluster_count = 0
		for iter_node, matches in matchings.items():
			for match in matches:
				match_1 = "{}___{}_{}".format(iter_node, match[0][0], match[0][1])
				match_2 = "{}___{}_{}".format(iter_node, match[1][0], match[1][1])
				c1 = iter_node_map[match_1]
				c2 = start_node_map[match_2]
				cluster_graph.add_edge(c1, c2)
				#node_graph.add_edge("{}___{}".format(iter_node, match[0]), "{}___{}".format(iter_node, match[1]))

		for cluster_sg in nx.connected_component_subgraphs(cluster_graph):
			clusters_to_combine = cluster_sg.nodes()

			new_nodes = []
			for cluster in clusters_to_combine:
				noident = cluster.split("_", 1)[1]
				if "iter" in cluster:
					new_nodes += iter_clusters[noident][0]
					del iter_clusters[noident]
				else:
					new_nodes += start_clusters[noident][0]
					del start_clusters[noident]
			new_nodes = self.process_new_nodes(new_nodes)
			new_cluster_key = "start_cluster_{}_{}".format(iteration, new_cluster_count)
			for node in new_nodes:
				start_node_map[node] = new_cluster_key
				indexes = [int(val) for val in node.split("___")[1].split("_")]
				node_doc = node.split("__")[0]
				start_doc[node_doc] = start_doc.get(node_doc, [])
				start_doc[node_doc].append(indexes)
			start_clusters[new_cluster_key] = new_nodes

		## Add iter clusters that are not there yet
		for key, value in iter_clusters.items():
			new_key = "start_" + str(iteration)
			for node in value[0]:
				start_node_map[node] = new_key
				indexes = [int(v) for v in node.split("___")[1].split("_")]
				doc = node.split("___")[0]
				start_doc[doc] = start_doc.get(doc, [])
				start_doc[doc].append(indexes)
			start_clusters[new_key] = value


	def load_iteration_folder_data(self, folder):
		files = os.listdir(folder)
		d = {}
		for filename in files:
			d.update(self.gload(folder + "/" + filename))
		return d

	def minimize_indexes(self, start_doc_mapping, iter_node_mapping):
		for mapping in [start_doc_mapping, iter_node_mapping]:
			for key, value in mapping.items():
				mapping[key] = self.remove_duplicates(value)

		##HORRIBLE :#
	def remove_duplicates(self, indexes):
		new_indexes = []
		for index in indexes:
			new_indexes.append(json.dumps(index))
		new_indexes = set(new_indexes)
		return [json.loads(index_list) for index_list in new_indexes]

	def combine_clusters(self, iterations):
		## Start by calculating maps for every iteration :) Use shelves to store
		start_folder = "{}/clusters/unfilled/iteration_{}".format(self.output_folder, 0)
		start_data = self.load_iteration_folder_data(start_folder)
		start_node_mapping, start_doc_mapping = self.create_mapping(start_data, "start")
		for i in range(1, iterations):
			iter_folder = "{}/clusters/unfilled/iteration_{}".format(self.output_folder, i)
			iter_data= self.load_iteration_folder_data(iter_folder)
			iter_node_mapping, iter_doc_mapping = self.create_mapping(iter_data, "iter")
			matchings = self.calculate_matching_nodes(start_doc_mapping, iter_doc_mapping)
			self.combine_matches(matchings, start_node_mapping, start_doc_mapping, start_data, iter_node_mapping, iter_doc_mapping, iter_data, i)
			self.minimize_indexes(start_doc_mapping, iter_node_mapping)
		print(start_data)
## get connected components: each subgraph is nodes that should be considered the same
## so for every subgraph,

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Clusterizing the results.")
	parser.add_argument("--output_folder", help="Output folder", required=True)
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--min_length", help="Minimum length of found hit", default=1, type=int)
	parser.add_argument("--max_length", help="Maximum length of found hit", default=100000, type=int)
	parser.add_argument("--node_similarity", help="Minimum node similarity to be conidered the same", type=float, default=0.90)
	parser.add_argument("--pre_split", help="If the data is presplit and needs to be flattened", default=False)
	parser.add_argument("--compress", help="If the data should be compressed mid clusterizing", default=False)
	parser.add_argument("--ver", default="1")
	args = parser.parse_args()

	if args.ver == "1":
		c = Clusterizer(output_folder=args.output_folder, min_length=args.min_length, max_length=args.max_length, threads=args.threads, node_similarity=args.node_similarity,  pre_split=args.pre_split, compress=args.compress)
		print(c.clusterize())
	else:
		c = ClusterizerVol2(output_folder=args.output_folder, min_length=args.min_length, max_length=args.max_length, threads=args.threads, node_similarity=args.node_similarity,  pre_split=args.pre_split, compress=args.compress)
		print(c.clusterize())
