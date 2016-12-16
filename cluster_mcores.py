import argparse,os,json,gzip,codecs,sys, numpy, multiprocessing
import networkx as nx
from operator import itemgetter
from text_encoder import TextEncoder
from multiprocessing import Process, Queue
import pickle

class QueueWorker(Process):

	def work(self, method, qin, quot):
		data = {}
		while True:
			obj = qin.get(True)
			if obj != None:
				if method == "tsv":
					data.update(self.read_tsv(obj[0], obj[1], obj[2], obj[3], obj[4]))
				elif method == "flatten":
					data.update(self.flatten_data(obj[0], obj[1], obj[2]))
				elif method == "stringify":
					data.update(self.stringify_data(obj[0], obj[1]))
				elif method == "raw_nodes":
					nodes = self.get_nodes(obj[0], obj[1], obj[2])
					for key, value in nodes.items():
						data[key] = data.get(key, set()) | value
				elif method == "similarity":
					data.update(self.cluster_by_similarity(obj[0], obj[1], obj[2]))
			else:
				quot.put(data)
				break

	def read_tsv(self, filename, folder_loc, min_length, max_length, compress=False):
		tsv_data = {}
		if filename.endswith(".gz"):
			with gzip.open(folder_loc + "/" + filename, "rb") as gzip_file:
				gdata = json.loads(str(gzip_file.read(), "utf-8"))
			for filen, data in gdata.items():
				tsv_data.update(self.read_data(data, min_length, max_length))
		elif filename.endswith(".tsv"):
			with codecs.open(folder_loc + "/" + filename, "r") as tsv_file:
				text = tsv_file.read()
				tsv_data.update(self.read_data(text, min_length, max_length))

		if compress:
			for key, value in tsv_data.items():
				tsv_data[key] = zlib.compress(pickle.dumps(value))
		return tsv_data

	def read_data(self, data, min_length, max_length):
		tsv_data = {}
		for line in data.split("\n"):
			if line.startswith("# Query:"):
				key = line.split(" ", 3)[3].strip()
				continue
			elif not line or line.startswith("#"):
				continue
			tabs = line.split("\t")
			other_key = tabs[0]
			if key == other_key:
				continue
			length = int(tabs[5])
			q_start = int(tabs[1])
			q_end = int(tabs[2])
			h_start = int(tabs[3])
			h_end = int(tabs[4])
			if not min_length <= length <= max_length:
				continue
			tsv_data[key] = tsv_data.get(key, [])
			tsv_data[key].append([q_start, q_end, h_start, h_end, length, other_key])
		return tsv_data

	## Combines splitted data
	def flatten_data(self, key, value, compress):
		dicti = {}
		new_hsps = []
		for sub in value:
			sub_key = sub[0]
			if compress:
				value = pickle.loads(zlib.decompress(value))
			query_ind = int(sub_key.split("__")[1].split("_")[0])
			query_extra = 0
			if query_ind != 0:
				query_extra = query_ind
			for hsp in sub[1]:
				if sub_key.split("__")[0] == hsp[5].split("__")[0]:
					continue
				hit_ind = int(hsp[5].split("__")[1].split("_")[0])
				hit_extra = 0
				if hit_ind != 0:
					hit_extra = hit_ind

				hsp[0] += query_extra
				hsp[1] += query_extra
				hsp[2] += hit_extra
				hsp[3] += hit_extra
				hsp[5] = hsp[5].split("__")[0]
				new_hsps.append(hsp)
		dicti[key] = new_hsps

		return dicti


	def stringify_data(self, data, compress):
		for key, value in data.items():
			if compress:
				value = pickle.loads(zlib.decompress(value))
			new_hsps = []
			for hsp in value:
				new_hsps.append([key + "___" + str(hsp[0]) + "_" + str(hsp[1]), hsp[5] + "___" + str(hsp[2]) + "_" + str(hsp[3])])
			if compress:
				data[key] = zlib.compress(pickle.dumps(new_hsps))
			else:
				data[key]= new_hsps
		return data

	def get_nodes(self, key, value, compress):
		nodes = {}
		if compress:
			value = pickle.loads(zlib.decompress(value))
		for hsp in value:
			start_node = hsp[0]
			end_node = hsp[1]
			end_begin = hsp[1].split("___")[0]
			nodes[key] = nodes.get(key, set())
			nodes[end_begin] = nodes.get(end_begin, set())
			nodes[key].add(start_node)
			nodes[end_begin].add(end_node)

		if compress:
			for key, value in nodes.items():
				nodes[key] = zlib.compress(pickle.dumps(value))

		return nodes

	def cluster_by_similarity(self, key, value, compress):
		ready_mappings = {}
		if compress:
			value = pickle.loads(zlib.decompress(value))
		trains = []
		mapping = {}
		for line in value:
			indexes = line.split("___")[1].split("_")
			trains.append([line, int(indexes[0]), int(indexes[1]), int(indexes[1])-int(indexes[0])])
		trains.sort(key=itemgetter(1))
		done_i = set()

		for i in range(0, len(trains)):
			if i in done_i:
				continue
			cluster = []
			current = trains[i]
			cluster.append(current)
			for j in range(i+1, len(trains)):
				if j in done_i:
					continue
				current_compare = trains[j]
				response = self.compare(current, current_compare)
				if response == -1:
					break
				if response == 1:
					cluster.append(current_compare)
					done_i.add(j)
				else:
					continue
			cluster.sort(key=itemgetter(3))
			new_name = cluster[0][0]
			for line in cluster:
				mapping[line[0]] = new_name
		if compress:
			ready_mappings[key] = zlib.compress(pickle.dumps(mapping))
		else:
			ready_mappings[key] = mapping

		return ready_mappings

	def compare(self, c1, c2):
		hsp_extra = c1[3]*0.2

		if c2[1] <= c1[1] + hsp_extra:
			if c1[2] - hsp_extra <= c2[2] <= c1[2] + hsp_extra:
				return 1
			else:
				return 0
		else:
			return -1


def read_data(location, min_length, max_length, compress, num_of_processes):
	data_dictionary = {}
	if os.path.isdir(location):
		files = os.listdir(location)
		files = [f for f in files if f.endswith(".gz")]
	else:
		fl = location.split("/")
		files = [fl.pop(-1)]
		location = "/".join(fl)

	processes, qins, qout = start_processes("tsv", num_of_processes)

	crnt_q = 0

	for f in files:
		qins[crnt_q].put([f, location, min_length, max_length, compress])
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	for i in range(0, len(qins)):
		qins[i].put(None)
		data_dictionary.update(qout.get())

	return data_dictionary


def flatten_data(data, compress, num_of_processes):
	keys = {}
	temp_data = {}
	data_dictionaries = []
	crnt_q = 0

	for key in list(data.keys()):
		q_key = key.split("__")[0]
		keys[q_key] = keys.get(q_key, [])
		keys[q_key].append(key)

	for key, value in keys.items():
			temp_data[key] = []
			for sub_key in value:
				temp_data[key].append([sub_key, data[sub_key]])

	processes, qins, qout = start_processes("flatten", num_of_processes)

	for key, value in temp_data.items():
		qins[crnt_q].put([key, value, compress])
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	for i in range(0, len(qins)):
		qins[i].put(None)
		data_dictionaries.append(qout.get())

	return data_dictionaries


def dict_to_list_of_dicts(data, num_of_lists):
	new_data = [{} for i in range(num_of_lists)]

	crnt = 0
	for key, value in data.items():
		new_data[crnt][key] = value
		crnt += 1
		if crnt == num_of_lists:
			crnt = 0

	return new_data

def stringify_data(data, compress, num_of_processes):
	if type(data) == dict:
		data = dict_to_list_of_dicts(data, num_of_processes)
	processes, qins, qout = start_processes("stringify", num_of_processes)

	crnt_q = 0

	for dictionary in data:
		qins[crnt_q].put([dictionary, compress])
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	dictionary = {}

	for i in range(0, len(qins)):
		qins[i].put(None)
		dictionary.update(qout.get())

	return dictionary



def get_raw_nodes(data, compress, num_of_processes):
	processes, qins, qout = start_processes("raw_nodes", num_of_processes)

	crnt_q = 0

	for key, value in data.items():
		qins[crnt_q].put([key, value, compress])
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	nodes = {}
	dictionaries = []
	for i in range(0, len(qins)):
		qins[i].put(None)
		dictionaries.append(qout.get())

	for dictionary in dictionaries:
		for key, value in dictionary.items():
			nodes[key] = nodes.get(key, set()) | value

	return nodes

def start_processes(method, num_of_processes):
	processes = []
	qins = []
	qout = Queue()

	for i in range(num_of_processes):
		qin = Queue()
		new_process = QueueWorker(target=QueueWorker.work, args=(QueueWorker(), method, qin, qout))
		qins.append(qin)
		processes.append(new_process)
		new_process.start()

	return processes, qins, qout


def cluster_data(data, compress, num_of_processes):
	processes, qins, qout = start_processes("similarity", num_of_processes)

	crnt_q = 0

	for key, value in data.items():
		qins[crnt_q].put([key, value, compress])
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	maps = {}
	for i in range(0, len(qins)):
		qins[i].put(None)
		maps.update(qout.get())

	return maps

def get_graph(data, maps):
	graph = nx.Graph()
	for key, value in data.items():
		for hsp in value:
			end_b = hsp[1].split("___")[0]
			start_node = maps[key][hsp[0]]
			end_node = maps[end_b][hsp[1]]
			graph.add_edge(start_node, end_node)
	return graph

def add_edges(graph, maps, data):
	for key, value in data.items():
		for hsp in value:
			start_b = hsp[0].split("___")[0]
			end_b = hsp[1].split("___")[0]
			graph.add_edge(maps[start_b][hsp[0]], maps[end_b][hsp[1]])

def get_subgraphs(graph):
	sgs = {}
	subgraphs = nx.connected_component_subgraphs(graph)
	for i, sg in enumerate(subgraphs):
		nodes = sg.nodes()
		nodes = uniq(nodes)
		if len(nodes) > 1:
			sgs["cluster_" + str(i)] = nodes
	return sgs

def uniq(nodes):
	uniques = {}
	for node in nodes:
		id_name = node.split("___")[0]
		uniques[id_name] = uniques.get(id_name, [])
		indexes = node.split("___")[1].split("_")
		length = int(indexes[1]) - int(indexes[0])
		uniques[id_name].append([node, length])

	nodes = []
	for key, value in uniques.items():
		value.sort(key=itemgetter(1))
		nodes.append(value[0][0])

	return nodes

def print_nodes(nodes, encoder, data_loc):
	for node in nodes:
		occtext_orig = get_orig_text(node[0], data_loc, node[5])
		print(encoder.get_original_text(occtext_orig, node[1], node[2]))
		print()
		print()
	for node in nodes:
		print(node[0], node[3], node[4])
	print("next")
	input()

def create_clusters_tsv(sub_graphs, out, metadata, examine, data_loc):

	with codecs.open(metadata + "/metadata.json", "r") as json_file:
		metadata = json.load(json_file)

	encoder = TextEncoder()
	clusters = {}
	done_i = 0
	for key, sg in sub_graphs.items():
		sys.stdout.write("Clusters created: %d/%d \r" % (done_i, len(sub_graphs)))
		sys.stdout.flush()
		done_i += 1
		f_nodes = []
		length = 0
		for node in sg:
			node_id = node.split("___")[0].split("__")[0]
			indexes = node.split("___")[1].split("_")
			title = " ".join(metadata[node_id]["title"].split())
			year = metadata[node_id]["year"]
			filename = metadata[node_id]["filename"]
			length += int(indexes[1]) - int(indexes[0])
			f_nodes.append([node_id, indexes[0], indexes[1], title, year, filename])

		f_nodes.sort(key=itemgetter(4))


		info = {}
		info["occyear"] = str(f_nodes[0][4]).strip()
		info["count"] = str(len(f_nodes)).strip()
		info["titles"] = []

		for node in f_nodes:
			info["titles"].append(str(node[3]) + "_" + str(node[4]) + "_" + node[0])

		occtext_orig = get_orig_text(f_nodes[0][0], data_loc, f_nodes[0][5])

		info["titles"] = info["titles"][0:31000]
		info["text"]= " ".join(encoder.get_original_text(occtext_orig, f_nodes[0][1], f_nodes[0][2]).split()).strip().replace('"', ' ').replace("'", " ")[0:31000]
		info["avglength"]= str(len(info["text"]))
		if examine:
			print_nodes(f_nodes, encoder, data_loc)
		clusters[key] = info
	return clusters

def create_clusters_full(sub_graphs, out, metadata, examine, data_loc):
	with codecs.open(metadata + "/metadata.json", "r") as json_file:
		metadata = json.load(json_file)

	encoder = TextEncoder()
	clusters = {}
	done_i = 0
	for key, sg in sub_graphs.items():
		sys.stdout.write("Clusters created: %d/%d \r" % (done_i, len(sub_graphs)))
		sys.stdout.flush()
		done_i += 1
		f_nodes = []
		length = 0
		for node in sg:
			node_id = node.split("___")[0].split("__")[0]
			indexes = node.split("___")[1].split("_")
			title = " ".join(metadata[node_id]["title"].split())
			year = metadata[node_id]["year"]
			filename = metadata[node_id]["filename"]
			length += int(indexes[1]) - int(indexes[0])
			full_orig_text = get_orig_text(node_id, data_loc, filename)
			orig_text = encoder.get_original_text(full_orig_text, indexes[0], indexes[1])
			f_nodes.append([node_id, title, year, orig_text])

		f_nodes.sort(key=itemgetter(2))
		clusters[key]={}
		clusters[key]["hits"] = f_nodes
		clusters[key]["size"] = len(f_nodes)
		clusters[key]["avglen"] = int(length / len(f_nodes))
		clusters[key]["occyear"] = f_nodes[0][2]

		if examine:
			print_nodes_full(f_nodes, encoder, data_loc)
	return clusters


def get_orig_text(id, data_loc, filename):
	if filename.endswith(".gz"):
		with gzip.open(data_loc + "/" + filename, "rb") as gzip_file:
			return str(gzip_file.read(), "utf-8")
	with codecs.open(data_loc + "/" + filename, "r") as json_file:
		gdata = json.load(json_file)

	return gdata[id]["text"]


def save(out, data, name):
	with gzip.open(out + "/" + name + ".gz", "wb") as gzip_file:
		gzip_file.write(bytes(json.dumps(data), "utf-8"))


def clusters_to_tsv(clusters, out):
	with codecs.open(out + "/clusters.tsv", "w") as csv_file:
		csv_file.write("CLUSTER_ID\tAVGLENGTH\tOCCYEAR\tCOUNT\tTITLES\tTEXT\n")
		for key, cluster_data in clusters.items():
			csv_file.write("\t".join([key, cluster_data["AVGLENGTH"], cluster_data["OCCYEAR"], cluster_data["COUNT"], "||".join(cluster_data["TITLES"]), cluster_data["TEXT"]]) + "\n")

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Clustering TSV result file(s).")
	parser.add_argument("-f", "--file_location", help="Location to the results file or folder with multiple result files.", required=True)
	parser.add_argument("-d", "--data_location", help="Location to the original data files / folder.")
	parser.add_argument("-min", "--min_length", help="Minimum length of text", required=True, type=int)
	parser.add_argument("-max", "--max_length", help="Maximum length of text", required=False, default=1000000000, type=int)
	parser.add_argument("-o", "--out", help="Output folder for everything.", required=True)
	parser.add_argument("-m", "--metadata", help="Location to the metadata file, if not set, assuming it's in the output folder.", default=None)
	parser.add_argument("-e", "--examine", action="store_true", help="Look at pairs", default=False)
	parser.add_argument("--num_of_processes", help="Number of processes to launch", default=1, type=int)
	parser.add_argument("--split", action="store_true", default=False, help="If the data was split. Splits should also be seen in the IDs, like ID__startindex_endindex")
	parser.add_argument("--subgraphs", help="Save subgraphs", action="store_true", default=False)
	parser.add_argument("--tsv", help="Save TSV", action="store_true", default=False)
	parser.add_argument("--full", help="Save full clusters", action="store_true", default=False)
	parser.add_argument("--compress", help="Compress in memory, saves RAM, not working atm", action="store_true", default=False)
	args = parser.parse_args()

	data = {}

	if args.metadata == None:
		args.metadata = args.out
	print("Reading data..")
	data = read_data(args.file_location, args.min_length, args.max_length, args.compress, args.num_of_processes)

	if args.split:
		print("Flattening data..")
		data = flatten_data(data, args.compress, args.num_of_processes)

	print("Stringifying...")
	data = stringify_data(data, args.compress, args.num_of_processes)

	print("Finding all nodes..")
	raw_nodes = get_raw_nodes(data, args.compress, args.num_of_processes)

	print("Calculating real nodes...")
	maps = cluster_data(raw_nodes, args.compress, args.num_of_processes)

	print("Filling graph...")
	graph = get_graph(data, maps)

	print("Getting all clusters...")
	sub_graphs = get_subgraphs(graph)

	if args.subgraphs:
		save(args.out, sub_graphs, "subgraphs")

	if args.tsv:
		print("Creating clusters...")
		clusters = create_clusters_tsv(sub_graphs, args.out, args.metadata, args.examine, args.data_location)
		print("Saving clusters as TSV...")
		clusters_to_tsv(clusters, args.out)

	if args.full:
		print("Creating clusters...")
		clusters = create_clusters_full(sub_graphs, args.out, args.metadata, args.examine, args.data_location)
		print("Saving clusters as JSON...")
		save(args.out, clusters, "full_clusters")



