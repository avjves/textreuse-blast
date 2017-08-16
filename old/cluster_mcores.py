import argparse,os,json,gzip,codecs,sys, numpy, multiprocessing
import networkx as nx
from operator import itemgetter
from text_encoder import TextEncoder
from multiprocessing import Process, Queue
from multiprocessing import Pool
import multiprocessing
import zlib
import pickle
import community
from natsort import natsorted


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
						if obj[2] and key in data:
							data[key] = pickle.loads(zlib.decompress(data[key]))
						try:

							data[key] = data.get(key, set()) | value
						except TypeError:
							print(value)
							raise ValueError
						#data[key]= data.get(key, []) + value
						if obj[2]:
							data[key] = zlib.compress(pickle.dumps(data[key]))
				elif method == "similarity":
					data.update(self.cluster_by_similarity(obj[0], obj[1], obj[2]))
				elif method == "create_full_clusters":
					if type(obj) == dict:
						meta = obj
					else:
						data.update(self.create_clusters_full(obj[0], obj[1], obj[2], meta))
			else:
				quot.put(data)
				break

	def read_tsv(self, filename, folder_loc, min_length, max_length, compress):
		print(filename, multiprocessing.current_process())
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
				#mag = key.split("/")[3].split("_")[0]
				continue
			elif not line or line.startswith("#"):
				continue
			tabs = line.split("\t")
			other_key = tabs[0]
			#other_mag = other_key.split("/")[3].split("_")[0]
			#if key == other_key or mag == other_mag:
			#	continue
			if key == other_key:
				continue
			length = int(tabs[5])
			q_start = int(tabs[1])
			q_end = int(tabs[2])
			h_start = int(tabs[3])
			h_end = int(tabs[4])
			alignment = float(tabs[6])
			if not min_length <= length <= max_length:
				continue
			if min_length < 300 and alignment < 0.85:
				continue
			tsv_data[key] = tsv_data.get(key, [])
			tsv_data[key].append([q_start, q_end, h_start, h_end, length, other_key])
		return tsv_data

	## Combines splitted data
	def flatten_data(self, key, value, compress):
		dicti = {}
		new_hsps = []
		for sub in value:
			if compress:
				sub[1] = pickle.loads(zlib.decompress(sub[1]))
			sub_key = sub[0]
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

		if compress:
			for key, value in dicti.items():
				dicti[key] = zlib.compress(pickle.dumps(value))

		return dicti


	def stringify_data(self, data, compress):
		for key, value in data.items():
			if compress:
				value = pickle.loads(zlib.decompress(value))
			new_hsps = []
			for hsp in value:
				new_hsps.append(["%s___%d_%d" % (key, hsp[0], hsp[1]), "%s___%d_%d" % (hsp[5], hsp[2], hsp[3])])
				#new_hsps.append([key + "___" + str(hsp[0]) + "_" + str(hsp[1]), hsp[5] + "___" + str(hsp[2]) + "_" + str(hsp[3])])
			if compress:
				data[key] = zlib.compress(pickle.dumps(new_hsps))
			else:
				data[key] = new_hsps
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
			#nodes[key]= nodes.get(key, [])
			#nodes[end_begin] = nodes.get(end_begin, [])
			#nodes[key].append(start_node)
			#nodes[end_begin].append(end_node)
		#if compress:
		#	for key, value in nodes.items():
		#		nodes[key] = zlib.compress(pickle.dumps(value))

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
				response = self.compare_v3(current, current_compare)
				if response == -1:
					break
				if response == 1:
					cluster.append(current_compare)
					done_i.add(j)
				else:
					continue
			#cluster.sort(key=itemgetter(3), reverse=True)
			begin = 0
			end = 0
			for clust in cluster:
				begin += clust[1]
				end += clust[2]
			new_name = "%s___%d_%d" % (cluster[0][0].split("___")[0], int(begin/len(cluster)), int(end/len(cluster)))
			#new_name = cluster[0][0]
			for line in cluster:
				mapping[line[0]] = new_name
		if compress:
			#ready_mappings[key] = zlib.compress(pickle.dumps(mapping))
			ready_mappings[key] = mapping
		else:
			ready_mappings[key] = mapping

		return ready_mappings


	#[0] = line itself [1] = index_begin [2] = index_end [3] = length
	def compare(self, c1, c2):
		if c1[3] < c2[3]:
			c1, c2 = c2, c1
		hsp_extra = c1[3]*0.1

		if c2[1] <= c1[1] + hsp_extra:
			if c1[2] - hsp_extra <= c2[2] <= c1[2] + hsp_extra:
				return 1
			else:
				return 0
		else:
			return -1

	def compare_v3(self, c1, c2, threshold=0.8):
		maxlen = max(c1[3], c2[3])
		extra = maxlen*0.1
		if c2[1] > c1[1] + extra:
			return -1
		else:
			overlap = set(range(c1[1], c1[2])).intersection(set(range(c2[1], c2[2])))
			if len(overlap)/maxlen >= threshold:
				return 1
			else:
				return 0

	def compare_v2(self, c1,c2):
		allowed_extra = c1[3]*0.2

		if c2[1] <= c1[1] + allowed_extra:
			extra = abs(c1[1]-c2[1]) + abs(c1[2]-c2[2])
			if extra <= allowed_extra:
				return 1
			else:
				return 0
		else:
			return -1

	def create_clusters_full(self, name, data, compress, meta):
		metadata = meta["metadata"]
		data_loc = meta["data_loc"]
		clusters = {}
		length = 0
		nodes = []
		encoder = TextEncoder()
		for node in data:
			node_id = node.split("___")[0]
			indexes = node.split("___")[1].split("_")
			title = " ".join(metadata[node_id]["title"].split())
			year = metadata[node_id]["year"]
			filename = metadata[node_id]["filename"]
			length += int(indexes[1]) - int(indexes[0])
			full_orig_text = self.get_orig_text(node_id, data_loc, filename)
			orig_text = encoder.get_original_text(full_orig_text, indexes[0], indexes[1])
			nodes.append([node_id, title, year, orig_text])

		nodes.sort(key=itemgetter(2))
		clusters[name] = {}
		clusters[name]["hits"]= nodes
		clusters[name]["size"] = len(nodes)
		clusters[name]["avglen"] = int(length / len(nodes))
		clusters[name]["occyear"] = nodes[0][2]


		if compress:
			clusters[name] = zlib.compress(pickle.dumps(clusters[name]))

		return clusters

	def get_orig_text(self,node_id, data_loc, filename):
		if filename.endswith(".gz"):
			with gzip.open(data_loc + "/" + filename, "rb") as gzip_file:
				data = json.loads(str(gzip_file.read(), "utf-8"))[node_id]
				#if type(data) == str:
				#	return data
				#else:
				return data["text"]
		with codecs.open(data_loc + "/" + filename, "r") as json_file:
			gdata = json.load(json_file)

		return gdata[node_id]["text"]

def work(worker, method, qin, qout):
	worker.work(method, qin, qout)

def read_data(location, min_length, max_length, compress, num_of_processes, start_index, end_index):
	data_dictionary = {}
	if os.path.isdir(location):
		files = os.listdir(location)
		files = natsorted([f for f in files if f.endswith(".gz")])
		if end_index == -1:
			files = files[start_index:]
		else:
			files = files[start_index:end_index]
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

	#for key in list(temp_data.keys()):
	for key, value in temp_data.items():
		value = temp_data[key]
		qins[crnt_q].put([key, value, compress])
		#del temp_data[key]
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	for i in range(0, len(qins)):
		qins[i].put(None)
		data_dictionaries.append(qout.get())

	return data_dictionaries

def stringify_data(data, compress, num_of_processes):
	if type(data) == dict:
		data = dict_to_list_of_dicts(data, num_of_processes)
	processes, qins, qout = start_processes("stringify", num_of_processes)

	crnt_q = 0

	for index, dictionary in enumerate(data):
		qins[crnt_q].put([dictionary, compress])
		#data[index] = {}
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	dictionary = {}

	for i in range(0, len(qins)):
		qins[i].put(None)
		dictionary.update(qout.get())

	return dictionary

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

def get_raw_nodes(data, compress, num_of_processes):
	processes, qins, qout = start_processes("raw_nodes", num_of_processes)

	crnt_q = 0


	##TODOODOT
	#compress = False

	#for key in list(data.keys()):
	for key, value in data.items():
		value = data[key]
		qins[crnt_q].put([key, value, compress])
		#del data[key]
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	nodes = {}
	dictionaries = []
	for i in range(0, len(qins)):
		qins[i].put(None)
		dictionaries.append(qout.get())

	if compress:
		for dictionary in dictionaries:
			for key, value in dictionary.items():
				if compress and key in nodes:
					nodes[key] = pickle.loads(zlib.decompress(nodes[key]))
				if compress:
					value = pickle.loads(zlib.decompress(value))
				nodes[key] = nodes.get(key, set()) | value
				if compress:
					nodes[key] = zlib.compress(pickle.dumps(nodes[key]))
	else:
		for dictionary in dictionaries:
			for key, value in dictionary.items():
				nodes[key] = nodes.get(key, set()) | value
		#for key, value in nodes.items():
		#	nodes[key] = zlib.compress(pickle.dumps(value))

	#for dictionary in dictionaries:
	#	for key, value in dictionary.items():
	#		if compress:
	#			value = pickle.loads(zlib.decompress(value))
	#		nodes[key]= nodes.get(key, set()) | value

	#for key, value in nodes.items():
	#	nodes[key] = zlib.compress(pickle.dumps(value))

	return nodes

def dict_to_list_of_dicts(data, num_of_lists):
	new_data = [{} for i in range(num_of_lists)]

	crnt = 0
	for key, value in data.items():
		new_data[crnt][key] = value
		crnt += 1
		if crnt == num_of_lists:
			crnt = 0

	return new_data

def start_processes(method, num_of_processes):
	processes = []
	qins = []
	qout = Queue()

	for i in range(num_of_processes):
		qin = Queue()
		#new_process = QueueWorker(target=QueueWorker.work, args=(QueueWorker(), method, qin, qout))
		new_process = Process(target=work, args=(QueueWorker(), method, qin, qout))

		qins.append(qin)
		processes.append(new_process)
		new_process.start()

	return processes, qins, qout

def get_graph(data, maps, compress):
	graph = nx.Graph()
	#for key, value in data.items():
	for key in list(data.keys()):
		value = data[key]
		if compress:
			value = pickle.loads(zlib.decompress(value))

		for hsp in value:
			end_b = hsp[1].split("___")[0]
			start_node = maps[key][hsp[0]]
			end_node = maps[end_b][hsp[1]]
			graph.add_edge(start_node, end_node)
		del data[key]
	return graph

def add_edges(graph, maps, data):
	for key, value in data.items():
		for hsp in value:
			start_b = hsp[0].split("___")[0]
			end_b = hsp[1].split("___")[0]
			graph.add_edge(maps[start_b][hsp[0]], maps[end_b][hsp[1]])

def get_subgraphs(graph, out, start, end):
	sgs = {}
	saved = 0
	cluster_id = 0
	subgraphs = nx.connected_component_subgraphs(graph)
	for i, sg in enumerate(subgraphs):
		nodes = sg.nodes()
		edges = sg.edges()
		sgs["cluster_" + str(cluster_id)] = [nodes, edges]
		cluster_id += 1
		#if len(sg.nodes()) > 20:
		#partitions = community.best_partition(sg)
		# 	clusters = {}
		# 	for key, value in partitions.items():
		# 		clusters[str(value)] = clusters.get(str(value), [])
		# 		clusters[str(value)].append(key)
		# 	for partition_key, nodes in clusters.items():
		# 		#nodes = sg.nodes()
		# 		nodes, length = uniq_and_length(nodes)
		# 		#nodes, length = uniq_and_length(nodes, edges)
		#
		# 		if len(nodes) > 1:
		# 			sgs["cluster_" + str(cluster_id)] = [nodes, length]
		# 			cluster_id += 1
		# else:
		# 	nodes = sg.nodes()
		# 	nodes, length = uniq_and_length(nodes)
		# 	if len(nodes) > 1:
		# 		sgs["cluster_" + str(cluster_id)] = [nodes, length]
		# 		cluster_id += 1
		if len(sgs) > 10000:
			save(out, sgs, "subgraphs_" + str(saved))
			saved +=1
			sgs.clear()
	save(out, sgs, "subgraphs_" + str(saved))
	saved +=1
	sgs.clear()
	return sgs

def uniq_and_length(nodes):
	uniques = {}

	for node in nodes:
		id_name = node.split("___")[0]
		uniques[id_name] = uniques.get(id_name, [])
		indexes = node.split("___")[1].split("_")
		length = int(indexes[1]) - int(indexes[0])
		uniques[id_name].append([node, length])



	nodes = []
	length = 0
	for key, value in uniques.items():
		value.sort(key=itemgetter(1), reverse=True)
		nodes.append(value[0][0])
		length += value[0][1]


	return nodes, int(length/len(nodes))


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
		for node in sg[0]:
			node_id = node.split("___")[0]
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

		info["titles"] = info["titles"][0:31000] ## limit, excel doesn't like too longs
		info["text"]= " ".join(encoder.get_original_text(occtext_orig, f_nodes[0][1], f_nodes[0][2]).split()).strip().replace('"', ' ').replace("'", " ")[0:31000]
		info["avglength"]= str(len(info["text"]))
		if examine:
			print_nodes(f_nodes, encoder, data_loc)
		clusters[key] = info
	return clusters

def cluster_full_mcores(sub_graphs, metadata, data_loc, num_of_processes, compress):
	meta = {}
	with codecs.open(metadata + "/metadata.json", "r") as json_file:
		metadata = json.load(json_file)
	meta["metadata"] = metadata
	meta["data_loc"] = data_loc

	processes, qins, qout = start_processes("create_full_clusters", num_of_processes)

	for qin in qins:
		qin.put(meta)

	crnt_q = 0
	for sg_name, sg_data in sub_graphs.items():
		qins[crnt_q].put([sg_name, sg_data[0], compress])
		crnt_q += 1
		if crnt_q == len(qins):
			crnt_q = 0

	clusters = {}

	for i in range(0, len(qins)):
		qins[i].put(None)
		clusters.update(qout.get())

	return clusters



def create_clusters_full(sub_graphs, metadata, examine, data_loc):
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
		for node in sg[0]:
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


def get_orig_text(node_id, data_loc, filename):
	if filename.endswith(".gz"):
		with gzip.open(data_loc + "/" + filename, "rb") as gzip_file:
			data = json.loads(str(gzip_file.read(), "utf-8"))[node_id]
			#if type(data) == str:
			#	return data
			#else:
			return data["text"]
	with codecs.open(data_loc + "/" + filename, "r") as json_file:
		gdata = json.load(json_file)

	return gdata[node_id]["text"]


def save(out, data, name, gzip=False):
	if gzip:
		with gzip.open(out + "/" + name + ".gz", "wb") as gzip_file:
			gzip_file.write(bytes(json.dumps(data), "utf-8"))
	else:
		with open(out + "/" + name + ".pickle", "wb") as pickle_file:
			pickle.dump(data, pickle_file)


def clusters_to_tsv(clusters, out):
	with codecs.open(out + "/clusters.tsv", "w") as csv_file:
		csv_file.write("CLUSTER_ID\tAVGLENGTH\tOCCYEAR\tCOUNT\tTITLES\tTEXT\n")
		for key, cluster_data in clusters.items():
			csv_file.write("\t".join([key, cluster_data["avglength"], cluster_data["occyear"], cluster_data["count"], "||".join(cluster_data["titles"]), cluster_data["text"]]) + "\n")

def save_pickle(data, name, output):
	with open(output + "/" + name + ".pickle", "wb") as pickle_file:
		pickle.dump(data, pickle_file)




if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Clustering TSV result file(s).")
	parser.add_argument("-f", "--file_location", help="Location to the results file or folder with multiple result files.", required=True)
	parser.add_argument("-d", "--data_location", help="Location to the original data files / folder.")
	parser.add_argument("-min", "--min_length", help="Minimum length of text", required=True, type=int)
	parser.add_argument("-max", "--max_length", help="Maximum length of text", required=False, default=1000000000, type=int)
	parser.add_argument("-out", "--out", help="Output folder for everything.", required=True)
	parser.add_argument("-m", "--metadata", help="Location to the metadata file, if not set, assuming it's in the output folder.", default=None)
	parser.add_argument("-e", "--examine", action="store_true", help="Look at pairs", default=False)
	parser.add_argument("--num_of_processes", help="Number of processes to launch", default=1, type=int)
	parser.add_argument("--split", action="store_true", default=False, help="If the data was split. Splits should also be seen in the IDs, like ID__startindex_endindex")
	parser.add_argument("--subgraphs", help="Save subgraphs", action="store_true", default=False)
	parser.add_argument("--tsv", help="Save TSV", action="store_true", default=False)
	parser.add_argument("--full", help="Save full clusters", action="store_true", default=False)
	parser.add_argument("--compress", help="Compress in memory, saves RAM, not working atm", action="store_true", default=False)
	parser.add_argument("--start_file", help="Start index for files to read from file list", default=0, type=int)
	parser.add_argument("--end_file", help="End index for files to read from file list", default=-1, type=int)
	args = parser.parse_args()

	data = {}

	if os.path.exists(args.out):
		raise FileExistsError("Output folder must not exists prior to running this program!")
	else:
		os.makedirs(args.out)

	print("PID: %d" % os.getpid())
	multiprocessing.set_start_method("spawn") ## no forking

	if args.metadata == None:
		args.metadata = args.out

	print("Reading data..")
	data = read_data(args.file_location, args.min_length, args.max_length, args.compress, args.num_of_processes, args.start_file, args.end_file)

	if args.split:
		print("Flattening data..")
		data = flatten_data(data, args.compress, args.num_of_processes)

	print("Stringifying...")
	data = stringify_data(data, args.compress, args.num_of_processes)

	print("Finding all nodes..")
	raw_nodes = get_raw_nodes(data, args.compress, args.num_of_processes)

	print("Calculating real nodes...")
	maps = cluster_data(raw_nodes, args.compress, args.num_of_processes)
	raw_nodes.clear() ## Not needed anymore

	print("Filling graph...")
	graph = get_graph(data, maps, args.compress)
	maps.clear() ##Not needed
	data.clear() ##Not needed

	print("Getting all clusters...")
	sub_graphs = get_subgraphs(graph, args.out, args.start_file, args.end_file)

	if args.subgraphs:
		print("Saving subgraphs...")
		save(args.out, sub_graphs, "subgraphs")

	if args.tsv:
		print("Creating TSV clusters...")
		clusters = create_clusters_tsv(sub_graphs, args.out, args.metadata, args.examine, args.data_location)
		print("Saving clusters as TSV...")
		clusters_to_tsv(clusters, args.out)

	if args.full:
		print("Creating full clusters...")
		clusters = cluster_full_mcores(sub_graphs, args.metadata, args.data_location, args.num_of_processes, args.compress)
		print("Saving clusters as JSON...")
		save(args.out, clusters, "full_clusters")



