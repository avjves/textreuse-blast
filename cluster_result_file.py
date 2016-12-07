import argparse,os,json,gzip,codecs,sys
import networkx as nx
from operator import itemgetter
sys.path.insert(0, "/home/avjves/oldsuomi")
from text_encoder import TextEncoder
from xml.etree import ElementTree as ET
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from natsort import natsorted
import numpy

def get_hsps_tsv(file_loc, min_length, max_length):
	tsv_data = {}
	key = ""
	with codecs.open(file_loc, "r") as tsv_file:
		data = tsv_file.readlines()
	for line in data:
		if line.startswith("# Query:"):
			key = line.split(" ", 3)[3].strip()

			continue
		elif not line or line.startswith("#"):
			continue
		tabs = line.split("\t")
		other_key = tabs[0]
		if key == other_key:
			continue
		ident = float(tabs[6])
		length = int(tabs[5])
		q_start = int(tabs[1])
		q_end = int(tabs[2])
		h_start = int(tabs[3])
		h_end = int(tabs[4])
		if length < min_length:
			continue
		if max_length > 0 and length > max_length:
			continue
		tsv_data[key] = tsv_data.get(key, [])
		tsv_data[key].append([q_start, q_end, h_start, h_end, length, other_key, ident])
	return tsv_data


def flatten_data(data):
	overlap = 0
	split_size = 100000
	keys = {}
	new_data = {}
	for key in list(data.keys()):
		q_key = key.split("__")[0]
		keys[q_key] = keys.get(q_key, [])
		keys[q_key].append(key)

	for key, value in keys.items():
		new_hsps = []
		for sub_key in value:
			query_ind = int(sub_key.split("__")[1].split("_")[0])
			query_extra = 0
			if query_ind != 0:
				query_extra = query_ind
			hsps = data[sub_key]
			for hsp in hsps:
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
		new_data[key] = new_hsps

	return new_data


def stringify(data):
	for key, value in data.items():
		new_hsps = []
		for hsp in value:
			new_hsps.append([key + "___" + str(hsp[0]) + "_" + str(hsp[1]), hsp[5] + "___" + str(hsp[2]) + "_" + str(hsp[3])])
		data[key] = new_hsps
	return data


def get_raw_nodes(data):
	nodes = {}
	for key, value in data.items():
		for hsp in value:
			start_node = hsp[0]
			end_node = hsp[1]
			end_begin = hsp[1].split("___")[0]
			nodes[key] = nodes.get(key, set())
			nodes[end_begin] = nodes.get(end_begin, set())
			nodes[key].add(start_node)
			nodes[end_begin].add(end_node)

	for key, value in nodes.items():
		nodes[key] = list(value)

	return nodes


def split_indexes(text):
	return text.split(" ")


def cluster_by_similarity(raw_nodes):
	ready_clusters = {}
	ready_mappings = {}
	gone = 0
	for key, value in raw_nodes.items():
		sys.stdout.write("Keys gone through: %d \t length %d \r" % (gone, len(value)))
		sys.stdout.flush()
		gone += 1
		trains = []
		clusters = []
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
				response = compare(current, current_compare)
				if response == -1:
					break
				if response == 1:
					cluster.append(current_compare)
					done_i.add(j)
				else:
					continue
			cluster.sort(key=itemgetter(3))
			new_name = cluster[0][0]
			clusters.append(new_name)
			for line in cluster:
				mapping[line[0]] = new_name
		ready_clusters[key] = clusters
		ready_mappings[key] = mapping

	return ready_clusters, ready_mappings

def compare(c1, c2):
	if c1[3] > 200:
		hsp_extra = c1[3]*0.2
	else:
		hsp_extra = c1[3]*0.2

	if c2[1] <= c1[1] + hsp_extra:
		if c2[2] >= c1[2] - hsp_extra and c2[2] <= c1[2] + hsp_extra:
			return 1
		else:
			return 0
	else:
		return -1


def add_nodes(graph, nodes):
	for key, value in nodes.items():
		for node in value:
			graph.add_node(node)

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

def print_nodes(nodes, encoder):
	for node in nodes:
		occtext_orig = get_orig_text(node[0], type)
		print(encoder.get_original_text(occtext_orig, node[1], node[2]))
		print()
		print()
	for node in nodes:
		print(node[0], node[3], node[4])
	print("next")
	input()

def create_clusters_tsv(sub_graphs, type, out, examine, data_loc):

	with codecs.open(out + "/metadata.json", "r") as json_file:
		metadata = json.load(json_file)

	encoder = TextEncoder(type)
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

		occtext_orig = get_orig_text(f_nodes[0][0], type, data_loc, f_nodes[0][5])

		info["titles"] = info["titles"][0:31000]
		info["text"]= " ".join(encoder.get_original_text(occtext_orig, f_nodes[0][1], f_nodes[0][2]).split()).strip().replace('"', ' ').replace("'", " ")[0:31000]
		info["avglength"]= str(len(info["text"]))
		if examine:
			print_nodes(f_nodes, encoder)
		clusters[key] = info
	#print()
	return clusters

def create_clusters_full(sub_graphs, type, out, examine, data_loc):
	with codecs.open(out + "/metadata.json", "r") as json_file:
		metadata = json.load(json_file)

	encoder = TextEncoder(type)
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
			full_orig_text = get_orig_text(node_id, type, data_loc, filename)
			orig_text = encoder.get_original_text(full_orig_text, indexes[0], indexes[1])
			f_nodes.append([node_id, title, year, orig_text])

		f_nodes.sort(key=itemgetter(2))
		clusters[key]={}
		clusters[key]["hits"] = f_nodes
		clusters[key]["size"]= len(f_nodes)
		clusters[key]["avglen"]= int(length / len(f_nodes))
		clusters[key]["occyear"]=f_nodes[0][2]

		if examine:
			print_nodes(f_nodes, encoder)
	return clusters



def get_orig_text(id, type, data_loc, filename):
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
			#print(cluster_data)
			#print(key)
			#print("\t".join([key, cluster_data["AVGLENGTH"], cluster_data["OCCYEAR"], cluster_data["COUNT"], "||".join(cluster_data["TITLES"]), cluster_data["TEXT"]]) + "\n")
			csv_file.write("\t".join([key, cluster_data["AVGLENGTH"], cluster_data["OCCYEAR"], cluster_data["COUNT"], "||".join(cluster_data["TITLES"]), cluster_data["TEXT"]]) + "\n")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Clustering a single XML or TSV-results file.")
	parser.add_argument("-f", "--file_location", help="Location to the results file.", required=True)
	parser.add_argument("-t", "--type", help="Type of blast. nucl or prot", required=True)
	parser.add_argument("-l", "--min_length", help="Minimum length of text", required=True, type=str)
	parser.add_argument("-o", "--out", help="Output folder.", required=True)
	parser.add_argument("-e", "--examine", action="store_true", help="Look at pairs", default=False)
	parser.add_argument("--split", action="store_true", help="If the data was split, ids are split like ID__indexstart_indexend")
	#parser.add_argument("-d", "--data", help="Location to original data file.", required=True)
	parser.add_argument("-d", "--data_location", help="Location to the original data files.")
	parser.add_argument("--subgraphs", help="Save subgraphs", action="store_true", default=False)
	parser.add_argument("--tsv", help="Save TSV", action="store_true", default=False)
	parser.add_argument("--full", help="Save full clusters", action="store_true", default=False)
	args = parser.parse_args()

	if "," in args.min_length:
		args.max_length = int(args.min_length.split(",")[1])
		args.min_length = int(args.min_length.split(",")[0])
	else:
		args.max_length = -1
		args.min_length = int(args.min_length)

	if args.file_location.lower().endswith(".tsv"):
		print("Reading TSV-file..")
		data = get_hsps_tsv(args.file_location, args.min_length, args.max_length)
	else:
		print("Wrong file format!")
		sys.exit(1)

	if args.split:
		print("Flatting data..")
		data = flatten_data(data)

	print("Stringifying...")
	data = stringify(data)
	print("Finding all nodes..")
	raw_nodes = get_raw_nodes(data)
	print("Calculating real nodes...")
	real_nodes, maps = cluster_by_similarity(raw_nodes)
	graph = nx.Graph()
	print("Adding nodes to graph...")
	add_nodes(graph, real_nodes)
	print("Adding edges to graph...")
	add_edges(graph, maps, data)
	print("Getting all clusters...")
	sub_graphs = get_subgraphs(graph)

	if args.subgraphs:
		save(args.out, sub_graphs, "subgraphs")

	if args.tsv:
		print("Creating clusters...")
		clusters = create_clusters_tsv(sub_graphs, args.type, args.out, args.examine, args.data_location)
		print("Saving clusters as TSV...")
		clusters_to_tsv(clusters, args.out)

	if args.full:
		print("Creating clusters...")
		clusters = create_clusters_full(sub_graphs, args.type, args.out, args.examine, args.data_location)
		print("Saving clusters as JSON")
		save(args.out, clusters, "full_clusters")




