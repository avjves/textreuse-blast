import os, gzip, json
from joblib import Parallel, delayed
from text_encoder import TextEncoder

class ClusterFiller:
	
	def __init__(self, data_location, output_folder, threads):
		self.data_location = data_location
		self.output_folder=output_folder
		self.threads=threads
		self.encoder = TextEncoder("FIN")
		
	def fill_clusters(self):
		cluster_files = os.listdir(self.output_folder + "/clusters")
		cluster_files = [f for f in cluster_files if f.endswith(".gz")] ## no folders here!
		filled_clusters = Parallel(n_jobs=self.threads)(delayed(self.fill_cluster)(filename) for filename in cluster_files)
		return filled_clusters
		
	def fill_cluster(self, filename):
		with gzip.open(self.output_folder + "/clusters/" + filename, "rt") as gzip_file:
			data = json.loads(gzip_file.read())
		filled_clusters = {}
		for cluster_key, cluster_data in data.items():
			filled_clusters[cluster_key] = self.fill(cluster_data)
		return filled_clusters
			
		## TODO ADD METADATA ARBITRARY METADATA :)
	def fill(self, data):
		cluster = {}
		length = 0
		hits = []
		for node in data[0]:
			text_id = node.split("___")[0]
			indexes = node.split("___")[1].split("_")
			orig_text = self.get_original_text(text_id)
			text, indices = self.encoder.decode_text(orig_text, indexes[0], indexes[1])
			hit_data = {}
			hit_data["text"] = text
			length += len(text)
			hit_data["node"] = node
			hits.append(hit_data)
			
		cluster["length"] = int(length/len(hits))
		cluster["hits"] = hits
		return cluster
	
		## TODO put this into LMDB or something
	def get_original_text(self, text_id):
		with open(self.data_location, "r") as json_file:
			for line in json_file:
				block = json.loads(line)
				if block["id"] == text_id:
					return block["text"]

##TODO REMOVE
if __name__ == "__main__":
	filler = ClusterFiller("out_test", 24)
	print(filler.fill_clusters())