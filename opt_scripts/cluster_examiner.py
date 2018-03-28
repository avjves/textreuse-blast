import argparse, os, gzip, json

class ClusterExaminer:

	def __init__(self, min_count, max_count, wait_for_input):
		self.min_count = min_count
		self.max_count = max_count
		self.wait_for_input = wait_for_input

	def examine_cluster_file(self, cluster_file):
		with gzip.open(cluster_file, "rt") as gzf:
			gd = json.loads(gzf.read())

		for cluster_key, cluster_data in gd.items():
			hits = cluster_data["hits"]
			hits.sort(key=lambda k: len(k["text"]), reverse=True)
			if not self.min_count <= len(hits) <= self.max_count: continue
			print("CLUSTER: {}\t ClUSTER LENGTH: {}".format(cluster_key, len(hits)))
			self.print_hits(hits)

	def print_hits(self, hits):
		for hit_i, hit in enumerate(hits):
			print(hit["text"])
			if self.wait_for_input:
				input("next cluster? Hit: {}/{}".format(hit_i + 1, len(hits)))

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Examine clusters")
	parser.add_argument("--cluster_folder", help="Cluster folder location")
	parser.add_argument("--cluster_file", help="Cluster file location")
	parser.add_argument("--min_count", help="Minimum hit count to examine", type=int, default=0)
	parser.add_argument("--max_count", help="Maximum hit count to examine", type=int, default=1000000)
	parser.add_argument("--wait_for_input", help="Whether to wait for input when printing hits", default=False, action="store_true")

	args = parser.parse_args()
	print(args)

	examiner = ClusterExaminer(min_count=args.min_count, max_count=args.max_count, wait_for_input=args.wait_for_input)
	if args.cluster_file != None:
		examiner.examine_cluster_file(args.cluster_file)
	elif args.cluster_folder != None:
		for filename in os.listdir(args.cluster_folder):
			examiner.examine_cluster_file(args.cluster_folder + "/" + filename)
	else:
		print("NO CLUSTERS SELECTED")
