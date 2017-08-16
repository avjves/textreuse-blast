import argparse
from blast import SingleBlastRunner
from clusterizer import Clusterizer
from filler import ClusterFiller

if __name__ == "__main__":
	
	parser = argparse.ArgumentParser(description="Software to find text reuse using BLAST.")
	parser.add_argument("--data", help="Location to either a single JSON file or a folder with multiple files", required=True)
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--output_folder", help="A folder where all data will be stored.", required=True)
	parser.add_argument("--e_value", help="E-value for BLAST. Lowering this will increase computing time, but will also find more hits and shorter", default=0.001)
	parser.add_argument("--word_size", help="Word size for BLAST. Basically character n-gram size.", default=6)
	parser.add_argument("--full", help="Save full data.")
	parser.add_argument("--langauge")
	min_l = 0
	max_l = 100000
	args = parser.parse_args()
	dp = DataPreparer(data=args.data, output_folder=args.output_folder, language=args.language)
	runner = SingleBlastRunner(data=args.data, output_folder=args.output_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads)
	runner.run()
	clusterizer = Clusterizer(output_folder=args.output_folder, min_length=min_l, max_length=max_l, threads=args.threads, node_similarity=0.90, pre_split=False)
	clusterizer.clusterize()
	filler = ClusterFiller(data_location=args.data, output_folder=args.output_folder, threads=args.threads)
	c = filler.fill_clusters()
	print(c)
	
	
	
	