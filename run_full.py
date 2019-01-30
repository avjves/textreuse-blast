import argparse, sys
from blast import SingleBlastRunner
from clusterizer import ClusterizerVol2 as Clusterizer
from data_preparer import DataPreparer
from filler import ClusterFiller
import text_logging

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Software to find text reuse using BLAST.")
	parser.add_argument("--data_folder", help="Location to either a single JSON file or a folder with multiple files", required=True)
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--output_folder", help="A folder where all data will be stored.", required=True)
	parser.add_argument("--e_value", help="E-value for BLAST. Lowering this will increase computing time, but will also find more hits and shorter", default=0.001)
	parser.add_argument("--word_size", help="Word size for BLAST. Basically character n-gram size.", default=6)
	parser.add_argument("--min_length", help="Minimum length of hits. Default = 0", default=0, type=int)
	parser.add_argument("--max_length", help="Maximum length of hits. Default = 100000", default=100000, type=int)
	parser.add_argument("--split_size", default=0, type=int)
	parser.add_argument("--language", help="Language to use.", default="FIN")
	parser.add_argument("--log_file", help="Whether to save all logging into a file as well.", default=None)
	args = parser.parse_args()

	logger = text_logging.get_logger()


	dp = DataPreparer(data_location=args.data_folder, output_folder=args.output_folder, language=args.language, threads=args.threads, split_size=args.split_size, logger=logger)
	dp.prepare_data()
	text_count = dp.get_text_count()
	runner = SingleBlastRunner(data=args.data_folder, output_folder=args.output_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads, text_count=text_count, logger=logger)
	runner.run()
	clusterizer = Clusterizer(output_folder=args.output_folder, min_length=args.min_length, max_length=args.max_length, threads=args.threads, node_similarity=0.90, pre_split=False, clusters_per_file=1000, min_alignment_score=0.0, files_per_iteration=20, start_round=-1, end_round=-1, alignment_ranges=None, logger=logger)
	clusterizer.clusterize()
	data_dbs = None
	info_dbs = None
	custom_unfilled = None
	custom_filled = None
	logger.info("Filling clusters...")
	filler = ClusterFiller(output_folder=args.output_folder, threads=args.threads, language=args.language, split_size=args.split_size, data_dbs=data_dbs, info_dbs=info_dbs, custom_unfilled=custom_unfilled, custom_filled=custom_filled, min_count=0)
	c = filler.fill_clusters()
	logger.info("Done. Results can be found at {}/clusters/filled".format(args.output_folder))
