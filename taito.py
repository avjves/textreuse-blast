import argparse, os
from shutil import copytree, rmtree
from blast import MultipleBlastRunner

''' to run on a cluster computer, it might be helpful to copy db to the hardrive of the node
	This file can be run instead of blast.py. Will make sure the data is on the node before running
	blast.py'''

	## DATA MUST BE PREPARED BEFORE RUNNING THIS 
	
	
def copy_output_folder_to_local(output_folder, local_folder):
	copytree(ouput_folder, local_folder)
	

def delete_local_data(local_folder):
	rmtree(local_folder)
	
if __name__ == "__main__":
	
	parser = argparse.ArgumentParser(description="Running the software in batches. Data must be prepared BEFORE running this, so that the data is already encoded and DB is ready")
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--output_folder", help="A folder where all data will be stored in the end.", required=True)
	parser.add_argument("--local_folder", help="Local folder where the data will be copied, if not present, no copying will be done")
	parser.add_argument("--e_value", help="E-value for BLAST. Lowering this will increase computing time, but will also find more hits and shorter", default=0.001)
	parser.add_argument("--word_size", help="Word size for BLAST. Basically character n-gram size.", default=6)
	parser.add_argument("--iter", help="Current iteration", type=int, required=True)
	parser.add_argument("--max_iter", help="Num of iterations", type=int, required=True)
	parser.add_argument("--text_count", help="Text count", type=int, required=True)
	parser.add_argument("--qpi", help="Queries per iteration", type=int, required=True)
	
	parser.add_argument("--full", help="Save full data.")
	parser.add_argument("--language")
	
	args = parser.parse_args()
	
	output_folder = "where"
	local_folder = "to"
	iter = 0
	
	if local_folder in args:
		copy_output_folder_to_local(args.output_folder, args.local_folder)
		runner = MultipleBlastRunner(output_folder=args.local_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads, iter=args.iter, max_iter=args.max_iter, queries_per_iter=args.qpi, text_count=args.text_count)
		runner.run()
		copy_local_data_back()	
		delete_local_data()
	else:
		runner = MultipleBlastRunner(output_folder=args.output_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads, iter=args.iter, max_iter=args.max_iter, queries_per_iter=args.qpi, text_count=args.text_count)
		runner.run()