import argparse, os, time, subprocess
from copy import deepcopy
from shutil import copytree, rmtree, copyfile
from blast import MultipleBlastRunner
import time
from text_logging import get_logger

''' to run on a cluster computer, it might be helpful to copy db to the hardrive of the node
	This file can be run instead of blast.py. Will make sure the data is on the node before running
	blast.py

	Data must be prepared (data_preparer.py) before running this!'''



def get_folder_size(folder):
	return int(subprocess.check_output(["du", "-s", folder]).split()[0].decode("utf-8"))



''' Copies the BLAST DB folder to local node. Multiple processes may run on the same node, so
	must first check if something is already being copied and then wait until it's done '''

def copy_output_folder_to_local(output_folder, local_folder, wait=True, wait_time=5):
	logger.info("Copying {} to {}...".format(output_folder, local_folder))
	if wait:
		if os.path.exists(local_folder):
			print("Folder exists.")
			original_size = get_folder_size(output_folder)
			while True:
				current_size = get_folder_size(local_folder)
				print("Current folder size: {}".format(current_size))
				if current_size < original_size:
					print("{} < {}, waiting {} seconds.".format(current_size, original_size, wait_time))
					time.sleep(wait_time)
				else:
					break
		else:
			try:
				copytree(output_folder, local_folder)
			except OSError:
				print("Couldn't copy, wait a while...")
				time.sleep(wait_time)
				copy_output_folder_to_local(output_folder, local_folder, wait, wait_time)
	else:
		copytree(output_folder, local_folder)


def delete_local_data(local_folder):
	rmtree(local_folder)

def copy_local_data_back(output_folder, batch_folder, iter):
	if not os.path.exists(batch_folder):
		os.path.makedirs(batch_folder)
	copyfile(output_folder + "/batches/iter_{}.tar.gz".format(iter), batch_folder + "/iter_{}.tar.gz".format(iter))


''' Run normal Multi batch BLAST '''

def run_normal(args):
	if args.local_folder != None:
		copy_output_folder_to_local(args.output_folder, args.local_folder)
		runner = MultipleBlastRunner(output_folder=args.local_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads, iter=args.iter, queries_per_iter=args.qpi, text_count=args.text_count, logger=args.logger)
		runner.run()
		copy_local_data_back(args.local_folder, args.batch_folder, args.iter)
		#delete_local_data(args.local_folder)
	else:
		runner = MultipleBlastRunner(output_folder=args.output_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads, iter=args.iter, queries_per_iter=args.qpi, text_count=args.text_count, logger=args.logger)
		runner.run()
		copy_local_data_back(args.output_folder, args.batch_folder, args.iter)

''' Use Taito preset == set local folder to TMPDIR '''

def run_taito(args):
	print("Using preset: Taito")
	args.local_folder = os.environ.get("TMPDIR") + "/" + args.output_folder.split("/")[-1]
	run_normal(args)

''' Use Taito with timilimit - keep running 1 at a time until not enough time to finish batch '''

def run_taito_timelimit(args):
	print("Using preset: Taito-timelimit")
	start_time_in_minutes, min_time_in_minutes = args.preset_info.split(";")
	start_time = time.time()


	for i_i, i in enumerate(range(args.iter*args.qpi, (args.iter+1)*args.qpi)):
		if os.path.exists(args.batch_folder + "/batches/iter_{}.tar.gz".format(i)) or os.path.exists(args.output_folder + "/batches/iter_{}.tar.gz".format(i)):
			print("Iter {} already done, skipping...".format(i))
			continue
		if not enough_time(start_time, min_time_in_minutes, start_time_in_minutes):
			print("Not enough time remaining, stopping...")
			break
		print("Iter {}".format(i))
		new_args = args
		new_args.local_folder = os.environ.get("TMPDIR") + "/" + args.output_folder.split("/")[-1]
		new_args.iter = i
		new_args.qpi = 1
		run_normal(new_args)

	## TODO after running everything, combine them into one iter

''' Check whether there's enough time to keep running the batch '''

def enough_time(start_time, min_time_in_minutes, start_time_in_minutes):
	curr_time = time.time()
	elapsed = (curr_time - start_time) / 60

	if int(start_time_in_minutes) - elapsed >  int(min_time_in_minutes):
		return True
	else:
		return False


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Running the software in batches. Data must be prepared BEFORE running this, so that the data is already encoded and DB is ready")
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--output_folder", help="A folder where all data will be stored in the end.", required=True)
	parser.add_argument("--batch_folder", help="Where to copy all the done batches", required=True)
	parser.add_argument("--local_folder", help="Local folder where the data will be copied, if not present, no copying will be done")
	parser.add_argument("--e_value", help="E-value for BLAST. Lowering this will increase computing time, but will also find more hits and shorter", default=0.001)
	parser.add_argument("--word_size", help="Word size for BLAST. Basically character n-gram size.", default=6)
	parser.add_argument("--iter", help="Current iteration", type=int, required=True)
	parser.add_argument("--text_count", help="Text count", type=int, required=True)
	parser.add_argument("--qpi", help="Queries per iteration", type=int, required=True)
	parser.add_argument("--log_file", help="Whether to save logging into file as well.", default=None)
	parser.add_argument("--preset", help="Some presets for certain systems.")
	parser.add_argument("--preset_info", help="Extra information for a given preset.")
	args = parser.parse_args()

	##logging
	logger = get_logger(args.log_file)
	args.logger = logger

	if args.preset == "taito":
		run_taito(args)
	elif args.preset == "taito-timelimit":
		run_taito_timelimit(args)
	else:
		run_normal(args)
