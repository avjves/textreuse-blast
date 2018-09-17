import logging, sys
def get_logger(log_file=None):
	logger = logging.getLogger("FULL BLAST")
	logger.setLevel(logging.DEBUG)
	ft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	st = logging.StreamHandler(sys.stdout)
	st.setLevel(logging.DEBUG)
	st.setFormatter(ft)
	logger.addHandler(st)
	if log_file:
		lf = logging.FileHandler(log_file)
		lf.setLevel(logging.DEBUG)
		lf.setFormatter(ft)
		logger.addHandler(lf)
	return logger
