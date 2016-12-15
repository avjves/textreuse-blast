**WORK IN PROGRESS**

This software is used to find repetitions in text.

Our slightly modified version of BLAST can be downloaded from:
<br>
http://bionlp-www.utu.fi/blast_hum/ncbi-blast-2.5.0+-src.tar.gz

To install just run the configure file and then <i> make </i>.
The ncbi-blast-2.5.0+-src/c++/ReleaseMT/bin folder needs to be added to the PATH variable for the software to see this.


Data can be in either one JSON file or multiple JSON files in a folder.

Data should be in such format:
<br>
{"text_1_name": {"text": \<text\>, "year": \<year\>, "title": \<title\>}, "text_2_name": {...}}

To run the full software, run the run.py file. This uses python3 and <i>joblib</i> and <i>networkx </i> custom libraries.

The arguments can be seen by running
<br>
<i> python3 run.py --help </i>
