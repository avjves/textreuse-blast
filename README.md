# Text Reuse Detection with BLAST

This is a repository for software to detect and clusterize text reuse. The software takes advantage of NCBI BLAST (Basic Local Alignment Search Tool, developed for aligning biomedical sequences) to detect reuse. The data is encoded into protein sequences, which BLAST can read. It then finds pairs where parts of documents overlap. These pairs are then clusterized based on their offset values, so that overlapping passages will be considered to be part of a same cluster.
The software takes advantage of multiple cores and it can be run in batches, so running it on cluster computers is possible. Depending on the size of the data, this might be a necessity, as the software can eat a lot of processing power.

## Installation

The software is written in python3, excluding the BLAST part. Thus, python3 must be installed.
Other libaries used are joblib, natsort and lmdb. These can easily be installed via pip:
```
pip3 install --user joblib natsort lmdb
```

A modified version of BLAST is also needed to run the software. This can be downloaded from:
```
http://bionlp-www.utu.fi/blast_hum/ncbi-blast-2.5.0+-src.tar.gz
```

After decompressing the source files, BLAST must be compiled. This can done by running:
```
cd ncbi-blast-2.5.0+-src ##Now inside the BLAST directory
cd c++
./configure ##Run configure file
make ##Compile the program
## binary files are now in in ncbi-blast-2.5.0+-src/c++/ReleaseMT/bin
```

The software expects BLAST binaries to be in PATH. This can be done by:
```
export PATH="/path/to/ncbi-blast-2.5.0+-src/c++/ReleaseMT/bin:$PATH"
## This should most likely be added to your .bashrc file, so it remains in different sessions as well
```

Now you are ready to run.


## Data formatting

Your input data needs to be gzipped file(s) in a folder. Each gzipped file should contain part of your data, where each document will be represented in JSON format on its own line. Each document can contain arbitrary amount of metadata that will be added to the found clusters at the end, but the bare bone data structure has fields text and doc_id. Text contains your text and doc_id is the name of the document.  
Example could look like this:
```
{"title": "NewspaperX", "date": "1907-03-18", "doc_id": "newspaperX_1907_03_18", "text": <text>}
{"title": "NewspaperY", "date": "1907-03-19", "doc_id": "newspaperY_1907_03_19", "text": <text>}
```
Here, title and date fields are optional metadata. To take full advantage of multiple cores, the data should be split into at least as many files as available cores, so the files can be read in parellel. This step is, however, not the most computationally intensive step, so having just one file works, too.

## Running

This software can be run in two ways: in one go, or in batches.

### In one go
If your data is small enough, you may want to run it in one go. This can be done by running run_full.py. This goes through all the above steps.

run_full.py arguments:

| Argument | Description |
| --- | --- |
| `data_folder` | Where the gzipped data files are located. |
| `output_folder` | Folder where to save all the data. |
| `language` | Which language the data is in. Currently supports "ENG" and "FIN" out of the box. Others must be manually added. |
| `threads` | Number of threads to use. |
| `split_size` | The size of the splits, if the document should be split into parts. Otherwise, ignore. This is useful if the documents have vastly different lengths, so splitting the data will allow each batch to be approximately same sized. |
| `e_value` | Setting for BLAST. This should be set to be very low. Lowering this value will decrease the required to computational time, but will also cut down shorter hits from the results. Default 1e-15. |
| `word_size` | Setting for BLAST. This is the size of word seeds to use to when finding repeated passages. Lowering will increase computation time, but if the data quality is bad, it might be necessary. Default=6. Range=2-7 |
| `min_length` | Minimum length of hits to clusterize. Decreasing this wilĺ not make the program a lot faster, as BLAST will still find these and they are just ignored in the clusterizer part. |
| `max_length` | Maximum length of hits to clusterize. |


### Batches

#### 1st phase: data_preparer.py
First begin by running the *data_preparer.py* file. This will read your data, produce databases, and encode the data to proteins (For BLAST to work).
Data preparer has multiple arguments that must be specified:

| Argument | Description |
| --- | --- |
| `data_location` | Location of the gzipped data files. |
| `output_folder` | Output folder for the data. This folder will be used in subsequent parts as well. |
| `threads` | Number of threads to use. |
| `language` | Which language the data is in. Currently supports "ENG" and "FIN" out of the box. Others must be manually added. |
| `split_size` | The size of the splits, if the document should be split into parts. Otherwise, ignore. This is useful if the documents have vastly different lengths, so splitting the data will allow each batch to be approximately same sized. |

Data preparer procudes databases that BLAST can use to compare the data.
#### 2nd phase: blast_batches.py
This is the part that should be ran in batches on cluster computers if they're available, as this is where the actual computation happens.

blast_batches.py arguments:

| Argument | Description |
| --- | --- |
| `output_folder` | This is the location of the folder that data_preparer produced.  |
| `local_folder` | Folder where to copy the data first. This is useful if you're running the data on cluster computers and want to copy the data to the cluster node first. (i.e. shared_location --> local_location) |
| `batch_folder` | Folder where to copy the results. This can be set to be the batches folder in output_folder, if you are not copying the the folder to local nodes or don't mind unnecessary transfers. |
| `threads` | Number of threads to use. |
| `e_value` | Setting for BLAST. This should be set to be very low. Lowering this value will decrease the required to computational time, but will also cut down shorter hits from the results. Default 1e-15. |
| `word_size` | Setting for BLAST. This is the size of word seeds to use to when finding repeated passages. Lowering will increase computation time, but if the data quality is bad, it might be necessary. Default=6. Range=2-7 |
| `iter` | Current iteration. This number needs to change for every batch. i.e, start from 0 -->|
| `text_count` | Must be the ACTUAL number of documents in the BLAST database. If you used split_size to split the data in parts, you may need to check this manually. Check extra information part to get help.|
| `qpi` | Queries to run per iteration. This must be constant between batches. |
| `preset` | Preset for preprogrammed cluster computer. Currently only working option = "taito". |

After running all batches, you need to copy all the results into *batches* folder in *output_folder*, if you didn't set this in the previous step.

#### 3rd phase: running clusterizer.py

Clusterizer.py reads in the batches. This can be run in two ways, as well. Either load everything into memory at once, or clusterize the data in batches. The style is of course faster, but takes more memory.

clusterizer.py arguments:

| Argument | Description |
| --- | --- |
| `output_folder` | This is the location of the folder that data_preparer produced. |
| `min_length` | Minimum length of hits to consider. |
| `max_length` | Maximum length of hits to consider. |
| `node_similarity` | The minimum similarity between two nodes in one document to consider them to be the same. I.e. how much the must overlap. Default 0.90. |
| `pre_split` | If the data is pre_split and you want to combine the parts back into one.|
| `files_per_iter` | Files to read per iteration. This means that only X many files are first read in and clusterized. In the next iteration, another X are clusterized. After going through all files once, the newly clusterized files are clusterized again. This is done until just one one iteration is done on a pass through the results.|
| `files_per_cluster`| Clusters per file saved. |
| `min_alignment_score` | Minimum alignment score of BLAST result to consider it a real hit. Default 0.0, so everything is considered a real hit. |
| `alignment_ranges` | Hit length ranges and what minimum alignment score to use there. Format: min_length_1,alignment_score_1,max_length_1;min_length_2,alignment_score_2,max_length_2 Example: 0,0.85,100;100,0.75,150 |
| `threads` | Number of threads to use. |

#### 4th phase: filler.py

This fills the clusters with actual text, instead of just document offsets.

filler.py arguments:

| Argument | Description |
| --- | --- |
| `output_folder` | This is the location of the folder that data_preparer produced. |
| `language` | Which language the data is in. Currently supports "ENG" and "FIN" out of the box. Others must be manually added. |
| `threads` | Number of threads to use. |
| `split_size` | The size of the splits if used. |
| `custom_data_DBs` | If you want to use a custom data DB(s) instead of assuming there is one in the given output_folder location. Format: 'path1;path2' |
| `custom_info_DBs` |  Same as above, but info DB.|
| `custom_unfilled` | Custom location of the unfilled clusters. |
| `custom_filled` | Custom output location for the filled clusters. |


At this point, the filled clusters can be found in the *clusters/filled* folder in the *output_folder*.



### Extra information


#### Finding the text count from a DB
To find the text count from the DB, you must navigate to the DB folder and use *blastp* cmmand to see the size. Below are instructions on how to do this. This assumes that the path is set to see *blastp* everywhere.

```
cd output_folder ##output_folder is the name given when running data_preparer etc.
cd db
blastp -db textdb
```
In the blastp output there is a number of how many sequences there are in the database. 
That is the text count.
