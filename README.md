# Spark_Streaming

## Overview

### Generated Click Stream data in JSON having certain attributes. Needed to read this JSON data in stream and do some transformation then write in CSV.
Then another process will read this stream of data do some aggregation and write in parquet format (tricky part as had to use foreachBatch).


## Used Spark Streaming on a local system to perform this task.

## Folder Structure
- python_file
  - main.py -> python file to generate the JSON data
  - data -> different folders for JSON, CSV, parquet data to be stored in. 
- Task_1.ipynb
- Task_2.ipynb

## How to Execute
# Before starting check that the folder inside the data folder are empty once that is checked.
# run the main.py file and that will generate the data
# run the task_1 and task_2 files they will start consuming the data the order or execution will not matter they will listen for files and once the see those they will start executing

## Task_1
# We use Spark Streaming api to read the data from JSON folder and do some tranformation from nested to flat.
# Provide a schema and change the datatypes of the columns
# Write that data in CSV to a CSV folder.

## Task_2

# We use read the data from CSV folder and do some aggregation based on a window with watermark defined
# Use the ForeachBatch to write the data in an append mode 
