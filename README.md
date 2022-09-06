# Spark_Streaming

### Overview

---

#### Generated Click Stream data in JSON having certain attributes. Needed to read this JSON data in stream and do some transformation then write in CSV in a different folder named as CSV.
#### Then another process will read this stream of data from the CSV folder do some aggregation and write in parquet format (tricky part as had to use foreachBatch).

---
### Table of Content
- [Folder Structure](#folder-structure)
- [Executing the Program](#executing-the-program)
- [Task_1](#task_1)
- [Task_2](#task_2)



---


### Folder Structure
- python_file
  - main.py     -> Python file to generate the JSON data
  - data        -> Inside this folder, more folders for JSON, CSV, parquet data to be stored in. 
- Task_1.ipynb  -> This task reads the JSON data does some transformation and writes data in CSV to another folder
- Task_2.ipynb  -> This task reads the CSV data does some transformation and writes data in Parquet to another folder

---

### Executing the Program
#### Before starting check that the folder inside the **data** folder is empty (CSV, Parquet, JSON, Checkpoints folder) once that is checked.
#### Run the main.py file and that will generate the data in **JSON**
#### Run the **Task_1** and **Task_2** files they will start consuming the data the order or execution will not matter they will listen for files and once the see those they will start executing

---

### Task_1
#### We use Spark Streaming api to read the data from JSON folder and do some tranformation from nested to flat.
- Analytics
  - Clicks
  - Impressions
- Sales
  - Quantity
  - Total_Price
- Datetime
#### Provide a schema and change the datatypes of the columns
#### Write that data in CSV to a CSV folder.

---

### Task_2

#### We use read the data from CSV folder and do some aggregation based on a window with watermark defined
#### Use the ForeachBatch to write the data in an append mode

#### Reason to use foreachBatch was that when we define the WaterMark of certain time that has to complete before it can write that to the folder
#### Lets say we defined a **Window** of 10 seconds and **Watermark** of 30 seconds. The output for this 10 second window will be displayed after 30 seconds when the watermark has passed. 
