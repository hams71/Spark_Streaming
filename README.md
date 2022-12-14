# Spark Streaming

Spark Structued Streaming to read and write data in stream and also perform aggreagations using tumbling window.

---

### Table of Contents
- [Overview](#overview)
- [Program Flow](#program-flow)
- [Folder Structure](#folder-structure)
- [Executing the Program](#executing-the-program)
- [Task_1](#task_1)
- [Task_2](#task_2)
- [Clean Up](#clean-up)
- [Note](#note)
- [Level Up](#level-up)
- [Documentation](#documentation)


---

### Overview


- The purpose of doing this was to learn a bit about Spark Streaming. 
- Generated Click Stream data in JSON having certain attributes. 
- Read JSON data in stream and did some transformation, write in CSV format in a different folder.
- Another process will read this stream of data from the CSV folder do some aggregation and write in parquet format (interesting part as had to use foreachBatch instead of writeStream).

---

### Program Flow

<p align="center">
  <img src="Images/Flow_3.jpg" width="850" >
</p>


---


### Folder Structure
- python_file
  - main.py   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;   - Python file to generate the JSON data
  - data      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  - Inside this folder, more folders for JSON, CSV, parquet data to be stored in.
    - json_files
    - csv_files
    - parquet_files  
- Task_1.ipynb   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- This task reads the JSON data does some transformation and writes data in CSV to another folder
- Task_2.ipynb   &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- This task reads the CSV data does some transformation and writes data in Parquet to another folder

- The checkpoints folder will be created after we execute Task_1 and Task_2.

---

### Executing the Program
- Before starting, check that the folders inside the **data** folder are empty (CSV, Parquet, JSON, Checkpoints folder) once that is checked.
- Run **main.py** file and that will generate the data in JSON.
- Run **Task_1** it will consume the JSON files from JSON folder and then will write data in CSV format to the CSV folder.
- Run **Task_2** it will consume the CSV files from the CSV folder and write the in Parquet format to parquet folder.
- Note the order of execution does not matter if you run Task_2 before Task_1 it will just wait for the files to come. 

---

### Task_1
- We use Spark Streaming to read the data from JSON folder and do some tranformation.
- The schema looks something like.
    - Analytics
      - Clicks
      - Impressions
    - Sales
      - Quantity
      - Total_Price
    - Datetime
- This is a nested schema will need to change this in order to write in CSV format.
- Provide a schema and change the datatypes of the columns so that aggregations can be performed.
- Write that data in CSV format to a CSV folder.

---

### Task_2

- We read the data from CSV folder and do some aggregation based on a window with watermark defined.
- We can output this data to our console for debugging purposes.
- Used the ForeachBatch to write the data in an append mode.

#### Tumbling Window and WaterMark

- Window the user can define for how much time frame they want to perform something. There are different types
  - Tumbling Window (No Overlap)
  - Sliding Windows (Overlap)
  - Session Windows (Event Driven)
- Watermark is used for handling late arriving data.

<p align="center">
  <img src="Images/window.JPG" width="750" >
</p>


- In the above example we define a window of 5 minutes and a watermark of 15 minutes.
- So an event occured at 12:04 (Event Time) will fall in the 12:00 - 12:05 window but due to some network issue we recieve this event at 12:11 (Arrive Time). This event will still fall in the 12:00 - 12:05 window as the Watermark was of 15 minutes (11 < 15). If it were to arrive at 12:17 (17 > 15) then it would have been discarded.
- Watermark is waiting for a certain timeframe for a late event if within time frame then will consider its value else will discard it.

- Reason to use foreachBatch was that when we define the WaterMark of certain time that has to complete before it can write that to the folder
- Lets say we defined a **Window** of 10 seconds and **Watermark** of 30 seconds. The complete output for this 10 second window will be displayed after 30 seconds when the watermark has passed and in the 10 seconds windows we will get empty data. 

---

### Clean Up

- After you have executed the code and want to stop it, use spark.stop() or restart the kernel in jupyter.
- We will have data files in all of the folder and have new checkpoint folders as well.
- You can continue the program if you only stopped the **writeStream** code part else if whole spark code stopped and you want to re-execute all clear all the data files and delete checkpoints as well.


---

### Note

- Do read the documentation of Spark Streaming.
- The outputModes (append, complete, update) do check them out and see for what formats they work and for which they do not.
- All of the above formats can work if we output to console but if we are writing to a file then only specific will work.
- Understand how foreach and foreachBatch work and can be used to push data to different databases.

---

### Level Up
- Currently we are just generating data and doing some transformations and writing it in different format.
- We can use Kafka, kafka producer will generate this data, can have multiple of them and then Spark Streaming can consume.
- We can use data lake, or even a database to store this data instead of writing this in a file.
- Create more dimension like date, product and have a fact to store the data allowing us to have historical data as well.
- Can use a visualization tool to get more insigths and that to in near real time. Power BI can consume stream data directly.

---

### Documentation
- [Spark Streaming Basic](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Spark Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
