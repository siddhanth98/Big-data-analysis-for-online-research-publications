# Homework 2

## Running the programs
- The driver program for every task uses the input shards text files from the path:
`src/main/resources/inputs/shards` 

- The programs were written with a manual hadoop 3.3.0 configuration in a windows 10 platform.

- First execute the following commands from a terminal in administrator mode:
`start-dfs.cmd`
`start-yarn.cmd`

- Ensure the corresponding output directory for the current task is empty:
`src/main/resources/outputs/job_outputs/task2` (for task2 as an example)
This is because the program copies the hdfs output directory to this local output directory after the job ends, and if the output directory already exists, then it will give an IOException, which can occur if the program is run a 2nd time without deleting the same. The directory cannot be deleted from inside the program as it requires administrator privileges to modify a directory written to by hadoop.

- From the project root directory, run `sbt clean compile assembly` which will create the jar of all sources along with all dependencies in the `target\scala-2.12` folder.

- Then from the same terminal, run an example driver program (task2) from the project root directory as follows:
`hadoop jar target\scala-2.12\thisProject-assembly-0.1.0-SNAPSHOT.jar hadoop.task2.Driver`

- The driver program manages moving the input shards from the local directory to the hdfs directory, deleting the hdfs output directory if it exists before running the program and copying the hdfs output directory to the following local output directory:
  `src/main/resources/outputs/job_outputs/task2` (for task2 as an example)
  
 - Then to create the CSV file for a task (task2 as an example), run the following:
 `runMain hadoop.task2.ComputeSpreadSheet output` 
 where the cmd argument 'output' is the name of the local output directory of task2 where the job output file 'part-r-00000' resides.
 The spreadsheet would be created in the following directory:
 `src/main/resources/spreadsheets` 

	### Parsing of xml file
	- The xml parsing programs reside in the `xmlparser` package.

	- The ScalaXmlProcessor program parses the entire dblp.xml file (which should reside in  the `src/main/resources/inputs` ) to first create smaller xml files in the `src/main/resources/inputs/shards/` directory.

	- Total number of tags in the dblp.xml file is observed to be 68,757,132.

	- If the main parser encounters the end of a valid publication tag (detailed below) and if the total number of tags encountered upto that point is just above an integer multiple of 500,000, then it will wrap all encountered tags in a smaller xml file and place it in the above mentioned directory.

	 - This results in a total of 138 smaller xml files.

	- The publication tags considered are:
		 1. Article
		 2. Inproceedings
		 3. Incollection  
		 4. Phdthesis

	- The XMLParser program will read all chunked xml files in sequence and will create the corresponding text files to be fed to the mappers. These 138 input files will be placed in the same directory as that of the chunked xml files.
	
	- Each input file will be read using a KeyValueTextInputFormat in which each line will have the main components of 1 publication.
	- 	`` `publication_title_name` `publication_year` `publication_venue_name` ``
		This will be considered the key where the backtick character "`" acts as a unique delimiter to extract required components.

	- `` `author1` `author2` `author3`.... `` 
	This will be considered the value as all the authors for that publication.


## Task Descriptions
Each task will create 138 map tasks by default for the corresponding 138 input text files to be processed in parallel and 1 reducer which will perform relevant operations and create the final output file.

### Task 1
- In this task the problem is to get the top 10 authors with the maximum number of publications in each venue.

- The **mapper** will take in the above mentioned key value text input, extract the venue and write the venue as the key and the list of authors for the publication as the value in the output.

- The **reducer** gets all aggregated author list as the value for a venue from all mappers. This list will have repeated author names as an author can be in multiple publications. It will traverse this list and will store the total publication count of each author in a **HashMap**. Then it will sort the authors based on this count in descending order and will write the top 10 as the output, where the key will be the venue and value will be the top 10 authors.

- The spreadsheet program will then read the `part-r-00000` job output file from the output directory for this task, and will put the venue and the list of 10 authors in the csv file.

- There are 3 tests defined for this task in the `hadoop.task1` package in the test directory. The 1st tests the mapper, the 2nd tests the reducer and the 3rd tests the overall mapreduce functionality. Mapper/Reducer input values are taken in from the `output_task1.conf` configuration file. To run this test file, execute:
`sbt testOnly hadoop.task1.TestTask1`

### Task 2
- For this task the problem is to find all authors who have published for more than 10 contiguous years.

- The **mapper** will take in the key and the value text input and for each author in the input value text, it will write a key-value output where the key is the author name and the value is the publication year component extracted from the key.

- The **reducer** will take the aggregated years for an author. It will traverse this list of years i.e. integers and if it finds a contiguous sequence of length >= 10, then it will write the output as " " as the key and the author name as the value, as only the author name is required for this task. Noting that there can be repeated year values for the same author, as an author can publish more than once in the same year, the reducer uses a **HashSet** to maintain the collection of years and puts that collection in a list for traversal.

- The spreadsheet program simply takes in these author names in the job output file and puts them in the spreadsheet, each author name in one record.

- Similar to the 1st task, 3 tests are defined for this task. It can be run as:
`sbt testOnly hadoop.task2.TestTask2`

### Task 3
- The problem is to find the all publications in each venue authored by a single person.

- The **mapper** takes in the key value input text, and if the value text just contains 1 author, then it will write the venue as the key and the publication title as the value as the output key-value pair. If there are more than 1 author in the value then it would not write any output.

- The **reducer** takes in the aggregated title names for a venue collected from all mappers, concatenates all title names together into a string and writes the venue as the key and the concatenated string as the value. It will do this for all venues eventually.

- The spreadsheet program will read the job output file and for each venue, it will put the venue and its corresponding list of titles present in the job output file as one record in the csv file.

- To run all 3 tests for this task, use: `sbt testOnly hadoop.task3.TestTask3`

### Task 4
- The problem is to find all publications authored by the maximum number of people in each venue.

- The **mapper** will take in the key value text input and will write the venue as the key and the title name and the number of authors in the value part of the input text as the value, in the output key-value pair as follows:
`key=venue : value=title_name authorCount`

- The **reducer** will take in the aggregated `title_name authorCount` values for a venue, and makes 2 passes over the value pairs. In the 1st pass it will find the largest number of authors that a title has for the given venue i.e. the maximum value of the authorCount of all value pairs. In the 2nd pass, it will collect all titles whose corresponding authorCount values match the maximum authorCount value in the 1st pass i.e. collect all titles having the largest number of authors for that venue. Then it simply writes the venue as the key and the collected title names as the value in the output key-value pair.

- The spreadsheet program will write each venue and corresponding names of all titles as 1 record in the csv file. It will do this for all venues present in the job output file.

- To run all 3 tests for this task, use: `testOnly hadoop.task4.TestTask4`

### Task 5
- The problem is to find the top 100 authors who publish with the maximum number of coAuthors.

- For this task, the driver program will create 2 jobs.
- In **Job1** the mapper will take in the key value text input, and for each author in the value text, it will write the author name as the key, and all the other authors as the corresponding value text. For example, for an input:
`` key = `Towards a Transaction Management System for DOM.` `1991` `GTE Laboratories Incorporated`; value = `Alejandro P. Buchmann` `M. Tamer zsu` `Dimitrios Georgakopoulos` ``
it will write the following output key value pairs:
	1. `key = Alejandro P. Buchmann ; value = M. Tamer zsu , Dimitrios Georgakopoulos`
	2. `key = M. Tamer zsu ; value = Alejandro P. Buchmann , Dimitrios Georgakopoulos`
	3. `key = Dimitrios Georgakopoulos ; value = Alejandro P. Buchmann , M. Tamer zsu`

	The reducer will take in the aggregated coAuthorCollection for each author from all mappers, and will use a hashSet to count the number of unique coAuthors that the given author has. Then it will write the author name as the key and the coAuthorCount as the value in its output. For (1) above, the output will be :
	`` key = `Alejandro P. Buchmann` ; value = 2 ``

- In **Job2** the mapper will read in the job1 output file from the hdfs output directory, and will simply invert the key and the value write the corresponding output, as:
`key = 2 ; value = Alejandro P. Buchmann`

	Note that the job2 mappers read input in the normal text value input format where the key determines the offset of each line and the value is all of the text in the line.
	Now when all mappers have finished executing the output key value pairs are sorted based on the key value in descending order using a custom descending intWritable comparator which job2 uses. Then the inverse sorted key value pairs will be input to the job2 reducer which will simply write the input directly as the output in the final output file in the /finalOutput hdfs output directory.

- The spreadsheet program will take in the job output file `part-r-00000` in the local output directory of task5 and will write the top 100 author details in the csv file.

### Task 6
- For this task, the problem is to find the top 100 authors who have the maximum number of single author publications or solo-publications.

- Similar to task5, this task uses 2 jobs.

- In **Job1**, the mapper will take in the input key value text and if the value contains just one author, only then will it write the output as:
`key = authorName ; value = 1` representing 1 solo publication by that author.
If there are more than 1 author then it would not write anything.
The reducer will take in the aggregated 1's from all mappers for an author, sum up all 1's which represent the total number of solo publications by that author and write the output as :
`key = authorName ; value = total-solo-publication-count`

- In **Job2**, the mapper will read the output file of job1 as a normal text input format, then it will invert the publication count and author name, and will write the output as:
`key = total-solo-publication-count ; value = authorName`
Similar to job5, outputs from all mappers are sorted in descending order of the publication count values (given by the output keys) using the custom descending intWritable comparator and will input the key value pairs to the reducer in that order.
The reducer will simply take in the input and write it as it is in the final output file.

- The spreadsheet program is same as that of job5, in that it will put the top 100 lines of the job2 output file in the csv file, each line as 1 record in the csv file.