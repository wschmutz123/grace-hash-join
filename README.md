# grace-hash-join

In this project I created a manual program that creates a Grace Hash Join which is an algorithm that is used in relational database management systems. This algorithm is used for combining two tables more efficiently by:

1. Partioniting:
- The two tables are paritioned into buckets using a hash function

2. Building:
- A hash table is created for partition in memory and stores the rows

3. Probing:
- The rows of the second table are hashed and looked up in the hash table allowing to match the keys

4. Output:
- The matching rows from both tables are combined and the join is completed.


The entire program uses c++

The program includes 6 classes:

  - Bucket:
    - Class that both adds the Disk (storage data) into the bucket and can get a specific Disk
   
  - Disk:
    - Class that reads data from txt files and adds the pages into memory
   
  - Join:
    - Class that takes in the Disks of data and partitions and hashes the data which outputs a vector of buckets
    - It then probs the bucket and sorts the page ids and the correct join is returned

  - Memory:
    - Class that performs various functions for the data in memory, including loading specific pages and writing pages into memory
    
  - Page:
    - Class that loads records from specific pages as well as gets specific record ids
   
  - Record:
    - Class that looks at a specific records, hashes the records and determines if the keys match  
