# Social-Network-Analysis-HadoopMapReduce
In the project I analyse and describe various social network parameters and statistics using Hadoop Map Reduce techniques.

To Run:
Create jar files and run the following commands relative to localpath

1)    hadoop jar jarpath classname /mutual.txt /outputMutual


2)    hadoop jar jarpath classname user1 user2 /mutual.txt middlejoin /userdata.txt outputDOB


3)    hadoop jar jarpath classname /mutual.txt /userdata.txt outputMinAge


4)    hadoop jar jarpath classname /userdata.txt /outputInverted


5)    hadoop jar jarpath classname /InvertedIndex_q4.txt /outputMaxOccurence


Input Files:

1) mutual.txt
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
Hence, each line represents a particular user’s friend list separated by comma
  
2) userdata.txt
<User> is a unique integer ID corresponding to a unique user and <Friends> is a comma separated list of unique IDs
corresponding to the friends of the user with the unique ID <User>. Note that the friendships are mutual (i.e., edges are undirected):
if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry 
for each side of each edge. So, when you make the pair, always consider (A, B) or (B, A) for 
user A and B but not both.
  

Problem Descriptions: 

1) Creating a MapReduce program in Hadoop that implements a simple “Mutual/Common 
friend list of two friends". The key idea is that if two people are friend then they have a lot of 
mutual/common friends. This program will find the common/mutual friend list for them.


2) Used In memory join at Mapper side. Given any two Users (they are friend) as input, output a list containing the dates of birth of all 
their mutual friends and the number of those friends who were born after 1995.

3) Used in-memory join at the Reducer. For each user displaying the User ID and minimum age of direct friends of this user.
  
4) constructing inverted index in the following ways.The map function parses each line in an input file, userdata.txt, and emits a sequence of <word, 
line number> pairs. The reduce function accepts all pairs for a given word, sorts the corresponding line numbers, and emits a <word, list(line numbers)> pair.
The set of all the output pairs forms a simple inverted index.
  
5) Using a Combiner to find all those words(s) whose number of occurrences is the maximum among all the words in 
the inverted index. Used the output from subpart 4)
