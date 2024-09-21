# Task 1 Overview

## Instructions

You may use DynamoDB instead of Cassandra for this assignment if needed.

In general, the goals of this task are to perform the following:

1. Extract and clean the transportation dataset, and then store the result in HDFS.
1. Answer 2 questions from Group 1, 3questions from Group 2, and both questions from Group 3 using Hadoop. Store the results for questions from Group 2 and Question 3.2 in Cassandra or DynamoDB. You can access these questions in the Project Overview.

## Part 1

Your first task is to clean and store the dataset. To accomplish this, you must first retrieve and extract the dataset from the EBS volume snapshot. Afterwards, you should explore the data set and decide on how you wish to clean it. The exact methodology used to clean the dataset is left to you. The cleaned data must be stored on HDFS.

Note: The dataset contains a large amount of information that will not be useful for this task. Before you start, you should explore the 
database directory
 and decide on what you want to keep and discard. Consider removing or combining redundant fields and storing the useful data in a format that makes it easier for you to answer your chosen questions. 

## Part 2

Your second task is to answer your chosen questions using Hadoop. As noted above, the results for questions from Group 2 and Question 3.2 should be stored in Cassandra or DynamoDB. The exact approach you use to answer these questions is again left to you. Whatever approaches you choose, make sure you briefly explain and justify them in your report. See the 
Task 1 Queries
 page for specific queries. 

## Submission

PDF Report
You must submit your report in PDF format. Your report should be no longer than 4-5 pages, 11 point font. Your report should include the following:

1. Give a brief overview of how you extracted and cleaned the data.
1. Give a brief overview of how you integrated each system.
1. What approaches and algorithms did you use to answer each question?
1. What are the results of each question? Use only the provided subset for questions from Group 2 and Question 3.2.
1. What system- or application-level optimizations (if any) did you employ?
1. Give your opinion about whether the results make sense and are useful in any way.
