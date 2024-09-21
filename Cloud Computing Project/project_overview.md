# Project Overview

The goal of the Capstone Project is to provide you an opportunity to synthesize the knowledge and skills you have learned from previous courses and apply them to solve real-world cloud computing challenges. You will work on a 
transportation dataset
 from the US Bureau of Transportation Statistics (BTS) that is hosted as an Amazon EBS volume snapshot.

The dataset used in the Project contains data and statistics from the US Department of Transportation on aviation, maritime, highway, transit, rail, pipeline, bike/pedestrian, and other modes of transportation in CSV format. The data is described in more detail by the 
Bureau of Transportation Statistics
. (Note that the dataset we are using does not extend beyond 2008, although more recent data is available from the previous link.) In this Project, we will concentrate exclusively on the aviation portion of the dataset, which contains flight data such as departure and arrival delays, flight times, etc. For an example of analysis using this dataset, see 
Which flight will get you there fastest?

You will be answering a common set of questions in different stacks/systems – in two batch processing systems (Apache Hadoop and Spark), and in a stream processing system (Apache Storm). After completing Task 1, you will be able to use the results you obtained to verify the results from Task 2.

The set of questions that must be answered using this dataset is provided in the next section. These questions involve discovering useful information such as the best day of week to fly to minimize delays, the most popular airports, the most on-time airlines, etc. Each task will require you to answer a subset of these questions using a particular set of distributed systems. The exact methodology used to answer the questions is largely left to you, but you must integrate and utilize the specified systems to arrive at your answers.

# Questions

For each task, you must answer a subset of the following questions. Each question is over the entire dataset, unless otherwise specified.

Group 1 (Answer any 2):

Rank the top 10 most popular airports by numbers of flights to/from the airport.

Rank the top 10 airlines by on-time arrival performance.

Rank the days of the week by on-time arrival performance.

Group 2 (Answer any 3):

For Questions 1 and 2 below, we are asking you to find, for each airport, the top 10 carriers and destination airports from that airport with respect to on-time departure performance. We are not asking you to rank the overall top 10 carriers/airports. For specific queries, see the 
Task 1 Queries
 and 
Task 2 Queries
.

For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.

For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.

For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y.

Group 3 (Answer both questions using Hadoop. You may also use Spark Streaming to answer Question 2.):

Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?

Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements (for specific queries, see the
 
Task 1 Queries
 and 
Task 2 Queries
):

a) The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs on January 5, 2008, Y-Z must depart on January 7, 2008.

b) Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.

c) Tom wants to arrive at each destination with as little delay as possible. You can assume you know the actual delay of each flight.

Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy constraints (a) and (b) and have the best individual performance with respect to constraint (c), if such flights exist.

For the queries in Group 2 and Question 3.2, you will need to compute the results for ALL input values (e.g., airport X, source-destination pair X-Y, etc.) for which the result is nonempty. These results should then be stored in Cassandra so that the results for an input value can be queried by a user. Then, closer to the grading deadline, we will give you sample queries (airports, flights, etc.) to include in your video demo and report.

For example, after completing Question 2.2, a user should be able to provide an airport code (such as “ATL”) and receive the top 10 airports in decreasing order of on-time departure performance from ATL. Note that for questions such as 2.3, you do not need to compute the values for all possible combinations of X-Y, but rather only for those such that a flight from X to Y exists.

# Submission

The submission you need to generate for each task consists of:

A report documenting what you have done with justification and explanation. The report should address all criteria in the grading rubric and in the submission details described in each task. The report will be no longer than 4-5 pages, 11 point font.

A video demonstration of the use of your system to answer the required questions. For the queries in Group 2 and Question 3.2, it will suffice to illustrate the results for a small subset of queries. The video demo should be no longer than 5 minutes.

Further details are provided in the instructions for each task. For your Task 2 report, you will also be asked to include a general comparison of the stacks used in each task (Hadoop, Storm, Spark).

Make sure you review the 
Syllabus
 for detailed information about deadlines for each task.

Getting Started
Official documentation for the systems used in this project are available at the following locations (there are many other guides and tutorials available online):

Hadoop
Storm
Spark
Cassandra
Kafka

The EBS snapshot ID for the transportation dataset is snap-e1608d88 for Linux/Unix and snap-37668b5e for Windows, in the us-east-1 (N. Virginia) region.
