# spark_scala_databricks
Analyzing graph which is a temporal network of interactions on the stack exchange web-site with Spark/Scala.

The script analyzes mathoverflow.csv using Spark and Scala on the Databricks platform. This graph is a temporal network of interactions on the stack exchange web-site MathOverflow. The dataset has three columns in the following format: ID of the source node (a user), ID of the target node (a user),  Unix timestamp (seconds since the epoch).

The script does:
- Remove the pairs where the questioner and the answerer are the same person. The subsequent operations are performed on this filtered dataframe.

- List the top 3 answerers who answered the highest number of questions, sorted in descending order of questions answered count. If there is a tie, list the individual with smaller node ID first.

- List the top 3 questioners who asked the highest number of questions, sorted in descending order of questions asked count. If there is a tie, list the individual with the smaller node ID first.

- List the top 5 most common answerer-questioner pairs, sorted in descending order of pair count. If there is a tie, list the pair with the smaller answerer node ID first. If there is still a tie, use the questioner node ID as the tie-breaker by listing the smaller questioner ID first.

- List, by month, the number of interactions (questions asked/answered) from September 1, 2010 (inclusively) to December 31, 2010 (inclusively).

- List the top 3 individuals with the most overall activity (i.e., highest total questions asked and questions answered).

This task uses the DataFrame API in Spark.

A template Scala notebook, `q2-skeleton.dbc` reads in a sample graph file `examplegraph.csv`. In the template, the input data is loaded into a dataframe, inferring the schema using reflection.

Note: You must use only Scala DataFrame operations for this task. You will lose points if you use SQL queries, Python, or R to manipulate a dataframe.
