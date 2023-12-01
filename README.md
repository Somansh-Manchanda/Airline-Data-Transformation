# Airline-Data-Transformation

Utilized PySpark within the Databricks environment to execute sophisticated data transformations on comprehensive airline datasets. This process involved optimizing the data structure, ensuring it is finely tuned and well-prepared for advanced analytical investigations and insights.

Total Records: Our dataset is substantial, boasting an impressive 123.5 million records, providing a wealth of information about flights.

Distinct Origins: We're looking at 347 different places where flights take off. That's a lot, showing how people travel all over the world.

Distinct Destinations: We found insights into 352 special places where flights land. It's like exploring a big map of flight connections.

Unique Carriers: Our study includes data from 29 different airlines. We're checking out how all these airlines work in the world of flights.

Data Columns: There are 29 columns in our dataset, like different pieces of a puzzle. They help us see a detailed picture for a deep analysis.

Years Covered: We're diving into the past from 1987 to 2008, exploring almost two decades of aviation history. It's like taking a trip back in time.

The transformations applied below illustrate the optimization of airline data for advanced analytics.

1.	Create a DF(airlines_1987_to_2008) from this path
     Dataset Path : %fs ls dbfs:/databricks-datasets/asa/airlines/
  	
2.  Create a PySpark Datatypes schema for the above DF
3.	View the dataframe
4.	Return count of records in dataframe
5.	Select the columns - Origin, Dest and Distance
6.	Filtering data with 'where' method, where Year = 2001
7.	 Create a new dataframe (airlines_1987_to_2008_drop_DayofMonth) exluding dropped column (“DayofMonth”) 
8.	Display new DataFrame
9.	Create column 'Weekend' and a new dataframe(AddNewColumn) and display
10.	Cast ActualElapsedTime column to integer and use printschema to verify
11.	Rename 'DepTime' to 'DepartureTime'
12.	Drop duplicate rows based on Year and Month and Create new df (Drop Rows)
13.	Display Sort by descending order for Year Column using sort()
14.	Group data according to Origin and returning count
15.	Group data according to dest and finding maximum value for each 'Dest'
16.	Write data in Delta format.



