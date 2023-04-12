# adventureworks-etl
ETL batch demo based on AdventureWorks database  in postgres using Apache Spark's *DataFrame & SQL* APIs to:
- Extract a subset of the tables from the AdventureWorks database (full db found in https://github.com/lorint/AdventureWorks-for-Postgres)
- Persist tables in *.parquet* format
- Perform basic cleaning for the columns
- Denormalize into a star schema by merging related tables

## How to execute
Must have sbt for scala, spark-submit and the AdventureWorks database running to execute the project, then:

- *sbt assembly*
- *spark-submit --class Main < appJarFolder >/< appJar >*

Where Main is the main file or entry point of the app, < appJarFolder > is the full path to the /target/scala-x.yz
folder containing the assembled *.jar* file, and < appJar > is the name of this *.jar*, example:
- *spark-submit --class Main /path/to/target/scala-2.12/AdventureWorksETL-assembly-0.1.0-SNAPSHOT.jar*