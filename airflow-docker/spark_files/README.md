# Spark Files

To build the data pipeline, there are some processes formulated as ETL, shorts for `Extract`, `Transform`, and `Load`. In the `Extract` process, we have to extract the CSV files and convert those files into parquet files.

```python
# extract_to_parquet.py

# import library
from pyspark.sql import SparkSession

# create session for Spark
spark = SparkSession \
    .builder \
    .appName("Transforming INPUT files into PARQUET files") \
    .getOrCreate()
```

The pieces of code above is defining the library that we use and creating Spark Session. Then, the next step is reading CSV files that we have uploaded into Cloud Storage with bitbucket name `final_project_ekoteguh` and folder `INPUT`. The converted files (parquet files) are stored into `RAW` folder.

```python
# extract_to_parquet.py

df_country = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/USCOUNTRY.csv")
df_country.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/uscountry.parquet")

df_port = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/USPORT.csv")
df_port.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/usport.parquet")

df_state = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/USSTATE.csv")
df_state.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/usstate.parquet")

df_airport = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/airport-codes_csv.csv")
df_airport.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/airportcodes.parquet")

df_temp = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/GlobalLandTemperaturesByCity.csv")
df_temp.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/globaltempbycity.parquet")

df_immigration = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/immigration_data_sample.csv")
df_immigration.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/imigration.parquet")

df_demographic = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/us-cities-demographics.csv")
df_demographic.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/demographic.parquet")
```

## Additional information

After I tried five times, similar problems occured: `Column name of df_immigration` and `failed: The number of columns doesn't match.`. The first problem I think because of column name.

```csv
City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count
```

And the second problem is because of delimiter where the default is comma (,) but the file contains semicolon (;). Therefore, we need to fix it.

```python
# The delimiter for this file (us-cities-demographics.csv) is semicolon (;)
# We have to change the default delimiter
df_demographic = spark.read.options(header="true", inferSchema="true", delimiter=";").csv("gs://final_project_ekoteguh/INPUT/us-cities-demographics.csv")
df_demographic.printSchema()
# There is a problem in Header of this df, 
# Therefore, we need to rename the header before write it into parquet file
# Header: City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count

# Rename all column names with new column names
demographic_new_col_names = ["City", "State", "MedianAge", "MalePopulation", "FemalePopulation", "TotalPopulation", "NumberOfVeterans", "ForeignBorn", "AverageHouseholdSize", "StateCode", "Race", "Count"]
df_demographic = df_demographic.toDF(*demographic_new_col_names)
```