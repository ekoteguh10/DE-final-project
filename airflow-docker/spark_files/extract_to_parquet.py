# import library
from pyspark.sql import SparkSession

# create session for Spark
spark = SparkSession \
    .builder \
    .appName("Transforming INPUT files into PARQUET files") \
    .getOrCreate()

# Before transforming, we need to read datasets
# After then, transform into PARQUET format files
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