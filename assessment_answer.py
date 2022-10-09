"""
Assessment of Mistplay
Author: Ying Yu

"""


import json
import os
import cryptocode
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType 
from pyspark.sql import Window

# Because the json stored in multiple dict object
# I changed the way it stored and load
file = "data.json"
DATA_PATH = os.path.join(os.path.dirname(__file__), "data.json")
def read_json_file(file):
    with open(file, "r") as r:
        response = r.read()
        response = response.replace("\n", "")
        response = response.replace("}{", "},{")
        response = "[" + response + "]"
        return json.loads(response)
json_data = read_json_file(DATA_PATH)

def save_file(df, output_path):
    df.coalesce(1).write.format("parquet").save(
    output_path, overwrite=True
)
    
# 1. Output the number of rows in the original input file.
number_of_row = len(json_data)
print(f"There are {number_of_row} rows in the original input file")

# 2. Dedupe the original data
def dedup_rows(input_data):
    seen = set()
    new_l = []
    duplicated = []
    for d in input_data:
        t = tuple(d.items())
        check = (t[0], t[-1])
        if check not in seen:
            seen.add(check)
            new_l.append(d)
        else:
            duplicated.append(d)
    return duplicated
duplicated_rows = dedup_rows(json_data)
 
#3. Output the number of rows removed.
number_of_rows_removed = len(duplicated_rows)
print(f"There are {number_of_rows_removed} duplicated rows ")
print(f"Duplicated Results: {duplicated_rows}")

# 4. Calculate the "rank" of the user 
spark = SparkSession.builder.appName("data_manipulation").getOrCreate()
spark.sparkContext.setCheckpointDir("tmp/spark/checkpoints")

df = spark.read.json(DATA_PATH)
partition = Window.partitionBy("age_group").orderBy(f.desc("user_score"))
df_with_age_group_rank = df.withColumn("age_group_rank", f.rank().over(partition))
df_with_age_group_rank.show()

# 5
output_result = df_with_age_group_rank.select("id", "email", "age_group").filter(
    f.col("age_group_rank") == 1
).toJSON().collect()
print(output_result)
"""
['{"id":"ed41f71b-6001-40c1-bbf6-d7dc4213ec75","email":"msachnoie@msn.com","age_group":1}',
 '{"id":"884e17c5-228d-4fae-9572-f15ca25728be","email":"rhalfacre1k@google.com.au","age_group":2}',
 '{"id":"cfe8ad95-a610-4b86-9ff0-b3421f3cfa3d","email":"cstientona2@yellowbook.com","age_group":3}',
 '{"id":"08af567c-4a4b-48ab-a56e-0bc817f4e2a4","email":"fpietzkerkf@networkadvertising.org","age_group":4}']
"""

# Q6: 'flatten' the widget_list
df_explode_widget_list = df_with_age_group_rank.withColumn(
    "widget_list", f.explode("widget_list")
)

df_explode_widget_list.coalesce(1).write.format("json").save(
    "df_explode_widget_list.json", overwrite=True
)

# Q7 print out row counts of new df
print(f"New total rows is: {df_explode_widget_list.count()}")

# Q8 Add two more columns to the data set,`widget_name`` and `widget_amount`
df_explode_widget_list = df_with_age_group_rank.withColumn(
    "widget_name", f.explode("widget_list.name")
).withColumn("widget_amount", f.explode("widget_list.amount"))

# Q9 
#encyrption function
def encrypt_value(mystring):
    encoded = cryptocode.encrypt(mystring,"mypassword")
    return encoded
#dencyrption function
def dencrypt_value(mystring):
    dencoded = cryptocode.decrypt(mystring,"mypassword")
    return dencoded
#pyspark udf function to do the encyrption and dencyrption
spark_udf_encrypt_value = f.udf(encrypt_value, StringType())
spark_udf_dencrypt_value = f.udf(dencrypt_value, StringType())

df_with_age_group_rank = df_with_age_group_rank.withColumn('encrypted_email',spark_udf_encrypt_value('email'))
#if you want to dencrypt the value, run the below function
#df_with_age_group_rank = df_with_age_group_rank.withColumn('dencrypt_email',spark_udf_dencrypt_value('encrypted_email'))

#10 store the table
save_file(df_with_age_group_rank, 'df_with_encryped_email_Q9.parqut')

#11 Create a new dataset with location and id
inverted_index_df = df_explode_widget_list.select('id',"location")
inverted_index_df.groupBy("location").agg(f.collect_set(f.col("id")).alias("ids"))

#12 store the table
save_file(inverted_index_df, 'inverted_index_df.parquet')
