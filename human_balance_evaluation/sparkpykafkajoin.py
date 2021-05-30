from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

spark = SparkSession.builder.appName('spark-kafka-join').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redis_df = spark\
.readStream\
.format('kafka')\
.option('subscribe', 'redis-server')\
.option('kafka.bootstrap.servers','localhost:9092')\
.option('startingOffsets','earliest')\
.load()

# Cast the value column in the streaming dataframe as a STRING 
redis_df = redis_df.selectExpr('cast(value as string) value')

# Parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"<base64-encoding>",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"<base64-encoding>",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

redis_df = redis_df.withColumn('value', from_json('value', redisMessageSchema))\
.select(col('value.*'))

# Storing them in a temporary view called RedisSortedSet
redis_df.createOrReplaceTempView('RedisSortedSet')

# Execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# The reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
redis_df = spark.sql('''
SELECT key,
zSetEntries[0].element AS encodedCustomer
FROM RedisSortedSet
''')

# Take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |<base64-encoding>   |
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+

redis_df = redis_df.withColumn('customer', unbase64(col('encodedCustomer')).cast('string'))

# Extract the underlying JSON format : {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
redis_df = redis_df.withColumn('customer', split('customer','"customer":').getItem(1))
redis_df = redis_df.withColumn('customer', expr('substring(customer, 1, length(customer) - 1)'))

# Parse the JSON in the Customer record
customerSchema = StructType(
    [
        StructField('customerName', StringType()),
        StructField('email', StringType()),
        StructField('phone', StringType()),
        StructField('birthDay', StringType()),
    ]
)
redis_df = redis_df.withColumn('customer', from_json('customer', customerSchema))\
.select(col('customer.*'))\

# Store in a temporary view called CustomerRecords
redis_df.createOrReplaceTempView('CustomerRecords')

# JSON parsing will set non-existent fields to null.
# So let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql('''
SELECT customerName, email, phone, birthDay
FROM CustomerRecords
WHERE email IS NOT NULL
AND birthDay IS NOT NULL
''')

# Split the birth year as a separate field from the birthday
emailAndBirthDayStreamingDF = emailAndBirthDayStreamingDF.withColumn('birthYear', split('birthDay', '-').getItem(0))

# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email', 'birthYear')

# Using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
df = spark\
.readStream\
.format('kafka')\
.option('subscribe', 'stedi-events')\
.option('kafka.bootstrap.servers','localhost:9092')\
.option('startingOffsets','earliest')\
.load()
                                   
# Cast the value column in the streaming dataframe as a STRING 
df = df.selectExpr('cast(key as string) key', 'cast(value as string) value')

# Parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+

eventSchema = StructType(
    [
        StructField('customer', StringType()),
        StructField('score', FloatType()),
        StructField('riskDate', DateType()),
    ]
)

df = df.withColumn('value', from_json('value', eventSchema))\
.select(col('value.*'))\

# Storing them in a temporary view called CustomerRisk
df.createOrReplaceTempView('CustomerRisk')

# Execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql('''
SELECT customer, score
FROM CustomerRisk
''')

# Join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
riskScoreByBirthYear = customerRiskStreamingDF.join(
    emailAndBirthYearStreamingDF,
    customerRiskStreamingDF.customer == emailAndBirthYearStreamingDF.email
)

# Sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 

riskScoreByBirthYear\
.selectExpr("CAST(customer AS string) AS key", "to_json(struct(*)) AS value")\
.writeStream\
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("topic", "customer-risk")\
.option("checkpointLocation","/tmp/kafkacheckpoint")\
.start()\
.awaitTermination()
