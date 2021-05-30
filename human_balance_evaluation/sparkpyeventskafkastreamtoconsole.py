from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

spark = SparkSession.builder.appName('spark-kafka-join').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

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
.select(col('value.*'))

# Storing them in a temporary view called CustomerRisk
df.createOrReplaceTempView('CustomerRisk')

# Execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql('''
SELECT customer, score
FROM CustomerRisk
''')

# Sink the customerRiskStreamingDF dataframe to the console in append mode
customerRiskStreamingDF\
.writeStream\
.outputMode('append')\
.format('console')\
.start()\
.awaitTermination()

# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 