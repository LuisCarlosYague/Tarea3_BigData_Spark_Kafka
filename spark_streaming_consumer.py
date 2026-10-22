from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime, avg
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType

spark = SparkSession.builder.appName('SensorDataAnalysis').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
schema = StructType([
    StructField('sensor_id', IntegerType()),
    StructField('temperature', FloatType()),
    StructField('humidity', FloatType()),
    StructField('timestamp', LongType())
])
df = (spark.readStream.format('kafka')
      .option('kafka.bootstrap.servers', 'localhost:9092')
      .option('subscribe', 'sensor_data')
      .option('startingOffsets', 'earliest').load())
parsed_df = (df.select(from_json(col('value').cast('string'), schema).alias('data'))
             .select('data.*')
             .withColumn('event_time', from_unixtime(col('timestamp')).cast('timestamp')))
windowed_stats = (parsed_df.groupBy(window(col('event_time'), '1 minute'), col('sensor_id'))
                  .agg(avg('temperature').alias('avg_temperature'),
                       avg('humidity').alias('avg_humidity')))
query = (windowed_stats.writeStream.outputMode('complete').format('console')
         .option('truncate', 'false')
         .option('checkpointLocation', '/tmp/tarea3_kafka_checkpoint')
         .start())
query.awaitTermination()
