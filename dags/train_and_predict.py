import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.ml.recommendation import ALS
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

def train_and_predict():
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Initialize Spark session
    logging.info("Initializing Spark session.")
    spark = SparkSession.builder \
        .appName("MovieLensALS") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Spark S3 configurations
    logging.info("Configuring Spark S3 settings.")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")
    spark.sparkContext.setLogLevel("WARN")

    logging.info("Spark session created successfully.")

    # Define schema for ratings data
    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", IntegerType(), True)
    ])

    # Load training and test data
    logging.info("Loading train and test datasets from MinIO.")
    try:
        train_data = spark.read.csv(f"s3a://{BUCKET_NAME}/train_ratings.csv", header=True, schema=schema)
        test_data = spark.read.csv(f"s3a://{BUCKET_NAME}/test_ratings.csv", header=True, schema=schema)
    except Exception as e:
        logging.error(f"Failed to read datasets: {e}")
        spark.stop()
        return

    logging.info("Previewing train and test datasets.")
    train_data.show(5, False)
    test_data.show(5, False)

    # Train ALS model
    logging.info("Training ALS model.")
    als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
    try:
        model = als.fit(train_data)
        logging.info("Model training completed successfully.")
    except Exception as e:
        logging.error(f"Model training failed: {e}")
        spark.stop()
        return

    # Save the trained model
    logging.info("Saving the trained model.")
    try:
        model.save("/shared_data/als_model")
        logging.info("Model saved successfully.")
    except Exception as e:
        logging.error(f"Failed to save the model: {e}")

    # Make predictions
    logging.info("Making predictions on the test dataset.")
    try:
        predictions = model.transform(test_data)
        predictions.write.format("csv").mode("overwrite").save(f"s3a://{BUCKET_NAME}/predictions")
        logging.info("Predictions written to MinIO successfully.")
    except Exception as e:
        logging.error(f"Prediction or saving results failed: {e}")

    # Stop Spark session
    spark.stop()
    logging.info("Spark session stopped.")
