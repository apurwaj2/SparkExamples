import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val movieSchema = new StructType().add("movieId",IntegerType,true).add("title",StringType,true).add("genres",StringType,true)
val reviewSchema = new StructType().add("userId",IntegerType,true).add("movieId",IntegerType,true).add("rating",DoubleType,true).add("timestamp",LongType,true)

val movies = spark.read.format("csv").option("header", "true").schema(movieSchema).load("/home/apurwa/Downloads/movies.csv")
val reviews = spark.read.format("csv").option("header", "true").schema(reviewSchema).load("/home/apurwa/Downloads/reviews.csv")

val result = movies
.join(reviews, movies("movieId") === reviews("movieId"))
.groupBy(movies("movieId"), movies("title"))
.agg(count("rating").alias("numReviews"))
.orderBy(desc("numReviews"), asc("title"))
.show(10)



