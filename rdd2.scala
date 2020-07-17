import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

def reviewTuple(line: String): (Int, (Float, Int)) = {
        val fields = line.split(',')
        return (fields(1).toInt, (fields(2).toFloat, 1))
    }

def movieTuple(line: String):(Int, (String)) = {
	val fields = line.split(',')
	return(fields(0).toInt, (fields(1).toString))
}

val reviews = sc.textFile("/home/apurwa/Downloads/reviews_large.csv", 5)
val movies = sc.textFile("/home/apurwa/Downloads/movies_large.csv", 5)

val headerReviews = reviews.first();
val headerMovies = movies.first();

val reviewData = reviews.filter(row => row != headerReviews)
val movieData = movies.filter(row => row != headerMovies)

val movieSplit = movieData.map(movieTuple)
val reviewSplit = reviewData.map(reviewTuple).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, (x._2._1 / x._2._2), x._2._2 ))

val reviewAvgRatingFilter  = reviewSplit.filter(x => x._2 > 4)

val reviewNumCount = reviewAvgRatingFilter.filter(x => x._3 > 10)

val reviewMovieIds = reviewNumCount.map(y => y._1)

val bcast = sc.broadcast(reviewMovieIds.collect())

val result = movieSplit.filter(r => bcast.value.contains(r._1))

result.coalesce(1).saveAsTextFile("/home/apurwa/outfile")
