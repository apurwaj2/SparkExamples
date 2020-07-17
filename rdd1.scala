import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

def reviewTuple(line: String): (Int, (Int)) = {
        val fields = line.split(',')
        return (fields(1).toInt, 1)
    }

def movieTuple(line: String):(Int, (String)) = {
	val fields = line.split(',')
	return(fields(0).toInt, (fields(1).toString))
}

/* Read text files into RDD by setting the number of partitions in the second argument */
val reviews = sc.textFile("/home/apurwa/Downloads/reviews_large.csv", 5)
val movies = sc.textFile("/home/apurwa/Downloads/movies_large.csv", 5)

val headerReviews = reviews.first();
val headerMovies = movies.first();

/*Remove the first row from both the RDDs which contains titles*/
val reviewData = reviews.filter(row => row != headerReviews)
val movieData = movies.filter(row => row != headerMovies)

val movieSplit = movieData.map(movieTuple)
val reviewSplit = reviewData.map(reviewTuple).reduceByKey((x, y) => (x + y))

val res = movieSplit.join(reviewSplit)

val sorted = res.sortBy(_._2._2, false) 

sorted.take(10)
