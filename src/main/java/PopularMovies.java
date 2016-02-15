import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 11130 on 14/02/16.
 */
public class PopularMovies {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Popular Movies").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/11130/udemy/SparkCourse/ml-100k/u.data");
        JavaPairRDD<String, Integer> movieId = lines.mapToPair(
                s -> new Tuple2<>(s.split("\t")[1], 1)
        );

        JavaPairRDD<String, Integer> countOfMovies = movieId.reduceByKey(
                (x, y) -> x + y
        );

        JavaPairRDD<Integer, String> flippedCountOfMovies = countOfMovies.mapToPair(
                s -> new Tuple2<Integer, String>(s._2(), s._1())
        ).sortByKey();

        List<Tuple2<Integer, String> > result = new ArrayList<>(flippedCountOfMovies.collect());

        for(Tuple2<Integer, String> t: result) {
            System.out.println(t._1() + " " + t._2());
        }


    }
}
