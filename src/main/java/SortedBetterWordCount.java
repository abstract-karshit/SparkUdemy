import com.google.common.base.CharMatcher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * Created by 11130 on 13/02/16.
 */
public class SortedBetterWordCount {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("SortedBetterWordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> book = sc.textFile("/Users/11130/udemy/SparkCourse/Book.txt");
        JavaRDD<String> words = book.flatMap(
                s -> Arrays.asList(s.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+"))
        );

        JavaPairRDD<String, Integer> countWord = words.mapToPair(
                s -> new Tuple2<>(s, 1)
        );

        JavaPairRDD<String, Integer> wordCount = countWord.reduceByKey(
                (x, y) -> x + y
        );

        JavaPairRDD<Integer, String> arrangedCount = wordCount.mapToPair(
                s -> new Tuple2<>(s._2(), s._1())
        ).sortByKey();

        List<Tuple2<Integer, String> > result = new ArrayList<>(arrangedCount.collect());

        for(Tuple2<Integer, String> t: result) {
            System.out.println(t._1() + " " + t._2());
        }

    }
}
