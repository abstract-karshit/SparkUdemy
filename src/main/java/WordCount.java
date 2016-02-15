import com.google.common.base.CharMatcher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 11130 on 13/02/16.
 */
public class WordCount {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> book = sc.textFile("/Users/11130/udemy/SparkCourse/Book.txt");
        JavaRDD<String> words = book.flatMap(
                s -> Arrays.asList(s.split(" "))
        );
/*One way of doing it : The Hadoop Way*/

//        JavaPairRDD<String, Integer> countWord = words.mapToPair(
//                s -> new Tuple2<>(s, 1)
//        );
//
//        JavaPairRDD<String, Integer> result = countWord.reduceByKey(
//                (x, y) -> x + y
//        );
//
//        Map<String, Integer> wordsCount = new HashMap<>(result.collectAsMap());
//
//        for (Map.Entry<String, Integer> m : wordsCount.entrySet()) {
//            if (CharMatcher.ASCII.matchesAllOf(m.getKey())) {
//                System.out.println(m.getKey() + " " + m.getValue());
//            }
//        }

/*The Spark Way*/

        Map<String, Long> wordsCount = new HashMap<>(words.countByValue());

        for (Map.Entry<String, Long> m : wordsCount.entrySet()) {
            if (CharMatcher.ASCII.matchesAllOf(m.getKey())) {
                System.out.println(m.getKey() + " " + m.getValue());
            }
        }

    }
}
