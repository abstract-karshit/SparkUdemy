import com.google.common.base.CharMatcher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 11130 on 13/02/16.
 */
public class BetterWordCount {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("BetterWordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> book = sc.textFile("/Users/11130/udemy/SparkCourse/Book.txt");
        JavaRDD<String> words = book.flatMap(
                s -> Arrays.asList(s.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+"))
        );

        Map<String, Long> wordsCount = new HashMap<>(words.countByValue());

        for (Map.Entry<String, Long> m : wordsCount.entrySet()) {
            if (CharMatcher.ASCII.matchesAllOf(m.getKey())) {
                System.out.println(m.getKey() + " " + m.getValue());
            }
        }

    }
}
