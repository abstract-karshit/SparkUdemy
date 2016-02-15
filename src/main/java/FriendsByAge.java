import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 11130 on 13/02/16.
 */
public class FriendsByAge {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("AverageFriendsByAge").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/11130/udemy/SparkCourse/fakefriends.csv");

        JavaPairRDD<Integer, Integer> ageAndFriends = lines.mapToPair(
                s -> new Tuple2<>(Integer.parseInt(s.split(",")[2]), Integer.parseInt(s.split(",")[3]))
        );

        JavaPairRDD<Integer, Tuple2<Integer, Integer> > totalsByAge =  ageAndFriends.mapValues(
                a -> new Tuple2<>(a, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2)
        );

        JavaPairRDD<Integer, Integer> averageByAge = totalsByAge.mapValues(
                x -> x._1 / x._2
        );

        Map<Integer, Integer> results = new HashMap<>(averageByAge.collectAsMap());

        for (Map.Entry<Integer, Integer> m : results.entrySet()) {
            System.out.println(m.getKey() + " " + m.getValue());
        }
    }
}
