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
public class SortedAmountSpent {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("SortedAmountSpendByCustomer").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/11130/udemy/SparkCourse/customer-orders.csv");
        JavaPairRDD<Integer, Double> amountSpent = lines.mapToPair(
                s -> {
                    String[] values = s.split(",");
                    return new Tuple2<>(Integer.parseInt(values[0]), Double.parseDouble(values[2]));
                }
        );

        JavaPairRDD<Integer, Double> totalAmountSpent = amountSpent.reduceByKey(
                (x, y) -> x + y
        );

        JavaPairRDD<Double, Integer> flippedTotalAmountSpent = totalAmountSpent.mapToPair(
                s -> new Tuple2<>(s._2(), s._1())
        ).sortByKey();

        List<Tuple2<Double, Integer>> result = new ArrayList<>(flippedTotalAmountSpent.collect());

        for (Tuple2<Double, Integer> t: result) {
            System.out.println(t._2() + " " + (double)Math.round(t._1() * 100.00) / 100.00);
        }
    }
}
