import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 11130 on 14/02/16.
 */
public class AmountSpent {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("AmountSpendByCustomer").setMaster("local[*]");
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

        Map<Integer, Double> result = new HashMap<>(totalAmountSpent.collectAsMap());

        for (Map.Entry<Integer, Double> m : result.entrySet()) {
            System.out.println(m.getKey() + " " + (double)Math.round(m.getValue() * 100.00) / 100.00);
        }
    }
}
