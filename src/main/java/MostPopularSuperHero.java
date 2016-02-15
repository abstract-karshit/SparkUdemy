
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by 11130 on 14/02/16.
 */
public class MostPopularSuperHero {

    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("MostPopularSuperHero").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        class HrDict {
            Map<Integer, String> getHeroDict() {
                Map<Integer, String> heroDict = new HashMap<>();
                BufferedReader br = null;

                try {

                    String sCurrentLine;

                    br = new BufferedReader(new FileReader("/Users/11130/udemy/SparkCourse/Marvel-Names.txt"));

                    while ((sCurrentLine = br.readLine()) != null) {
                        String str = sCurrentLine;
                        String[] fields = str.split(" ", 2);
                        heroDict.put(Integer.parseInt(fields[0]), fields[1]);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

                return heroDict;
            }
        }

        class DummyComparator implements Serializable, Comparator<Tuple2<Integer, String> >{
            @Override
            public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
                return Integer.compare(o1._1(), o2._1());
            }
        }

        Broadcast<Map<Integer, String> > heroDict = sc.broadcast(new HrDict().getHeroDict());
        JavaRDD<String> lines = sc.textFile("/Users/11130/udemy/SparkCourse/Marvel-Graph.txt");

        JavaPairRDD<Integer, Integer> countOfOccurences = lines.mapToPair(
                s -> {
                    String[] heroes = s.split(" ");
                    return new Tuple2<>(Integer.parseInt(heroes[0]), heroes.length - 1);
                }
        ).reduceByKey(
                (x, y) -> x + y
        );

        JavaPairRDD<Integer, String> flippedCountOfOccurences = countOfOccurences.mapToPair(
                s -> new Tuple2<>(s._2(), heroDict.getValue().get(s._1()))
        );


        //Tuple2<Integer, String> result = flippedCountOfOccurences.max()

        Tuple2<Integer, String> result = flippedCountOfOccurences.max(new DummyComparator());

        System.out.println("The most populat superhero is " + result._2() + " with " + result._1() + " number of occurences");

    }
}
