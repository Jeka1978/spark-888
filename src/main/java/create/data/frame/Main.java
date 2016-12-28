package create.data.frame;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

/**
 * Created by Evegeny on 28/12/2016.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("stam");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("one;1;sunday", "two;2;monday", "three;3;Tuesday"));


    }
}
