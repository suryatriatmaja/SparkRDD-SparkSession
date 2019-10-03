import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

public class SparkRDDReadFileFlatMap {
    public static void main(String[] args) {
        //Spark Config
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.ui.port", "4040").setAppName("Spark");

        //Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //RDD Read File
        JavaRDD<String> rdd = sparkContext.textFile("file path").flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                //you can set function split here to flat
                return Arrays.asList(s.split("/")).iterator();
            }
        });

        //foreach spark for looping data result map
        rdd.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                //print your mapping data
                System.out.println(s);
            }
        });
    }
}
