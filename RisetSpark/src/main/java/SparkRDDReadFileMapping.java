import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SparkRDDReadFileMapping {
    public static void main(String[] args) {
        //Spark Config
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.ui.port", "4040").setAppName("Spark");

        //Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //RDD Read File
        JavaRDD<String> rdd = sparkContext.textFile("file path").map(new Function<String, String>() {
            public String call(String s) throws Exception {
                //here you can mapping data like do you want..


                return s;
                //you can change return data like your mapping
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
