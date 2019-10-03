import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SparkRDDReadFileTuple2 {
    public static void main(String[] args) {
        //Spark Config
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.ui.port", "4040").setAppName("Spark");

        //Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Tuple2<String,String>> rdd = sparkContext.wholeTextFiles("file path").map(new Function<Tuple2<String, String>, Tuple2<String, String>>() {
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Tuple2<String, String>(stringStringTuple2._1,stringStringTuple2._2);
            }
        });

        //foreach spark for looping data Tuple2
        rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2._2);
                System.out.println(stringStringTuple2._1);
            }
        });


    }
}
