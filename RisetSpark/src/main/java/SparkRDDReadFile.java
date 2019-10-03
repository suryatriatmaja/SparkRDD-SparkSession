import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkRDDReadFile {
    public static void main(String[] args) {
        //Spark Config
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "4g").set("spark.ui.port", "4040").setAppName("Spark");

        //Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //RDD Read File
        JavaRDD<String> rdd = sparkContext.textFile("file path");
        rdd.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
