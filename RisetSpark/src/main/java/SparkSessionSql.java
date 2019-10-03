import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionSql {
    public static void main(String[] args) {
        //Spark Config
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.ui.port", "4040").setAppName("Spark");
        //Spark Session / Spark Sql setting
        SparkSession sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();

        //Transform data file to dataset with sparksession , the file can csv,json, etc.
        //json file transform example to dataset
        Dataset<Row> dataset = sparkSession.read().json("file path");
        dataset.show();
        //csv file transform example to dataset
        Dataset<Row> dataset1 = sparkSession.read().csv("file path");
        dataset1.show();
        //Text file transform example to dataset
        Dataset<String> dataset2 = sparkSession.read().textFile("file path");
        dataset2.show();
        //Table transform example to dataset
        Dataset<Row> dataset3 = sparkSession.read().table("table");
        dataset3.show();
        //Text transform example to dataset
        Dataset<Row> dataset4 = sparkSession.read().text("table");
        dataset4.show();
    }
}
