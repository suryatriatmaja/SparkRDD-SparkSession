import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionSqlQuery {
    public static void main(String[] args) {
        //Spark Config
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.ui.port", "4040").setAppName("Spark");
        //Spark Session / Spark Sql setting
        SparkSession sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();

        //Transform data file to dataset with sparksession , the file can csv,json, etc.
        //json file transform example to dataset
        Dataset<Row> dataset = sparkSession.read().json("file path");
        //Give the table name
        dataset.createOrReplaceTempView("TableDataset");
        //New Dataset for sql query
        Dataset<Row> dataset1 = sparkSession.sql("select id, name from TableDataset");
        dataset1.show();
    }
}
