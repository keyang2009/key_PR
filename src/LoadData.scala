import org.apache.spark.sql.{SparkSession, DataFrame}

class LoadData(spark: SparkSession) {
  def getCsvData(filepath: String): DataFrame ={
    var data = spark.read.format("csv").option("header", "True").option("inferSchema", true).load(filepath).na.fill("NA")
    data
  }
}
