import org.apache.log4j.{Level,Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, GraphLoader}
object Entrance {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("prstudy")
    val filepath = "C:\\Users\\key\\IdeaProjects\\key_PR\\data\\followers.txt"
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc,filepath)
    val ranks = graph.pageRank(0.001).vertices
    val users = sc.textFile("C:\\Users\\key\\IdeaProjects\\key_PR\\data\\users.txt").map {
        line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
      }
    //
    val ranksByUsername = users.join(ranks).map {
      case(id , (username,rank)) => (username,rank)
    }
    //
    println(ranksByUsername.collect().mkString("\n"))


  }
}
