package com.github.grzegorzwitkowski.elasticsearchsparkindexer

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object ElasticsearchIndexerJob {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("ElasticsearchIndexerJob")
      .setMaster("local[1]")
      .set("es.nodes", "localhost")
      .set("es.port", "9200")
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("users.csv")
      .map(csvLine => User.fromCsv(csvLine))
      .saveToEs("myshop/users")
  }
}

case class User(firstName: String, lastName: String, age: Int)

object User {

  def fromCsv(csvLine: String): User = {

    val columns: Array[String] = csvLine.split(",")
    new User(
      firstName = columns(0),
      lastName = columns(1),
      age = columns(2).toInt
    )
  }
}
