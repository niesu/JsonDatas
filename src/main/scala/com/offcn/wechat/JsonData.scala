package com.offcn.wechat

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object JsonData {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("JsonParse").master("local[*]").getOrCreate()

		val df: DataFrame = spark.read.json("Data/json")
		df.printSchema()
		df.createOrReplaceTempView("a")
		df.show()

//		spark.sql("select topicType from a").show()

		spark.stop()
	}
}
