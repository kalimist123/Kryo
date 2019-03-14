package com.diaceutics.kryo

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object ReadCSVFile {
  case class Employee(empno:String, ename:String, designation:String, manager:String, hire_date:String, sal:String , deptno:String)


  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val textRDD = sc.textFile("Kryo\\src\\resources\\emp_data.csv")
    //println(textRDD.foreach(println)
    val empRdd = textRDD.map {
      line =>
        val col = line.split(",")
        Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6))
    }
    /*
    val empDF = empRdd.toDF()
    empDF.show()
    */


      val empDF= sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("Kryo\\src\\resources\\emp_data.csv")

    empDF.show()

  }
}