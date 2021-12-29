package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

public class ExamResults {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		//rddExampleTuple();
		System.setProperty("hadoop.home.dir","C:\\hadoop");
		
		SparkSession sparkSession = SparkSession.builder()
									.appName("testingSql")
									.master("local[*]")
									.config("spark.sql.warehouse.dir","file:///C:/tmp/").getOrCreate();
		
		sparkSession.udf().register("hasPassed", (String grade) -> grade.equals("A+") ,DataTypes.BooleanType);
		
		
		Dataset<Row> examResults = sparkSession.read().option("header", true).csv("src//main//resources//exams//students.csv");
		//examResults = examResults.withColumn("pass", lit(col("grade").equalTo("A+")));
		examResults = examResults.withColumn("pass", callUDF("hasPassed",col("grade")));
		examResults.show();
		
		/*
		 * 
		 * Dataset<Row> max = examResults.groupBy("subject")
		 * .agg(functions.max(functions.col("score").cast(DataTypes.IntegerType)).
		 * alias("Max Score.")
		 * ,functions.min(functions.col("score").cast(DataTypes.IntegerType)).
		 * alias("Min Score.")
		 * ,functions.mean(functions.col("score").cast(DataTypes.IntegerType)).
		 * alias("Mean Score."));
		 */

/*
 * Dataset<Row> dataSetDataFrames = examResults.select(col("subject"),
 * functions.date_format(col("datetime"), "MMMM").alias("month"),
 * functions.date_format(col("datetime"),
 * "M").alias("monthNum").cast(DataTypes.IntegerType));
 * 
 * Dataset<Row> count =
 * dataSetDataFrames.groupBy("level").pivot("monthNum").count();
 */			 
		
		//count.show();
		
		//exerciseExamResult(examResults);
		
		
		
	}

	private static void exerciseExamResult(Dataset<Row> examResults) {
		examResults= examResults.groupBy("subject").pivot("year")
				.agg(	 round(avg   (col("score")),2).as("Average")
						,round(stddev(col("score")),2).as("Std_Dev")
					);
		examResults.show();
	}

	private static Object max() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
