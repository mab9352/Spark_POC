package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.aopalliance.reflect.Metadata;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.mutable.Mutable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.MathArrays.Function;
import org.apache.hadoop.hdfs.server.namenode.FileDataServlet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import scala.Tuple2;
import scala.tools.nsc.transform.SpecializeTypes.Implementation;

public class Main {

	private static Dataset<Row> sql;

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		//rddExampleTuple();
		System.setProperty("hadoop.home.dir","C:\\hadoop");
		
		SparkSession sparkSession = SparkSession.builder()
									.appName("testingSql")
									.master("local[*]")
									.config("spark.sql.warehouse.dir","file:///C:/tmp/").getOrCreate();
		
		
		
		Dataset<Row> dataset = sparkSession.read().option("header", true).csv("C:\\mab\\projects\\eclipse-workspace\\Practicals\\Starting Workspace\\Project\\src\\main\\resources\\biglog.txt");
		
		
		//sparkSession.udf().re
				
		
		//SQL conventional
		/*
		 * dataset.createOrReplaceTempView("logging_table");
		 * Dataset<Row> results = sparkSession.sql("select level," +
		 * "date_format(datetime,'MMM') as month , " + "count(1) as total " +
		 * "from logging_table group by level, month order by cast(first(date_format(datetime,'M'))as int)"
		 * );
		 * 		//results = results.drop("monthNum");
				results.show(100);
		 */
		//dataset = dataset.selectExpr("level","date_format(datetime,'MMM') as month");
		
		dataset = dataset.select(col("level"),
								functions.date_format(col("datetime"), "MMM").alias("month"),
								functions.date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		
		dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
		dataset.orderBy(col("monthnum"),col("level"));
		
		dataset = dataset.drop(col("monthnum"));
		
		dataset.show(100);
		
		dataset.explain(); // the query plan on system console.	
		
		/* using a temp view.
		 * dataset.createOrReplaceTempView("subject_view"); Dataset<Row> sql =
		 * sparkSession.sql("select distinct(year) from subject_view order by year desc"
		 * ); sql.show();
		 */		
		
		//sqlGroupByInmemory();
		
		
		
		
		sparkSession.stop();
		//sparkRddExample();
//	
//		2 algo for grouping 
//		sortagg  - > uses sort algo and then groups for matching keys - as size increases sort will take more than linear nlogo(n)
//					memory efficient 
//					 
//					W
//		Hashaggregation -> creates a hash key - value map O(n)
//				if a key is found spark hash aggregation overrides the value and updates  the count or agg Function
//				double the size of data set  it will take twice as long
//				Expensive memory 
//				
//				hashAggregation over sort is optimization.
//				hashAgg can be done only if the  data in the value is Mutable
//				 this is diff from java hashing agg 
//				 this is stored in native memory  - not jvm 
//				 
		
		
		
	}

	private static void bigDataSqlExample(SparkSession sparkSession) {
		Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
		//sqlGroupBy(dataset);
		Dataset<Row> dataSetDataFrames = dataset.select(col("level"),
				functions.date_format(col("datetime"), "MMMM").alias("month"),
				functions.date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType));
		
		Dataset<Row> count = dataSetDataFrames.groupBy("level").pivot("monthNum").count();
						 
		
		count.show();
	}

	private static void sqlGroupBy(Dataset<Row> dataset) {
		Dataset<Row> dataSetDataFrames = dataset.select(col("level"),
															functions.date_format(col("datetime"), "MMMM").alias("month"),
															functions.date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType))
											   .groupBy(col("level"),col("month"),col("monthNum"))
											   .count()
											   .orderBy(col("monthNum"))
											   .drop(col("monthNum"));
	}

	private static void sqlGroupByInmemory() {
		System.setProperty("hadoop.home.dir","C:\\hadoop");
		
		SparkSession sparkSession = SparkSession.builder()
									.appName("testingSql")
									.master("local[*]")
									.config("spark.sql.warehouse.dir","file:///C:/tmp/").getOrCreate();
		
		Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		
		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

		
		StructField[] structFields = new StructField[] { 
				new StructField("level",DataTypes.StringType,false,org.apache.spark.sql.types.Metadata.empty()),
				new StructField("datetime",DataTypes.StringType ,false,org.apache.spark.sql.types.Metadata.empty())
		};
		StructType schema = new StructType(structFields);
		Dataset<Row> createDataFrame = sparkSession.createDataFrame(inMemory,schema );
		createDataFrame.createOrReplaceTempView("logging_table");
		
		//Dataset<Row> sql = sparkSession.sql("select  level , count(datetime) from logging_table group by level"); 	
		
		 Dataset<Row> sql = sparkSession.sql("select level , date_format(datetime,'MMMM')  as month , count(1) as total from logging_table  group by level , month");
		 sql.show();
		/*
		 * inputData.add("ERROR : Tuesday 4 Septemnber 0605");
		 * inputData.add("INFO : Tuesday 4 Septemnber 0610");
		 * inputData.add("INFO : Tuesday 4 Septemnber 0611");
		 * inputData.add("WARN : Tuesday 4 Septemnber 0615");
		 * inputData.add("WARN : Tuesday 4 Septemnber 0625");
		 */
	
		//extracted(dataset);
		//An RDD is being build up - and execution plan is being created.
		//Only when a action is executed - In a cluster world the worker nodes are doing their task.
		//Dataset<Row> modernArtSql = dataset.filter("subject = 'Modern Art' AND year >= 2007");
		//modernArtSql.show();	
		
		//Dataset<Row> modernArtLambda =  dataset.filter(row -> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year")) >= 2007);
		//modernArtLambda.show();
		
		//Dataset<Row> filterExp = dataset.filter(col("subject").equalTo("Modern Art").or(col("subject").like("G%")).and(col("year").geq(2007)));
	 	//filterExp.show();
	}

	private static void extracted(Dataset<Row> dataset) {
		Row first = dataset.first();
		
		
		String subject  = first.getAs("subject").toString();
		System.out.println("subject is "+ subject);
		
		Integer year = Integer.valueOf(first.getAs("year"));
		System.out.println("Year is "+ year);
		System.out.println(dataset.count());
		
		
		
	}

	private static void sparkRddExample() {
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[5]");
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		List<Tuple2<Integer,Integer>> visitsRaw = new ArrayList<Tuple2<Integer,Integer>>();
		visitsRaw.add(new Tuple2<Integer, Integer>(1, 5));
		visitsRaw.add(new Tuple2<Integer, Integer>(2, 10));
		visitsRaw.add(new Tuple2<Integer, Integer>(42, 21));
		
		
		List<Tuple2<Integer,String>> usesrRaw = new ArrayList<Tuple2<Integer,String>>();
		usesrRaw.add(new Tuple2<Integer, String>(1, "John"));
		usesrRaw.add(new Tuple2<Integer, String>(2, "Mary"));
		usesrRaw.add(new Tuple2<Integer, String>(4, "Andrew"));
		usesrRaw.add(new Tuple2<Integer, String>(6, "Mathew"));
		usesrRaw.add(new Tuple2<Integer, String>(3, "James"));
		usesrRaw.add(new Tuple2<Integer, String>(5, "Thomas"));
		usesrRaw.add(new Tuple2<Integer, String>(7, "David"));
		usesrRaw.add(new Tuple2<Integer, String>(9, "Peter"));
		usesrRaw.add(new Tuple2<Integer, String>(10, "George"));
		     	 
		
		JavaPairRDD<Integer, Integer> visits  = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer, String> users = sc.parallelizePairs(usesrRaw);
		
		//JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visits.join(users);
		 JavaPairRDD<Integer, Tuple2<String, org.apache.spark.api.java.Optional<Integer>>> leftOuterJoinRdd = users.leftOuterJoin(visits);
		 JavaPairRDD<Integer, Tuple2<org.apache.spark.api.java.Optional<Integer>, String>> rightOuterJoinRdd = visits.rightOuterJoin(users);
		 
		 JavaPairRDD<Integer, Tuple2<org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<String>>> fullOuterJoin = visits.fullOuterJoin(users);
		 
		  JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesian = visits.cartesian(users);
		  System.out.println("------------------");
		 fullOuterJoin.foreach(tuple -> System.out.println(tuple));
		 System.out.println("------------------");
		 cartesian.foreach(tuple -> System.out.println(tuple));
		 System.out.println("------------------");
		 //leftOuterJoinRdd.foreach(tuple -> System.out.println(tuple));
		//System.out.println("------------------");
		//rightOuterJoinRdd.foreach(tuple -> System.out.println(tuple));
		sc.close();	
		
		//JavaSparkContext sc = rddExample();
		 
		//JavaSparkContext sc = filterNFlatMap(inputData, conf);
		
		//JavaSparkContext sc = rddMapPair(inputData);
		
		//JavaRDD<String,String> 
		
		//Tuple2<String,String>☺
		
		/*
		 * JavaPairRDD<String, String> pariRdd = origLogMesgs.mapToPair(rawValue -> {
		 * String[] split = StringUtils.split(rawValue, ":"); return new Tuple2<String,
		 * String>(split[0], split[1]); }); JavaPairRDD<String, Iterable<String>>
		 * groupByKey = pariRdd.groupByKey();
		 * 
		 */
		
		/*
		 * JavaPairRDD<String,Long> pariRdd = origLogMesgs.mapToPair( rawValue -> {
		 * String[] split = StringUtils.split(rawValue, ":"); return new Tuple2<String,
		 * Long>(split[0],1L); }); JavaPairRDD<String, Long> reduceByKey =
		 * pariRdd.reduceByKey((value1,value2)->value1+value2);
		 * reduceByKey.foreach(tuple -> System.out.println(tuple._1+" has " +
		 * tuple._2+" instances"));
		 */
		
		
		
		//System.out.println(sqrtRDD);
	}

	private static JavaSparkContext rddExample() {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN : Tuesday 4 Septemnber 0405");
		inputData.add("ERROR : Tuesday 4 Septemnber 0605");
		inputData.add("INFO : Tuesday 4 Septemnber 0610");
		inputData.add("INFO : Tuesday 4 Septemnber 0611");
		inputData.add("WARN : Tuesday 4 Septemnber 0615");
		inputData.add("WARN : Tuesday 4 Septemnber 0625");
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[5]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> initialRDD = sc.textFile("C:\\mab\\projects\\eclipse-workspace\\Practicals\\Starting Workspace\\Project\\src\\main\\resources\\subtitles\\input-spring.txt"); 

		 initialRDD.map(value -> value.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
		.filter(value -> (value.trim().length() > 0))
		.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		.filter(value -> (value.trim().length() > 0))
		.filter(value -> (Util.isNotBoring(value)))
		.mapToPair(rawValue -> new Tuple2<String, Long>(rawValue, 1L))
		.reduceByKey((value1,value2) -> value1 + value2)
		.mapToPair(tuple -> new Tuple2<Long,String>(tuple._2,tuple._1))
		.sortByKey(false)
		.collect()
		.forEach(tuple -> System.out.println(tuple));
		//.coalesce(1)
		//.getNumPartitions();
		//System.out.println(numPartitions);
		return sc;
	}

	private static JavaSparkContext filterNFlatMap(List<String> inputData, SparkConf conf) {
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.parallelize(inputData)
		.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		.filter(value -> (value.length() > 1))
		.filter(value -> !(NumberUtils.isNumber(value)))
		.foreach(tuple -> System.out.println(tuple));
		return sc;
	}

	private static JavaSparkContext rddMapPair(List<String> inputData) {
		//We need to understand this syntax.
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.parallelize(inputData).mapToPair(rawValue -> {
			 			return new  Tuple2<String, Long>(StringUtils.split(rawValue, ":")[0],1L);
		}).reduceByKey((value1,value2)->value1+value2)
		.foreach(tuple -> System.out.println(tuple._1+" has " + tuple._2+" instances"));
		return sc;
	}

	private static void rddExampleTuple() {
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(135);
		inputData.add(235);
		inputData.add(335);
		inputData.add(435);
		inputData.add(535);

		//We need to understand this syntax.
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> myRDD = sc.parallelize(inputData);
		Integer originalIntegers = myRDD.reduce((Integer value1, Integer value2) -> value1 + value2);
		//JavaRDD<IntegerWithSqaureRoots> sqrtRDD = myRDD.map(value1 -> new IntegerWithSqaureRoots(value1));
		JavaRDD<Tuple2<Integer,Double>> sqrtRDDTuple = myRDD.map(value -> new Tuple2<Integer,Double>(value,Math.sqrt(value)));
		sqrtRDDTuple.foreach(value -> System.out.println(value));
		
		Tuple2<Integer,Double> myValue = new Tuple2<Integer,Double>(9,3.0);
		
		System.out.println(originalIntegers);
		System.out.println(sqrtRDDTuple);
		System.out.println(sqrtRDDTuple.count());
		System.out.println(myRDD.count());
	
	}
	 
 	

}
