package com.volkan.dse;
//
//import static com.datastax.spark.connector.CassandraJavaUtil.*;
//
//
///**
// * Created by vcivelek on 21/09/2016.
// */
//public class Spark {
//
//    HiveContext hiveContext;
//
//    public void connect(String appname) {
//        // create a new configuration
//        SparkConf conf = new SparkConf().setAppName(appname);
//        // create a Spark context
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        DataFrame employees = hiveContext.sql("SELECT * FROM company.employees");
//        employees.registerTempTable("employees");
//    }
//
//}
