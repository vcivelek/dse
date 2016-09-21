package com.volkan.dse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by vcivelek on 19/09/2016.
 */
public class Csv {

    private final static String DIRECTORY_WITH_CSVS = "/Users/vcivelek/personal/projects/dse/src/main/resources/";

    void populateFlights(Dse dse){
        try {
            Class.forName("org.relique.jdbc.csv.CsvDriver");
            Properties props = new Properties();
            // Column names and column data types.
            props.put("suppressHeaders", "true");
            props.put("headerline",
                    "ID,YEAR,DAY_OF_MONTH,FL_DATE,AIRLINE_ID,CARRIER,FL_NUM," +
                    "ORIGIN_AIRPORT_ID,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,DEST," +
                    "DEST_CITY_NAME,DEST_STATE_ABR,DEP_TIME,ARR_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,DISTANCE");
            props.put("columnTypes", "Int,Int,Int,Date,Int,String,Int,Int,String,String,String,String," +
                    "String,String,Time,Time,Time,Time,Int");
            props.put("dateFormat", "yyyy/MM/dd");
            props.put("timeFormat", "HHmm");
            Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + DIRECTORY_WITH_CSVS, props);
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery("SELECT ID,YEAR,DAY_OF_MONTH,FL_DATE,AIRLINE_ID,CARRIER,FL_NUM," +
                    "ORIGIN_AIRPORT_ID,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,DEST," +
                    "DEST_CITY_NAME,DEST_STATE_ABR,DEP_TIME,ARR_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,DISTANCE" +
                    " FROM flights_from_pg");
            while (results.next()) {
                // Fetch column values with methods that match the column data types.
                // Load the flight table
                dse.loadData(results.getInt(1), results.getInt(2), results.getInt(3), results.getDate(4),
                        results.getInt(5), results.getString(6), results.getInt(7), results.getInt(8),
                        results.getString(9), results.getString(10), results.getString(11),
                        results.getString(12), results.getString(13), results.getString(14),
                        results.getTime(15), results.getTime(16), results.getTime(17), results.getTime(18),
                        results.getInt(19));
                // Load the DailyFlightsByAirtime table
                dse.loadDailyFlightsByAirtime(results.getString(9), results.getString(12), results.getDate(4), results.getTime(18),results.getTime(15));
                // Load the DailyFlightsByOri table
                dse.loadDailyFlightsByOri(results.getString(9), results.getDate(4),results.getTime(15));
            }
            conn.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
