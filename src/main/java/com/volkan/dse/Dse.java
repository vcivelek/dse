package com.volkan.dse;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;

/**
 * Created by vcivelek on 19/09/2016.
 */
public class Dse {

    private Cluster cluster;

    private Session session;


    public void connect(String[] contactPoints, int port) {

        cluster = Cluster.builder()
                .addContactPoints(contactPoints).withPort(port)
                .build();

        System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());
        session = cluster.connect();
    }

    /**
     * Creates the schema (keyspace) and initial flight table
     */
    public void createSchema() {
//        session.execute("CREATE KEYSPACE IF NOT EXISTS ap WITH replication " +
//                "= {'class':'NetworkTopologyStrategy', 'europe-west1-b': '2'} AND durable_writes = true;");
        session.execute("CREATE KEYSPACE IF NOT EXISTS ap WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor': 1 };");
        session.execute(
                "CREATE TABLE IF NOT EXISTS ap.flights (" +
                        "id int PRIMARY KEY," +
                        "actual_elapsed_time timestamp," +
                        "air_time timestamp," +
                        "airline_id int," +
                        "arr_time timestamp," +
                        "carrier text," +
                        "day_of_month int," +
                        "dep_time timestamp," +
                        "dest text," +
                        "dest_city_name text," +
                        "dest_state_abr text," +
                        "distance int," +
                        "fl_date timestamp," +
                        "fl_num int," +
                        "origin text," +
                        "origin_airport_id int," +
                        "origin_city_name text," +
                        "origin_state_abr text," +
                        "year int" +
                        ");");

        session.execute("CREATE INDEX IF NOT EXISTS ON ap.flights (origin);");
    }

    /**
     * Inserts data into the table.
     */
    public void loadData(int p1, int p2, int p3, java.sql.Date p4, int p5, String p6, int p7,
                         int p8, String p9, String p10, String p11, String p12, String p13, String p14, java.sql.Time p15,
                         java.sql.Time p16, java.sql.Time p17, java.sql.Time p18, int p19) {
        String cqlInsert = String.format("INSERT INTO ap.flights (id,year,day_of_month,fl_date,airline_id,carrier,fl_num," +
                        "origin_airport_id,origin,origin_city_name,origin_state_abr,dest," +
                        "dest_city_name,dest_state_abr,dep_time,arr_time,actual_elapsed_time,air_time,distance) " +
                        "VALUES (" +
                        "%d, %d, %d, '%s', %d, '%s', %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s %sZ', '%s %sZ', '%s %sZ', '%s %sZ', %d" +
                        ");",
                p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p4, p15, p4, p16, p4, p17, p4, p18, p19);

        session.execute(cqlInsert);
    }

    /**
     * Creates the daily flights table ordered by dep time.
     */
    public void createDailyFlightsByOri() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS ap.daily_flights_by_ori (" +
                    "origin text," +
                    "fl_date timestamp," +
                    "dep_time timestamp," +
                    "PRIMARY KEY ((origin, fl_date), dep_time)" +
                    ") WITH CLUSTERING ORDER BY (dep_time desc);"
        );
    }

    /**
     * Loads the daily flights table by Ori.
     */
    public void loadDailyFlightsByOri(String ori, java.sql.Date date, java.sql.Time dep_time) {
        String cqlInsert = String.format("INSERT INTO ap.daily_flights_by_ori (origin,fl_date,dep_time) " +
                        "VALUES ('%s', '%s', '%s %sZ');", ori, date, date, dep_time);
        session.execute(cqlInsert);
    }

    /**
     * Creates the daily flights table ordered by dep time with having 10m slots in the partition.
     * There are 6 x 24 10m slot a day. [0..143]
     */
    public void createDailyFlightsByAirtime() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS ap.daily_flights_by_airtime (" +
                        "origin text," +
                        "dest text," +
                        "slot int," +
                        "fl_date timestamp," +
                        "dep_time timestamp," +
                        "PRIMARY KEY ((fl_date, slot), dep_time));"
        );
    }

    /**
     * Loads the daily flights table by Airtime.
     */
    public void loadDailyFlightsByAirtime(String ori, String dest, java.sql.Date fl_date, java.sql.Time air_time, java.sql.Time dep_time) {
        // calculate the slots and make an insert for each slot.
        ArrayList<Integer> slots = new ArrayList<Integer>();
        slots = findSlots(dep_time, air_time);
        for(Integer slot : slots){
            String cqlInsert = String.format("INSERT INTO ap.daily_flights_by_airtime (origin, dest, slot, fl_date,dep_time) " +
                    "VALUES ('%s', '%s', %d, '%s', '%s %sZ');", ori, dest, slot, fl_date, fl_date, dep_time);
            session.execute(cqlInsert);
        }
    }

    public ArrayList findSlots(java.sql.Time dep_time, java.sql.Time air_time){
        //This is mapping the hour/minute to 10m slots ranging from [0..143]
        int fromHours = dep_time.getHours();
        int fromMinutes = dep_time.getMinutes();
        int toHours = fromHours + air_time.getHours();
        int toMinutes = fromMinutes + air_time.getMinutes();

        if (toMinutes >= 60) {
            toHours++;
            toMinutes = toMinutes % 60;
        }

        // Now do the mapping.
        int slotStart = fromHours *  6 + fromMinutes;
        int slotEnd = toHours *  6 + toMinutes;
        ArrayList<Integer> slots = new ArrayList<Integer>();
        for(int i = slotStart; i < slotEnd; i++){
            //TODO However this does not handle the overnight flights. It needs to be done.
            if(i == 144){
                break;
            }
            slots.add(i);
        }
        return slots;
    }

    /**
     * Queries and displays data.
     */
    public void querySchema(String origin, String date) {

        ResultSet results = session.execute(
                "SELECT * FROM ap.daily_flights_by_ori " +
                        "WHERE origin = '"+origin+"' AND fl_date = '"+date+"';");

        System.out.printf("%-30s\t%-20s\t%-20s%n", "origin", "fl_date", "dep_time");
        System.out.println("-------------------------------+-----------------------+--------------------");
        int i = 0;
        for (Row row : results) {
            i++;
            System.out.printf("%-30s\t%-20s\t%-20s%n",
                    row.getString("origin"),
                    row.getTimestamp("fl_date"),
                    row.getTimestamp("dep_time"));

        }
        System.out.println("Total Count for "+origin+" on "+date+" : " + i);
    }

    /**
     * Closes the session and the cluster.
     */
    public void close() {
        session.close();
        cluster.close();
    }
}
