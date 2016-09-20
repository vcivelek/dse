package com.volkan.dse;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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
        session.execute("CREATE KEYSPACE IF NOT EXISTS ap WITH replication " +
                "= {'class':'NetworkTopologyStrategy', 'europe-west1-b': '2'} AND durable_writes = true;");

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

    }

    /**
     * Creates the daily flights table ordered by dep time.
     */
    public void createDailyFlightsByOriTable() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS ap.daily_flights_by_ori (" +
                    "origin_airport_id int," +
                    "fl_date timestamp," +
                    "dep_time timestamp," +
                    "PRIMARY KEY ((origin_airport_id, fl_date), dep_time)" +
                    ") WITH CLUSTERING ORDER BY (dep_time desc);"
        );
    }

    /**
     * Inserts data into the table.
     */
    public void loadData(int p1, int p2, int p3, java.sql.Timestamp p4, int p5, String p6, int p7,
                         int p8, String p9, String p10, String p11, String p12, String p13, String p14, java.sql.Timestamp p15,
                         java.sql.Timestamp p16, java.sql.Timestamp p17, java.sql.Timestamp p18, int p19) {
        String cqlInsert = String.format("INSERT INTO simplex.songs (id,year,day_of_month,fl_date,airline_id,carrier,fl_num," +
                        "origin_airport_id,origin,origin_city_name,origin_state_abr,dest," +
                        "dest_city_name,dest_state_abr,dep_time,arr_time,actual_elapsed_time,air_time,distance) " +
                        "VALUES (" +
                        "%d, %d, %d, %s, %d, %s, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d" +
                        ");",
                         p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19);

        session.execute(cqlInsert);
    }

    /**
     * Queries and displays data.
     */
    public void querySchema() {

        ResultSet results = session.execute(
                "SELECT * FROM ap.flights " +
                        "WHERE id = 1;");

        System.out.printf("%-30s\t%-20s\t%-20s%n", "origin", "dest", "air_time");
        System.out.println("-------------------------------+-----------------------+--------------------");

        for (Row row : results) {

            System.out.printf("%-30s\t%-20s\t%-20s%n",
                    row.getString("origin"),
                    row.getString("dest"),
                    row.getString("air_time"));
            // ...

        }

    }

    /**
     * Closes the session and the cluster.
     */
    public void close() {
        session.close();
        cluster.close();
    }
}
