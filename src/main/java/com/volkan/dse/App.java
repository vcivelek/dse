package com.volkan.dse;

import java.util.ArrayList;

/**
 * Created by vcivelek on 19/09/2016.
 */
public class App {
    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    public static void main(String[] args) {

        Dse dse = new Dse();
        Csv csv = new Csv();

        try {
            dse.connect(CONTACT_POINTS, PORT);
//            // Create the keyspace and tables.
//            dse.createSchema();
//            dse.createDailyFlightsByOri();
//            dse.createDailyFlightsByAirtime();
//            // Populate them with the flights_from_pg.csv
//            csv.populateFlights(dse);
//            // Fetch some data.
            dse.querySchema();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            dse.close();
        }
    }
}
