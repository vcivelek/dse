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
            java.util.Date date = new java.util.Date();
            java.sql.Time t = new java.sql.Time(date.getTime());
            ArrayList<Integer> foo = new ArrayList<Integer>();
            foo = dse.findSlots(t, t);
            for (Integer x : foo) {
                System.out.println(x);
            }
//            dse.loadDailyFlightsByAirtime();
//            dse.connect(CONTACT_POINTS, PORT);
//            dse.createSchema();
//            csv.populateFlights(dse);
//            dse.querySchema();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
//            dse.close();
        }
    }
}
