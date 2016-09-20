package com.volkan.dse;

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
//            dse.createSchema();
            csv.populateFlights(dse);
            dse.querySchema();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            dse.close();
        }
    }
}
