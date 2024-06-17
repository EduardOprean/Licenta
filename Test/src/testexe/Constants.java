package testexe;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Constants {

    public static int NO_OF_DATA_CENTERS;
    public static int NO_OF_VMS;
    public static int NO_OF_TASKS;

    static {
        Properties properties = new Properties();
        try (InputStream input = Constants.class.getResourceAsStream("/config.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties. Using default values.");
                // Set default values
                NO_OF_DATA_CENTERS = 1;
                NO_OF_VMS = 5;
                NO_OF_TASKS = 10;
            } else {
                properties.load(input);
                NO_OF_DATA_CENTERS = Integer.parseInt(properties.getProperty("no_of_data_centers", "1"));
                NO_OF_VMS = Integer.parseInt(properties.getProperty("no_of_vms", "5"));
                NO_OF_TASKS = Integer.parseInt(properties.getProperty("no_of_tasks", "10"));
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Set default values in case of exception
            NO_OF_DATA_CENTERS = 1;
            NO_OF_VMS = 5;
            NO_OF_TASKS = 10;
        }
    }
}
