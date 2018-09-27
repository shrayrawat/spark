package ReservationSystem.BusBookingApp;

import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().config("spark.master", "local").getOrCreate();
		spark.read().;

	}
}
