package com.exercise.supplydemand;

public class MainSparkApp {
	public static void main(String[] args) {
		while (true) {
			Thread sparkStreamingThread = new Thread(new Runnable() {

				@Override
				public void run() {
					SupplyJob.readFromKafkaTopic();
				}
			});
			sparkStreamingThread.start();

			System.out.println("IN between");
			DemandJob.readFromKafkaTopic();

		}

	}

}
