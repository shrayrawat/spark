package com.kafka.producer;

import java.util.concurrent.ExecutionException;

public class MainApp {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Thread sparkStreamingThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new SupplyKafkaProducer().run();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		sparkStreamingThread.start();
		new DemandKafkaProducer().run();

	}

}
