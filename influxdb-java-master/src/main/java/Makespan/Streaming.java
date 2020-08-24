package Makespan;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.collections.MultiMap;

/*
 * Copyright (c) 2019 Umit Demirbaga
 * Newcastle University
 */

public class Streaming extends Thread {
	
	
	public final static List<String> runningMapsName1 = new ArrayList<String>();

	public final static List<Double> runningMapsProgress1 = new ArrayList<Double>();
	public final static List<Double> progressListNorm = new ArrayList<Double>();

	public final static List<Integer> runningMapsExecutionTime1 = new ArrayList<Integer>();
	public final static List<Double> exacTimeListNorm = new ArrayList<Double>();

	public final static List<Double> runningMapsPerformanceNorm = new ArrayList<Double>();
	public final static List<Double> runningMapsPerformanceNorm1 = new ArrayList<Double>();

	public final static List<String> lowPerformanceRunningMapsName1 = new ArrayList<String>();
	
	public final static double factor = 1.5;
	
	public static double runningMapsPerformanceMedian1;
	
	public static int caseId;
	
	

	@Override
	public void run() {
		ConnectionFactory factory = new ConnectionFactory();

		factory.setUsername("admin");
		factory.setPassword("admin");
		factory.setVirtualHost("/");
		factory.setHost("localhost");
		factory.setPort(5672);

		String QUEUE_NAME = "UMIT_QUEUE2";

		Connection connection = null;
		try {
			connection = factory.newConnection();
		} catch (IOException | TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Channel channel = null;
		try {
			channel = connection.createChannel();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

//		InfluxDB influxDB = InfluxDBFactory.connect("http://192.168.56.6:8086", "umit", "umit");
//		// Flush every 2000 Points, at least every 100ms
//		influxDB.enableBatch(2000, 10, TimeUnit.MILLISECONDS);
//
//		String dbName = "Metrics";

		

		DefaultConsumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");

				System.out.println(" [x] Received >>>>  '" + message + "'");

				String[] values = message.split(";");

				caseId = Integer.valueOf(values[0]);
				String mapId = "";
				double process = 0.0;
				int execTime = 0;
				double result = 0;
				
				runningMapsName1.clear();
				runningMapsProgress1.clear();
				progressListNorm.clear();
				runningMapsExecutionTime1.clear();
				exacTimeListNorm.clear();
				runningMapsPerformanceNorm.clear();
				runningMapsPerformanceNorm1.clear();
				lowPerformanceRunningMapsName1.clear();

				for (int i = 1; i < values.length; i++) {
					mapId = values[i].split(",")[0];
					process = Double.parseDouble(values[i].split(",")[1]);
					execTime = Integer.parseInt(values[i].split(",")[2]);

//					System.out.println("mapId= " + mapId);
					runningMapsName1.add(mapId);
//					System.out.println("process= " + process);
					runningMapsProgress1.add(process);
//					System.out.println("execTime= " + execTime);
					runningMapsExecutionTime1.add(execTime);
				}

				double min = Collections.min(runningMapsExecutionTime1);
				double max = Collections.max(runningMapsExecutionTime1);
				result = 0.0;

				if (min != max) {
					for (int i = 0; i < runningMapsExecutionTime1.size(); i++) {
						result = (runningMapsExecutionTime1.get(i) - min) / (max - min);
						if (result == 0) {
							result = 0.1;
						}
						exacTimeListNorm.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
					}
				}

				min = Collections.min(runningMapsProgress1);
				max = Collections.max(runningMapsProgress1);
				result = 0.0;

				if (min != max) {
					for (int i = 0; i < runningMapsProgress1.size(); i++) {
						result = (runningMapsProgress1.get(i) - min) / (max - min);
						progressListNorm.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
					}
				}

				for (int i = 0; i < progressListNorm.size(); i++) {
					result = progressListNorm.get(i) / exacTimeListNorm.get(i);
					result = Double.parseDouble(new DecimalFormat("##.##").format(result));
					runningMapsPerformanceNorm.add(result);
				}

				for (int i = 0; i < runningMapsPerformanceNorm.size(); i++) {
					runningMapsPerformanceNorm1.add(runningMapsPerformanceNorm.get(i));
				}

				Collections.sort(runningMapsPerformanceNorm);

				int n = runningMapsPerformanceNorm.size();
				runningMapsPerformanceMedian1 = 0.0;

				if (n > 0) {
					if (n % 2 != 0) {
						runningMapsPerformanceMedian1 = runningMapsPerformanceNorm.get(n / 2);
					} else {
						runningMapsPerformanceMedian1 = (runningMapsPerformanceNorm.get((n - 1) / 2)
								+ runningMapsPerformanceNorm.get(n / 2)) / 2;
					}
				}

				if (runningMapsPerformanceNorm1.size() > 0) {
					for (int i = 0; i < runningMapsPerformanceNorm1.size(); i++) {
						if (runningMapsPerformanceNorm1.get(i) * factor < runningMapsPerformanceMedian1) {
							lowPerformanceRunningMapsName1.add(runningMapsName1.get(i));
						}
					}
				}

//				System.out.println("runningMapsName1 = " + runningMapsName1);
//				System.out.println("runningMapsExecutionTime1 = " + runningMapsExecutionTime1);
//				System.out.println("exacTimeListNorm = " + exacTimeListNorm);
//				System.out.println("runningMapsProgress1 = " + runningMapsProgress1);
//				System.out.println("progressListNorm = " + progressListNorm);
//
//				System.out.println("runningMapsPerformance1 = " + runningMapsPerformanceNorm1);
//				System.out.println("lowPerformanceRunningMapsName1 = " + lowPerformanceRunningMapsName1);
//				System.out.println();
//
//				System.out.println("****************************************");

			}
		};
		try {
			channel.basicConsume(QUEUE_NAME, true, consumer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}