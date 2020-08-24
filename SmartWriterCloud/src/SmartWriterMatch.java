import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Set;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.collections.MultiMap;

/*
 * Copyright (c) 2019 Umit Demirbaga
 * Newcastle University
 */

public class SmartWriterMatch {
	public static String QUEUE_NAME = "UMIT_QUEUE";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();

		factory.setUsername("admin");
		factory.setPassword("admin");
		factory.setVirtualHost("/");
		factory.setHost("localhost");
		factory.setPort(5672);

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		MultiMap mMap = new MultiValueMap();
		MultiMap rMap = new MultiValueMap();
		Set<String> keys = mMap.keySet();
		Set<String> keys2 = rMap.keySet();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		InfluxDB influxDB = InfluxDBFactory.connect("http://192.168.56.6:8086", "umit", "umit");
		// Flush every 2000 Points, at least every 100ms
		influxDB.enableBatch(2000, 10, TimeUnit.MILLISECONDS);

		String dbName = "deneme";

		DefaultConsumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");

//		        System.out.println(" [x] Received '" + message + "'");     

				boolean durum = true;

				String b = "";
				String[] values = message.split(",");

				for (String exp : values) {
//					a = exp.split("=")[0];
					b = exp.split("=")[1];
					break;
				}

//				if ((b.equals("mapInfo")) || (b.contains("reduceInfo"))) {
//					SmartMatcher sm = new SmartMatcher();
//					sm.match(message);
//					durum = false;
//				}
				if (durum) {
					System.out.println(" [x] Received >>>>  '" + message + "'");
					long timer = System.currentTimeMillis();
					TimeUnit timer2 = TimeUnit.MILLISECONDS;
					for (int i = 0; i < values.length; i++) {
						Point point2 = Point.measurement(b).time(timer, timer2)
								.addField(values[i].split("=")[0], values[i].split("=")[1]).build();
						influxDB.write(dbName, "autogen", point2);
					}
				} // if (durum)

			}
		};
		channel.basicConsume(QUEUE_NAME, true, consumer);
	}
}