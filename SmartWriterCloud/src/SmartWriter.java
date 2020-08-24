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

public class SmartWriter {

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();

		factory.setUsername("admin");
		factory.setPassword("admin");
		factory.setVirtualHost("/");
		factory.setHost("localhost");
		factory.setPort(5672);

		String QUEUE_NAME = "UMIT_QUEUE";

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		InfluxDB influxDB = InfluxDBFactory.connect("http://172.31.24.165:8086", "umit", "umit");
		// Flush every 2000 Points, at least every 100ms
		influxDB.enableBatch(2000, 10, TimeUnit.MILLISECONDS);

		String dbName = "Metrics";

		DefaultConsumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");

				String b = "";
				String[] values = message.split(",");

				for (String exp : values) {
					b = exp.split("=")[1];
					break;
				}

				System.out.println(" [x] Received >>>>  '" + message + "'");

				long timer = System.currentTimeMillis();
				TimeUnit timer2 = TimeUnit.MILLISECONDS;
				try {
					for (int i = 1; i < values.length; i++) {
						Point point2 = Point.measurement(b).time(timer, timer2)
								.addField(values[i].split("=")[0], values[i].split("=")[1]).build();
						influxDB.write(dbName, "autogen", point2);
					}
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		};
		channel.basicConsume(QUEUE_NAME, true, consumer);
	}
}