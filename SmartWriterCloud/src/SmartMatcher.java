import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.map.MultiValueMap;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import com.rabbitmq.client.impl.nio.NioLoopContext;

import org.apache.commons.collections.MultiMap;

/*
 * Copyright (c) 2019 Umit Demirbaga
 * Newcastle University
 */

public class SmartMatcher {

	static MultiValueMap mMap = new MultiValueMap();
	static MultiMap rMap = new MultiValueMap();
	
	 // get all the set of keys
	static Set<String> reduceKeys = rMap.keySet();
	
	
	public void match(String message) {
		
		Set<String> mapKeys = mMap.keySet();

		MultiMap resultMap = new MultiValueMap();
		Set<String> resultKeys = resultMap.keySet();
		
		
		String b = "";
		String jobId = "";

		String[] values = message.split(",");

//		for (String exp : values) {
//			b = exp.split("=")[1];
//			break;
//		}
		
		
		b = values[0].split("=")[1];
		
		jobId = values[4].split("=")[1];
		

		if (b.equals("mapInfo")) {
			mMap.put(values[2].split("=")[1], values[1].split("=")[1]);
		}

		else {
			rMap.put(values[2].split("=")[1], values[1].split("=")[1]);
		}

		String dbName = "Metrics";
		

		InfluxDB influxDB = InfluxDBFactory.connect("http://192.168.56.6:8086", "umit", "umit");
		String deger = null;
//		boolean durum = false; 
		
		String a1 = null;
		String a2 = null;
		
//		System.out.println("******");
//
//		System.out.println("    Maps part");
//
//		for (String key : mapKeys) {
//			System.out.println("Key = " + key + "       Values = " + mMap.get(key));
//			System.out.println();
//		}
//
//		System.out.println("    Reduces part");
//		for (String key : reduceKeys) {
//			System.out.println("Key = " + key + "       Values = " + rMap.get(key));
//			System.out.println();
//		}

//		System.out.println();
		
		for (String key : mapKeys) {
			for (String key1 : reduceKeys) {
				if (key.contains(key1)) {
					Object mVal = mMap.get(key);
					Object rVal = rMap.get(key);
					if (mVal != null && rVal !=null) { 
						
						String x = mVal.toString();
						String y = rVal.toString();
						x = x.substring(1, x.length() - 1); // delete []
						y = y.substring(1, y.length() - 1); // delete []
	
						String[] xArray = x.split("\\s*,\\s*");
	
						for (String name : xArray) {
							System.out.println(name + "    is going to    " + y + "    Size: " + key.getBytes().length);
							
							
							deger = "metricsType=" + "matchInfo" + ",mapId=" + name + ",reduceId=" + y + ",size=" + key.getBytes().length + ",jobId=" + jobId;
//							System.out.println(deger);
//							System.out.println(">>> key = " + key);
//							System.out.println(">>> value = " + mMap.get(key));
							
							String a = mMap.get(key).toString();
							a1 = a.substring(1, a.length() - 1);   // value
							a2 = key.toString();     // key

							
							
							resultMap.put(name, y);
							
							String message1 = deger;
							String b1 = "";
							String[] values1 = message1.split(",");
							
							for (String exp : values1) {
//								a = exp.split("=")[0];
								b1 = exp.split("=")[1];					
								break;
							}
							
							
							
					        
					        // Flush every 2000 Points, at least every 100ms
//					        influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
							
							long timer = System.currentTimeMillis();
							TimeUnit timer2 = TimeUnit.MILLISECONDS;
							for (int i = 0; i < values1.length; i++) {					
								Point point2 = Point.measurement(b1)
										.time(timer, timer2)
					                    .addField(values1[i].split("=")[0], values1[i].split("=")[1])
					                    .build();
					            influxDB.write(dbName, "autogen", point2);
							}	
							
							
						}
					}
				}	
			}
		}
		Object a11 = a1;
		Object a22 = a2;
		if (a1 != null && a2 != null) {
//			System.out.println(">>>> removing  "+ mMap.remove(a22, a11));
//			System.out.println(">>>> removing  "+ mMap.remove(a22));
//			System.out.println(">>>> removing  "+ rMap.remove(a22));
			mMap.remove(a22);
			rMap.remove(a22);
		}
		
//		if (!resultMap.isEmpty()) {
//			System.out.println("***********************************************************");
//		}
	}
	

	
	
}
