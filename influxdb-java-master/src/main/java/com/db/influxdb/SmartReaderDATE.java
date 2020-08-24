package com.db.influxdb;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.db.influxdb.Configuration;
import com.db.influxdb.DataReader;
import com.db.influxdb.DataWriter;
import com.db.influxdb.Query;
import com.db.influxdb.ResultSet;
import com.db.influxdb.Utilities;
import com.google.gson.Gson;

// for DATE
public class SmartReaderDATE {

	public static Configuration configuration = new Configuration("192.168.56.6", "8086", "umit", "umit", "Metrics");
	public static Query query = new Query();
	public static String jobId = "1583079278697_0002";
	public static String blockId = "0";
	public static String hostName = "slave1";
	public static String mapId = "Map1";

	public static void main(String[] args) throws Exception, ParseException {
		getDate("1583079278697_0002");
	}

	public static Long getDate(String jobId)
			throws IOException, URISyntaxException, ParseException, java.text.ParseException {
		Long millis = null;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select jobsRunning from cluster order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			
			long now = Instant.now().toEpochMilli();
			System.out.println("y= " + now);

			String[] values = (obj6.get(0).toString()).split(",");
			String date = values[0].replace("[", "").replace("\"", "").replace("\\", "");
						
			System.out.println("date= " + date);
			millis = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(date).getTime();
			System.out.println("x= " + millis);
			
			
//			Thread.sleep(1000);
			
			long now1 = Instant.now().toEpochMilli();
			System.out.println("y= " + now1);

			System.out.println("fark = "  + (now1 - now));


		} catch (Exception e) {
			System.out.println(e);
			millis = (long) 0;
		}
		return millis;
	}

}
