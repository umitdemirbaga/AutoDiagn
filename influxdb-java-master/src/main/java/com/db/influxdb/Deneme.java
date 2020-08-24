package com.db.influxdb;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;

public class Deneme {
	
	static Configuration configuration = new Configuration("192.168.56.6", "8086", "umit", "umit", "deneme");
	static Query query = new Query();

	public static void main(String[] args) throws IOException, URISyntaxException, ParseException {
		

		int runningSpeculativeMapsNum = 0;
		
		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();
		query1.setCustomQuery(
				"select count(mapId) from map where jobId='1578887888577_0009' and mapId=~/_/ order by desc limit " + 10);
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
		String[] values = (obj6.get(0).toString()).split(",");
		System.out.println(values[1]);
	
		String x = values[1].replace("]", "").replace("\"", "");
		System.out.println(x);
		double a = Double.valueOf(x);
		int y = (int) a;
		System.out.println(y);

	}

}
