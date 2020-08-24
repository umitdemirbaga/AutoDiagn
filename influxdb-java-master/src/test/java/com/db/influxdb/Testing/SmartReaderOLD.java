package com.db.influxdb.Testing;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class SmartReaderOLD {
	public static void main(String[] args) throws Exception {
//		initiate();
//		writeData();
		readData();
	}

	public static void readData() throws IOException, URISyntaxException, ParseException {
		Configuration configuration = new Configuration("192.168.56.6", "8086", "umit", "umit", "dataLocality");

		Query query = new Query();
		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);

		ResultSet resultSet = dataReader.getResult();

		Query query1 = new Query();

		query1.setCustomQuery("select date from sigar where hostname='slave2' limit 5");

		dataReader.setQuery(query1);
		resultSet = dataReader.getResult();

		Gson gson = new Gson();
		String jsonStr = gson.toJson(resultSet);

//		System.out.println("1- " + jsonStr);  

		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(jsonStr);
		System.out.println("1- " + obj);
		JSONArray obj2 = (JSONArray) obj.get("results");
		System.out.println("2- " + obj2);
		JSONObject obj3 = (JSONObject) obj2.get(0);
		JSONArray obj4 = (JSONArray) obj3.get("series");
		System.out.println("3- " + obj4);
		JSONObject obj5 = (JSONObject) obj4.get(0);
		JSONArray obj6 = (JSONArray) obj5.get("values");
		System.out.println("4- " + obj6);

		System.out.println(obj6.size());
		for (int i = 0; i < obj6.size(); i++) {
			System.out.println(obj6.get(i));

//			String[] values = (obj6.get(i).toString()).split(",");
//			System.out.println(values[1].replaceAll("[.]", ""));

		}

		///////////////////////////////////////////////////////////////////////

		String text = resultSet.toString();

		System.out.println("Result= " + text);
		int total = 0;

		if (text != null && !text.isEmpty()) {
			text = text.substring(4, text.length() - 4);
			System.out.println("text: " + text);

			String text2 = text.replace("[", "");
			System.out.println("xxxxx: " + text2);
			String[] resultArray = text2.split("],");
			System.out.println("The number of results: " + resultArray.length);

			for (int i = 0; i < resultArray.length; i++) {
				System.out.println(resultArray[i]);

//								for (int j = 1; j < text2.length; j++) {
////					System.out.println(">>>>>>>>>>>>>>> "+text2[j] + ",");
////					total += Integer.parseInt(text2[1].replaceAll("\\s+", ""));
//				}
//				System.out.println();
//			}
			}
			System.out.println("Total: " + total);
		}   // if 

//		for (int i = 0; i < resultArray.length; i++) {
//			System.out.println(resultArray[i].replaceAll("[<>\\[\\]-]", ""));
//		}

//		String text1 = resultSet.toString();
//		String json = new Gson().toJson(resultSet);
//		System.out.println(json);

	}

}
