package Makespan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.db.influxdb.Configuration;
import com.db.influxdb.DataReader;
import com.db.influxdb.Query;
import com.db.influxdb.ResultSet;
import com.db.influxdb.SmartReader;
import com.google.gson.Gson;

public class Deneme {

	public static void main(String[] args)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		String rabbitMQhost = "192.168.56.6"; // laptop
		Configuration configuration = new Configuration(rabbitMQhost, "8086", "umit", "umit", "Metrics");
		Query query = new Query();

		String jobId = "1610082682631_0002";
		
		List<String> lowDownloadNodesNames = new ArrayList<String>();
		try {
			if (!(SmartReader.getLowDownloadNodesName(jobId) == null)) {
				lowDownloadNodesNames = SmartReader.lowDownloadNodes; 
			}
		} catch (IOException | URISyntaxException | ParseException e1) {
			e1.printStackTrace();
		}
		System.out.println("lowDownloadNodesNames = " + lowDownloadNodesNames);
		
		System.out.println("---------------");
		
		System.out.println(SmartReader.getDataNodesNames(jobId));

		List<String> getLowDownloadNodesName = new ArrayList<String>();
		List<String> dataNodesNames = new ArrayList<String>();
		dataNodesNames = SmartReader.getDataNodesNames(jobId);
		HashMap<String, Double> info = new HashMap<String, Double>();
//		dataNodesNames.add("slave1");
//		dataNodesNames.add("slave2");
		String nodeName = "";
		double download = 0;
		double total = 0;
		double average = 0;

		for (int k = 0; k < dataNodesNames.size(); k++) {

			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select hostName, downloadMbps from resource where hostName='"
						+ dataNodesNames.get(k) + "' order by desc limit 1");
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

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					nodeName = (values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					download = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));
					total += download;
					info.put(nodeName, download);
				}
			} catch (Exception e) {
				System.out.println("no getLowDownloadNodesName!");
				getLowDownloadNodesName = null;
			}
		}

		average = total / dataNodesNames.size();
		System.out.println("all info = " + info);
		System.out.println("average = " + average); 
		System.out.println();
		
		for (Entry<String, Double> entry : info.entrySet()) {
//		    String key = entry.getKey();
//		    double value = entry.getValue();
		    if (entry.getValue() < average) {
		    	getLowDownloadNodesName.add(entry.getKey());
			}
		}
		
		System.out.println("getLowDownloadNodesName = " + getLowDownloadNodesName);


	}

}
