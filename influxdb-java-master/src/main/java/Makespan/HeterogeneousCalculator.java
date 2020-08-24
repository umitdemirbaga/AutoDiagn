package Makespan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import org.json.simple.parser.ParseException;

import com.db.influxdb.SmartReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class HeterogeneousCalculator extends Thread {

	public static List<String> getSlowDataNodesName = new ArrayList<String>();

	public void run() {
//	public static void main(String[] args) throws Exception {

		List<String> mapsRunningHighNodesHostName = new ArrayList<String>();

		List<String> stragglersRunningSlowNodesHostName = new ArrayList<String>();

		List<Integer> getSucceededMapsRunningHighNodesExecTime = new ArrayList<Integer>(); 
		
		List<String> stragglersRunningSlowNodes = new ArrayList<String>();

		Double averageMapsRunningHighNodesTime = 0.0;

		Double averageStragglersRunningSlowNodesTime = 0.0;

		Double totalLossTime = 0.0;
		
		int getMapsOnSlowDataNodesNum = 0; 
		
		int stragglersRunningSlowNodesNum = 0;

		Connection connection = null;
		Channel channel = null;

		ConnectionFactory factory = new ConnectionFactory();
		factory = new ConnectionFactory();

		factory.setUsername("admin");
		factory.setPassword("admin");
		factory.setVirtualHost("/");
		factory.setHost(SmartReader.rabbitMQhost);
		factory.setPort(5672);

		String QUEUE_NAME = "UMIT_QUEUE";

		try {
			connection = factory.newConnection();
		} catch (IOException | TimeoutException e1) {
			e1.printStackTrace();
		}
		try {
			channel = connection.createChannel();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		String jobId = DetectorForMakespan.jobId;
//		String jobId = "15875691421830001";

		System.out.println();
		System.out.println("****************************************************************************");

		System.out.println("Debugging results are being analysed now for Heterogeneous Cluster!..");
		System.out.println();

		System.out.println();
		System.out.println("Processing...");
		System.out.println();

		String last = "";

		if (DataLocality.checkClusterHomogeneity) {
			System.out.println("The cluster is homogeneous!..");
		} else {

//			List<String> getAllStragglers = new ArrayList<String>();
//			for (int i = 0; i < DataLocality.getAllStragglers.size(); i++) {
//				getAllStragglers.add(DataLocality.getAllStragglers.get(i));
//			}
//			System.out.println("All getAllStragglers 		= " + getAllStragglers);
//			
//			String getAllStragglers1 = "";
//			for (int i = 0; i < getAllStragglers.size(); i++) {
//				if (i == 0) {
//					getAllStragglers1 = getAllStragglers.get(i).toString();
//				} else {
//					getAllStragglers1 = getAllStragglers1 + "-"
//							+ getAllStragglers.get(i);
//				}
//			}
			/////////////////////

			List<String> getDataNodesName = new ArrayList<String>();
			try {
				getDataNodesName = SmartReader.getDataNodesNames(jobId);
			} catch (IOException | URISyntaxException | ParseException e2) {
				e2.printStackTrace();
			}
			System.out.println("getDataNodesName 		= " + getDataNodesName);

			List<String> getHighDataNodesName = new ArrayList<String>();
			try {
				getHighDataNodesName = SmartReader.getHighDataNodesName(jobId);
			} catch (IOException | URISyntaxException | ParseException e2) {
				e2.printStackTrace();
			}
			System.out.println("getHighDataNodesName 			= " + getHighDataNodesName);

			getDataNodesName.removeAll(getHighDataNodesName);

			for (int i = 0; i < getDataNodesName.size(); i++) {
				if (!getSlowDataNodesName.contains(getDataNodesName.get(i))) {
					getSlowDataNodesName.add(getDataNodesName.get(i));
				}
			}
			System.out.println("getSlowDataNodesName 			= " + getSlowDataNodesName);

			List<String> getMapsOnSlowDataNodes = new ArrayList<String>();

			try {
				getMapsOnSlowDataNodes = SmartReader.getMapsOnSlowDataNodes(jobId);
			} catch (IOException | URISyntaxException | ParseException | InterruptedException e2) {
				e2.printStackTrace();
			}
		
			
			
			System.out.println("getMapsOnSlowDataNodes 			= " + getMapsOnSlowDataNodes);

			String stragglersRunningSlowNodes1 = "";
			

			if (getMapsOnSlowDataNodes != null && !getMapsOnSlowDataNodes.isEmpty()) {
				String getMapsOnSlowDataNodes1 = "";
				for (int i = 0; i < getMapsOnSlowDataNodes.size(); i++) {
					if (i == 0) {
						getMapsOnSlowDataNodes1 = getMapsOnSlowDataNodes.get(i).toString();
					} else {
						getMapsOnSlowDataNodes1 = getMapsOnSlowDataNodes1 + "-" + getMapsOnSlowDataNodes.get(i);
					}
				}

				////////////////////

				for (int i = 0; i < HeterogeneousCluster.stragglersRunningSlowNodes.size(); i++) {
					stragglersRunningSlowNodes.add(HeterogeneousCluster.stragglersRunningSlowNodes.get(i));
				}
				System.out.println("All stragglersRunningSlowNodes 		= " + stragglersRunningSlowNodes);
				System.out.println();

				stragglersRunningSlowNodes1 = "";
				for (int i = 0; i < stragglersRunningSlowNodes.size(); i++) {
					if (i == 0) {
						stragglersRunningSlowNodes1 = stragglersRunningSlowNodes.get(i).toString();
					} else {
						stragglersRunningSlowNodes1 = stragglersRunningSlowNodes1 + "-"
								+ stragglersRunningSlowNodes.get(i);
					}
				}

				getMapsOnSlowDataNodesNum = getMapsOnSlowDataNodes.size();
				getMapsOnSlowDataNodes.retainAll(stragglersRunningSlowNodes);
				double accuracy = 0.0;

				if (getMapsOnSlowDataNodes.size() != 0) {
					accuracy = (100 * getMapsOnSlowDataNodes.size()) / getMapsOnSlowDataNodesNum;
				} else {
					accuracy = 0.0;
				}

				System.out.println("Heterogeneous accuracy = " + accuracy);
				System.out.println();

				last = "metricsType=" + "xheterogeneousAccuracy" + ",getAllMapsOnSlowDataNodes="
						+ getMapsOnSlowDataNodes1 + ",getAllMapsOnSlowDataNodesNum=" + getMapsOnSlowDataNodesNum
						+ ",stragglersRunningSlowNodes=" + stragglersRunningSlowNodes1
						+ ",stragglersRunningSlowNodesNum=" + stragglersRunningSlowNodes.size() + ",accuracy="
						+ accuracy + ",jobId=" + jobId;

				System.out.println(" [x] Sent >>>>  '" + last + "'");
				System.out.println();

				try {
					channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}

			} else {
				last = "metricsType=" + "xheterogeneousAccuracy" + ",getAllMapsOnSlowDataNodes=" + 0
						+ ",getAllMapsOnSlowDataNodesNum=" + 0 + ",stragglersRunningSlowNodes=" + 0
						+ ",stragglersRunningSlowNodesNum=" + 0 + ",accuracy=" + 0 + ",jobId=" + jobId;

				System.out.println(" [x] Sent >>>>  '" + last + "'");
				System.out.println();

				try {
					channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			System.out.println("----------------------------------------");

			try {
				mapsRunningHighNodesHostName = SmartReader.getSucceededMapsRunningHighNodesHostName(jobId);
			} catch (IOException | URISyntaxException | ParseException | InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println("succeededMapsRunningHighNodes 				= "
					+ SmartReader.getSucceededMapsNameRunningHighNodes1);

			String succeededMapsRunningHighNodes1 = "";
			for (int i = 0; i < SmartReader.getSucceededMapsNameRunningHighNodes1.size(); i++) {
				if (i == 0) {
					succeededMapsRunningHighNodes1 = SmartReader.getSucceededMapsNameRunningHighNodes1.get(i)
							.toString();
				} else {
					succeededMapsRunningHighNodes1 = succeededMapsRunningHighNodes1 + "-"
							+ SmartReader.getSucceededMapsNameRunningHighNodes1.get(i);
				}
			}

			System.out.println("succeededMapsRunningHighNodesHostName			= " + mapsRunningHighNodesHostName);

			try {
				getSucceededMapsRunningHighNodesExecTime = SmartReader.getSucceededMapsRunningHighNodesExecTime(jobId);
			} catch (IOException | URISyntaxException | ParseException e) {
				e.printStackTrace();
			}

			System.out.println(
					"succeededMapsRunningHighNodesExecTime 			= " + getSucceededMapsRunningHighNodesExecTime);

			String getSucceededMapsRunningHighNodesExecTime1 = "";
			for (int i = 0; i < getSucceededMapsRunningHighNodesExecTime.size(); i++) {
				if (i == 0) {
					getSucceededMapsRunningHighNodesExecTime1 = getSucceededMapsRunningHighNodesExecTime.get(i)
							.toString();
				} else {
					getSucceededMapsRunningHighNodesExecTime1 = getSucceededMapsRunningHighNodesExecTime1 + "-"
							+ getSucceededMapsRunningHighNodesExecTime.get(i);
				}
			}

			averageMapsRunningHighNodesTime = getSucceededMapsRunningHighNodesExecTime.stream().mapToInt(val -> val)
					.average().orElse(0.0);

			averageMapsRunningHighNodesTime = Double
					.parseDouble(new DecimalFormat("##.##").format(averageMapsRunningHighNodesTime));

			System.out.println(
					"Avg execution time for maps running on high nodes 	= " + averageMapsRunningHighNodesTime);

			System.out.println();

			last = "metricsType=" + "xheterogeneousHighInfo" + ",succeededMapsRunningHighNodes="
					+ succeededMapsRunningHighNodes1 + ",succeededMapsRunningHighNodesExecTime="
					+ getSucceededMapsRunningHighNodesExecTime1 + ",avgTime=" + averageMapsRunningHighNodesTime
					+ ",jobId=" + jobId;

			System.out.println(" [x] Sent >>>>  '" + last + "'");
			System.out.println();

			try {
				channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println();

			///////////////////////////////////////////////////////////

			System.out.println("stragglersRunningSlowNodes 				= " + stragglersRunningSlowNodes);
			
			stragglersRunningSlowNodesNum = stragglersRunningSlowNodes.size();

//			stragglersRunningSlowNodes1

			try {
				stragglersRunningSlowNodesHostName = SmartReader.getStragglersRunningSlowNodesHostName(jobId);
			} catch (IOException | URISyntaxException | ParseException | InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println("stragglersRunningSlowNodesHostName 			= " + stragglersRunningSlowNodesHostName);

			List<Integer> stragglersRunningSlowNodesExecTime = new ArrayList<Integer>();
			try {
				stragglersRunningSlowNodesExecTime = SmartReader.stragglersRunningSlowNodesExecTime(jobId);
			} catch (IOException | URISyntaxException | ParseException e1) {
				e1.printStackTrace();
			}
			System.out.println("stragglersRunningSlowNodesExecTime 			= " + stragglersRunningSlowNodesExecTime);

			String stragglersRunningSlowNodesExecTime1 = "";
			for (int i = 0; i < stragglersRunningSlowNodesExecTime.size(); i++) {
				if (i == 0) {
					stragglersRunningSlowNodesExecTime1 = stragglersRunningSlowNodesExecTime.get(i).toString();
				} else {
					stragglersRunningSlowNodesExecTime1 = stragglersRunningSlowNodesExecTime1 + "-"
							+ stragglersRunningSlowNodesExecTime.get(i);
				}
			}

			averageStragglersRunningSlowNodesTime = stragglersRunningSlowNodesExecTime.stream().mapToInt(val -> val)
					.average().orElse(0.0);

			averageStragglersRunningSlowNodesTime = Double
					.parseDouble(new DecimalFormat("##.##").format(averageStragglersRunningSlowNodesTime));

			System.out.println("Avg execution time for stragglers running on slow nodes = "
					+ averageStragglersRunningSlowNodesTime);

			System.out.println();

			totalLossTime = averageStragglersRunningSlowNodesTime - averageMapsRunningHighNodesTime;
			totalLossTime = Double.parseDouble(new DecimalFormat("##.##").format(totalLossTime));

			System.out.println("totalLossTime = " + totalLossTime);

			System.out.println();

			last = "metricsType=" + "xheterogeneousSlowInfo" + ",stragglersRunningSlowNodes="
					+ stragglersRunningSlowNodes1 + ",stragglersRunningSlowNodesExecTime="
					+ stragglersRunningSlowNodesExecTime1 + ",avgTime=" + averageStragglersRunningSlowNodesTime
					+ ",totalLossTime=" + totalLossTime + ",jobId=" + jobId;

			System.out.println(" [x] Sent >>>>  '" + last + "'");

			try {
				channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println();

		}
		System.out.println();

		System.out.println();
		System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
		System.out.println(
				"Heterogeneous Cluster Stragglers Finder for the job '" + jobId + "' is completed successfully...");
		System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
//		System.out.println();
//
//		List<String> getAllStragglers = new ArrayList<String>();
//		for (int i = 0; i < DataLocality.getAllStragglers.size(); i++) {
//			getAllStragglers.add(DataLocality.getAllStragglers.get(i));
//		}
//		
//		List<String> reasonableStragglers = new ArrayList<String>();
//		
//		for (int i = 0; i < DataLocalityCalculator.nonLocalStragglers.size(); i++) {
//			reasonableStragglers.add(DataLocalityCalculator.nonLocalStragglers.get(i));
//		}
//		
//		for (int i = 0; i < stragglersRunningSlowNodes.size(); i++) {
//			if (!reasonableStragglers.contains(stragglersRunningSlowNodes.get(i))) {
//				reasonableStragglers.add(stragglersRunningSlowNodes.get(i));
//			}
//		}
//		
//		
//		System.out.println("All getAllStragglers 		= " + getAllStragglers);
//		System.out.println("Reasonable stragglers 		= " + reasonableStragglers);
//		System.out.println();
//		
//		int getAllStragglersNum = getAllStragglers.size();
//		int reasonableStragglersNum = reasonableStragglers.size();
//		
//		double stragglersAccuracy = (reasonableStragglersNum * 100) / getAllStragglersNum; 
//		
//		
//		
////		String getAllStragglers1 = "";
////		for (int i = 0; i < getAllStragglers.size(); i++) {
////			if (i == 0) {
////				getAllStragglers1 = getAllStragglers.get(i).toString();
////			} else {
////				getAllStragglers1 = getAllStragglers1 + "-"
////						+ getAllStragglers.get(i);
////			}
////		}
//		System.out.println("Overall accuracy: " + stragglersAccuracy);
//		System.out.println();
//		
//		last = "metricsType=" + "xstragglersAccuracy" 
//				+ ",allStragglersNum=" + getAllStragglersNum 
//				+ ",reasonableStragglersNum=" + reasonableStragglersNum
//				+ ",stragglersAccuracy=" + stragglersAccuracy 
//				+ ",jobId=" + jobId;
//
//		System.out.println(" [x] Sent >>>>  '" + last + "'");
//		System.out.println();
//
//		try {
//			channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
//		System.out.println("Stragglers Finder for the job '" + jobId + "' is completed successfully...");
//		System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
//		System.out.println();
		
		
		System.exit(0);

	}
//	}

}
