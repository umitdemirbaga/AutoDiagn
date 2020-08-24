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

public class DataLocalityCalculator extends Thread {

	public static HeterogeneousCalculator threadHeterogeneousCalculator = new HeterogeneousCalculator();

	public static List<String> finishedAllLocalMapsName = new ArrayList<String>();

	public static List<String> finishedAllNonLocalMapsNameYEDEKxxx = new ArrayList<String>();
	
	public static List<String> nonLocalStragglers = new ArrayList<String>();

	public static int finishedAllNonLocalMapsNameNum = 0;
	
	public static int nonLocalStragglersNum = 0;

	public void run() {

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

//		Scanner read = new Scanner(System.in);
//		System.out.print("Enter the jobId: ");
//		String jobId = read.next();

		String jobId = DetectorForMakespan.jobId;

//		String jobId = "15872919672270001"; // DELETE THIS

		System.out.println();
		System.out.println("****************************************************************************");

		System.out.println("Debugging results are being analysed now for Data Locality!..");
		System.out.println();

		System.out.println();
		System.out.println("Processing...");
		System.out.println();

		String last = "";
		Double averageLocalMapsTime = 0.0;
		Double averageNonLocalMapsTime = 0.0;
		Double totalLossTime = 0.0;
		double accuracy = 0.0;
		

		List<String> finishedAllNonLocalMapsName = new ArrayList<String>();

		List<String> finishedAllNonLocalMapsNameYEDEK = new ArrayList<String>();

		// works
		for (int i = 0; i < DataLocality.getAllNonLocalMaps.size(); i++) {
			finishedAllNonLocalMapsName.add(DataLocality.getAllNonLocalMaps.get(i));
		}

		if (finishedAllNonLocalMapsName.size() > 0) {

			finishedAllNonLocalMapsNameYEDEK = finishedAllNonLocalMapsName;

			finishedAllNonLocalMapsNameNum = finishedAllNonLocalMapsName.size();

			System.out.println("finishedAllNonLocalMapsName 	= " + finishedAllNonLocalMapsName);

			String finishedAllNonLocalMapsName1 = "";

			for (int i = 0; i < finishedAllNonLocalMapsName.size(); i++) {
				if (i == 0) {
					finishedAllNonLocalMapsName1 = finishedAllNonLocalMapsName.get(i).toString();
				} else {
					finishedAllNonLocalMapsName1 = finishedAllNonLocalMapsName1 + "-"
							+ finishedAllNonLocalMapsName.get(i);
				}
			}

//			List<String> nonLocalStragglersYEDEK = new ArrayList<String>();

			for (int i = 0; i < DataLocality.nonLocalRunningStragglers.size(); i++) {
				nonLocalStragglers.add(DataLocality.nonLocalRunningStragglers.get(i));
			}

			System.out.println("nonLocalStragglers 		= " + nonLocalStragglers);
			
			nonLocalStragglersNum = nonLocalStragglers.size();

//			nonLocalStragglersYEDEK = nonLocalStragglers;

			String nonLocalStragglers1 = "";

			for (int i = 0; i < nonLocalStragglers.size(); i++) {
				if (i == 0) {
					nonLocalStragglers1 = nonLocalStragglers.get(i).toString();
				} else {
					nonLocalStragglers1 = nonLocalStragglers1 + "-" + nonLocalStragglers.get(i);
				}
			}

			System.out.println();

			finishedAllNonLocalMapsName.retainAll(nonLocalStragglers);

			if (finishedAllNonLocalMapsNameNum != 0) {
				accuracy = (100 * finishedAllNonLocalMapsName.size()) / finishedAllNonLocalMapsNameNum;
			} else {
				accuracy = 0.0;
			}

			if (nonLocalStragglers.size() == 0) {
				nonLocalStragglers1 = "0";
			}

			System.out.println("DataLocality accuracy = " + accuracy);
			System.out.println();

			last = "metricsType=" + "xdataLocalityAccuracy" + ",nonLocalMaps=" + finishedAllNonLocalMapsName1
					+ ",nonLocalMapsNum=" + finishedAllNonLocalMapsNameNum + ",nonLocalStragglers="
					+ nonLocalStragglers1 + ",nonLocalStragglersNum=" + nonLocalStragglers.size() + ",accuracy="
					+ accuracy + ",jobId=" + jobId;

			System.out.println(" [x] Sent >>>>  '" + last + "'");
			System.out.println();

			try {
				channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}

		} else {
			System.out.println("There is no non-local maps for this job!");

			last = "metricsType=" + "xdataLocalityAccuracy" + ",nonLocalMaps=" + 0 + ",nonLocalMapsNum=" + 0
					+ ",nonLocalStragglers=" + 0 + ",nonLocalStragglersNum=" + 0 + ",accuracy=" + 0.0 + ",jobId="
					+ jobId;

			System.out.println(" [x] Sent >>>>  '" + last + "'");
			System.out.println();

			try {
				channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// /////////////////////////////////////////////////////////////////////////////////////////////////////

		System.out.println("------------------------------------------");
		System.out.println();

		List<String> finishedAllNormalMapsName = new ArrayList<String>();

		try {
			finishedAllNormalMapsName = SmartReader.getFinishedAllNormalMapsName(jobId);
		} catch (IOException | URISyntaxException | ParseException e) {
			e.printStackTrace();
		}

		// no need to send this to DB
		System.out.println("finishedAllNormalMapsName 		= " + finishedAllNormalMapsName);
		System.out.println("-------------------------");

		System.out.println();

		List<String> finishedAllNonLocalNormalMapsName = new ArrayList<String>();

		try {
			finishedAllNonLocalNormalMapsName = SmartReader.getFinishedAllNonLocalNormalMapsName(jobId);
		} catch (IOException | URISyntaxException | ParseException e) {
			e.printStackTrace();
		}

		System.out.println("finishedAllNonLocalNormalMapsName 	= " + finishedAllNonLocalNormalMapsName);

		String finishedAllNonLocalNormalMapsName1 = "";
		for (int i = 0; i < finishedAllNonLocalNormalMapsName.size(); i++) {
			if (i == 0) {
				finishedAllNonLocalNormalMapsName1 = finishedAllNonLocalNormalMapsName.get(i).toString();
			} else {
				finishedAllNonLocalNormalMapsName1 = finishedAllNonLocalNormalMapsName1 + "-"
						+ finishedAllNonLocalNormalMapsName.get(i);
			}
		}

		List<Integer> finishedAllNonLocalMapsExecTime = new ArrayList<Integer>();

		try {
			finishedAllNonLocalMapsExecTime = SmartReader.getFinishedAllNonLocalMapsExecTime(jobId);
		} catch (IOException | URISyntaxException | ParseException e) {
			e.printStackTrace();
		}

		System.out.println("finishedAllNonLocalMapsExecTime 	= " + finishedAllNonLocalMapsExecTime);

		String finishedAllNonLocalMapsExecTime1 = "";
		for (int i = 0; i < finishedAllNonLocalMapsExecTime.size(); i++) {
			if (i == 0) {
				finishedAllNonLocalMapsExecTime1 = finishedAllNonLocalMapsExecTime.get(i).toString();
			} else {
				finishedAllNonLocalMapsExecTime1 = finishedAllNonLocalMapsExecTime1 + "-"
						+ finishedAllNonLocalMapsExecTime.get(i);
			}
		}

		averageNonLocalMapsTime = finishedAllNonLocalMapsExecTime.stream().mapToInt(val -> val).average().orElse(0.0);

		averageNonLocalMapsTime = Double.parseDouble(new DecimalFormat("##.##").format(averageNonLocalMapsTime));

		System.out.println("Average exec time for non-local maps	= " + averageNonLocalMapsTime);
		System.out.println();

		if (finishedAllNonLocalNormalMapsName1 == "") {
			finishedAllNonLocalNormalMapsName1 = "0";
			finishedAllNonLocalMapsExecTime1 = "0";
		}

		last = "metricsType=" + "xdataLocalityNonLocalInfo" + ",nonLocalMapsName=" + finishedAllNonLocalNormalMapsName1
				+ ",nonLocalMapsExecTime=" + finishedAllNonLocalMapsExecTime1 + ",averageNonLocal="
				+ averageNonLocalMapsTime + ",jobId=" + jobId;

		System.out.println(" [x] Sent >>>>  '" + last + "'");
		System.out.println();
		System.out.println();

		try {
			channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		// --------------------------------------------------------

		finishedAllNormalMapsName.removeAll(finishedAllNonLocalNormalMapsName);

		finishedAllLocalMapsName = finishedAllNormalMapsName; // ismi karistirmamak icin yeni olusturdum.
		System.out.println("finishedAllLocalMapsName 		= " + finishedAllLocalMapsName);

		String finishedAllLocalMapsName1 = "";
		for (int i = 0; i < finishedAllLocalMapsName.size(); i++) {
			if (i == 0) {
				finishedAllLocalMapsName1 = finishedAllLocalMapsName.get(i).toString();
			} else {
				finishedAllLocalMapsName1 = finishedAllLocalMapsName1 + "-" + finishedAllLocalMapsName.get(i);
			}
		}

		List<Integer> finishedAllLocalMapsExecTime = new ArrayList<Integer>();

		try {
			finishedAllLocalMapsExecTime = SmartReader.getFinishedAllLocalMapsExecTime(jobId);
		} catch (IOException | URISyntaxException | ParseException e) {
			e.printStackTrace();
		}

		System.out.println("finishedAllLocalMapsExecTime 		= " + finishedAllLocalMapsExecTime);

		String finishedAllLocalMapsExecTime1 = "";
		for (int i = 0; i < finishedAllLocalMapsExecTime.size(); i++) {
			if (i == 0) {
				finishedAllLocalMapsExecTime1 = finishedAllLocalMapsExecTime.get(i).toString();
			} else {
				finishedAllLocalMapsExecTime1 = finishedAllLocalMapsExecTime1 + "-"
						+ finishedAllLocalMapsExecTime.get(i);
			}
		}

		averageLocalMapsTime = finishedAllLocalMapsExecTime.stream().mapToInt(val -> val).average().orElse(0.0);

		averageLocalMapsTime = Double.parseDouble(new DecimalFormat("##.##").format(averageLocalMapsTime));

		System.out.println("Average exec time for local maps	= " + averageLocalMapsTime);
		System.out.println();

		if (!(averageNonLocalMapsTime == 0)) {
			totalLossTime = averageNonLocalMapsTime - averageLocalMapsTime;
			totalLossTime = Double.parseDouble(new DecimalFormat("##.##").format(totalLossTime));
		} else {
			totalLossTime = 0.0;
		}

		System.out.println("totalLossTime= " + totalLossTime + "  seconds.");
		System.out.println();

		last = "metricsType=" + "xdataLocalityLocalInfo" + ",localMapsName=" + finishedAllLocalMapsName1
				+ ",localMapsExecTime=" + finishedAllLocalMapsExecTime1 + ",averageLocal=" + averageLocalMapsTime
				+ ",totalLossTime=" + totalLossTime + ",jobId=" + jobId;

		System.out.println(" [x] Sent >>>>  '" + last + "'");
		System.out.println();

		try {
			channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
		System.out.println("Data Locality Stragglers Finder for the job '" + jobId + "' is completed successfully...");
		System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
		System.out.println();

		if (!DataLocality.checkClusterHomogeneity) {
			threadHeterogeneousCalculator.start();
		} else {
//			List<String> getAllStragglers = new ArrayList<String>();
//			for (int i = 0; i < DataLocality.getAllStragglers.size(); i++) {
//				getAllStragglers.add(DataLocality.getAllStragglers.get(i));
//			}
//			System.out.println("All getAllStragglers 		= " + getAllStragglers);
//			System.out.println("Reasonable stragglers 		= " + nonLocalStragglers);
//
//			int getAllStragglersNum = getAllStragglers.size();
//			double stragglersAccuracy = 0.0;
//
//			if (getAllStragglersNum == 0) {
//				stragglersAccuracy = 0.0;
//			} else {
//				stragglersAccuracy = (nonLocalStragglersNum * 100) / getAllStragglersNum;
//			}
//
////			String getAllStragglers1 = "";
////			for (int i = 0; i < getAllStragglers.size(); i++) {
////				if (i == 0) {
////					getAllStragglers1 = getAllStragglers.get(i).toString();
////				} else {
////					getAllStragglers1 = getAllStragglers1 + "-"
////							+ getAllStragglers.get(i);
////				}
////			}
//			System.out.println("Overall accuracy: " + stragglersAccuracy);
//			System.out.println();
//
//			last = "metricsType=" + "xstragglersAccuracy" + ",allStragglersNum=" + getAllStragglersNum
//					+ ",reasonableStragglersNum=" + finishedAllNonLocalMapsNameNum + ",stragglersAccuracy="
//					+ stragglersAccuracy + ",jobId=" + jobId;
//
//			System.out.println(" [x] Sent >>>>  '" + last + "'");
//			System.out.println();
//
//			try {
//				channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//
//			System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
//			System.out.println("Stragglers Finder for the job '" + jobId + "' is completed successfully...");
//			System.out.println("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");
//			System.out.println();

			System.exit(0);
		}

	}

}
