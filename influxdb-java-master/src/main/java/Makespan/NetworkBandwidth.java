package Makespan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.db.influxdb.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.json.simple.parser.ParseException;

public class NetworkBandwidth extends Thread {

	public static List<String> getAllStragglers = new ArrayList<String>();
	
	public static List<String> allStragglersDueToLowDownloadSpeed = new ArrayList<String>();

	public static double getJobMapProgress = 0;

	@Override
	public void run() {

		List<Double> performanceWithFactor = new ArrayList<Double>();

		String jobId = DetectorForMakespan.jobId;

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

		String last = "";

		List<String> getLowPerformanceRunningMapsName = new ArrayList<String>();

		int caseId = 0;
		int preCaseId = 0;

		while (true) {

			try {
				getJobMapProgress = SmartReader.getJobMapProgress(jobId);
			} catch (IOException | URISyntaxException | ParseException e1) {
				e1.printStackTrace();
			}

			if (getJobMapProgress == 100.0) {
				System.out.println();
				System.out.println("****************************************************************************");
				System.out.println("Debugging the job '" + jobId + "' is completed successfully...");
				System.out.println("****************************************************************************");

				System.out.println("allStragglersDueToLowDownloadSpeed	= " + allStragglersDueToLowDownloadSpeed);
				
				System.out.println();
				
				System.out.println("getAllStragglers 		= " + getAllStragglers);
				
				System.out.println();

				break;
			}

			try {
				if (!(SmartReader.getLowPerformanceRunningMapsName(jobId) == null)) {
					getLowPerformanceRunningMapsName = SmartReader.lowPerformanceRunningMapsName1;
				}
			} catch (IOException | URISyntaxException | ParseException | InterruptedException e1) {
				e1.printStackTrace();
			}

			if (getLowPerformanceRunningMapsName.size() > 0) {

				System.out.println(" ***** Stragglers has been detected! *****");
				System.out.println();
				System.out.println("< Network Bandwidth >");
				System.out.println("---------------------------------------");
				System.out.println("All the running maps			: " + SmartReader.runningMapsName1);

				System.out
						.println("runningMapsPerformanceMedian		: " + SmartReader.runningMapsPerformanceMedian1);

//						System.out.println("runningMapsPerformance			: " + SmartReader.runningMapsPerformance1); 

				System.out.println("runningMapsPerformanceNorm1		: " + SmartReader.runningMapsPerformanceNorm1);

				System.out.println("progressListNorm			: " + SmartReader.progressListNorm);

				System.out.println("exacTimeListNorm			: " + SmartReader.exacTimeListNorm);

				System.out.println("runningMapsProgress			: " + SmartReader.runningMapsProgress1);

				System.out.println("runningMapsExecutionTime		: " + SmartReader.runningMapsExecutionTime1);

				performanceWithFactor.clear();

				for (int i = 0; i < SmartReader.runningMapsPerformanceNorm1.size(); i++) {
					performanceWithFactor.add(Double.parseDouble(new DecimalFormat("##.##")
							.format(SmartReader.runningMapsPerformanceNorm1.get(i) * SmartReader.factor)));
				}

				System.out.println("performanceWithFactorNorm		: " + performanceWithFactor);

				System.out.println("Stragglers				: " + getLowPerformanceRunningMapsName);

				for (int i = 0; i < getLowPerformanceRunningMapsName.size(); i++) {
					if (!getAllStragglers.contains(getLowPerformanceRunningMapsName.get(i))) {
						getAllStragglers.add(getLowPerformanceRunningMapsName.get(i));
					}
				}

				caseId = SmartReader.runningMapsCaseId;

				System.out.println("caseId					: " + caseId);

				System.out.println("---------------------------------------");
				System.out.println();

				// to get the name of nodes that have low download bandwidth.
				List<String> lowDownloadNodesNames = new ArrayList<String>();

				try {
					if (!(SmartReader.getLowDownloadNodesName(jobId) == null)) {
						lowDownloadNodesNames = SmartReader.lowDownloadNodes; 
					}
				} catch (IOException | URISyntaxException | ParseException e1) {
					e1.printStackTrace();
				}
				

				if (lowDownloadNodesNames.size() == 0) {
					System.out.println("All nodes's download speeds are normal!");
					System.out.println("dataNodesNamesForBandwidth		: " + SmartReader.dataNodesNamesForBandwidth);
					System.out.println("datanodes download speeds		: " + SmartReader.dataNodesDownloadSpeeds);
					System.out.println();
				} else {
					System.out.println("Some nodes' download speeds are not normal!!!"); 
					System.out.println();
					
					System.out.println("dataNodesNamesForBandwidth		: " + SmartReader.dataNodesNamesForBandwidth);
					System.out.println("datanodes download speeds		: " + SmartReader.dataNodesDownloadSpeeds);
					System.out.println("lowDownloadNodesNames			: " + lowDownloadNodesNames);

					// to get the name of the nodes that host stragglers.
					List<String> nodesHostLowPerformanceRunningMaps = new ArrayList<String>();
					try {
						if (!(SmartReader.getNodesHostLowPerformanceRunningMaps(jobId) == null)) {
							nodesHostLowPerformanceRunningMaps = SmartReader.getNodesHostLowPerformanceRunningMaps1;
						}
					} catch (IOException | URISyntaxException | ParseException e1) {
						e1.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println("nodesHostLowPerformanceRunningMaps	: " + nodesHostLowPerformanceRunningMaps);

					// to get the name of the maps hosted on the nodes that have low download bandwidth.
					List<String> getMapsOnLowDownloadNodes = new ArrayList<String>();					
					try {
						if (!(SmartReader.getMapsOnLowDownloadNodes(jobId) == null)) {
							getMapsOnLowDownloadNodes = SmartReader.mapsOnLowDownloadNodes; 
						}
					} catch (IOException | URISyntaxException | ParseException e1) {
						e1.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println("getMapsOnLowDownloadNodes		: " + getMapsOnLowDownloadNodes);

//					// to get restarted maps name
//					List<String> getMapsRestarted = new ArrayList<String>();
//					try {
//						if (!(SmartReader.getMapsRestarted(jobId) == null)) {
//							getMapsRestarted = SmartReader.mapsRestarted;
//						}
//					} catch (IOException | URISyntaxException | ParseException e1) {
//						e1.printStackTrace();
//					}
//					System.out.println("getMapsRestarted			: " + getMapsRestarted);

					// to get the name of stragglers stemming from the nodes that have low download speed
					List<String> stragglersDueToLowDownloadSpeed = new ArrayList<String>();
					
					getMapsOnLowDownloadNodes.retainAll(getLowPerformanceRunningMapsName);
					
					stragglersDueToLowDownloadSpeed = getMapsOnLowDownloadNodes;
					
					System.out.println("stragglersDueToLowDownloadSpeed	: " + stragglersDueToLowDownloadSpeed);

					if (stragglersDueToLowDownloadSpeed.size() > 0) {
						System.out.println();
						System.out.println("!!! Nodes that have low download speeds have caused stragglers!");
						System.out.println("	Nodes that have low download speeds		---> " + lowDownloadNodesNames);
						System.out.println("	Stragglers stemming from latency 		---> "+ stragglersDueToLowDownloadSpeed);
						System.out.println();
						
						for (int i = 0; i < stragglersDueToLowDownloadSpeed.size(); i++) {
						if (!allStragglersDueToLowDownloadSpeed.contains(stragglersDueToLowDownloadSpeed.get(i))) {
							allStragglersDueToLowDownloadSpeed.add(stragglersDueToLowDownloadSpeed.get(i));
						}
					}

						if (caseId != preCaseId) {

							// *********************************************************************
							// sending info to the DB.
							
							String lowDownloadNodesNames1 = "";
							for (int i = 0; i < lowDownloadNodesNames.size(); i++) {
								if (i == 0) {
									lowDownloadNodesNames1 = lowDownloadNodesNames.get(i);
								} else {
									lowDownloadNodesNames1 = lowDownloadNodesNames1 + "-"
											+ lowDownloadNodesNames.get(i);
								}
							}
							
							
							String stragglersDueToLowDownloadSpeed1 = "";
							for (int i = 0; i < stragglersDueToLowDownloadSpeed.size(); i++) {
								if (i == 0) {
									stragglersDueToLowDownloadSpeed1 = stragglersDueToLowDownloadSpeed.get(i);
								} else {
									stragglersDueToLowDownloadSpeed1 = stragglersDueToLowDownloadSpeed1 + "-"
											+ stragglersDueToLowDownloadSpeed.get(i);
								}
							}

							last = "metricsType=" + "XnetworkBandwidthStragglers" + ",caseId=" + caseId
									+ ",lowDownloadNodesNames=" + lowDownloadNodesNames1
									+ ",stragglersDueToLowDownloadSpeed=" + stragglersDueToLowDownloadSpeed1
									+ ",jobId=" + jobId;

							System.out.println(" [x] Sent >>>>  '" + last + "'");

							try {
								channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
							} catch (IOException e) {
								e.printStackTrace();
							}

							// -----------------------------------------------------------

							preCaseId = caseId;

						} else {
							System.out.println("The info has already been sent to the DB..");
							System.out.println();
						}

					} else {
						System.out.println("The nodes that have low download speed do not cause straggler problem.");
						System.out.println();
					}
				}

			} else {
				System.out.println();
				System.out.println("There is no any stragglers...");
				System.out.println();
			}

			System.out.println("******************************************************************************");

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} // while
	} // run
}
