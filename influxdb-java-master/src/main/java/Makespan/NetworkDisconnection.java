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

public class NetworkDisconnection extends Thread {

	public static List<String> getAllStragglers = new ArrayList<String>();

	public static List<String> allStragglersDueToDisconnection = new ArrayList<String>();

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
		
		String amHost = "";

		try {
			amHost = SmartReader.getAppMasterLocation(jobId);
		} catch (IOException | URISyntaxException | ParseException e3) {
			e3.printStackTrace();
		}
		

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

//				System.out.println("getAllStragglers 		= " + getAllStragglers);

				// to get running restarted maps name
				List<String> getAllMapsRestarted = new ArrayList<String>();
				try {
					if (!(SmartReader.getAllMapsRestarted(jobId) == null)) {
						getAllMapsRestarted = SmartReader.getAllMapsRestarted(jobId);
					}
				} catch (IOException | URISyntaxException | ParseException e1) {
					e1.printStackTrace();
				}
				System.out.println();
				System.out.println("getAllMapsRestarted			: " + getAllMapsRestarted);
				System.out.println();

				System.out.println("allStragglersDueToDisconnection		: " + allStragglersDueToDisconnection);
				System.out.println();
				double accuracy = 0.0;

				if (getAllMapsRestarted.size() >0) {
					accuracy = (allStragglersDueToDisconnection.size() * 100) / getAllMapsRestarted.size();
					System.out.println("accuracy for Network disconnection = " + accuracy);
					System.out.println();
				} else {
					System.out.println("There is no any restarted maps");
				}
				

				// sending the results

				String getAllMapsRestarted1 = "";
				for (int i = 0; i < getAllMapsRestarted.size(); i++) {
					if (i == 0) {
						getAllMapsRestarted1 = getAllMapsRestarted.get(i);
					} else {
						getAllMapsRestarted1 = getAllMapsRestarted1 + "-" + getAllMapsRestarted.get(i);
					}
				}

				String stragglersDueToNodeDisconnection1 = "";
				for (int i = 0; i < allStragglersDueToDisconnection.size(); i++) {
					if (i == 0) {
						stragglersDueToNodeDisconnection1 = allStragglersDueToDisconnection.get(i);
					} else {
						stragglersDueToNodeDisconnection1 = stragglersDueToNodeDisconnection1 + "-"
								+ allStragglersDueToDisconnection.get(i);
					}
				}

				last = "metricsType=" + "XnetworkDisconnectionAccuracy" + ",caseId=" + caseId + ",getAllMapsRestarted="
						+ getAllMapsRestarted1 + ",stragglersDueToNodeDisconnection="
						+ stragglersDueToNodeDisconnection1 + ",accuracy=" + accuracy + ",jobId=" + jobId;

				System.out.println(" [x] Sent >>>>  '" + last + "'");
				System.out.println();
				try {
					channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}

				System.out.println("FINISHED!");

				break;
			}

			// getRunningMapsNameNew  OR  getRunningMapsNameNew2  is executed in this method. 
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
				System.out.println("< Network Disconnection >");
				System.out.println("---------------------------------------");
				System.out.println("All the running maps			: " + SmartReader.runningMapsName1);

//				System.out.println("runningMapsPerformanceMedian		: " + SmartReader.runningMapsPerformanceMedian1);
//
//				System.out.println("runningMapsPerformanceNorm1		: " + SmartReader.runningMapsPerformanceNorm1);
//
//				System.out.println("progressListNorm			: " + SmartReader.progressListNorm);
//
//				System.out.println("exacTimeListNorm			: " + SmartReader.exacTimeListNorm);
//
//				System.out.println("runningMapsProgress			: " + SmartReader.runningMapsProgress1);
//
//				System.out.println("runningMapsExecutionTime		: " + SmartReader.runningMapsExecutionTime1);

				performanceWithFactor.clear();

				for (int i = 0; i < SmartReader.runningMapsPerformanceNorm1.size(); i++) {
					performanceWithFactor.add(Double.parseDouble(new DecimalFormat("##.##")
							.format(SmartReader.runningMapsPerformanceNorm1.get(i) * SmartReader.factor)));
				}

//				System.out.println("performanceWithFactorNorm		: " + performanceWithFactor);

				System.out.println("Stragglers				: " + getLowPerformanceRunningMapsName);

				for (int i = 0; i < getLowPerformanceRunningMapsName.size(); i++) {
					if (!getAllStragglers.contains(getLowPerformanceRunningMapsName.get(i))) {
						getAllStragglers.add(getLowPerformanceRunningMapsName.get(i));
					}
				}
				System.out.println();

//				try {
//					System.out
//							.println("nodesHostRunningMaps			: " + SmartReader.getNodesHostRunningMaps(jobId));
//				} catch (IOException | URISyntaxException | ParseException e2) {
//					e2.printStackTrace();
//				}

				System.out.println("amHost 					: " + amHost);
				
				
				try {
					System.out
							.println("numMapsPending				: " + SmartReader.getNumMapsPending(jobId));
				} catch (IOException | URISyntaxException | ParseException e2) {
					e2.printStackTrace();
				}
				
								
				System.out.println();

				caseId = SmartReader.runningMapsCaseId;

				System.out.println("caseId					: " + caseId);

				System.out.println("---------------------------------------");
				System.out.println();

				// to get the name of disconnected nodes.
				List<String> disconnectedNodesNames = new ArrayList<String>();

				try {
					if (!(SmartReader.getDisconnectedNodesName(jobId) == null)) {
						disconnectedNodesNames = SmartReader.disconnectedNodes;
					}
				} catch (IOException | URISyntaxException | ParseException e1) {
					e1.printStackTrace();
				}

				if (disconnectedNodesNames.size() == 0) {
					System.out.println("There is no disconnected nodes!");
					System.out.println();
				} else {

					System.out.println("disconnectedNodesNames			: " + disconnectedNodesNames);

					// to get running restarted maps name
					List<String> getrunningMapsRestarted = new ArrayList<String>();
//					try {
//						if (!(SmartReader.getRunningMapsRestarted(jobId) == null)) {
//							getrunningMapsRestarted = SmartReader.runningMapsRestarted;
//						}
//					} catch (IOException | URISyntaxException | ParseException e1) {
//						e1.printStackTrace();
//					}
					
					for (int i = 0; i < SmartReader.runningMapsName1.size(); i++) {
						if (SmartReader.runningMapsName1.get(i).contains("_")) {
							getrunningMapsRestarted.add(SmartReader.runningMapsName1.get(i));
						}
					}
					 

					if (getrunningMapsRestarted.size() > 0) {

						System.out.println("getrunningMapsRestarted			: " + getrunningMapsRestarted);

						// to get the name of stragglers stemming from node disconnection
						List<String> stragglersDueToNodeDisconnection = new ArrayList<String>();

						getrunningMapsRestarted.retainAll(getLowPerformanceRunningMapsName);

						stragglersDueToNodeDisconnection = getrunningMapsRestarted;

						if (stragglersDueToNodeDisconnection.size() > 0) {
							System.out.println();
							System.out.println("!!! Disconnected nodes have caused stragglers!");
							System.out.println("	Disconnected nodes 				---> " + disconnectedNodesNames);
							System.out.println("	Stragglers stemming from node disconnection 	---> "
									+ stragglersDueToNodeDisconnection);
							System.out.println();

							for (int i = 0; i < stragglersDueToNodeDisconnection.size(); i++) {
								if (!allStragglersDueToDisconnection
										.contains(stragglersDueToNodeDisconnection.get(i))) {
									allStragglersDueToDisconnection.add(stragglersDueToNodeDisconnection.get(i));
								}
							}

							if (caseId != preCaseId) {

								// *********************************************************************
								// sending info to the DB.

								String disconnectedNodesNames1 = "";
								for (int i = 0; i < disconnectedNodesNames.size(); i++) {
									if (i == 0) {
										disconnectedNodesNames1 = disconnectedNodesNames.get(i);
									} else {
										disconnectedNodesNames1 = disconnectedNodesNames1 + "-"
												+ disconnectedNodesNames.get(i);
									}
								}

								String stragglersDueToNodeDisconnection1 = "";
								for (int i = 0; i < stragglersDueToNodeDisconnection.size(); i++) {
									if (i == 0) {
										stragglersDueToNodeDisconnection1 = stragglersDueToNodeDisconnection.get(i);
									} else {
										stragglersDueToNodeDisconnection1 = stragglersDueToNodeDisconnection1 + "-"
												+ stragglersDueToNodeDisconnection.get(i);
									}
								}

								last = "metricsType=" + "XnetworkDisconnectionStragglers" + ",caseId=" + caseId
										+ ",disconnectedNodesNames=" + disconnectedNodesNames1
										+ ",stragglersDueToNodeDisconnection=" + stragglersDueToNodeDisconnection1
										+ ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");
								System.out.println();

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								preCaseId = caseId;

							} else {
								System.out.println();
								System.out.println("The info has already been sent to the DB..");
								System.out.println();
							}

						} else {
							System.out.println();
							System.out.println("Disconnected nodes do not cause straggler problem.");
							System.out.println();
						}
					} else {
						System.out.println();
						System.out.println("There is no any restarted tasks due to Disconnected nodes.");
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

		System.exit(0);
	} // run
}
