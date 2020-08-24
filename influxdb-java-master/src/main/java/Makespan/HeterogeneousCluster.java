package Makespan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.db.influxdb.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.json.simple.parser.ParseException;

public class HeterogeneousCluster extends Thread {

	ResourceLock lock;

	HeterogeneousCluster(ResourceLock lock) {
		this.lock = lock;
	}

//	public static boolean checkClusterHomogeneity = false;
	
//	public static List<String> getMapsOnSlowDataNode = new ArrayList<String>();

	public static List<String> stragglersRunningSlowNodes = new ArrayList<String>();

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

		boolean checkHeterogeneousCluster = false;

		List<String> lowPerformanceRunningMapsName = new ArrayList<String>();
		List<String> getNodesHostLowPerformanceRunningMaps = new ArrayList<String>();
		List<String> getDataNodesNameHavingMaxVCoresNum = new ArrayList<String>();
		List<String> getLowPerformanceRunningMapsHostName = new ArrayList<String>();
		List<String> stragglersNameRunningSlowHosts = new ArrayList<String>();

		int caseId = 0;
		int preCaseId = 0;

		while (true) {
			try {
				synchronized (lock) {
					while (lock.flag != 2) {
						lock.wait();
					}

					if (DataLocality.getJobMapProgress == 100.0) {
//						lock.flag = 3;
						lock.flag = 1;
						lock.notifyAll();
						break;
					}

					System.out.println();

					stragglersNameRunningSlowHosts.clear();

					lowPerformanceRunningMapsName = DataLocality.lowPerformanceRunningMapsName1;

					if (lowPerformanceRunningMapsName.size() > 0) {

						System.out.println("< HeterogeneousCluster > ");
						System.out.println("---------------------------------------");
						System.out.println("All the running maps			: " + DataLocality.runningMapsName1);

						System.out.println(
								"runningMapsPerformanceMedian		: " + DataLocality.runningMapsPerformanceMedian1);

//							System.out.println("runningMapsPerformance			: " + SmartReader.runningMapsPerformance1);

						System.out.println(
								"runningMapsPerformanceNorm1		: " + DataLocality.runningMapsPerformanceNorm1);

						System.out.println("progressListNorm			: " + DataLocality.progressListNorm);

						System.out.println("exacTimeListNorm			: " + DataLocality.exacTimeListNorm);

						System.out.println("runningMapsProgress			: " + DataLocality.runningMapsProgress1);

						System.out
								.println("runningMapsExecutionTime		: " + DataLocality.runningMapsExecutionTime1);

						performanceWithFactor.clear();

						for (int i = 0; i < DataLocality.runningMapsPerformanceNorm1.size(); i++) {
							performanceWithFactor.add(Double.parseDouble(new DecimalFormat("##.##")
									.format(DataLocality.runningMapsPerformanceNorm1.get(i) * DataLocality.factor)));
						}

						System.out.println("performanceWithFactorNorm		: " + performanceWithFactor);

						System.out.println("Outliers				: " + lowPerformanceRunningMapsName);

						caseId = DataLocality.caseId;

						System.out.println("caseId					: " + caseId);
						System.out.println("---------------------------------------");

						System.out.println();

						// to check if the nodes which host the running maps are homogeneous or not.
						try {
							checkHeterogeneousCluster = SmartReader.checkHeterogeneousCluster(jobId);
						} catch (IOException | URISyntaxException | ParseException e) {
							e.printStackTrace();
						}

						getNodesHostLowPerformanceRunningMaps = SmartReader.getNodesHostLowPerformanceRunningMaps1;
						getLowPerformanceRunningMapsHostName = SmartReader.getLowPerformanceRunningMapsHostName1;
						getDataNodesNameHavingMaxVCoresNum = SmartReader.getDataNodesNameHavingMaxVCoresNum1;

						System.out.println(
								"getNodesHostLowPerformanceRunningMaps	= " + getNodesHostLowPerformanceRunningMaps);
						System.out.println(
								"getLowPerformanceRunningMapsHostName	= " + getLowPerformanceRunningMapsHostName);
						System.out.println(
								"getDataNodesNameHavingMaxVCoresNum	= " + getDataNodesNameHavingMaxVCoresNum);

						if (checkHeterogeneousCluster && (!(getDataNodesNameHavingMaxVCoresNum == null))) {

							getNodesHostLowPerformanceRunningMaps.removeAll(getDataNodesNameHavingMaxVCoresNum);

							// ignore this if I don't need the name of the name of stragglers running in
							// slow nodes.
							for (int i = 0; i < getLowPerformanceRunningMapsHostName.size(); i++) {
								for (int j = 0; j < getNodesHostLowPerformanceRunningMaps.size(); j++) {
									if (getLowPerformanceRunningMapsHostName.get(i)
											.equalsIgnoreCase(getNodesHostLowPerformanceRunningMaps.get(j))) {
										stragglersNameRunningSlowHosts.add(lowPerformanceRunningMapsName.get(i));
									}
								}
							}

							for (int i = 0; i < stragglersNameRunningSlowHosts.size(); i++) {
								if (!stragglersRunningSlowNodes.contains(stragglersNameRunningSlowHosts.get(i))) {
									stragglersRunningSlowNodes.add(stragglersNameRunningSlowHosts.get(i));
								}
							}

							System.out.println();
							System.out.println("!!! Heterogeneous Cluster has caused the makespan problem.");
							System.out.println();
							System.out.println(
									"The nodes which have less resource and have caused the makespan problem 	= "
											+ getNodesHostLowPerformanceRunningMaps);
							System.out.println(
									"The stragglers running in the nodes   " + getNodesHostLowPerformanceRunningMaps
											+ "   which have less resource	= " + stragglersNameRunningSlowHosts);

							System.out.println();

							if (caseId != preCaseId) {

								// *********************************************************************
								// sending info to the DB.
								// -----------------------------------------------------------

								String runningMapsName = "";
								for (int i = 0; i < DataLocality.runningMapsName1.size(); i++) {
									if (i == 0) {
										runningMapsName = DataLocality.runningMapsName1.get(i);
									} else {
										runningMapsName = runningMapsName + "-" + DataLocality.runningMapsName1.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousMapsName" + ",caseId=" + caseId + ",mapsName="
										+ runningMapsName + ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								String runningMapsProgress = "";
								for (int i = 0; i < DataLocality.runningMapsProgress1.size(); i++) {
									if (i == 0) {
										runningMapsProgress = DataLocality.runningMapsProgress1.get(i) + "";
									} else {
										runningMapsProgress = runningMapsProgress + "-"
												+ DataLocality.runningMapsProgress1.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousMapsProgress" + ",caseId=" + caseId
										+ ",mapsProgress=" + runningMapsProgress + ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								String runningMapsExecutionTime = "";
								for (int i = 0; i < DataLocality.runningMapsExecutionTime1.size(); i++) {
									if (i == 0) {
										runningMapsExecutionTime = DataLocality.runningMapsExecutionTime1.get(i) + "";
									} else {
										runningMapsExecutionTime = runningMapsExecutionTime + "-"
												+ DataLocality.runningMapsExecutionTime1.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousMapsExecTime" + ",caseId=" + caseId
										+ ",mapsExecTime=" + runningMapsExecutionTime + ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								String runningMapsPerformance = "";
								for (int i = 0; i < DataLocality.runningMapsPerformanceNorm1.size(); i++) {
									if (i == 0) {
										runningMapsPerformance = DataLocality.runningMapsPerformanceNorm1.get(i) + "";
									} else {
										runningMapsPerformance = runningMapsPerformance + "-"
												+ DataLocality.runningMapsPerformanceNorm1.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousMapsPerformance" + ",caseId=" + caseId
										+ ",mapsPerformance=" + runningMapsPerformance + ",median="
										+ Streaming.runningMapsPerformanceMedian1 + ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								String stragglers = "";
								for (int i = 0; i < Streaming.lowPerformanceRunningMapsName1.size(); i++) {
									if (i == 0) {
										stragglers = Streaming.lowPerformanceRunningMapsName1.get(i);
									} else {
										stragglers = stragglers + "-"
												+ Streaming.lowPerformanceRunningMapsName1.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousStragglers" + ",caseId=" + caseId + ",mapsName="
										+ stragglers + ",factor=" + SmartReader.factor + ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								String stragglersHosts = "";
								for (int i = 0; i < getLowPerformanceRunningMapsHostName.size(); i++) {
									if (i == 0) {
										stragglersHosts = getLowPerformanceRunningMapsHostName.get(i) + "";
									} else {
										stragglersHosts = stragglersHosts + "-"
												+ getLowPerformanceRunningMapsHostName.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousStragglersHosts" + ",caseId=" + caseId
										+ ",stragglersHosts=" + stragglersHosts + ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								String nodesHostStragglers = "";
								for (int i = 0; i < getNodesHostLowPerformanceRunningMaps.size(); i++) {
									if (i == 0) {
										nodesHostStragglers = getNodesHostLowPerformanceRunningMaps.get(i) + "";
									} else {
										nodesHostStragglers = nodesHostStragglers + "-"
												+ getNodesHostLowPerformanceRunningMaps.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousNodesHostStragglers" + ",caseId=" + caseId
										+ ",nodesHostStragglers=" + nodesHostStragglers + ",jobId=" + jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								// -----------------------------------------------------------

								String stragglersRunningSlowHosts = "";
								for (int i = 0; i < stragglersNameRunningSlowHosts.size(); i++) {
									if (i == 0) {
										stragglersRunningSlowHosts = stragglersNameRunningSlowHosts.get(i) + "";
									} else {
										stragglersRunningSlowHosts = stragglersRunningSlowHosts + "-"
												+ stragglersNameRunningSlowHosts.get(i);
									}
								}

								last = "metricsType=" + "XheterogeneousStragglersRunningSlowNodes" + ",caseId=" + caseId
										+ ",stragglersRunningSlowHosts=" + stragglersRunningSlowHosts + ",jobId="
										+ jobId;

								System.out.println(" [x] Sent >>>>  '" + last + "'");

								try {
									channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
								} catch (IOException e) {
									e.printStackTrace();
								}

								System.out.println(
										"******************************************************************************");

								preCaseId = caseId;
							} else {
								System.out.println("The info has already been sent to the DB..");
								System.out.println(
										"******************************************************************************");
							}

						} else {
							System.out.println();
							System.out.println("Heterogeneous Cluster does not make makespan problem");
							System.out.println(
									"******************************************************************************");
						}
					}

					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
//						lock.flag = 3;
					lock.flag = 1;
					lock.notifyAll();

				}

			} catch (Exception e) {
				System.out.println("Exception in Heteronegeous cluster: " + e.getMessage());
			}
//		}
		} // while
	}
}
