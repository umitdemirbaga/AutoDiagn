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

public class UnnecessarySpeculation extends Thread {

	ResourceLock lock;

	UnnecessarySpeculation(ResourceLock lock) {
		this.lock = lock;
	}

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

		List<String> runningSpeculativeMapsName = new ArrayList<String>();
		boolean checkUnnecessarySpeculation = false;
		double getJobMapProgress = 0;

		List<String> getLowPerformanceRunningMapsName = new ArrayList<String>();
		List<String> affectedStragglers = new ArrayList<String>();

		int caseId = 0;
		int preCaseId = 0;
		try {
			synchronized (lock) {
				while (true) {

					while (lock.flag != 3) {
						lock.wait();
					}

//					try {
//						getJobMapProgress = SmartReader.getJobMapProgress(jobId);
//					} catch (IOException | URISyntaxException | ParseException e1) {
//						e1.printStackTrace();
//					}
					if (DataLocality.getJobMapProgress == 100.0) {
						lock.flag = 1;
						lock.notifyAll();
						break;
					}

//					try {
//						SmartReader.getLowPerformanceRunningMapsName(jobId);
//					} catch (IOException | URISyntaxException | ParseException | InterruptedException e2) {
//						e2.printStackTrace();
//					}

					getLowPerformanceRunningMapsName = DataLocality.lowPerformanceRunningMapsName1;

					if (getLowPerformanceRunningMapsName.size() > 0) {

//						System.out.println("Stragglers has been detected!..");
//						System.out.println();
						System.out.println("< UnnecessarySpeculation >");
						System.out.println("---------------------------------------");
						System.out.println("All the running maps			: " + DataLocality.runningMapsName1);

						System.out.println(
								"runningMapsPerformanceMedian		: " + DataLocality.runningMapsPerformanceMedian1);

						System.out.println("runningMapsPerformance			: " + DataLocality.runningMapsPerformanceNorm1);

						performanceWithFactor.clear();

						for (int i = 0; i < DataLocality.runningMapsPerformanceNorm1.size(); i++) {
							performanceWithFactor.add(Double.parseDouble(new DecimalFormat("##.##")
									.format(DataLocality.runningMapsPerformanceNorm1.get(i) * DataLocality.factor)));
						}

						System.out.println("performanceWithFactor			: " + performanceWithFactor);

						System.out.println("runningMapsProgress			: " + DataLocality.runningMapsProgress1);

						System.out
								.println("runningMapsExecutionTime		: " + DataLocality.runningMapsExecutionTime1);

						System.out.println("Outliers				: " + getLowPerformanceRunningMapsName);

						caseId = DataLocality.caseId;

						System.out.println("caseId					: " + caseId);
						System.out.println("---------------------------------------");
						System.out.println();

						try {
							runningSpeculativeMapsName = SmartReader.getRunningSpeculativeMapsName(jobId);
						} catch (IOException | URISyntaxException | ParseException e1) {
							e1.printStackTrace();
						}

						if (runningSpeculativeMapsName.size() > 0) {

							System.out.println();
							System.out.println("runningSpeculativeMapsName		= " + runningSpeculativeMapsName); // YYY

//							getLowPerformanceRunningMapsName.retainAll(runningSpeculativeMapsName);
//							
//							System.out.println("affectedStragglers			= " + getLowPerformanceRunningMapsName);

							try {
								checkUnnecessarySpeculation = SmartReader.checkUnnecessarySpeculation(jobId);
							} catch (IOException | URISyntaxException | ParseException e) {
								e.printStackTrace();
							}

							System.out.println("getRunningSpeculativeMapsHostName 	= "
									+ SmartReader.getRunningSpeculativeMapsHostName1);
							System.out.println("getNodesHostRunningSpeculativeMaps	= "
									+ SmartReader.getNodesHostRunningSpeculativeMaps1);
							System.out.println("StragglersHostName 			= "
									+ SmartReader.getLowPerformanceRunningMapsHostName1);
							System.out.println("getNodesCommon				= " + SmartReader.getNodesCommon1);
							System.out.println(
									"getNodesCommonCpuUsage			= " + SmartReader.getNodesCommonCpuUsage1);
							System.out.println(
									"getNodesCommonMemoryUsage		= " + SmartReader.getNodesCommonMemoryUsage1);

							if (checkUnnecessarySpeculation) {
								System.out.println();
								System.out.println(
										"!!! Unnecessary speculative executions have caused the makespan problem.");
								System.out.println();
								System.out
										.println("Stragglers running in the same node with speculative execution(s): ");
								System.out.println("These are ------> ");
								System.out.println();

								for (int i = 0; i < SmartReader.getLowPerformanceRunningMapsHostName1.size(); i++) {
									for (int j = 0; j < SmartReader.getNodesCommon1.size(); j++) {
										if (SmartReader.getLowPerformanceRunningMapsHostName1.get(i).equalsIgnoreCase(SmartReader.getNodesCommon1.get(j))) {
											if (!getLowPerformanceRunningMapsName.get(i).contains("_")) {
												if (!affectedStragglers.contains(getLowPerformanceRunningMapsName.get(i))) {
													affectedStragglers.add(getLowPerformanceRunningMapsName.get(i));
												}
											}
										}
									}
								}

								System.out.println("affectedStragglers = " + affectedStragglers);

								if (caseId != preCaseId) {

									// *********************************************************************
									// sending info to the DB.
									// -----------------------------------------------------------

									String runningMapsName = "";
									for (int i = 0; i < DataLocality.runningMapsName1.size(); i++) {
										if (i == 0) {
											runningMapsName = DataLocality.runningMapsName1.get(i);
										} else {
											runningMapsName = runningMapsName + "-"
													+ DataLocality.runningMapsName1.get(i);
										}
									}

									last = "metricsType=" + "XunSpeculationMapsName" + ",caseId=" + caseId
											+ ",mapsName=" + runningMapsName + ",jobId=" + jobId;

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

									last = "metricsType=" + "XunSpeculationMapsProgress" + ",caseId=" + caseId
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
											runningMapsExecutionTime = DataLocality.runningMapsExecutionTime1.get(i)
													+ "";
										} else {
											runningMapsExecutionTime = runningMapsExecutionTime + "-"
													+ DataLocality.runningMapsExecutionTime1.get(i);
										}
									}

									last = "metricsType=" + "XunSpeculationMapsExecTime" + ",caseId=" + caseId
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

									last = "metricsType=" + "XunSpeculationMapsPerformance" + ",caseId=" + caseId
											+ ",mapsPerformance=" + runningMapsPerformance + ",median="
											+ DataLocality.runningMapsPerformanceMedian1 + ",jobId=" + jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									String stragglers = "";
									for (int i = 0; i < DataLocality.lowPerformanceRunningMapsName1.size(); i++) {
										if (i == 0) {
											stragglers = DataLocality.lowPerformanceRunningMapsName1.get(i);
										} else {
											stragglers = stragglers + "-"
													+ DataLocality.lowPerformanceRunningMapsName1.get(i);
										}
									}

									last = "metricsType=" + "XunSpeculationStragglers" + ",caseId=" + caseId
											+ ",mapsName=" + stragglers + ",factor=" + DataLocality.factor + ",jobId="
											+ jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									System.out.println();

									System.out.println(
											"******************************************************************************");
									System.out.println();
									System.out.println(
											"******************************************************************************");
									System.out.println();
									preCaseId = caseId;
								} else {
									System.out.println("The info has already been sent to the DB..");
									System.out.println(
											"******************************************************************************");
								}
							} else {
								System.out.println();
								System.out.println("Speculative executions do not make makespan problem.");
								System.out.println(
										"******************************************************************************");
								System.out.println();
								System.out.println(
										"******************************************************************************");
							}

						} else {
							System.out.println();
							System.out.println("There is no any speculative execution for now!..");
							System.out.println(
									"******************************************************************************");
							System.out.println();
							System.out.println(
									"******************************************************************************");
						}

					} else {
//						System.out.println();
//						System.out.println("There is no any stragglers...");
//						System.out
//						.println("******************************************************************************");
					}

					try {
						TimeUnit.SECONDS.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					lock.flag = 1;
					lock.notifyAll();
				} // while
			}

		} catch (Exception e) {
			System.out.println("Exception 1: " + e.getMessage());
		}
	}
}
