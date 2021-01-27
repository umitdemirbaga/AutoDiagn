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

public class DataLocality extends Thread {

	public static List<String> nonLocalRunningStragglers = new ArrayList<String>();

	public static List<String> getAllStragglers = new ArrayList<String>();

	public static List<String> getAllNonLocalMaps = new ArrayList<String>();

	public static DataLocalityCalculator threadDataLocalityCalculator = new DataLocalityCalculator();

	public static double getJobMapProgress = 0;

	public static boolean checkClusterHomogeneity = false;

	ResourceLock lock;

	DataLocality(ResourceLock lock) {
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

		List<String> nonLocalRunningMapsName = new ArrayList<String>();
		List<String> getLowPerformanceRunningMapsName = new ArrayList<String>();

		int caseId = 0;
		int preCaseId = 0;

		// to check all nodes. If the cluster is homogeneous, no need to check all.
		try {
			checkClusterHomogeneity = SmartReader.checkClusterHomogeneity(jobId);
		} catch (IOException | URISyntaxException | ParseException e1) {
			e1.printStackTrace();
		}

		if (checkClusterHomogeneity) {
			System.out.println("The cluster is homogeneous...");
			System.out.println();
			last = "metricsType=" + "XheterogeneousInfo" + ",heterogeneous=" + "false" + ",jobId=" + jobId;
			System.out.println(" [x] Sent >>>>  '" + last + "'");
			System.out.println();
			System.out.println("****************************************************************************");
			System.out.println();
			try {
				channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("The cluster is heteroheneous...");
			System.out.println();
			last = "metricsType=" + "XheterogeneousInfo" + ",heterogeneous=" + "true" + ",jobId=" + jobId;
			System.out.println(" [x] Sent >>>>  '" + last + "'");
			System.out.println();
			System.out.println("****************************************************************************");
			System.out.println();
			try {
				channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		while (true) {
//			try {
				synchronized (lock) {

					if (!checkClusterHomogeneity) {
						while (lock.flag != 1) {
							try {
								lock.wait();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}

//					long now = Instant.now().toEpochMilli();
//					System.out.println("y= " + now);

					try {
						getJobMapProgress = SmartReader.getJobMapProgress(jobId);
					} catch (IOException | URISyntaxException | ParseException e1) {
						e1.printStackTrace();
					}

					if (getJobMapProgress == 100.0) {
						System.out.println();
						System.out.println(
								"****************************************************************************");
						System.out.println("Debugging the job '" + jobId + "' is completed successfully...");
						System.out.println(
								"****************************************************************************");

						System.out.println();

						if (!checkClusterHomogeneity) {
							lock.flag = 2;
							lock.notifyAll();
						}

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
						System.out.println("< DataLocality >");
						System.out.println("---------------------------------------");
						System.out.println("All the running maps			: " + SmartReader.runningMapsName1);

						System.out.println(
								"runningMapsPerformanceMedian		: " + SmartReader.runningMapsPerformanceMedian1);

//						System.out.println("runningMapsPerformance			: " + SmartReader.runningMapsPerformance1); 

						System.out.println(
								"runningMapsPerformanceNorm1		: " + SmartReader.runningMapsPerformanceNorm1);

						System.out.println("progressListNorm			: " + SmartReader.progressListNorm);

						System.out.println("exacTimeListNorm			: " + SmartReader.exacTimeListNorm);

						System.out.println("runningMapsProgress			: " + SmartReader.runningMapsProgress1);

						System.out
								.println("runningMapsExecutionTime		: " + SmartReader.runningMapsExecutionTime1);

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

						try {
							if (!(SmartReader.getNonLocalRunningMapsName(jobId) == null)) {
								nonLocalRunningMapsName = SmartReader.nonLocalRunningMapsName1;
							}
						} catch (IOException | URISyntaxException | ParseException e) {
						}
						System.out.println("nonLocalRunningMapsName			: " + nonLocalRunningMapsName);

						for (int i = 0; i < nonLocalRunningMapsName.size(); i++) {
							if (!getAllNonLocalMaps.contains(nonLocalRunningMapsName.get(i))) {
								getAllNonLocalMaps.add(nonLocalRunningMapsName.get(i));
							}
						}

						String nonLocalRunningMapsName1 = "";
						for (int i = 0; i < nonLocalRunningMapsName.size(); i++) {
							if (i == 0) {
								nonLocalRunningMapsName1 = nonLocalRunningMapsName.get(i);
							} else {
								nonLocalRunningMapsName1 = nonLocalRunningMapsName1 + "-"
										+ nonLocalRunningMapsName.get(i);
							}
						}

						caseId = SmartReader.runningMapsCaseId;

						System.out.println("caseId					: " + caseId);

						System.out.println("---------------------------------------");
						System.out.println();

						if (nonLocalRunningMapsName.size() == 0) {
							System.out.println("There is no non-local task!");
						} else {

							nonLocalRunningMapsName.retainAll(getLowPerformanceRunningMapsName);

							if (nonLocalRunningMapsName.size() > 0) {
								System.out.println(
										"!!! Non-local maps which have low performance that prolong the makespan.");
								System.out.println("	These are  ---> " + nonLocalRunningMapsName);

								for (int i = 0; i < nonLocalRunningMapsName.size(); i++) {
									if (!nonLocalRunningStragglers.contains(nonLocalRunningMapsName.get(i))) {
										nonLocalRunningStragglers.add(nonLocalRunningMapsName.get(i));
									}
								}
								System.out.println();

								if (caseId != preCaseId) {

									// *********************************************************************
									// sending info to the DB.

									// -----------------------------------------------------------

									// getting the "nonLocalMapsName" is done upper, as we change the
									// "nonLocalRunningMapsName" list using "retainAll" method.
									last = "metricsType=" + "XdataLocalityNonLocalMapsName" + ",caseId=" + caseId
											+ ",nonLocalMapsName=" + nonLocalRunningMapsName1 + ",jobId=" + jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									String runningMapsName = "";
									for (int i = 0; i < SmartReader.runningMapsName1.size(); i++) {
										if (i == 0) {
											runningMapsName = SmartReader.runningMapsName1.get(i);
										} else {
											runningMapsName = runningMapsName + "-"
													+ SmartReader.runningMapsName1.get(i);
										}
									}

									last = "metricsType=" + "XdataLocalityMapsName" + ",caseId=" + caseId + ",mapsName="
											+ runningMapsName + ",jobId=" + jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									String runningMapsPerformance = "";
									for (int i = 0; i < SmartReader.runningMapsPerformance1.size(); i++) {
										if (i == 0) {
											runningMapsPerformance = SmartReader.runningMapsPerformance1.get(i) + "";
										} else {
											runningMapsPerformance = runningMapsPerformance + "-"
													+ SmartReader.runningMapsPerformance1.get(i);
										}
									}

									last = "metricsType=" + "XdataLocalityMapsPerformance" + ",caseId=" + caseId
											+ ",mapsPerformance=" + runningMapsPerformance + ",median="
											+ SmartReader.runningMapsPerformanceMedian1 + ",jobId=" + jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									String runningMapsProgress = "";
									for (int i = 0; i < SmartReader.runningMapsProgress1.size(); i++) {
										if (i == 0) {
											runningMapsProgress = SmartReader.runningMapsProgress1.get(i) + "";
										} else {
											runningMapsProgress = runningMapsProgress + "-"
													+ SmartReader.runningMapsProgress1.get(i);
										}
									}

									last = "metricsType=" + "XdataLocalityMapsProgress" + ",caseId=" + caseId
											+ ",mapsProgress=" + runningMapsProgress + ",jobId=" + jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									String runningMapsExecutionTime = "";
									for (int i = 0; i < SmartReader.runningMapsExecutionTime1.size(); i++) {
										if (i == 0) {
											runningMapsExecutionTime = SmartReader.runningMapsExecutionTime1.get(i)
													+ "";
										} else {
											runningMapsExecutionTime = runningMapsExecutionTime + "-"
													+ SmartReader.runningMapsExecutionTime1.get(i);
										}
									}

									last = "metricsType=" + "XdataLocalityMapsExecTime" + ",caseId=" + caseId
											+ ",mapsExecTime=" + runningMapsExecutionTime + ",jobId=" + jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									String stragglers = "";
									for (int i = 0; i < getLowPerformanceRunningMapsName.size(); i++) {
										if (i == 0) {
											stragglers = getLowPerformanceRunningMapsName.get(i);
										} else {
											stragglers = stragglers + "-" + getLowPerformanceRunningMapsName.get(i);
										}
									}

									last = "metricsType=" + "XdataLocalityStragglers" + ",caseId=" + caseId
											+ ",mapsName=" + stragglers + ",factor=" + SmartReader.factor + ",jobId="
											+ jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									// -----------------------------------------------------------

									String nonLocalStragglers = "";
									for (int i = 0; i < nonLocalRunningMapsName.size(); i++) {
										if (i == 0) {
											nonLocalStragglers = nonLocalRunningMapsName.get(i) + "";
										} else {
											nonLocalStragglers = nonLocalStragglers + "-"
													+ nonLocalRunningMapsName.get(i);
										}
									}

									last = "metricsType=" + "XdataLocalityNonLocalStragglers" + ",caseId=" + caseId
											+ ",nonLocalStragglers=" + nonLocalStragglers + ",jobId=" + jobId;

									System.out.println(" [x] Sent >>>>  '" + last + "'");

									try {
										channel.basicPublish("", QUEUE_NAME, null, last.getBytes());
									} catch (IOException e) {
										e.printStackTrace();
									}

									preCaseId = caseId;

								} else {
									System.out.println("The info has already been sent to the DB..");
								}

							} else {
								System.out.println("Non-local maps do not make the makespan problem..");
							}
						}

					} else {
						System.out.println();
						System.out.println("There is no any stragglers...");
					}

					System.out
							.println("******************************************************************************");

//					long now1 = Instant.now().toEpochMilli();
//					System.out.println("y= " + now1);
//
//					System.out.println("fark = "  + (now1 - now));

//					try {
//						TimeUnit.SECONDS.sleep(1);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}

					if (!checkClusterHomogeneity) {
						lock.flag = 2;
						lock.notifyAll();
					} else {
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}

//			} catch (Exception e) {
//				System.out.println("Exception in DataLocality: " + e.getMessage());
//				System.out.println(e);
//			}
		} // while
//		threadDataLocalityCalculator.start();
	}
}
