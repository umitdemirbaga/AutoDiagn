package Makespan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.db.influxdb.*;

import org.json.simple.parser.ParseException;

public class ResourceOverAllocation extends Thread {

	public void run() {
		String jobId = DetectorForMakespan.jobId;
		int numMapsPending = 0;
		double clusterActualCpuUsage = 0;
		double clusterActualMemoryUsage = 0;
//		List<Integer> runningContainersVCores = null;
		List<Integer> runningContainersMemory = null;
		List<Double> runningMapsMemoryUsage = null;
		List<Double> runningMapsCpuUsage = null;

		boolean resourceAllocation = true;
		int getRunningMapsNum = 0;
		double getJobMapProgress = 0;

		while (true) {

			try {
				numMapsPending = SmartReader.getNumMapsPending(jobId);
			} catch (IOException | URISyntaxException | ParseException e1) {
				e1.printStackTrace();
			}
			
			try {
				getJobMapProgress = SmartReader.getJobMapProgress(jobId);
			} catch (IOException | URISyntaxException | ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if (getJobMapProgress == 100.0) {
				break;
			}
			
			try {
				getRunningMapsNum = SmartReader.getRunningContainersNum(jobId) - 1;
			} catch (IOException | URISyntaxException | ParseException e1) {
				e1.printStackTrace();
			}

			if ((getRunningMapsNum > 0) && (numMapsPending > 0)) {
				resourceAllocation = true;

				try {
					clusterActualCpuUsage = SmartReader.getDataNodesAverageCpuUsage(jobId);
				} catch (IOException | URISyntaxException | ParseException e1) {
					e1.printStackTrace();
				}

				try {
					clusterActualMemoryUsage = SmartReader.getDataNodesAverageMemoryUsage(jobId);
				} catch (IOException | URISyntaxException | ParseException e1) {
					e1.printStackTrace();
				}

				if (clusterActualCpuUsage < 95 || clusterActualMemoryUsage < 95) {

//				try {
//					runningContainersVCores = SmartReader.getRunningContainersVCores(jobId);
//				} catch (IOException | URISyntaxException | ParseException e1) {
//					e1.printStackTrace();
//				}

					try {
						runningContainersMemory = SmartReader.getRunningContainersMemory(jobId);
					} catch (IOException | URISyntaxException | ParseException e1) {
						e1.printStackTrace();
					}

					try {
						runningMapsCpuUsage = SmartReader.getRunningMapsCpuUsage(jobId);
					} catch (IOException | URISyntaxException | ParseException e1) {
						e1.printStackTrace();
					}

					try {
						runningMapsMemoryUsage = SmartReader.getRunningMapsMemoryUsage(jobId);
					} catch (IOException | URISyntaxException | ParseException e1) {
						e1.printStackTrace();
					}

					System.out.println("numMapsPending= " + numMapsPending);
					System.out.println("clusterCpuUsage= " + clusterActualCpuUsage);
					System.out.println("clusterMemoryUsage= " + clusterActualMemoryUsage);
//					System.out.println("runningContainersVCores = " + runningContainersVCores);
					System.out.println("runningContainersMemory= " + runningContainersMemory);
					System.out.println("runningMapsMemoryUsage= " + runningMapsMemoryUsage);
					System.out.println("runningMapsCpuUsage= " + runningMapsCpuUsage);
					System.out.println();

					for (int i = 0; i < getRunningMapsNum; i++) {
						if (Double.valueOf(runningContainersMemory.get(i)) // do this for cpu too.
								.compareTo(runningMapsMemoryUsage.get(i)) < 0) {
							resourceAllocation = false;
							break;
						}
					}
					
					
					for (int i = 0; i < getRunningMapsNum; i++) {
						if (Double.valueOf(runningContainersMemory.get(i)) // do this for cpu too.
								.compareTo(runningMapsMemoryUsage.get(i)) < 0) {
							resourceAllocation = false;
							break;
						}
					}
				}

				if (!resourceAllocation) {
					System.out.println("!!! There is Resource over-allocation problem, which prolongs the makespan.");
				} else
					System.out.println("There is no Resource over-allocation problem. ");

				System.out.println("--------------------------");
			} // if
			else if ((getRunningMapsNum > 0) && (numMapsPending == 0)) {
				System.out.println("There is no Resource over-allocation problem. ");
			}
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} // while
	}
}
