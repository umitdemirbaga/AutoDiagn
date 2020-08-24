package Makespan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.json.simple.parser.ParseException;

import com.db.influxdb.SmartReader;

public class DetectorForMakespan {

	public static ResourceLock lock = new ResourceLock();

	public static Streaming threadStreaming = new Streaming();
	public static DataLocality threadDataLocality = new DataLocality(lock);
	public static HeterogeneousCluster threadHeterogeneousCluster = new HeterogeneousCluster(lock);
//	public static UnnecessarySpeculation threadUnnecessarySpeculation = new UnnecessarySpeculation(lock);
//	public static ResourceOverAllocation threadResourceOverAllocation = new ResourceOverAllocation();
//	public static ResourceShortage threadResourceShortage = new ResourceShortage();

	public static String jobId = "";

	public static void main(String[] args) throws Exception {

//		jobId= "1583079278697_0002";

//		Scanner read = new Scanner(System.in);
//		System.out.println("Enter the jobId: ");
//		jobId = read.next();
		// 1583079278697_0002

		threadStreaming.setPriority(Thread.MAX_PRIORITY);
		threadDataLocality.setPriority(threadStreaming.getPriority() - 1);
		threadHeterogeneousCluster.setPriority(threadDataLocality.getPriority() - 2);
//		threadUnnecessarySpeculation.setPriority(threadDataLocality.getPriority() - 3);
//		threadResourceShortage.setPriority(threadDataLocality.getPriority()- 4);
//		threadResourceOverAllocation.setPriority(Thread.MIN_PRIORITY);
		

		int getNumJobs = SmartReader.getNumJobs();
		String lastJobNo = SmartReader.getLastJobNo();
		double lastJobMapProgress = SmartReader.getLastJobMapProgress(lastJobNo);

		if (lastJobNo.equalsIgnoreCase("")) {
			System.out.println();
			System.out.println(">>>> Db 'Metrics' is empty or does not exist! <<<<  ");
			System.out.println("-------------------------------------------------------");
		}

		// to trigger the makespan detector
		System.out.println();
		System.out.println("Waiting for a new job to debug!");

		while (true) {

			if (SmartReader.getNumJobs() != 0) {

				if (SmartReader.getNumJobs() > getNumJobs) {
					jobId = SmartReader.getLastJobNo();
					System.out.println();
					System.out.println();
					System.out.println("-----------------------------------------------------------");
					System.out.println("The job '" + jobId + "' is debugging now..");
					System.out.println("-----------------------------------------------------------");
					System.out.println();
					break;
				
					// means the last job has not finished.
				} else if ((SmartReader.getNumJobs() == getNumJobs) && (lastJobMapProgress < 100.0)) { 
					jobId = SmartReader.getLastJobNo();
					System.out.println();
					System.out.println("The job '" + jobId + "' already running is debugging...");
					System.out.println("------------------------------------------------------------");
					System.out.println();

					break;
				} else if ((SmartReader.getNumJobs() == getNumJobs) && (lastJobMapProgress == 100.0)) {
					System.out.print(".");
				}
			} else if ((SmartReader.getNumJobs() == 0)) {
				System.out.print(".");
			}

			try {
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} // while

		Thread.sleep(3000); // ACTIVE THIS

		threadStreaming.start();
		threadDataLocality.start();
		threadHeterogeneousCluster.start();
//		threadUnnecessarySpeculation.start();
//		threadResourceOverAllocation.start();
//		threadResourceShortage.start();

	} // main class

}
