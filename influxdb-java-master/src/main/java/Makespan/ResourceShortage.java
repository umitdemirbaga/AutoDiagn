package Makespan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.db.influxdb.*;

import org.json.simple.parser.ParseException;

public class ResourceShortage extends Thread {

	public void run() {
		String jobId = DetectorForMakespan.jobId;
		boolean checkResourceShortage = false;
		int getRunningMapsNum = 0; 
		double getJobMapProgress = 0;

		while (true) {
			
			try {
				getJobMapProgress = SmartReader.getJobMapProgress(jobId);
			} catch (IOException | URISyntaxException | ParseException e1) {
				e1.printStackTrace();
			}
			if (getJobMapProgress == 100.0) {
				break;
			}
			
			try {
				getRunningMapsNum = SmartReader.getRunningMapsNum(jobId);
			} catch (IOException | URISyntaxException | ParseException e1) {
				e1.printStackTrace();
			}

			if (getRunningMapsNum > 0) {
				
				// consider disk usage info from resource  !!!!!!!!!!!!!!!!!!!!

				try {
					checkResourceShortage = SmartReader.checkResourceShortage(jobId);
				} catch (IOException | URISyntaxException | ParseException e) {
					e.printStackTrace();
				}

				if (checkResourceShortage) {
					System.out.println(
							"!!! Not having enough resource have caused the makespan problem.");;
				} else
					System.out.println("There is no Map Synchronization problem...");

				System.out.println("--------------------------");

			} // if

			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
}
