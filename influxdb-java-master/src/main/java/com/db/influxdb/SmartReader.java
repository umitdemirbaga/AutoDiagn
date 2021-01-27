package com.db.influxdb;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.db.influxdb.Configuration;
import com.db.influxdb.DataReader;
import com.db.influxdb.DataWriter;
import com.db.influxdb.Query;
import com.db.influxdb.ResultSet;
import com.db.influxdb.Utilities;
import com.google.gson.Gson;

import Makespan.DataLocality;
import Makespan.DataLocalityCalculator;
import Makespan.DetectorForMakespan;
import Makespan.HeterogeneousCalculator;
import Makespan.HeterogeneousCluster;

/*
 * Copyright (c) 2019 Umit Demirbaga
 * Newcastle University
 */

public class SmartReader {

//	public static String rabbitMQhost = "172.31.0.117"; // AWS
//	public static String rabbitMQhost = "192.168.56.6"; // laptop
	
	public static String rabbitMQhost = DetectorForMakespan.rabbitMQhost;
	public static Configuration configuration = new Configuration(rabbitMQhost, "8086", "umit", "umit", "Metrics");
	public static Query query = new Query();
	public static String jobId = "15892993967090013";
	public static String blockId = "0";
	public static String hostName = "slave1";
	public static String mapId = "Map1";

	public static double factor = 1.5;

	public static List<String> runningMapsName1 = new ArrayList<String>();

	public static List<String> lowPerformanceRunningMapsName1 = new ArrayList<String>();

	public static List<String> nonLocalRunningMapsName1 = new ArrayList<String>();

	public static double runningMapsPerformanceMedian1 = 0.0;

	public static List<Double> runningMapsPerformance1 = new ArrayList<Double>();

	public static List<Double> runningMapsPerformanceNorm1 = new ArrayList<Double>();

	public static List<Double> runningMapsProgress1 = new ArrayList<Double>();

	public static List<Integer> runningMapsExecutionTime1 = new ArrayList<Integer>();

	public static List<Double> exacTimeListNorm = new ArrayList<Double>();

	public static List<Double> progressListNorm = new ArrayList<Double>();

	public static List<String> runningSpeculativeMapsName1 = new ArrayList<String>();

//	public static List<String> runningNormalMapsName1 = new ArrayList<String>();

	public static List<String> getNodesHostLowPerformanceRunningMaps1 = new ArrayList<String>();

	public static List<String> getDataNodesNameHavingMaxVCoresNum1 = new ArrayList<String>();

	public static List<String> getRunningNormalMapsName1 = new ArrayList<String>();

	public static List<String> getNodesHostRunningNormalMaps1 = new ArrayList<String>();

	public static List<String> getLowPerformanceRunningMapsHostName1 = new ArrayList<String>();

	public static List<String> getNodesCommon1 = new ArrayList<String>();

	public static List<Double> getNodesCommonCpuUsage1 = new ArrayList<Double>();

	public static List<Double> getNodesCommonMemoryUsage1 = new ArrayList<Double>();

	public static List<String> getNodesHostRunningSpeculativeMaps1 = new ArrayList<String>();

	public static List<String> getRunningNormalMapsHostName1 = new ArrayList<String>();

	public static List<String> getRunningSpeculativeMapsHostName1 = new ArrayList<String>();

	public static int runningMapsCaseId = 0;

	public static List<String> succeededMapsName1 = new ArrayList<String>();

	public static List<String> getSucceededStragglersNameRunningSlowNodes1 = new ArrayList<String>();

	public static List<String> getSucceededMapsNameRunningHighNodes1 = new ArrayList<String>();

	public static List<String> killedMapsName1 = new ArrayList<String>();

	public static List<String> getFinishedAllMapsName1 = new ArrayList<String>();

	public static List<String> finishedAllNonLocalMapsName1 = new ArrayList<String>();

	// NETWORK ISSUES
	// ****************************************************************************************

	public static List<String> disconnectedNodes = new ArrayList<String>();

	public static List<String> mapsOnDisconnectedNodes = new ArrayList<String>();

	public static List<String> runningMapsRestarted = new ArrayList<String>();

	public static List<String> lowDownloadNodes = new ArrayList<String>();

	public static List<String> mapsOnLowDownloadNodes = new ArrayList<String>();
	
	public static List<String> dataNodesNamesForBandwidth = new ArrayList<String>();
	
	public static List<Double> dataNodesDownloadSpeeds = new ArrayList<Double>();

//	public static List<String> runningMapsName() {
//		return runningMapsName1;
//	}

//	public static void main(String[] args) throws Exception {
//		System.out.println("getMapsOnSlowDataNodes11 = " + getMapsOnSlowDataNodes(jobId));
//		System.out.println("getDataNodesNameHavingMaxVCoresNum = " + getDataNodesNameHavingMaxVCoresNum(jobId));
//		System.exit(0);
//		System.out.println("getSucceededMapsName: " + getSucceededMapsName(jobId));
//
//		System.out.println("getKilledMapsNum: " + getKilledMapsNum(jobId));
//
//		System.out.println("getKilledMapsName: " + getKilledMapsName(jobId));
//
//		System.out.println("getFinishedAllNormalMapsName: " + getFinishedAllNormalMapsName(jobId));
//
//		System.out.println("getFinishedAllMapsName: " + getFinishedAllMapsName(jobId));

// for realTime  and PENDING and NO DATA LOCALITY PROBLEM      (homogeneous cluster) -->   1582987641806_0001  --> Metrics  
// for realTime  and SPECULATIVE 							   (homogeneous cluster) -->   1583079278697_0002  --> Metrics 

// for real-time 	(homogeneous cluster) --> 1582673845027_0008    --> Metrics 
// for batch 		(homogeneous cluster) --> 1582673845027_0007    --> Metrics 	

//		System.out.println("checkJobPerformance: " + checkJobPerformance(jobId));
//		// return the performance of the job.

//		System.out.println("getNodesHostLowProgressRunningMaps: " + getNodesHostLowProgressRunningMaps(jobId));
		// return the unique name of the host which hosts the maps which show low
		// progress.

//		System.out.println(
//		"getRunningMapsProgressConfidenceInterval: " + getRunningMapsProgressConfidenceInterval(jobId));
//		// return the confidence interval of the progress of the running maps

//		System.out.println("getRunningMapsExecutionTimeConfidenceInterval: " + getRunningMapsExecutionTimeConfidenceInterval(jobId));
		// return the confidence interval of the execution time of the running maps

//		System.out.println("getHighExecutionTimeRunningMapsHostName: " + getHighExecutionTimeRunningMapsHostName(jobId));
		// return the name of the host which hosts the maps which show high
		// executiontime.

//		System.out.println("getLowProgressRunningMapsName: " + getLowProgressRunningMapsName(jobId));
//		// return the name of the maps which show low progress.

//		System.out.println("getLowProgressRunningMaps: " + getLowProgressRunningMaps(jobId));
//		// return the progress of the maps having low progress as a percent.

//		System.out.println("getLowProgressRunningMapsHostName: " + getLowProgressRunningMapsHostName(jobId));
//		// return the name of the host which hosts the maps which show low progress.

//		System.out.println("getHighExecutionTimeRunningMapsName: " + getHighExecutionTimeRunningMapsName(jobId));
		// return the name of the maps having high execution time out of Confidence
		// Interval.

//		System.out.println("getHighExecutionTimeRunningMaps: " + getHighExecutionTimeRunningMaps(jobId));
		// return the execution time of the maps having high execution time out of
		// Confidence Interval.

//		System.out.println("checkRunningReducesPerformance: " + checkRunningReducesPerformance(jobId));
//		// return the performance of all the maps.

//		System.out.println("checkSucceededReducesPerformance: " + checkSucceededReducesPerformance(jobId));
//		// return the performance of all the reduces.

//		while (true) {
//	
//		System.out.println("getDate: " + getDate(jobId));
//		// return the data as an epoch time.
//
//		System.out.println("calculateDownloadKb: " + calculateDownloadKb(jobId));
//		// return the total size for downloaded data in kb.
//
//		System.out.println("checkMapDataLocality: " + checkMapDataLocality(jobId, mapId));
//		// return false if the map has no data locally
//
//		System.out.println("------------------------------------------------------");
//
//		System.out.println("getNumJobs: " + getNumJobs());
//		// return the number of jobs in the database.
//
//		System.out.println("getLastJobNo: " + getLastJobNo());
//		// return the no of the last job in the database.
//
//		System.out.println("getLastJobMapProgress: " + getLastJobMapProgress(jobId));
//		// return the map progress of the last job in the database.
//
//		System.out.println("getMasterNodeName: " + getMasterNodeName(jobId));
//		// return the host name of the master node
//
//		System.out.println("getDataNodesNames: " + getDataNodesNames(jobId));
//		// return the names of the datanodes.
//
//		System.out.println("getHighDataNodesName: " + getHighDataNodesName(jobId));
//
//		System.out.println("getMasterNodeCpuUsage: " + getMasterNodeCpuUsage());
//		// return the cpu usage of the master node.
//
//		System.out.println("getMasterNodeMemoryUsage: " + getMasterNodeMemoryUsage());
//		// return the memory usage of the master node.
//
//		System.out.println("getDataNodesCpuUsage: " + getDataNodesCpuUsage(jobId));
//		// return the cpu usage of the all nodes.
//
//		System.out.println("getDataNodesAverageCpuUsage: " + getDataNodesAverageCpuUsage(jobId));
//		// return the average cpu usage of the all nodes.
//
//		System.out.println("getDataNodesMemoryUsage: " + getDataNodesMemoryUsage(jobId));
//		// return the memory usage of the all nodes.
//
//		System.out.println("getDataNodesAverageMemoryUsage: " + getDataNodesAverageMemoryUsage(jobId));
//		// return the average memory usage of the all nodes.
//
//		System.out.println("getDataNodesVCoresNum: " + getDataNodesVCoresNum(jobId));
//		// return the cpu VCores num of the all nodes.
//
//		System.out.println("getDataNodesNameHavingMaxVCoresNum: " + getDataNodesNameHavingMaxVCoresNum(jobId));
//		// return the name of the nodes which has max VCores num.
//
//		System.out.println("getDataNodesTotalMemory: " + getDataNodesTotalMemory(jobId));
//		// return the memory usage of the all nodes.
//
//		System.out.println("getDataNodesDiskReadSpeed: " + getDataNodesDiskReadSpeed(jobId));
//		// return the disk read speed of all the nodes as MB/s (megabit per second)
//
//		System.out.println("getDataNodesDiskWriteSpeed: " + getDataNodesDiskWriteSpeed(jobId));
//		// return the disk write speed of all the nodes as Mb/s (megabit per second)
//
//		System.out.println("------------------------------------------------------");
//
//		System.out.println("getMapHost: " + getMapHost(jobId, mapId));
//		// return the name of the node this map ran on.
//
//		System.out.println("getMapBlockId: " + getMapBlockId(jobId, mapId));
//		// return the blockId of the block this map processes.
//
//		System.out.println("getJobMakespan: " + getJobMakespan(jobId));
//		// return the elapsed time since the job started in sec.
//
//		System.out.println("getJobProgress: " + getJobProgress(jobId));
//		// return the progress of the job as a percent.
//
//		System.out.println("getClusterCpuUsage: " + getClusterCpuUsage(jobId));
//		// return the percentage of the resources usage of the cluster.
//
//		System.out.println("getClusterMemoryUsage: " + getClusterMemoryUsage(jobId));
//		// return the percentage of the resources usage of the cluster.
//
//		System.out.println("getJobClusterUsage: " + getJobClusterUsage(jobId));
//		// return the percentage of the resources of the cluster that the job is using.
//
//		System.out.println("getClusterAvailableMemory: " + getClusterAvailableMemory(jobId));
//		// return the available memory in the cluster in mb.
//
//		System.out.println("getClusterAvailableVCores: " + getClusterAvailableVCores(jobId));
//		// return the number of available virtual cores in the cluster.
//
//		System.out.println("getClusterTotalDataNodes: " + getClusterTotalDataNodes(jobId));
//		// return the total number of data nodes.
//
//		System.out.println("------------------------------------------------------");
//
//		System.out.println("getInputDataSize: " + getInputDataSize(jobId));
//		// return the size of input data in mb.
//
//		System.out.println("getAppMasterLocation: " + getAppMasterLocation(jobId));
//		// return the location of Application Master for the job.
//
////		System.out.println("getInputReplicaNum: " + getInputReplicaNum(jobId));
////		// returns the number of replication of the input file.
////
////		System.out.println("getInputLocations: " + getInputLocations(jobId));
////		// returns the locations of the blocks of the input files.
//
//		System.out.println("getBlockLocations: " + getBlockLocations(jobId, blockId));
//		// returns the locations of the block.
//
//		System.out.println("getBlockReplicaNum: " + getBlockReplicaNum(jobId, blockId));
//		// returns the number of replication of the block.
//
//		System.out.println("getBlockSizeMB: " + getBlockSizeMB(jobId, blockId));
//		// returns the size of the blocks of the input files.
//
//		System.out.println("------------------------------------------------------");
//
//		System.out.println("getTotalMapNum: " + getTotalMapNum(jobId));
//		// return the total number of maps.
//
//		System.out.println("getTotalTasksNum: " + getTotalTasksNum(jobId));
//		// return the total number of maps including speculative executions.
//
//		System.out.println("getRunningMapsNum: " + getRunningMapsNum(jobId));
//		// return the total number of running maps.
//
//		System.out.println("getNumMapsPending: " + getNumMapsPending(jobId));
//		// return the number of map still to be run.
//
//		System.out.println("getRunningNormalMapsName: " + getRunningNormalMapsName(jobId));
////		return the names of running normal maps. 
//
//		System.out.println("getRunningNormalMapsHostName: " + getRunningNormalMapsHostName(jobId));
//		// return the name of the host which hosts the normal maps.
//
//		System.out.println("getNodesHostRunningNormalMaps: " + getNodesHostRunningNormalMaps(jobId));
//		// return the names of nodes which host the running normal maps.
//
//		System.out.println("getRunningSpeculativeMapsNum: " + getRunningSpeculativeMapsNum(jobId));
//		// return the names of speculative running maps.
//
//		System.out.println("getRunningSpeculativeMapsName: " + getRunningSpeculativeMapsName(jobId));
////		return the names of speculative running maps.
//
//		System.out.println("getRunningSpeculativeMapsHostName: " + getRunningSpeculativeMapsHostName(jobId));
//		// return the name of the host which hosts the speculative executions.
//
//		System.out.println("getNodesHostRunningSpeculativeMaps: " + getNodesHostRunningSpeculativeMaps(jobId));
//		// return the names of nodes which host the running speculative maps.
//
//		System.out.println("getNodesHostSpeculativeMapsCpuUsage: " + getNodesHostSpeculativeMapsCpuUsage(jobId));
////		return the CPU usage of the nodes which host the speculative executions. 
//
//		System.out.println("getNodesHostSpeculativeMapsMemoryUsage: " + getNodesHostSpeculativeMapsMemoryUsage(jobId));
////		return the Memory usage of the nodes which host the speculative executions. 
//
//		System.out.println("getNodesCommon: " + getNodesCommon(jobId));
//		// return the names of nodes which host the running normal maps and speculative
//		// executions.
//
//		System.out.println("getNodesCommonCpuUsage: " + getNodesCommonCpuUsage(jobId));
////		return the CPU usage of the common nodes.  
//
//		System.out.println("getNodesCommonMemoryUsage: " + getNodesCommonMemoryUsage(jobId));
////		return the Memory usage of the common nodes.  
//
//		System.out.println("getRunningContainersNum: " + getRunningContainersNum(jobId));
//		// return the total number of containers in the cluster.
//
//		System.out.println("getRunningMapsName: " + getRunningMapsName(jobId));
////		return the names of running maps.
//
//		System.out.println("getRunningMapsCaseId: " + getRunningMapsCaseId(jobId));
////		return the case Id of running maps.
//
//		System.out.println("getRunningMapsMemoryUsage: " + getRunningMapsMemoryUsage(jobId));
////		// return the physicalMemoryMb of running maps.
//
//		System.out.println("getRunningMapsCpuUsage: " + getRunningMapsCpuUsage(jobId));
////		// return the physicalMemoryMb of running maps.
//
//		System.out.println("getRunningMapsExecutionTime: " + getRunningMapsExecutionTime(jobId));
//		// return the execution time since the running maps have started in sec.
//
//		System.out.println();
//
//		System.out.println("getRunningMapsProgress: " + getRunningMapsProgress(jobId));
//		// return the progress of all the maps as a percent.
//
//		System.out.println("getRunningMapsPerformance: " + getRunningMapsPerformance(jobId));
//		// return the performance of all the running maps.
//
//		System.out.println("getRunningMapsAveragePerformance: " + getRunningMapsAveragePerformance(jobId));
//		// return the average performance of all the running maps.
//
////		System.out.println("getRunningMapsAveragePerformance: " + getRunningMapsAveragePerformanceOLD(jobId));
////		// return the median value of all the running maps' performance.
//
////		System.out.println("getLowPerformanceRunningMapsName: " + getLowPerformanceRunningMapsNameOLD(jobId));
////		// return the name of the maps which show low performance.
//
//		System.out.println("getLowPerformanceRunningMapsName: " + getLowPerformanceRunningMapsName(jobId));
//		// return the name of the maps which show low performance median.
//
//		System.out.println("getLowPerformanceRunningMapsHostName: " + getLowPerformanceRunningMapsHostName(jobId));
//		// return the name of the host which hosts the maps which show low progress.
//
//		System.out.println("getNodesHostLowPerformanceRunningMaps: " + getNodesHostLowPerformanceRunningMaps(jobId));
//		// return the unique name of the host which hosts the maps which show low
//		// performance.
//
//		System.out.println("getLocalRunningMapsPerformance: " + getLocalRunningMapsPerformance(jobId));
//		// return the performance of all the local maps.
//
//		System.out.println("getNonLocalRunningMapsPerformance: " + getNonLocalRunningMapsPerformance(jobId));
//		// return the performance of all the non-local maps.
//
//		System.out.println();
//
//		System.out.println("getLocalRunningMapsName: " + getLocalRunningMapsName(jobId));
//		// return the name of the maps which has no local data
//
//		System.out.println("getNonLocalRunningMapsName: " + getNonLocalRunningMapsName(jobId));
//		// return the name of the maps which has no local data.
//
//		System.out.println("getNonLocalRunningMapsProgress: " + getNonLocalRunningMapsProgress(jobId));
//		// return the progress of all the maps which has no local data as a percent.
//
//		System.out.println("getLowestMapSpecs: " + getLowestMapSpecs(jobId));
//		// return the name and locations of the slowest map.
//
//		System.out.println("getJobMapProgress: " + getJobMapProgress(jobId));
//		// return the average progress of all the maps as a percent.
//
//		System.out.println("getRunningMapAssignedContainers: " + getRunningMapAssignedContainers(jobId));
//		// return the container id which host the running maps.
//
//		System.out.println("getRunningContainersVCores: " + getRunningContainersVCores(jobId));
//		// return the allocated VCores for the containers which host the running maps.
//
//		System.out.println("getRunningContainersMemory: " + getRunningContainersMemory(jobId));
//		// return the allocated memory for the containers which host the running maps.
//
//		System.out.println("getRunningMapsHostName: " + getRunningMapsHostName(jobId));
//		// return the names of all nodes which host the running maps.
//
//		System.out.println("getNodesHostRunningMaps: " + getNodesHostRunningMaps(jobId));
//		// return the names of nodes which host the running maps, for getting with disk
//		// read speed.
//
//		System.out.println("getNodeTotalNumRunningContainers: " + getNodeTotalNumRunningContainers(jobId));
//		// return the number of running containers for each node.
//
//		System.out.println("getNodeTotalNumRunningMapsEachNode: " + getNodeTotalNumRunningMapsEachNode(jobId));
//		// return the number of running maps for each node.
//
//		System.out.println("------------------------------------------------------");
//
//		System.out.println("getSucceededMapsNum: " + getSucceededMapsNum(jobId));
//		// return the number of completed maps.
//
//		System.out.println("getSucceededMapsName: " + getSucceededMapsName(jobId));
//		// return the names of succeeded maps.
//
//		System.out.println("getSucceededMapsExecTime: " + getSucceededMapsExecTime(jobId));
//		// return the execution time of succeeded maps.
//
//		System.out.println("getSucceededLocalMapsName: " + getSucceededLocalMapsName(jobId));
//		// return the name of non-local succeeded.
//
//		System.out.println("getSucceededLocalMapsExecTime: " + getSucceededLocalMapsExecTime(jobId));
//		// return the execution time of succeeded maps.
//
//		System.out.println("getSucceededNonLocalMapsName: " + getSucceededNonLocalMapsName(jobId));
//		// return the name of non-local succeeded.
//
//		System.out.println("getSucceededNonLocalMapsExecTime: " + getSucceededNonLocalMapsExecTime(jobId));
//		// return the execution time of succeeded maps.
//
//		System.exit(0);
//
//		System.out.println("getSucceededMapsHostName: " + getSucceededMapsHostName(jobId));
//		// return the names of all nodes which host the succeeded maps.
//
//		System.out.println("getNodesHostSucceededMaps: " + getNodesHostSucceededMaps(jobId));
//		// return the names of nodes which host the succeeded maps.
//
//		System.out.println("getNodesHostSucceededMapsDiskReadSpeed: " + getNodesHostSucceededMapsDiskReadSpeed(jobId));
//		// return the disk read speed of the nodes which host the succeeded maps GB/s
//		// (gigabit per second)
//
//		System.out.println("------------------------------------------------------");
//
//		System.out.println("getRunningReducesNum: " + getRunningReducesNum(jobId));
//		// return the total number of running maps.
//
//		System.out.println("getRunningReducesName: " + getRunningReducesName(jobId));
////		// return the names of running reduces.
//
//		System.out.println("getRunningReducesHostName: " + getRunningReducesHostName(jobId));
////		// return the names of all nodes which host the running reduces.
//
//		System.out.println("getNodesHostRunningReduces: " + getNodesHostRunningReduces(jobId));
////		// return the names of nodes which host the running reduces.
//
//		System.out.println("getRunningReducesShuffleExecutionTime: " + getRunningReducesShuffleExecutionTime(jobId));
//		// return the execution time since the running reduces have started in sec.
//
//		System.out.println("getRunningReducesShuffleMb: " + getRunningReducesShuffleMb(jobId));
//		// return the data size of maps outputs copied to reduce in mb.
//
//		System.out.println("getRunningReducesMergedMapsNum: " + getRunningReducesMergedMapsNum(jobId));
//		// return the number of merged maps for each reduce.
//
//		System.out.println("getSucceededReducesNum: " + getSucceededReducesNum(jobId));
//		// return the number of succeeded reduces.
//
//		System.out.println("getSucceededReducesName: " + getSucceededReducesName(jobId));
//		// return the name of succeeded reduces.
//
//		System.out.println("getSucceededReducesHostName: " + getSucceededReducesHostName(jobId));
//		// return the names of all nodes which host the succeeded maps.
//
//		System.out.println("getSucceededReducesMergedMapsNum: " + getSucceededReducesMergedMapsNum(jobId));
//		// return the number of merged maps for each reduce.
//
//		System.out
//				.println("getSucceededReducesShuffleExecutionTime: " + getSucceededReducesShuffleExecutionTime(jobId));
//		// return the execution time since the running reduces have started in sec.
//
//		System.out.println("getSucceededReducesShuffleMb: " + getSucceededReducesShuffleMb(jobId));
//		// return the data size of maps outputs copied to reduce in mb.
//
//		System.out.println("------------------------------------------------------");
//
//		System.out.println("checkMasterNodeType: " + checkMasterNodeType(jobId));
//		// return false if the master node is used as a datanode as well.
//
//		System.out.println("checkMapNumTuning: " + checkMapNumTuning(jobId, blockId));
//		// return false if the number of maps is not set appropriately!..
//
//		System.out.println("checkRunningMapsDataLocality: " + checkRunningMapsDataLocality(jobId));
//		// return false if any running maps has no data locally
//
//		System.out.println("checkClusterHomogeneity: " + checkClusterHomogeneity(jobId));
//		// return false the cluster is not homogeneous.
////
////		System.out.println("checkMapsSynchronization: " + checkMapsSynchronization(jobId));
////		// return the reasons for desynchronization if there is.
//
//		System.out.println("checkResourceShortage: " + checkResourceShortage(jobId));
//		// return true if there is a resource shortage problem.
//
//		System.out.println("checkUnnecessarySpeculation: " + checkUnnecessarySpeculation(jobId));
//		// return true if there is a unnecessary speculation.
//
//		System.out.println("checkHeterogeneousCluster: " + checkHeterogeneousCluster(jobId));
//		// return true if Heterogeneous cluster makes problem.
//		
//		System.out.println("getSucceededStragglersRunningSlowNodesHostName yyy + " + getSucceededStragglersRunningSlowNodesHostName(jobId));

//		System.out.println("******************************************************************************************************************");

//			TimeUnit.SECONDS.sleep(2);
//		} // while

//	}

	public static List<Double> getRunningMapsProgressConfidenceInterval(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Double> progressConfidenceInterval = new ArrayList<Double>();

		List<Double> runningMapsPerformance = new ArrayList<Double>();
		runningMapsPerformance = getRunningMapsProgress(jobId);

		SummaryStatistics stats = new SummaryStatistics();
		for (double val : runningMapsPerformance) {
			stats.addValue(val);
		}

		// Calculate 95% confidence interval
		double ci = calcMeanCI(stats, 0.95);
//        System.out.println(String.format("Mean: %f", stats.getMean()));
		double lower = stats.getMean() - ci;
		double upper = stats.getMean() + ci;
//        System.out.println(String.format("Confidence Interval 95%%: %f, %f", lower, upper));
		progressConfidenceInterval.add(lower);
		progressConfidenceInterval.add(upper);

		return progressConfidenceInterval;
	}

	public static List<Double> getRunningMapsExecutionTimeConfidenceInterval(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Double> executionTimeConfidenceInterval = new ArrayList<Double>();

		List<Integer> runningMapsExecutiontime = new ArrayList<Integer>();
		runningMapsExecutiontime = getRunningMapsExecutionTime(jobId);

		SummaryStatistics stats = new SummaryStatistics();
		for (double val : runningMapsExecutiontime) {
			stats.addValue(val);
		}

		// Calculate 95% confidence interval
		double ci = calcMeanCI(stats, 0.95);
//        System.out.println(String.format("Mean: %f", stats.getMean()));
		double lower = stats.getMean() - ci;
		double upper = stats.getMean() + ci;
//        System.out.println(String.format("Confidence Interval 95%%: %f, %f", lower, upper));
		executionTimeConfidenceInterval.add(lower);
		executionTimeConfidenceInterval.add(upper);

		return executionTimeConfidenceInterval;
	}

	private static double calcMeanCI(SummaryStatistics stats, double level) {
		try {
			// Create T Distribution with N-1 degrees of freedom
			TDistribution tDist = new TDistribution(stats.getN() - 1);
			// Calculate critical value
			double critVal = tDist.inverseCumulativeProbability(1.0 - (1 - level) / 2);
			// Calculate confidence interval
			return critVal * stats.getStandardDeviation() / Math.sqrt(stats.getN());
		} catch (MathIllegalArgumentException e) {
			return Double.NaN;
		}
	}

	public static String getAppMasterLocation(String jobId) throws IOException, URISyntaxException, ParseException {

		String appMasterLocation = "";
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select amHost from job where jobId='" + jobId + "' limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			appMasterLocation = (values[1].replace("]", "").replace("\"", ""));
		} catch (Exception e) {
			System.out.println("no appMasterLocation!");
			appMasterLocation = "";
		}
		return appMasterLocation;
	}

	// returns the number of replication of the input file.
	public static int getBlockReplicaNum(String jobId, String blockId)
			throws IOException, URISyntaxException, ParseException {

		int blockReplicaNum = 0;
		if (!blockId.equalsIgnoreCase("-1")) {

			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");
				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery(
						"select replicaNum from inputLoc where jobId='" + jobId + "' and blockId='" + blockId + "'");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");
				String[] values = (obj6.get(0).toString()).split(",");
				blockReplicaNum = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));

//				System.out.println("inputReplicaNum= " + inputReplicaNum);
			} catch (Exception e) {
//				System.out.println("no blockReplicaNum!");
				blockReplicaNum = 0;
			}
		}
		return blockReplicaNum;
	}

	// returns the number of replication of the input file.
	public static int getTotalTasksNum(String jobId) throws IOException, URISyntaxException, ParseException {

		int getTotalTasksNum = 0;

		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select totalRunningMapsNum from mapNum where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			getTotalTasksNum = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));

//				System.out.println("inputReplicaNum= " + inputReplicaNum);
		} catch (Exception e) {
//			System.out.println("no blockReplicaNum!");
			getTotalTasksNum = 0;
		}

		return getTotalTasksNum;
	}

//	public static List<String> getInputLocations(String jobId) throws IOException, URISyntaxException, ParseException {
//
//		int replicaNum = getInputReplicaNum(jobId);
//
//		List<String> inputLocations = new ArrayList<String>();
//
//		try {
//			query.setMeasurement("sigar");
//			query.setLimit(1000);
//			query.fillNullValues("0");
//			DataReader dataReader = new DataReader(query, configuration);
//			ResultSet resultSet = dataReader.getResult();
//			Query query1 = new Query();
//
//			for (int k = 0; k < replicaNum; k++) {
//				query1.setCustomQuery("select host" + (k + 1) + " from inputLoc where jobId='" + jobId + "'");
//				dataReader.setQuery(query1);
//				resultSet = dataReader.getResult();
//				Gson gson = new Gson();
//				String jsonStr = gson.toJson(resultSet);
//				JSONParser parser = new JSONParser();
//				JSONObject obj = (JSONObject) parser.parse(jsonStr);
//				JSONArray obj2 = (JSONArray) obj.get("results");
//				JSONObject obj3 = (JSONObject) obj2.get(0);
//				JSONArray obj4 = (JSONArray) obj3.get("series");
//				JSONObject obj5 = (JSONObject) obj4.get(0);
//				JSONArray obj6 = (JSONArray) obj5.get("values");
//
//				String[] values = (obj6.get(0).toString()).split(",");
//				inputLocations.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
//			}
//
//		} catch (Exception e) {
//			System.out.println("no inputLocations!");
//			inputLocations = null;
//		}
//		return inputLocations;
//	}

	public static List<String> getBlockLocations(String jobId, String blockId)
			throws IOException, URISyntaxException, ParseException {

//		int replicaNum = getInputReplicaNum(jobId);

		int replicaNum = getBlockReplicaNum(jobId, blockId);

		List<String> blockLocations = new ArrayList<String>();

		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();

			for (int k = 0; k < replicaNum; k++) {
				query1.setCustomQuery("select host" + (k + 1) + " from inputLoc where jobId='" + jobId
						+ "' and blockId='" + blockId + "'");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				blockLocations.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
			}

		} catch (Exception e) {
			System.out.println("no blockLocations!");
			blockLocations = null;
		}
		return blockLocations;
	}

	public static double getBlockSizeMB(String jobId, String blockId)
			throws IOException, URISyntaxException, ParseException {

		double getBlockSizeMB = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select blockSizeMB from inputLoc where blockId='" + blockId + "' and jobId='" + jobId + "'");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			getBlockSizeMB = Double.valueOf(values[1].replace("]", "").replace("\"", ""));

//				System.out.println("getBlockSizeMB= " + getBlockSizeMB);
		} catch (Exception e) {
//			System.out.println("no getBlockSizeMB!");
			getBlockSizeMB = 0;
		}
		return getBlockSizeMB;
	}

	public static boolean checkMapDataLocality(String jobId, String mapId)

			throws IOException, URISyntaxException, ParseException {
		String mapHost = getMapHost(jobId, mapId);
		String mapBlockId = getMapBlockId(jobId, mapId);

		List<String> getBlockLocations = new ArrayList<String>();

		getBlockLocations = getBlockLocations(jobId, mapBlockId);

//		System.out.println("getBlockLocations = " + getBlockLocations);

//		System.out.println("mapHost: " + mapHost);
//		System.out.println("mapBlockId: " + mapBlockId);
//		System.out.println("getBlockLocations: " + getBlockLocations);

		if (getBlockLocations.contains(mapHost)) {
//			System.out.println("Map host has the data. ");
			return true;
		} else {
//			System.out.println("This map has data locality problem!!! ");
			return false;
		}
	}

	public static boolean checkMapState(String jobId, String mapId)
			throws IOException, URISyntaxException, ParseException {

		String mapState = null;

		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select state from map where jobId='" + jobId + "' and mapId='" + mapId
					+ "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			mapState = values[1].replace("]", "").replace("\"", "");

		} catch (Exception e) {
			System.out.println("no mapState!");
			mapState = null;
		}

		if (mapState.equals("SUCCEEDED")) {
			return true;
		} else {
			return false;
		}
	}

	// return if any running maps has the datalocality problem.
	public static boolean checkRunningMapsDataLocality(String jobId)
			throws IOException, URISyntaxException, ParseException {

//		int runningMapsNum = getRunningMapsName(jobId).size();

		int runningMapsNum = 0;

		if (!(runningMapsName1 == null)) {
			runningMapsNum = runningMapsName1.size();
		}

//		List<String> runningMapsName = getRunningMapsName(jobId);

//		List<String> runningMapsName = runningMapsName1;

		int nonLocalMapNum = 0;

		try {
			if (runningMapsNum != 0) {
				String mapId = "";
				for (int i = 0; i < runningMapsNum; i++) {
					mapId = runningMapsName1.get(i);
//					System.out.println("mapId= " + mapId);
//					System.out.println("checkMapDataLocality = " + checkMapDataLocality(jobId, mapId));
					if (checkMapDataLocality(jobId, mapId)) {
//					System.out.println(mapId + " has the data locally..");
					} else {
//						System.out.println(mapId + " has data locality problem!!!!");
						nonLocalMapNum++;
					}
				}
//				System.out.println("-------------------");
			}
		} catch (Exception e) {
			System.out.println("no nonLocalMapNum!");
			nonLocalMapNum = 0;
		}

		if (nonLocalMapNum == 0) {
			return true;
		} else {
//			System.out.println(nonLocalMapNum + " maps have datalocality problem!.. ");
			return false;
		}
	}

	// return the name of the maps which has no local data.
	public static List<String> getLocalRunningMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapsName = getRunningMapsName(jobId);
		List<String> localRunningMapsName = new ArrayList<>();

		int runningMapsNum = 0;
		if (!(runningMapsName == null)) {
			runningMapsNum = runningMapsName.size();
		}

		try {
			if (runningMapsNum != 0) {
				String mapId = "";
				for (int i = 0; i < runningMapsNum; i++) {
					mapId = runningMapsName.get(i);
					if (checkMapDataLocality(jobId, mapId)) {
						localRunningMapsName.add(mapId);
					}
				}
			}
		} catch (Exception e) {
			System.out.println("no localRunningMapsName!");
			localRunningMapsName = null;
		}
		return localRunningMapsName;
	}

	// return the name of the maps which has no local data.
	public static List<String> getNonLocalRunningMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

//		List<String> runningMapsName = new ArrayList<>();
		List<String> nonLocalRunningMapsName = new ArrayList<>();

//		runningMapsName = runningMapsName1;

		try {
			if (!(runningMapsName1 == null)) {
				String mapId = "";

				for (int i = 0; i < runningMapsName1.size(); i++) {
					mapId = runningMapsName1.get(i);

					if (checkMapDataLocality(jobId, mapId)) {
//					System.out.println(mapId + " has the data locally..");
					} else {
						nonLocalRunningMapsName.add(mapId);
					}
//					Thread.sleep(50);
				}
			}
		} catch (Exception e) {
			System.out.println("no nonLocalRunningMapsName!");
			nonLocalRunningMapsName = null;
			nonLocalRunningMapsName1 = null;
		}

		nonLocalRunningMapsName1 = null;
		nonLocalRunningMapsName1 = nonLocalRunningMapsName;
		return nonLocalRunningMapsName;
	}

	public static List<String> getSucceededLocalMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededMapsName = getSucceededMapsName(jobId);
		List<String> succeededNonLocalMapsName = getSucceededNonLocalMapsName(jobId);

		try {
			if (succeededMapsName.size() != 0) {
				succeededMapsName.removeAll(succeededNonLocalMapsName);
			}
		} catch (Exception e) {
			System.out.println("no succeededNonLocalMapsName!");
			succeededMapsName = null;
		}

		return succeededMapsName;
	}

	public static List<String> getSucceededNonLocalMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededMapsName = getSucceededMapsName(jobId);
		List<String> succeededNonLocalMapsName = new ArrayList<>();

		try {
			if (succeededMapsName.size() != 0) {
				String mapId = "";

				for (int i = 0; i < succeededMapsName.size(); i++) {
					mapId = succeededMapsName.get(i);

					if (checkMapDataLocality(jobId, mapId)) {
//					System.out.println(mapId + " has the data locally..");
					} else {
						succeededNonLocalMapsName.add(mapId);
					}
//					Thread.sleep(50);
				}
			}
		} catch (Exception e) {
			System.out.println("no getSucceededNonLocalMapsName!");
			succeededNonLocalMapsName = null;
		}

		return succeededNonLocalMapsName;
	}

	public static List<String> getSucceededNonLocalStragglersName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nonLocalRunningStragglers = DataLocality.nonLocalRunningStragglers;
		List<String> succeededNonLocalStragglersName = new ArrayList<>();

		try {
			if (nonLocalRunningStragglers.size() != 0) {
				String mapId = "";

				for (int i = 0; i < nonLocalRunningStragglers.size(); i++) {
					mapId = nonLocalRunningStragglers.get(i);

					if (checkMapState(jobId, mapId)) {
						succeededNonLocalStragglersName.add(mapId);
					} else {
					}
				}
			}
		} catch (Exception e) {
			System.out.println("no succeededNonLocalStragglersName!");
			succeededNonLocalStragglersName = null;
		}

		return succeededNonLocalStragglersName;
	}

	public static List<String> getRunningMapAssignedContainers(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapsName = getRunningMapsName(jobId);
		int runningMapsNum = runningMapsName.size();
		List<String> runningMapAssignedContainers = new ArrayList<String>();

		for (int i = 0; i < runningMapsNum; i++) {

			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");
				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select assignedContainer from map where mapId='" + runningMapsName.get(i)
						+ "' and jobId='" + jobId + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");
				String[] values = (obj6.get(0).toString()).split(",");
				runningMapAssignedContainers.add(values[1].replace("]", "").replace("\"", ""));
			} catch (Exception e) {
				System.out.println("no runningMapAssignedContainers!");
				runningMapAssignedContainers = null;
			}
		}
		return runningMapAssignedContainers;
	}

	public static List<Integer> getRunningContainersVCores(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapAssignedContainers = getRunningMapAssignedContainers(jobId);
		List<Integer> runningContainersVCores = new ArrayList<Integer>();
		int numRunningMapAssignedContainers = runningMapAssignedContainers.size();

		for (int i = 0; i < numRunningMapAssignedContainers; i++) {

			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");
				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select allocatedVCores from container where containerId='"
						+ runningMapAssignedContainers.get(i) + "' and jobId='" + jobId + "'");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");
				String[] values = (obj6.get(0).toString()).split(",");
				runningContainersVCores.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "")));
			} catch (Exception e) {
				System.out.println("no runningContainersVCores!");
				runningContainersVCores = null;
			}
		}
		return runningContainersVCores;
	}

	public static List<Integer> getRunningContainersMemory(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapAssignedContainers = getRunningMapAssignedContainers(jobId);
		List<Integer> runningContainersMemory = new ArrayList<Integer>();
		int numRunningMapAssignedContainers = runningMapAssignedContainers.size();

		for (int i = 0; i < numRunningMapAssignedContainers; i++) {

			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");
				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select allocatedMemoryMb from container where containerId='"
						+ runningMapAssignedContainers.get(i) + "' and jobId='" + jobId + "'");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");
				String[] values = (obj6.get(0).toString()).split(",");
				runningContainersMemory.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "")));
			} catch (Exception e) {
//				System.out.println("no runningContainersMemory!");
				runningContainersMemory = null;
			}
		}
		return runningContainersMemory;
	}

	public static double getInputDataSize(String jobId) throws IOException, URISyntaxException, ParseException {
		double inputSize = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select inputSizeMb from dataDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			inputSize = Double.valueOf((values[1].replace("]", "").replace("\"", "")));
		} catch (Exception e) {
			System.out.println("no inputSize!");
			inputSize = 0;
		}
		return inputSize;
	}

	public static Long getDate(String jobId)
			throws IOException, URISyntaxException, ParseException, java.text.ParseException {
		Long millis = null;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select date from resource where hostName='slave2' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			String[] values = (obj6.get(0).toString()).split(",");
			String date = values[1].replace("]", "").replace("\"", "").replace("\\", "");
//			System.out.println(date);
			millis = new SimpleDateFormat().parse(date).getTime();
//			System.out.println(millis);

		} catch (Exception e) {
			System.out.println("no millis!");
			millis = (long) 0;
		}
		return millis;
	}

	public static int calculateDownloadKb(String jobId) throws IOException, URISyntaxException, ParseException {
		int total = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select downloadKb from resource where hostName='slave2'");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

//		System.out.println("the number of results: " + obj6.size());
			for (int i = 0; i < obj6.size(); i++) {
				String[] values = (obj6.get(i).toString()).split(",");
				int x = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//			System.out.println(x);
				total += x;
			}
//		System.out.println("Total= " + total);
		} catch (Exception e) {
			System.out.println("no total!");
			total = 0;
		}
		return total;
	}

	// return the total number of maps
	public static int getTotalMapNum(String jobId) throws IOException, URISyntaxException, ParseException {
		int totalMapNum = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select mapsTotal from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			totalMapNum = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("totalMapNum= " + totalMapNum);
		} catch (Exception e) {
			System.out.println("no totalMapNum!");
			totalMapNum = 0;
		}
		return totalMapNum;
	}

	// return the number of running maps
	public static int getRunningSpeculativeMapsNum(String jobId)
			throws IOException, URISyntaxException, ParseException {
		int runningSpeculativeMapsNum = 0;

		try {
			runningSpeculativeMapsNum = (getRunningContainersNum(jobId) - 1) - getRunningMapsNum(jobId);
			if (runningSpeculativeMapsNum < 0) {
				runningSpeculativeMapsNum = 0;
			}
		} catch (Exception e) {
			System.out.println("no runningSpeculativeMapsNum!");
			runningSpeculativeMapsNum = 0;
		}
		if (getRunningContainersNum(jobId) == 0) {
			runningSpeculativeMapsNum = 0;
		}
		return runningSpeculativeMapsNum;
	}

	// return the number of running maps
	public static int getRunningMapsNum(String jobId) throws IOException, URISyntaxException, ParseException {
		int runningMapsNum = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select mapsRunning from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			runningMapsNum = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("runningMapsNum= " + runningMapsNum);
		} catch (Exception e) {
			System.out.println("no runningMapsNum!");
			runningMapsNum = 0;
		}
		return runningMapsNum;
	}

	// return the name of the running maps
	public static List<String> getRunningMapsName(String jobId) throws IOException, URISyntaxException, ParseException {

		List<String> runningMapsName = new ArrayList<String>();
//		int getTotalTasksNum = getRunningContainersNum(jobId);

		int getTotalTasksNum = getTotalTasksNum(jobId);

//		System.out.println("getTotalTasksNum = " + getTotalTasksNum);

		String value = "";

		if (getTotalTasksNum > 1) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select mapId from map where jobId='" + jobId
						+ "' and state='RUNNING' and blockId <> '-1' order by desc limit " + getTotalTasksNum);
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					value = values[1].replace("]", "").replace("\"", "").replace("\\", "");
					if (!runningMapsName.contains(value)) {
						runningMapsName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					}

//					Thread.sleep(50);
				}
			} catch (Exception e) {
				System.out.println("no runningMapsName!");
				runningMapsName = null;
			}

			runningMapsName1 = runningMapsName;
		}
		return runningMapsName;
	}

	// return the name of the running maps
	public static List<String> getRunningMapsNameNew(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> runningMapsName = new ArrayList<String>();

		while (true) {
			runningMapsCaseId = getRunningMapsCaseId(jobId) - 1;

			if (runningMapsCaseId > 0) {
				break;
			} else {
				Thread.sleep(1000);
			}
		}

		String value = "";

//		if (runningMapsCaseId > 1) {
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");

			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select mapId from map where jobId='" + jobId + "' and state='RUNNING' and blockId <> '-1' and caseId='" + runningMapsCaseId + "' order by desc");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			for (int i = 0; i < obj6.size(); i++) {
				String[] values = (obj6.get(i).toString()).split(",");
				value = values[1].replace("]", "").replace("\"", "").replace("\\", "");
				if (!runningMapsName.contains(value)) {
					runningMapsName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			}
//				runningMapsName1 = null; // don't use 'clear' for only this in this method "getRunningMapsNameNew"
//				runningMapsName1 = runningMapsName;
		} catch (Exception e) {
			System.out.println("no runningMapsNameNew!");
//			System.out.println(e);
			runningMapsName = null;
			runningMapsName1 = null;
		}

//		} // if
		runningMapsName1 = null; // don't use 'clear' for only this in this method "getRunningMapsNameNew"
		runningMapsName1 = runningMapsName;
		return runningMapsName;
	}
	
	// return the name of the running maps
	public static List<String> getRunningMapsNameNew2(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> runningMapsName = new ArrayList<String>();

		while (true) {
			runningMapsCaseId = getRunningMapsCaseId(jobId) - 1;

			if (runningMapsCaseId > 0) {
				break;
			} else {
				Thread.sleep(1000);
			}
		}

		String value = "";

//		if (runningMapsCaseId > 1) {
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");

			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select mapId from map where jobId='" + jobId + "' and state='RUNNING' and progress <> '0.00' and caseId='" + runningMapsCaseId + "' order by desc");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			for (int i = 0; i < obj6.size(); i++) {
				String[] values = (obj6.get(i).toString()).split(",");
				value = values[1].replace("]", "").replace("\"", "").replace("\\", "");
				if (!runningMapsName.contains(value)) {
					runningMapsName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			}
//				runningMapsName1 = null; // don't use 'clear' for only this in this method "getRunningMapsNameNew"
//				runningMapsName1 = runningMapsName;
		} catch (Exception e) {
			System.out.println("no runningMapsNameNew!");
//			System.out.println(e);
			runningMapsName = null;
			runningMapsName1 = null;
		}

//		} // if
		runningMapsName1 = null; // don't use 'clear' for only this in this method "getRunningMapsNameNew"
		runningMapsName1 = runningMapsName;
		return runningMapsName;
	}

	public static int getRunningMapsCaseId(String jobId) throws IOException, URISyntaxException, ParseException {

		int caseId = -1;

		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select caseId from map where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			caseId = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));

		} catch (Exception e) {
			System.out.println("no caseId!");
			caseId = 0;
		}

//		runningMapsCaseId = caseId;

		return caseId;
	}

	// return the name of the running speculative maps
	public static List<String> getRunningNormalMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getRunningNormalMapsName = new ArrayList<String>();

		if (!(runningMapsName1 == null)) {
			for (int i = 0; i < runningMapsName1.size(); i++) {
				if (!runningMapsName1.get(i).contains("_")) {
					getRunningNormalMapsName.add(runningMapsName1.get(i));
				}
			}
		}

		getRunningNormalMapsName1 = getRunningNormalMapsName;
		return getRunningNormalMapsName;
	}

	// return the name of the running speculative maps
	public static List<String> getRunningSpeculativeMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningSpeculativeMapsName = new ArrayList<String>();

		if (!(runningMapsName1 == null)) {
			for (int i = 0; i < runningMapsName1.size(); i++) {
				if (runningMapsName1.get(i).contains("_")) {
					runningSpeculativeMapsName.add(runningMapsName1.get(i));
				}
			}
		}

		runningSpeculativeMapsName1 = runningSpeculativeMapsName;
		return runningSpeculativeMapsName;
	}

	// return the name of the running reduces
	public static List<String> getRunningReducesName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningReducesName = new ArrayList<String>();
		int getTotalReducesNum = getRunningReducesNum(jobId);
		String value = "";

		if (getTotalReducesNum > 0) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select reduceId from reduce where jobId='" + jobId
						+ "' and state='RUNNING' order by desc limit " + getTotalReducesNum);
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					runningReducesName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					value = values[1].replace("]", "").replace("\"", "").replace("\\", "");
					if (!runningReducesName.contains(value)) {
						runningReducesName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					}
				}
			} catch (Exception e) {
//				System.out.println("no runningReducesName!");
				runningReducesName = null;
			}
		}
		return runningReducesName;
	}

	// return the name of all the nodes which host running reduces
	public static List<String> getRunningReducesHostName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningReducesHostName = new ArrayList<String>();

		int runningReducesNum = getRunningReducesNum(jobId);
		String value = "";

		if (runningReducesNum > 0) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select host from reduce where jobId='" + jobId
						+ "' and state='RUNNING' order by desc limit " + runningReducesNum);
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					value = values[1].replace("]", "").replace("\"", "").replace("\\", "");
					if (!runningReducesHostName.contains(value)) {
						runningReducesHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					}

				}
			} catch (Exception e) {
				System.out.println("no runningReducesHostName!");
				runningReducesHostName = null;
			}
		}
		return runningReducesHostName;
	}

	// return the unique name of the nodes which host running maps
	public static List<String> getNodesHostRunningReduces(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningReducesHostName = getRunningReducesHostName(jobId);

		List<String> getNodesHostRunningReduces = new ArrayList<String>();

		try {
			if (runningReducesHostName.size() > 0) {
				for (int i = 0; i < runningReducesHostName.size(); i++) {
					if (!getNodesHostRunningReduces.contains(runningReducesHostName.get(i))) {
						getNodesHostRunningReduces.add(runningReducesHostName.get(i));
					}
				}
			}
		} catch (Exception e) {
			System.out.println("no getNodesHostRunningReduces!");
			getNodesHostRunningReduces = null;
		}

		return getNodesHostRunningReduces;
	}

	public static List<Double> getRunningMapsMemoryUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapsName = getRunningMapsName(jobId);

		int runningMapsNum = runningMapsName.size();

		List<Double> runningMapsMemoryUsage = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < runningMapsNum; k++) {
			try {
				query1.setCustomQuery("select physicalMemoryMb from mapDetails where mapId='" + runningMapsName.get(k)
						+ "' and jobId='" + jobId + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				if (obj4 == null) {
					runningMapsMemoryUsage.add(0.0);
				} else {
					JSONObject obj5 = (JSONObject) obj4.get(0);
					JSONArray obj6 = (JSONArray) obj5.get("values");

					String[] values = (obj6.get(0).toString()).split(",");
					runningMapsMemoryUsage
							.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));
				}

			} catch (Exception e) {
				System.out.println("no runningMapsMemoryUsage!");
				runningMapsMemoryUsage = null;
			}
		}
		return runningMapsMemoryUsage;
	}

	public static List<Double> getRunningMapsCpuUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Double> runningMapsCpuUsage = new ArrayList<Double>();
		try {

			List<String> runningMapsName = getRunningMapsName(jobId);
//		System.out.println("runningMapsName= " + runningMapsName);
			List<String> dataNodesName = getDataNodesNames(jobId);
//		System.out.println("dataNodesName= " + dataNodesName);
			int dataNodeNum = dataNodesName.size();
//		System.out.println("dataNodeNum= " + dataNodeNum);

//		System.out.println("getRunningMapsMemoryUsage= "+ getRunningMapsMemoryUsage(jobId));

			int runningMapsNum = runningMapsName.size();
//		System.out.println(runningMapsNum);

			List<Integer> nodeTotalNumRunningMapsEachNode = getNodeTotalNumRunningMapsEachNode(jobId);

			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();

			for (int k = 0; k < dataNodeNum; k++) { // 3
//			System.out.println("dataNodesName " + k + " = " + dataNodesName.get(k));
				for (int i = 0; i < nodeTotalNumRunningMapsEachNode.get(k); i++) { //
//				System.out.println("nodeTotalNumRunningMapsEachNode = " + nodeTotalNumRunningMapsEachNode.get(k));

					try {
						query1.setCustomQuery("select procCpu from resourceProc where hostName='" + dataNodesName.get(k)
								+ "' and procName='YarnChild' order by desc limit "
								+ nodeTotalNumRunningMapsEachNode.get(k));

// select procCpu from resourceProc where hostName = 'slave3' and procName='YarnChild' order by desc limit 1

						dataReader.setQuery(query1);
						resultSet = dataReader.getResult();
						Gson gson = new Gson();
						String jsonStr = gson.toJson(resultSet);
						JSONParser parser = new JSONParser();
						JSONObject obj = (JSONObject) parser.parse(jsonStr);
						JSONArray obj2 = (JSONArray) obj.get("results");
						JSONObject obj3 = (JSONObject) obj2.get(0);
						JSONArray obj4 = (JSONArray) obj3.get("series");

//					runningMapsCpuUsage.add(0.0);

						JSONObject obj5 = (JSONObject) obj4.get(0);
						JSONArray obj6 = (JSONArray) obj5.get("values");

						String[] values = (obj6.get(i).toString()).split(","); // important for getting the info.

						runningMapsCpuUsage.add(
								Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

					} catch (Exception e) {
						System.out.println("no runningMapsCpuUsage!");
						runningMapsCpuUsage = null;
					}
				}
			}
		} catch (Exception e) {
			System.out.println("no runningMapsCpuUsage!");
			runningMapsCpuUsage = null;
		}
		return runningMapsCpuUsage;
	}

	// return the name of the nodes which host running maps
	public static List<String> getRunningMapsHostName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapsHostName = new ArrayList<String>();
		
		int getTotalRunningMapsNum = 0;
		
		try {
			getTotalRunningMapsNum = runningMapsName1.size();
		} catch (Exception e) {
			getTotalRunningMapsNum = 0;
		}

		

		if (getTotalRunningMapsNum > 1) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
//				query1.setCustomQuery("select host from map where jobId='" + jobId + "' and state='RUNNING' order by desc limit " + (getTotalRunningMapsNum - 1));

				query1.setCustomQuery("select host from map where jobId='" + jobId
						+ "' and state='RUNNING' and blockId <> '-1' and caseId='" + runningMapsCaseId
						+ "' order by desc");

				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					runningMapsHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			} catch (Exception e) {
				System.out.println("no runningMapsHostName!");
				runningMapsHostName = null;
			}
		}
		return runningMapsHostName;
	}

	// return the name of the nodes which host running maps
	public static List<String> getNodesHostRunningMaps(String jobId)
			throws IOException, URISyntaxException, ParseException {
		
		List<String> getRunningMapsHostName = new ArrayList<String>();
		
		try {
			getRunningMapsHostName = getRunningMapsHostName(jobId);
		} catch (Exception e) {
			getRunningMapsHostName = null;
		}

		List<String> getNodesHostRunningMaps = new ArrayList<String>();

		if (getRunningMapsHostName.size() > 0) {
			for (int i = 0; i < getRunningMapsHostName.size(); i++) {
				if (!getNodesHostRunningMaps.contains(getRunningMapsHostName.get(i))) {
					getNodesHostRunningMaps.add(getRunningMapsHostName.get(i));
				}
			}
		}
		return getNodesHostRunningMaps;
	}

	// return the name of the nodes which host running speculative maps
	public static List<String> getNodesHostRunningSpeculativeMaps(String jobId)
			throws IOException, URISyntaxException, ParseException {

		getRunningSpeculativeMapsHostName(jobId);
		List<String> getRunningSpeculativeMapsHostName = new ArrayList<String>();
		for (int i = 0; i < getRunningSpeculativeMapsHostName1.size(); i++) {
			getRunningSpeculativeMapsHostName.add(getRunningSpeculativeMapsHostName1.get(i));
		}

		List<String> getNodesHostRunningSpeculativeMaps = new ArrayList<String>();

		if (getRunningSpeculativeMapsHostName.size() > 0) {
			for (int i = 0; i < getRunningSpeculativeMapsHostName.size(); i++) {
				if (!getNodesHostRunningSpeculativeMaps.contains(getRunningSpeculativeMapsHostName.get(i))) {
					getNodesHostRunningSpeculativeMaps.add(getRunningSpeculativeMapsHostName.get(i));
				}
			}
		}
		getNodesHostRunningSpeculativeMaps1 = getNodesHostRunningSpeculativeMaps;
		return getNodesHostRunningSpeculativeMaps;
	}

	// return the name of the nodes which host running normal maps
	public static List<String> getNodesHostRunningNormalMaps(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getRunningNormalMapsHostName = getRunningNormalMapsHostName(jobId);

		List<String> getNodesHostRunningNormalMaps = new ArrayList<String>();

		if (getRunningNormalMapsHostName.size() > 0) {
			for (int i = 0; i < getRunningNormalMapsHostName.size(); i++) {
				if (!getNodesHostRunningNormalMaps.contains(getRunningNormalMapsHostName.get(i))) {
					getNodesHostRunningNormalMaps.add(getRunningNormalMapsHostName.get(i));
				}
			}
		}
		getNodesHostRunningNormalMaps1 = getNodesHostRunningNormalMaps;
		return getNodesHostRunningNormalMaps;
	}

	// return the name of the nodes which host running maps
	public static List<String> getNodesHostLowProgressRunningMaps(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getLowProgressRunningMapsHostName = getLowProgressRunningMapsHostName(jobId);

		List<String> getNodesHostLowProgressRunningMaps = new ArrayList<String>();

		if (getLowProgressRunningMapsHostName.size() != 0) {
			for (int i = 0; i < getLowProgressRunningMapsHostName.size(); i++) {
				if (!getNodesHostLowProgressRunningMaps.contains(getLowProgressRunningMapsHostName.get(i))) {
					getNodesHostLowProgressRunningMaps.add(getLowProgressRunningMapsHostName.get(i));
				}
			}
		}
		return getNodesHostLowProgressRunningMaps;
	}

	// return the name of the nodes which host running maps
	public static List<String> getNodesHostLowPerformanceRunningMaps(String jobId) // ***************************************************************
																					// UPDATED_VERSION_2
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> getLowPerformanceRunningMapsHostName = getLowPerformanceRunningMapsHostName(jobId);

		List<String> getNodesHostLowPerformanceRunningMaps = new ArrayList<String>();

//		if (getLowPerformanceRunningMapsHostName.size() != 0) {
//			for (int i = 0; i < getLowPerformanceRunningMapsHostName.size(); i++) {
//				if (!getNodesHostLowPerformanceRunningMaps.contains(getLowPerformanceRunningMapsHostName.get(i))) {
//					getNodesHostLowPerformanceRunningMaps.add(getLowPerformanceRunningMapsHostName.get(i));
//				}
//			}
//		}

		getNodesHostLowPerformanceRunningMaps = getLowPerformanceRunningMapsHostName.stream().distinct()
				.collect(Collectors.toList());

		getNodesHostLowPerformanceRunningMaps1.clear();
		getNodesHostLowPerformanceRunningMaps1 = getNodesHostLowPerformanceRunningMaps;

		return getNodesHostLowPerformanceRunningMaps;
	}

	public static List<Double> getNodesHostSucceededMapsDiskReadSpeed(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nodesName = getNodesHostSucceededMaps(jobId);
		int totalNodes = nodesName.size();
		List<Double> getNodesHostSucceededMapsDiskReadSpeed = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select diskReadSpeedMb from resource where hostName='" + nodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getNodesHostSucceededMapsDiskReadSpeed
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getNodesHostSucceededMapsDiskReadSpeed!");
				getNodesHostSucceededMapsDiskReadSpeed = null;
			}
		}
		return getNodesHostSucceededMapsDiskReadSpeed;
	}

	// return the number of succeeded maps.
	public static int getSucceededMapsNum(String jobId) throws IOException, URISyntaxException, ParseException {
		int succeededMapsNum = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select mapsCompleted from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			succeededMapsNum = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));

		} catch (Exception e) {
			System.out.println("no succeededMapsNum!");
			succeededMapsNum = 0;
		}
		return succeededMapsNum;
	}

	public static int getKilledMapsNum(String jobId) throws IOException, URISyntaxException, ParseException {
		int killedMapsNum = 0;
		double killedMapsNum1 = 0.0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select count(mapId) from map where jobId='" + jobId + "' and state='KILLED'");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			killedMapsNum = (int) Double.parseDouble(values[1].replace("]", "").replace("\"", ""));

		} catch (Exception e) {
			System.out.println("no killedMapsNum!");
			killedMapsNum = 0;
		}
		return killedMapsNum;
	}

	// return the name of the succeeded maps
	public static List<String> getSucceededMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededMapsName = new ArrayList<String>();
		int getCompletedMapsNum = getSucceededMapsNum(jobId);

		if (getCompletedMapsNum != 0) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery(
						"select mapId from map where jobId='" + jobId + "' and state='SUCCEEDED' order by desc");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					succeededMapsName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			} catch (Exception e) {
				System.out.println("no succeededMapsName!");
				succeededMapsName = null;
			}
		}

		succeededMapsName1 = succeededMapsName;
		return succeededMapsName;
	}

	public static List<Integer> getSucceededMapsExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> succeededMapsExecTime = new ArrayList<Integer>();
		List<String> succeededMapsName = getSucceededMapsName(jobId);

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < succeededMapsName.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ succeededMapsName.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				succeededMapsExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no succeededMapsExecTime!");
				succeededMapsExecTime = null;
			}
		}
		return succeededMapsExecTime;
	}

	public static List<Integer> getSucceededLocalMapsExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> succeededLocalMapsExecTime = new ArrayList<Integer>();
		List<String> succeededLocalMapsName = getSucceededLocalMapsName(jobId);

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < succeededLocalMapsName.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ succeededLocalMapsName.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				succeededLocalMapsExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no succeededLocalMapsExecTime!");
				succeededLocalMapsExecTime = null;
			}
		}
		return succeededLocalMapsExecTime;
	}

	public static List<Integer> getSucceededNonLocalMapsExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> succeededNonLocalMapsExecTime = new ArrayList<Integer>();
		List<String> succeededNonLocalMapsName = getSucceededNonLocalMapsName(jobId);

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < succeededNonLocalMapsName.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ succeededNonLocalMapsName.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				succeededNonLocalMapsExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no succeededNonLocalMapsExecTime!");
				succeededNonLocalMapsExecTime = null;
			}
		}
		return succeededNonLocalMapsExecTime;
	}

	public static List<Integer> getSucceededNonLocalStragglersExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> succeededNonLocalStragglersExecTime = new ArrayList<Integer>();
		List<String> succeededNonLocalStragglersName = getSucceededNonLocalStragglersName(jobId);

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < succeededNonLocalStragglersName.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ succeededNonLocalStragglersName.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				succeededNonLocalStragglersExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no succeededNonLocalStragglersExecTime!");
				succeededNonLocalStragglersExecTime = null;
			}
		}
		return succeededNonLocalStragglersExecTime;
	}

	// return the name of the nodes which host succeeded maps
	public static List<String> getSucceededMapsHostName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededMapsHostName = new ArrayList<String>();

		int getCompletedMapsNum = getSucceededMapsNum(jobId);

		if (getCompletedMapsNum != 0) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select host from map where jobId='" + jobId
						+ "' and state='SUCCEEDED' order by desc limit " + getCompletedMapsNum);
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					succeededMapsHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			} catch (Exception e) {
				System.out.println("no succeededMapsHostName!");
				succeededMapsHostName = null;
			}
		}
		return succeededMapsHostName;
	}

	// return the name of the nodes which host succeeded reduces
	public static List<String> getSucceededReducesHostName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededReducesHostName = new ArrayList<String>();

		int getCompletedReducesNum = getSucceededReducesNum(jobId);

		if (getCompletedReducesNum != 0) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select host from reduce where jobId='" + jobId
						+ "' and state='SUCCEEDED' order by desc limit " + getCompletedReducesNum);
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					succeededReducesHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			} catch (Exception e) {
				System.out.println("no succeededReducesHostName!");
				succeededReducesHostName = null;
			}
		}
		return succeededReducesHostName;
	}

	// return the name of the nodes which host running maps
	public static List<String> getNodesHostSucceededMaps(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededMapsHostName = getSucceededMapsHostName(jobId);

		List<String> getNodesHostSucceededMaps = new ArrayList<String>();
		int succeededMapsHostNameNum = 0;

		if (!(succeededMapsHostName == null)) {
			succeededMapsHostNameNum = succeededMapsHostName.size();
		}

		if (succeededMapsHostNameNum > 0) {
			for (int i = 0; i < succeededMapsHostName.size(); i++) {
				if (!getNodesHostSucceededMaps.contains(succeededMapsHostName.get(i))) {
					getNodesHostSucceededMaps.add(succeededMapsHostName.get(i));
				}
			}
		}
		return getNodesHostSucceededMaps;
	}

	// return the number of running reduce
	public static int getRunningReducesNum(String jobId) throws IOException, URISyntaxException, ParseException {
		int runningReducesNum = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select reducesRunning from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			runningReducesNum = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));

//			runningReducesNum = 1; // just for trying the reduce. DELETE THIS. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

		} catch (Exception e) {
			System.out.println("no runningReducesNum!");
			runningReducesNum = 0;
		}
		return runningReducesNum;
	}

	public static boolean checkMapNumTuning(String jobId, String blockId)
			throws IOException, URISyntaxException, ParseException {

		double inputSize = getInputDataSize(jobId);
		double blockSizeMB = getBlockSizeMB(jobId, blockId);

		if (inputSize > 1048576) {
			if (blockSizeMB >= 256 && blockSizeMB <= 512) {
				return true;
			} else
				return false;
		} else {
			if (blockSizeMB == 128) {
				return true;
			} else
				return false;
		}

		// if inputSize is bigger than 1 TB, then check the block size of the input
		// data.
		// Then, ....

//		int idealNum = (int) (inputSize / 128) + 2;
//		int mapsNum = getTotalMapNum(jobId);
//
//		if (idealNum < mapsNum) {
////			System.out.println("Number of map is not set appropriately!!!");
//			return false;
//		} else {
////			System.out.println("There is no probs on tuning the number of maps.");
//			return true;
//		}
	}

	public static int getJobMakespan(String jobId) throws IOException, URISyntaxException, ParseException {

		int jobMakespan = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select makespanSec from job where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			jobMakespan = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("jobMakespan= " + jobMakespan);
		} catch (Exception e) {
			System.out.println("no jobMakespan!");
			jobMakespan = 0;
		}
		return jobMakespan;
	}

	public static double getJobProgress(String jobId) throws IOException, URISyntaxException, ParseException {

		double jobProgress = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select progress from job where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			jobProgress = Double.valueOf(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("jobProgress= " + jobProgress);
		} catch (Exception e) {
			System.out.println("no jobProgress!");
			jobProgress = 0;
		}
		return jobProgress;
	}

	public static int getNumMapsPending(String jobId) throws IOException, URISyntaxException, ParseException {

		int numMapsPending = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select mapsPending from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			numMapsPending = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("numMapsPending= " + numMapsPending);
		} catch (Exception e) {
			System.out.println("no numMapsPending!");
			numMapsPending = 0;
		}
		return numMapsPending;
	}

	public static List<Integer> getNodeTotalNumRunningContainers(String jobId)
			throws IOException, URISyntaxException, ParseException {

		int totalNodes = getClusterTotalDataNodes(jobId);
//		System.out.println("totalNodes = " + totalNodes);
		List<String> dataNodesName = getDataNodesNames(jobId);
//		System.out.println("dataNodesName = " + dataNodesName);
		List<Integer> nodeTotalNumRunningContainers = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select numRunningContainers from node where nodeHostName='"
						+ dataNodesName.get(k) + "' and jobId='" + jobId + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				nodeTotalNumRunningContainers
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
//				System.out.println("no nodeTotalNumRunningContainers!");
				nodeTotalNumRunningContainers = null;
			}
		}
		return nodeTotalNumRunningContainers;
	}

	public static List<Integer> getNodeTotalNumRunningMapsEachNode(String jobId)
			throws IOException, URISyntaxException, ParseException {
		List<Integer> nodeTotalNumRunningMapsEachNode = new ArrayList<Integer>();
		try {
			String appMasterLocation = getAppMasterLocation(jobId);
			List<String> dataNodesName = getDataNodesNames(jobId);

			List<Integer> nodeTotalNumRunningContainers = getNodeTotalNumRunningContainers(jobId);

			for (int i = 0; i < dataNodesName.size(); i++) {
				if (dataNodesName.get(i).equalsIgnoreCase(appMasterLocation)
						&& !(nodeTotalNumRunningContainers.get(i) == 0)) {
					nodeTotalNumRunningContainers.set(i, nodeTotalNumRunningContainers.get(i) - 1);
				}
			}
			nodeTotalNumRunningMapsEachNode = nodeTotalNumRunningContainers;

		} catch (Exception e) {
			System.out.println("no nodeTotalNumRunningMapsEachNode!");
			nodeTotalNumRunningMapsEachNode = null;
		}

		return nodeTotalNumRunningMapsEachNode;
	}

	public static double getClusterCpuUsage(String jobId) throws IOException, URISyntaxException, ParseException {

		double clusterCpuUsage = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select cpuUsage from cluster where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			clusterCpuUsage = Double.valueOf(values[1].replace("]", "").replace("\"", ""));
		} catch (Exception e) {
			System.out.println("no clusterCpuUsage!");
			clusterCpuUsage = 0;
		}
		return clusterCpuUsage;
	}

	public static double getClusterMemoryUsage(String jobId) throws IOException, URISyntaxException, ParseException {

		double clusterMemoryUsage = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select memoryUsage from cluster where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			clusterMemoryUsage = Double.valueOf(values[1].replace("]", "").replace("\"", ""));
		} catch (Exception e) {
			System.out.println("no clusterMemoryUsage!");
			clusterMemoryUsage = 0;
		}
		return clusterMemoryUsage;
	}

	public static double getJobClusterUsage(String jobId) throws IOException, URISyntaxException, ParseException {

		double jobClusterUsage = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select jobClusterUsage from job where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			jobClusterUsage = Double.valueOf(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("getJobClusterUsage= " + getJobClusterUsage);
		} catch (Exception e) {
			System.out.println("no jobClusterUsage!");
			jobClusterUsage = 0;
		}
		return jobClusterUsage;
	}

	public static int getClusterAvailableMemory(String jobId) throws IOException, URISyntaxException, ParseException {
		int clusterAvailableMemory = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select availableMemoryMb from cluster where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			clusterAvailableMemory = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("clusterAvailableMemory= " + clusterAvailableMemory);
		} catch (Exception e) {
			System.out.println("no clusterAvailableMemory!");
			clusterAvailableMemory = 0;
		}
		return clusterAvailableMemory;
	}

	public static int getClusterAvailableVCores(String jobId) throws IOException, URISyntaxException, ParseException {

		int clusterAvailableVCores = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select availableVCores from cluster where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			clusterAvailableVCores = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//			System.out.println("clusterAvailableVCores= " + clusterAvailableVCores);
		} catch (Exception e) {
			System.out.println("no clusterAvailableVCores!");
			clusterAvailableVCores = 0;
		}
		return clusterAvailableVCores;
	}

	public static List<Double> getRunningMapsProgress(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapsName = getRunningMapsName(jobId); // ???

		int runningMapsNum = runningMapsName.size();

//		int totalNodes = getClusterTotalDataNodes(jobId);

		List<Double> getRunningMapsProgress = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < runningMapsNum; i++) {
			try {
				query1.setCustomQuery(
						"select progress from map where mapId='" + runningMapsName.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getRunningMapsProgress
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getRunningMapsProgress!");
				getRunningMapsProgress = null;
			}
		}
		return getRunningMapsProgress;
	}

	public static List<Double> getLowProgressRunningMaps(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getLowProgressRunningMapsName = getLowProgressRunningMapsName(jobId);

		int getLowProgressRunningMapsNum = getLowProgressRunningMapsName.size();

//		int totalNodes = getClusterTotalDataNodes(jobId);

		List<Double> getLowProgressRunningMaps = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < getLowProgressRunningMapsNum; i++) {
			try {
				query1.setCustomQuery("select progress from map where mapId='" + getLowProgressRunningMapsName.get(i)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getLowProgressRunningMaps
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getLowProgressRunningMaps!");
				getLowProgressRunningMaps = null;
			}
		}
		return getLowProgressRunningMaps;
	}

//	public static List<Integer> getHighExecutionTimeRunningMaps(String jobId)
//			throws IOException, URISyntaxException, ParseException {
//
//		List<String> getHighExecutionTimeRunningMapsName = getHighExecutionTimeRunningMapsName(jobId);
//
//		int getHighExecutionTimeRunningMapsNum = getHighExecutionTimeRunningMapsName.size();
//
////		int totalNodes = getClusterTotalDataNodes(jobId);
//
//		List<Integer> getHighExecutionTimeRunningMaps = new ArrayList<Integer>();
//
//		query.setMeasurement("sigar");
//		query.setLimit(1000);
//		query.fillNullValues("0");
//		DataReader dataReader = new DataReader(query, configuration);
//		ResultSet resultSet = dataReader.getResult();
//		Query query1 = new Query();
//
//		for (int i = 0; i < getHighExecutionTimeRunningMapsNum; i++) {
//			try {
//				query1.setCustomQuery("select executionTimeSec from map where mapId='"
//						+ getHighExecutionTimeRunningMapsName.get(i) + "' order by desc limit 1");
//				dataReader.setQuery(query1);
//				resultSet = dataReader.getResult();
//				Gson gson = new Gson();
//				String jsonStr = gson.toJson(resultSet);
//				JSONParser parser = new JSONParser();
//				JSONObject obj = (JSONObject) parser.parse(jsonStr);
//				JSONArray obj2 = (JSONArray) obj.get("results");
//				JSONObject obj3 = (JSONObject) obj2.get(0);
//				JSONArray obj4 = (JSONArray) obj3.get("series");
//				JSONObject obj5 = (JSONObject) obj4.get(0);
//				JSONArray obj6 = (JSONArray) obj5.get("values");
//
//				String[] values = (obj6.get(0).toString()).split(",");
//				getHighExecutionTimeRunningMaps
//						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));
//
//			} catch (Exception e) {
//				System.out.println("no getHighExecutionTimeRunningMaps!");
//				getHighExecutionTimeRunningMaps = null;
//			}
//		}
//		return getHighExecutionTimeRunningMaps;
//	}

	public static List<String> getLowestMapSpecs(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> getSlowestMapSpecs = new ArrayList<String>();
		String getSlowestMapName = "";
		String getSlowestMapHostName = "";

		List<Double> getRunningMapsPerformance = new ArrayList<Double>();
		List<String> getRunningMapsName = new ArrayList<String>();
		List<String> getRunningMapsHostName = new ArrayList<String>();

		if (!(getRunningMapsPerformance(jobId) == null)) {
			getRunningMapsPerformance = getRunningMapsPerformance(jobId);
		}

		if (!(getRunningMapsName(jobId) == null)) {
			getRunningMapsName = getRunningMapsName(jobId);
		}

		if (!(getRunningMapsHostName(jobId) == null)) {
			getRunningMapsHostName = getRunningMapsHostName(jobId);
		}
		if (getRunningMapsPerformance.size() > 0) {

			int slowestMapIndex = getRunningMapsPerformance.indexOf(Collections.min(getRunningMapsPerformance));

			getSlowestMapSpecs.add(getRunningMapsName.get(slowestMapIndex));
			getSlowestMapSpecs.add(getRunningMapsHostName.get(slowestMapIndex));
		}

		return getSlowestMapSpecs;
	}

	public static List<String> getLowProgressRunningMapsHostName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> lowProgressRunningMapsName = getLowProgressRunningMapsName(jobId);

		int lowProgressRunningMapsNum = lowProgressRunningMapsName.size();

//		int totalNodes = getClusterTotalDataNodes(jobId);

		List<String> getLowProgressRunningMapsHostName = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < lowProgressRunningMapsNum; i++) {
			try {
				query1.setCustomQuery("select host from map where mapId='" + lowProgressRunningMapsName.get(i)
						+ "' and state='RUNNING' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getLowProgressRunningMapsHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no getLowProgressRunningMapsHostName!");
				getLowProgressRunningMapsHostName = null;
			}
		}
		return getLowProgressRunningMapsHostName;
	}

	public static List<String> getLowPerformanceRunningMapsHostName(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

//		List<String> lowPerformanceRunningMapsName = getLowPerformanceRunningMapsName(jobId);

		List<String> lowPerformanceRunningMapsName = new ArrayList<String>();

		for (int i = 0; i < lowPerformanceRunningMapsName1.size(); i++) {
			lowPerformanceRunningMapsName.add(lowPerformanceRunningMapsName1.get(i));
		}

		int lowPerformanceRunningMapsNum = lowPerformanceRunningMapsName.size();

//		int totalNodes = getClusterTotalDataNodes(jobId);

		List<String> getLowPerformanceRunningMapsHostName = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < lowPerformanceRunningMapsNum; i++) {
			try {
				query1.setCustomQuery("select host from map where mapId='" + lowPerformanceRunningMapsName.get(i)
						+ "' and state='RUNNING' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getLowPerformanceRunningMapsHostName
						.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no getLowPerformanceRunningMapsHostName!");
				getLowPerformanceRunningMapsHostName = null;
			}
		}

		getLowPerformanceRunningMapsHostName1 = getLowPerformanceRunningMapsHostName;
		return getLowPerformanceRunningMapsHostName;
	}

	public static List<String> getRunningSpeculativeMapsHostName(String jobId)
			throws IOException, URISyntaxException, ParseException {

//		List<String> runningSpeculativeMapsName = getRunningSpeculativeMapsName(jobId);

		List<String> runningSpeculativeMapsName = new ArrayList<String>();

		for (int i = 0; i < runningSpeculativeMapsName1.size(); i++) {
			runningSpeculativeMapsName.add(runningSpeculativeMapsName1.get(i));
		}

		int runningSpeculativeMapsNum = runningSpeculativeMapsName.size();

//		int totalNodes = getClusterTotalDataNodes(jobId);

		List<String> getRunningSpeculativeMapsHostName = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < runningSpeculativeMapsNum; i++) {
			try {
				query1.setCustomQuery("select host from map where mapId='" + runningSpeculativeMapsName.get(i)
						+ "' and state='RUNNING' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getRunningSpeculativeMapsHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no getRunningSpeculativeMapsHostName!");
				getRunningSpeculativeMapsHostName = null;
			}
		}
		getRunningSpeculativeMapsHostName1 = getRunningSpeculativeMapsHostName;
		return getRunningSpeculativeMapsHostName;
	}

	public static List<String> getRunningNormalMapsHostName(String jobId)
			throws IOException, URISyntaxException, ParseException {

//		List<String> runningNormalMapsName = getRunningNormalMapsName(jobId);

		getRunningNormalMapsName(jobId);

		List<String> runningNormalMapsName = new ArrayList<String>();

		for (int i = 0; i < getRunningNormalMapsName1.size(); i++) {
			runningNormalMapsName.add(getRunningNormalMapsName1.get(i));
		}

		int runningNormalMapsNum = runningNormalMapsName.size();

		List<String> getRunningNormalMapsHostName = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < runningNormalMapsNum; i++) {
			try {
				query1.setCustomQuery("select host from map where mapId='" + runningNormalMapsName.get(i)
						+ "' and state='RUNNING' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getRunningNormalMapsHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no getRunningNormalMapsHostName!");
				getRunningNormalMapsHostName = null;
			}
		}
		getRunningNormalMapsHostName1 = getRunningNormalMapsHostName;
		return getRunningNormalMapsHostName;
	}

	public static List<String> getNodesCommon(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> getNodesCommon = new ArrayList<String>();

		List<String> getNodesHostLowPerformanceRunningMaps = new ArrayList<String>();

		if (!(getNodesHostLowPerformanceRunningMaps(jobId) == null)) {
			for (int i = 0; i < getNodesHostLowPerformanceRunningMaps1.size(); i++) {
				getNodesHostLowPerformanceRunningMaps.add(getNodesHostLowPerformanceRunningMaps1.get(i));
			}
		}

		List<String> getNodesHostRunningSpeculativeMaps = new ArrayList<String>();

//		if (!(getRunningSpeculativeMapsName(jobId) == null)) {
//			getNodesHostRunningSpeculativeMaps = getNodesHostRunningSpeculativeMaps(jobId);
//		}

		if (!(runningSpeculativeMapsName1 == null)) {
			getNodesHostRunningSpeculativeMaps = getNodesHostRunningSpeculativeMaps(jobId);
		}

		if ((getNodesHostRunningSpeculativeMaps.size() > 0) && (getNodesHostLowPerformanceRunningMaps.size() > 0)) {
			// get the common elements.
			getNodesCommon = getNodesHostRunningSpeculativeMaps.stream()
					.filter(getNodesHostLowPerformanceRunningMaps::contains).collect(toList());
		}

		getNodesCommon1.clear();
		getNodesCommon1 = getNodesCommon;
		return getNodesCommon;
	}

//	public static List<String> getHighExecutionTimeRunningMapsHostName(String jobId)
//			throws IOException, URISyntaxException, ParseException {
//
//		List<String> highExecutionTimeRunningMapsName = getHighExecutionTimeRunningMapsName(jobId);
//
//		int highExecutionTimeRunningMapsNum = highExecutionTimeRunningMapsName.size();
//
////		int totalNodes = getClusterTotalDataNodes(jobId);
//
//		List<String> getHighExecutionTimeRunningMapsHostName = new ArrayList<String>();
//
//		query.setMeasurement("sigar");
//		query.setLimit(1000);
//		query.fillNullValues("0");
//		DataReader dataReader = new DataReader(query, configuration);
//		ResultSet resultSet = dataReader.getResult();
//		Query query1 = new Query();
//
//		for (int i = 0; i < highExecutionTimeRunningMapsNum; i++) {
//			try {
//				query1.setCustomQuery("select host from map where mapId='" + highExecutionTimeRunningMapsName.get(i)
//						+ "' and state='RUNNING' order by desc limit 1");
//				dataReader.setQuery(query1);
//				resultSet = dataReader.getResult();
//				Gson gson = new Gson();
//				String jsonStr = gson.toJson(resultSet);
//				JSONParser parser = new JSONParser();
//				JSONObject obj = (JSONObject) parser.parse(jsonStr);
//				JSONArray obj2 = (JSONArray) obj.get("results");
//				JSONObject obj3 = (JSONObject) obj2.get(0);
//				JSONArray obj4 = (JSONArray) obj3.get("series");
//				JSONObject obj5 = (JSONObject) obj4.get(0);
//				JSONArray obj6 = (JSONArray) obj5.get("values");
//
//				String[] values = (obj6.get(0).toString()).split(",");
//				getHighExecutionTimeRunningMapsHostName
//						.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
//
//			} catch (Exception e) {
//				System.out.println("no getHighExecutionTimeRunningMapsHostName!");
//				getHighExecutionTimeRunningMapsHostName = null;
//			}
//		}
//		return getHighExecutionTimeRunningMapsHostName;
//	}

	public static List<Double> getNonLocalRunningMapsProgress(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nonLocalRunningMapsName = getNonLocalRunningMapsName(jobId);

		int nonLocalRunningMapsNum = nonLocalRunningMapsName.size();

//		int totalNodes = getClusterTotalDataNodes(jobId);

		List<Double> getNonLocalRunningMapsProgress = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < nonLocalRunningMapsNum; i++) {
			try {
				query1.setCustomQuery("select progress from map where mapId='" + nonLocalRunningMapsName.get(i)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getNonLocalRunningMapsProgress
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getNonLocalRunningMapsProgress!");
				getNonLocalRunningMapsProgress = null;
			}
		}
		return getNonLocalRunningMapsProgress;
	}

	public static double getJobMapProgress(String jobId) throws IOException, URISyntaxException, ParseException {

		double jobProgress = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select mapProgress from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			jobProgress = Double.valueOf(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("jobProgress= " + jobProgress);
		} catch (Exception e) {
			System.out.println("no jobProgress!");
			jobProgress = 0;
		}
		return jobProgress;
	}

	// DON'T DELETE

//	public static List<Double> getRunningMapsPerformanceWithoutNORMALIZATION(String jobId)
//			throws IOException, URISyntaxException, ParseException, InterruptedException {
//
////		List<String> runningMapsName = new ArrayList<String>();
////		int runningMapsNum = 0;
//
////		if (!(getRunningMapsName(jobId) == null)) {
////			runningMapsName = getRunningMapsName(jobId);
////			runningMapsNum = runningMapsName.size();
////		}
//
////		runningMapsName = runningMapsName1;
//
////		System.out.println("runningMapsName in getRunningMapsPerformance = " + runningMapsName);
//
//		List<Double> runningMapsPerformance = new ArrayList<Double>();
//		int executionTime = 0;
//		double progress = 0;
//		double result = 0;
//
//		query.setMeasurement("sigar");
//		query.setLimit(1000);
//		query.fillNullValues("0");
//		DataReader dataReader = new DataReader(query, configuration);
//		ResultSet resultSet = dataReader.getResult();
//		Query query1 = new Query();
//
//		runningMapsProgress1.clear();
//		runningMapsExecutionTime1.clear();
//
//		if (!(runningMapsName1 == null)) {
//			for (int k = 0; k < runningMapsName1.size(); k++) {
//				try {
//					query1.setCustomQuery("select executionTimeSec, progress from map where jobId='" + jobId
//							+ "' and mapId='" + runningMapsName1.get(k) + "' order by desc limit 1");
//					dataReader.setQuery(query1);
//					resultSet = dataReader.getResult();
//					Gson gson = new Gson();
//					String jsonStr = gson.toJson(resultSet);
//					JSONParser parser = new JSONParser();
//					JSONObject obj = (JSONObject) parser.parse(jsonStr);
//					JSONArray obj2 = (JSONArray) obj.get("results");
//					JSONObject obj3 = (JSONObject) obj2.get(0);
//					JSONArray obj4 = (JSONArray) obj3.get("series");
//					JSONObject obj5 = (JSONObject) obj4.get(0);
//					JSONArray obj6 = (JSONArray) obj5.get("values");
//
//					String[] values = (obj6.get(0).toString()).split(",");
//					executionTime = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
//					progress = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));
//
//					runningMapsProgress1.add(progress);
//					runningMapsExecutionTime1.add(executionTime);
//
////				System.out.println("executionTime= " + executionTime + "     progress= " + progress);
//
//					if (executionTime == 0) {
//						result = 0;
//					} else
//						result = progress / executionTime;
//
////				System.out.println("result= " + result);
//
//					runningMapsPerformance.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
//
//				} catch (Exception e) {
////				System.out.println("no runningMapsPerformance!");
////				runningMapsPerformance = null;
//					runningMapsPerformance.add(0.0);
//				}
//			}
//		}
//
//		runningMapsPerformance1.clear();
//		runningMapsPerformance1 = runningMapsPerformance;
//		return runningMapsPerformance;
//	}

	public static List<Double> getRunningMapsPerformance(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<Double> runningMapsPerformance = new ArrayList<Double>();
		List<Double> runningMapsPerformanceNorm = new ArrayList<Double>();

		int executionTime = 0;
		double progress = 0;
		double result = 0;

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		exacTimeListNorm.clear();
		progressListNorm.clear();
		runningMapsProgress1.clear();
		runningMapsExecutionTime1.clear();

		if (!(runningMapsName1 == null)) {
			for (int k = 0; k < runningMapsName1.size(); k++) {
				try {
					query1.setCustomQuery("select executionTimeSec, progress from map where jobId='" + jobId
							+ "' and mapId='" + runningMapsName1.get(k) + "' order by desc limit 1");
					dataReader.setQuery(query1);
					resultSet = dataReader.getResult();
					Gson gson = new Gson();
					String jsonStr = gson.toJson(resultSet);
					JSONParser parser = new JSONParser();
					JSONObject obj = (JSONObject) parser.parse(jsonStr);
					JSONArray obj2 = (JSONArray) obj.get("results");
					JSONObject obj3 = (JSONObject) obj2.get(0);
					JSONArray obj4 = (JSONArray) obj3.get("series");
					JSONObject obj5 = (JSONObject) obj4.get(0);
					JSONArray obj6 = (JSONArray) obj5.get("values");

					String[] values = (obj6.get(0).toString()).split(",");
					executionTime = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					progress = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));

					runningMapsProgress1.add(progress);
					runningMapsExecutionTime1.add(executionTime);

//				System.out.println("executionTime= " + executionTime + "     progress= " + progress);

//					if (executionTime == 0) {
//						result = 0;
//					} else
//						result = progress / executionTime;

//				System.out.println("result= " + result);

				} catch (Exception e) {
					System.out.println("no runningMapsPerformance!");
					runningMapsPerformance = null;
					runningMapsPerformanceNorm = null;
				}
			} // for

			double min = Collections.min(runningMapsExecutionTime1);
			double max = Collections.max(runningMapsExecutionTime1);
			result = 0.0;

			if (min != max) {
				for (int i = 0; i < runningMapsExecutionTime1.size(); i++) {
					result = (runningMapsExecutionTime1.get(i) - min) / (max - min);
					if (result == 0) {
						result = 0.1;
					}
					exacTimeListNorm.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
				}
			}

			min = Collections.min(runningMapsProgress1);
			max = Collections.max(runningMapsProgress1);
			result = 0.0;

			if (min != max) {
				for (int i = 0; i < runningMapsProgress1.size(); i++) {
					result = (runningMapsProgress1.get(i) - min) / (max - min);
					progressListNorm.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
				}
			}

			for (int i = 0; i < progressListNorm.size(); i++) {
				result = progressListNorm.get(i) / exacTimeListNorm.get(i);
				result = Double.parseDouble(new DecimalFormat("##.##").format(result));
				runningMapsPerformanceNorm.add(result);
			}

			runningMapsPerformanceNorm1.clear();
			runningMapsPerformanceNorm1 = runningMapsPerformanceNorm;

//			runningMapsPerformance.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
		}
		runningMapsPerformance1.clear();
		runningMapsPerformance1 = runningMapsPerformance;
		return runningMapsPerformance;
	}

	public static List<Double> getRunningMapsPerformanceNew(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

//		List<String> runningMapsName = new ArrayList<String>();

//		runningMapsName = runningMapsName1;
		int caseId = runningMapsCaseId;

//		System.out.println("runningMapsName in getRunningMapsPerformance = " + runningMapsName);

		List<Double> runningMapsPerformance = new ArrayList<Double>();
		List<Double> runningMapsPerformanceNorm = new ArrayList<Double>();
		int executionTime = 0;
		double progress = 0;
		double result = 0;

		exacTimeListNorm.clear();
		progressListNorm.clear();
		runningMapsProgress1.clear();
		runningMapsExecutionTime1.clear();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		runningMapsProgress1.clear();
		runningMapsExecutionTime1.clear();

		if (!(runningMapsName1 == null)) {

			for (int k = 0; k < runningMapsName1.size(); k++) {
				try {
					query1.setCustomQuery(
							"select elapsedTimeSec, progress from map where jobId='" + jobId + "' and mapId='"
									+ runningMapsName1.get(k) + "' and caseId='" + caseId + "' order by desc limit 1");
					dataReader.setQuery(query1);
					resultSet = dataReader.getResult();
					Gson gson = new Gson();
					String jsonStr = gson.toJson(resultSet);
					JSONParser parser = new JSONParser();
					JSONObject obj = (JSONObject) parser.parse(jsonStr);
					JSONArray obj2 = (JSONArray) obj.get("results");
					JSONObject obj3 = (JSONObject) obj2.get(0);
					JSONArray obj4 = (JSONArray) obj3.get("series");
					JSONObject obj5 = (JSONObject) obj4.get(0);
					JSONArray obj6 = (JSONArray) obj5.get("values");

					String[] values = (obj6.get(0).toString()).split(",");
					executionTime = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					progress = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));

					runningMapsProgress1.add(progress);
					runningMapsExecutionTime1.add(executionTime);

//				System.out.println("executionTime= " + executionTime + "     progress= " + progress);

//					if (executionTime == 0) {
//						result = 0;
//					} else
//						result = progress / executionTime;

//				System.out.println("result= " + result);

//					runningMapsPerformance.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));

				} catch (Exception e) {
					System.out.println("no runningMapsPerformance!");
					runningMapsPerformance = null;
					runningMapsPerformanceNorm = null;
				}
			} // for

			double min = Collections.min(runningMapsExecutionTime1);
			double max = Collections.max(runningMapsExecutionTime1);
			result = 0.0;

			if (min != max) {
				for (int i = 0; i < runningMapsExecutionTime1.size(); i++) {
					result = (runningMapsExecutionTime1.get(i) - min) / (max - min);
					if (result == 0.0) {
						result = 0.1;
					}
					exacTimeListNorm.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
				}

				min = Collections.min(runningMapsProgress1);
				max = Collections.max(runningMapsProgress1);
				result = 0.0;

				if (min != max) {
					for (int i = 0; i < runningMapsProgress1.size(); i++) {
						result = (runningMapsProgress1.get(i) - min) / (max - min);
						progressListNorm.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
					}

					for (int i = 0; i < runningMapsName1.size(); i++) {
						if (!(progressListNorm.get(i) == 0.0) && !(exacTimeListNorm.get(i) == 0.0)) {
							result = progressListNorm.get(i) / exacTimeListNorm.get(i);
						} else {
							result = 0.0;
						}

						result = Double.parseDouble(new DecimalFormat("##.##").format(result));
						runningMapsPerformanceNorm.add(result);
					}
				}
			}

			runningMapsPerformanceNorm1.clear();
			runningMapsPerformanceNorm1 = runningMapsPerformanceNorm;
		}
		runningMapsPerformance1.clear();
		runningMapsPerformance1 = runningMapsPerformanceNorm;
		return runningMapsPerformanceNorm;
	}

//	public static double getRunningMapsAveragePerformanceOLD(String jobId)
//			throws IOException, URISyntaxException, ParseException { // ?????????????????????????????????????????????
//
//		double mapsAveragePerformance = 0;
//		List<Double> runningMapsPerformance = new ArrayList<>();
//
//		try {
//			runningMapsPerformance = getRunningMapsPerformance(jobId);
//
//			mapsAveragePerformance = (runningMapsPerformance.stream().mapToDouble(Double::doubleValue).sum())
//					/ runningMapsPerformance.size();
//
//		} catch (Exception e) {
//			System.out.println("no mapsAveragePerformance!");
//			mapsAveragePerformance = 0;
//		}
//		return mapsAveragePerformance;
//	}

	public static double getRunningMapsAveragePerformance(String jobId)
			throws IOException, URISyntaxException, ParseException {

		double mapsPerformanceMedian = 0;
		int n = 0;

		List<Double> runningMapsPerformance = new ArrayList<>();

		try {
//			runningMapsPerformance = getRunningMapsPerformance(jobId);

//			runningMapsPerformance = runningMapsPerformance1;

			for (int i = 0; i < runningMapsPerformanceNorm1.size(); i++) {
				runningMapsPerformance.add(runningMapsPerformanceNorm1.get(i));
			}

			Collections.sort(runningMapsPerformance);

			n = runningMapsPerformance.size();

			if (n % 2 != 0) {
				mapsPerformanceMedian = runningMapsPerformance.get(n / 2);
			} else
				mapsPerformanceMedian = (runningMapsPerformance.get((n - 1) / 2) + runningMapsPerformance.get(n / 2))
						/ 2;

		} catch (Exception e) {
//			System.out.println("no mapsPerformanceMedian!");
			mapsPerformanceMedian = 0;
		}
		runningMapsPerformanceMedian1 = (Double.parseDouble(new DecimalFormat("##.##").format(mapsPerformanceMedian)));
		return mapsPerformanceMedian;
	}

	public static List<String> getLowProgressRunningMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> lowProgressRunningMapsName = new ArrayList<String>();

		List<Double> getConfidenceInterval = getRunningMapsProgressConfidenceInterval(jobId);
		double lower = getConfidenceInterval.get(0);
		double upper = getConfidenceInterval.get(1);

		List<String> runningMapsName = new ArrayList<String>();

		if (!(getRunningMapsName(jobId) == null)) {
			runningMapsName = getRunningMapsName(jobId);
		}

		List<Double> runningMapsProgress = getRunningMapsProgress(jobId);

		for (int i = 0; i < runningMapsProgress.size(); i++) {
			if (runningMapsProgress.get(i) < lower) {
				lowProgressRunningMapsName.add(runningMapsName.get(i));
			}
		}

		return lowProgressRunningMapsName;
	}

//	public static List<String> getLowPerformanceRunningMapsNameOLD(String jobId) //// NOT USING
//			throws IOException, URISyntaxException, ParseException {
//
//		List<String> lowPerformanceRunningMapsName = new ArrayList<String>();
//
//		double mapsAveragePerformance = getRunningMapsAveragePerformance(jobId);
//
//		List<String> runningMapsName = new ArrayList<String>();
//
//		if (!(getRunningMapsName(jobId) == null)) {
//			runningMapsName = getRunningMapsName(jobId);
//		}
//
//		List<Double> runningMapsPerformance = getRunningMapsPerformance(jobId);
//
//		for (int i = 0; i < runningMapsPerformance.size(); i++) {
//			if (runningMapsPerformance.get(i) < mapsAveragePerformance) {
//				lowPerformanceRunningMapsName.add(runningMapsName.get(i));
//			}
//		}
//
//		return lowPerformanceRunningMapsName;
//	}

	public static List<String> getLowPerformanceRunningMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

//		List<String> runningMapsName = new ArrayList<String>();

		List<String> lowPerformanceRunningMapsName = new ArrayList<String>();

//		getRunningMapsNameNew(jobId);
		getRunningMapsNameNew2(jobId);

		if (!(runningMapsName1 == null)) {

//			List<Double> runningMapsPerformance = getRunningMapsPerformance(jobId);

			List<Double> runningMapsPerformance = getRunningMapsPerformanceNew(jobId);

//			System.out.println("runningMapsPerformance xxx " + runningMapsPerformance);

			double runningMapsPerformanceMedian = getRunningMapsAveragePerformance(jobId);

//			System.out.println("runningMapsPerformanceMedian xxx " + runningMapsPerformanceMedian);
//			System.out.println();

			if (runningMapsPerformance.size() > 0) {
				for (int i = 0; i < runningMapsPerformance.size(); i++) {
					if (runningMapsPerformance.get(i) * factor < runningMapsPerformanceMedian) {
						lowPerformanceRunningMapsName.add(runningMapsName1.get(i));
					}
				}
			}

			lowPerformanceRunningMapsName1.clear();
			lowPerformanceRunningMapsName1 = lowPerformanceRunningMapsName;

		}
		return lowPerformanceRunningMapsName;
	}

//	public static List<String> getHighExecutionTimeRunningMapsName(String jobId)
//			throws IOException, URISyntaxException, ParseException {
//
//		List<String> getHighExecutionTimeRunningMapsName = new ArrayList<String>();
//
//		List<Double> getConfidenceInterval = getRunningMapsExecutionTimeConfidenceInterval(jobId);
//		double lower = getConfidenceInterval.get(0);
//		double upper = getConfidenceInterval.get(1);
//
//		List<String> runningMapsName = new ArrayList<String>();
//
//		if (!(getRunningMapsName(jobId) == null)) {
//			runningMapsName = getRunningMapsName(jobId);
//		}
//
//		List<Integer> runningMapsExecutionTime = getRunningMapsExecutionTime(jobId);
//
//		for (int i = 0; i < runningMapsExecutionTime.size(); i++) {
//			if (runningMapsExecutionTime.get(i) > upper) {
//				getHighExecutionTimeRunningMapsName.add(runningMapsName.get(i));
//			}
//		}
//
//		return getHighExecutionTimeRunningMapsName;
//	}

	public static List<Double> getLocalRunningMapsPerformance(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> localRunningMapsName = new ArrayList<String>();
		int localRunningMapsNum = 0;

		if (!(getRunningMapsName(jobId) == null)) {
			localRunningMapsName = getLocalRunningMapsName(jobId);
			localRunningMapsNum = localRunningMapsName.size();
		}

		List<Double> localRunningMapsPerformance = new ArrayList<Double>();
		int executionTime = 0;
		double progress = 0;
		double result = 0;

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < localRunningMapsNum; k++) {
			try {
				query1.setCustomQuery("select executionTimeSec, progress from map where jobId='" + jobId
						+ "' and mapId='" + localRunningMapsName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				executionTime = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				progress = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));

//				System.out.println("executionTime= " + executionTime + "     progress= " + progress);

				if (executionTime == 0) {
					result = 0;
				} else
					result = progress / executionTime;

				localRunningMapsPerformance.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));

			} catch (Exception e) {
				System.out.println("no localRunningMapsPerformance!");
//				runningMapsPerformance = null;
				localRunningMapsPerformance.add(0.0);
			}
		}
		return localRunningMapsPerformance;
	}

	public static List<Double> getNonLocalRunningMapsPerformance(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nonLocalRunningMapsName = new ArrayList<String>();
		int nonLocalRunningMapsNum = 0;

		if (!(getRunningMapsName(jobId) == null)) {
			nonLocalRunningMapsName = getNonLocalRunningMapsName(jobId);
			nonLocalRunningMapsNum = nonLocalRunningMapsName.size();
		}

		List<Double> nonLocalRunningMapsPerformance = new ArrayList<Double>();
		int executionTime = 0;
		double progress = 0;
		double result = 0;

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < nonLocalRunningMapsNum; k++) {
			try {
				query1.setCustomQuery("select executionTimeSec, progress from map where jobId='" + jobId
						+ "' and mapId='" + nonLocalRunningMapsName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				executionTime = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				progress = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));

//				System.out.println("executionTime= " + executionTime + "     progress= " + progress);

				if (executionTime == 0) {
					result = 0;
				} else
					result = progress / executionTime;

				nonLocalRunningMapsPerformance.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));

			} catch (Exception e) {
				System.out.println("no nonLocalRunningMapsPerformance!");
//				runningMapsPerformance = null;
				nonLocalRunningMapsPerformance.add(0.0);
			}
		}
		return nonLocalRunningMapsPerformance;
	}

//	public static List<Double> checkRunningReducesPerformance(String jobId)
//			throws IOException, URISyntaxException, ParseException {
//
//		List<String> runningReducesName = getRunningReducesName(jobId);
//
//		int runningReducesNum = 0;
//
//		if (!(runningReducesName == null)) {
//			runningReducesNum = runningReducesName.size();
//		}
//
//		List<Integer> runningReducesShuffleExecutionTime = getRunningReducesShuffleExecutionTime(jobId);
//		List<Double> runningReducesShuffleMb = getRunningReducesShuffleMb(jobId);
//		List<Double> runningReducesPerformance = new ArrayList<Double>();
//
//		double result = 0;
//
//		for (int k = 0; k < runningReducesNum; k++) {
//			try {
////				System.out.println("executionTime= " + runningReducesShuffleExecutionTime.get(k) + "     progress= "
////						+ runningReducesShuffleMb.get(k));
//
//				if (runningReducesShuffleExecutionTime.get(k) == 0) {
//					result = 0;
//				} else
//					result = runningReducesShuffleMb.get(k) / runningReducesShuffleExecutionTime.get(k);
//
//				runningReducesPerformance.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
//
//			} catch (Exception e) {
//				System.out.println("no runningReducesPerformance!");
//
//				runningReducesPerformance.add(0.0);
//			}
//		}
//		return runningReducesPerformance;
//	}
//
//	public static List<Double> checkSucceededReducesPerformance(String jobId)
//			throws IOException, URISyntaxException, ParseException {
//
//		List<String> succeededReducesName = getSucceededReducesName(jobId);
//
//		int succeededReducesNum = 0;
//
//		if (!(succeededReducesName == null)) {
//			succeededReducesNum = succeededReducesName.size();
//		}
//
//		List<Integer> succeededReducesShuffleExecutionTime = getSucceededReducesShuffleExecutionTime(jobId);
//		List<Double> succeededReducesShuffleMb = getSucceededReducesShuffleMb(jobId);
//		List<Double> succeededReducesPerformance = new ArrayList<Double>();
//
//		double result = 0;
//
//		for (int k = 0; k < succeededReducesNum; k++) {
//			try {
////				System.out.println("executionTime= " + runningReducesShuffleExecutionTime.get(k) + "     progress= "
////						+ runningReducesShuffleMb.get(k));
//
//				if (succeededReducesShuffleExecutionTime.get(k) == 0) {
//					result = 0;
//				} else
//					result = succeededReducesShuffleMb.get(k) / succeededReducesShuffleExecutionTime.get(k);
//
//				succeededReducesPerformance.add(Double.parseDouble(new DecimalFormat("##.##").format(result)));
//
//			} catch (Exception e) {
//				System.out.println("no succeededReducesPerformance!");
//
//				succeededReducesPerformance.add(0.0);
//			}
//		}
//		return succeededReducesPerformance;
//	}

	public static double checkJobPerformance(String jobId) throws IOException, URISyntaxException, ParseException { // ?????????????????????????????????????????????

		// NOT FINISHED need to compare with the input data size. //
		// ?????????????????????????????????????????????????????????????????????????????
		double jobPerformance = 0;
		int makespan = 0;
		double progress = 0;
		double result = 0;
		double inputDataSize = getInputDataSize(jobId);

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		try {
			query1.setCustomQuery(
					"select makespanSec, progress from job where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			String[] values = (obj6.get(0).toString()).split(",");
			makespan = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
			progress = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));

//				System.out.println("executionTime= " + executionTime + "     progress= " + progress);

			if (makespan == 0) {
				result = 0;
			} else
				result = progress / makespan;

			jobPerformance = Double.parseDouble(new DecimalFormat("##.##").format(result));

		} catch (Exception e) {
			System.out.println("no jobPerformance!");
			jobPerformance = 0;
		}
		return jobPerformance;
	}

	public static List<Integer> getRunningMapsExecutionTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningMapsName = getRunningMapsName(jobId);

		int runningMapsNum = 0;

		if (!(runningMapsName == null)) {
			runningMapsNum = runningMapsName.size();
		}

		List<Integer> getRunningMapsExecutionTime = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < runningMapsNum; k++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where mapId='" + runningMapsName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getRunningMapsExecutionTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getRunningMapsExecutionTime!");
				getRunningMapsExecutionTime = null;
			}
		}
		return getRunningMapsExecutionTime;
	}

	public static List<Integer> getRunningReducesShuffleExecutionTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningReducesName = getRunningReducesName(jobId);
		int runningReducesNum = 0;

		if (!(runningReducesName == null)) {
			runningReducesNum = runningReducesName.size();
		}

		List<Integer> getRunningReducesShuffleExecutionTime = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < runningReducesNum; k++) {
			try {
				query1.setCustomQuery("select shuffleExecutionTimeSec from reduce where reduceId='"
						+ runningReducesName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getRunningReducesShuffleExecutionTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getRunningReducesShuffleExecutionTime!");
				getRunningReducesShuffleExecutionTime = null;
			}
		}
		return getRunningReducesShuffleExecutionTime;
	}

	public static List<Integer> getSucceededReducesShuffleExecutionTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededReducesName = getSucceededReducesName(jobId);
		int succeededReducesNum = 0;

		if (!(succeededReducesName == null)) {
			succeededReducesNum = succeededReducesName.size();
		}

		List<Integer> getSucceededReducesShuffleExecutionTime = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < succeededReducesNum; k++) {
			try {
				query1.setCustomQuery("select shuffleExecutionTimeSec from reduce where reduceId='"
						+ succeededReducesName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getSucceededReducesShuffleExecutionTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getSucceededReducesShuffleExecutionTime!");
				getSucceededReducesShuffleExecutionTime = null;
			}
		}
		return getSucceededReducesShuffleExecutionTime;
	}

	public static List<Double> getRunningReducesShuffleMb(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningReducesName = getRunningReducesName(jobId);

		int runningReducesNum = 0;

		if (!(runningReducesName == null)) {
			runningReducesNum = runningReducesName.size();
		}

		List<Double> getRunningReducesShuffleMb = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < runningReducesNum; k++) {
			try {
				query1.setCustomQuery("select reduceShuffleMb from reduceDetails where reduceId='"
						+ runningReducesName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getRunningReducesShuffleMb
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
//				System.out.println("no getRunningReducesShuffleMb!");
				getRunningReducesShuffleMb.add(0.0);
			}
		}
		return getRunningReducesShuffleMb;
	}

	public static List<Double> getSucceededReducesShuffleMb(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededReducesName = getSucceededReducesName(jobId);

		int succeededReducesNum = 0;

		if (!(succeededReducesName == null)) {
			succeededReducesNum = succeededReducesName.size();
		}

		List<Double> getSucceededReducesShuffleMb = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < succeededReducesNum; k++) {
			try {
				query1.setCustomQuery("select reduceShuffleMb from reduceDetails where reduceId='"
						+ succeededReducesName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getSucceededReducesShuffleMb
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getSucceededReducesShuffleMb!");
				getSucceededReducesShuffleMb.add(0.0);
			}
		}
		return getSucceededReducesShuffleMb;
	}

	public static List<Integer> getRunningReducesMergedMapsNum(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> runningReducesName = getRunningReducesName(jobId);

		int runningReducesNum = 0;
		if (!(runningReducesName == null)) {
			runningReducesNum = runningReducesName.size();
		}
		List<Integer> getRunningReducesMergedMapsNum = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < runningReducesNum; k++) {
			try {
				query1.setCustomQuery("select mergedMaps from reduceDetails where reduceId='"
						+ runningReducesName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getRunningReducesMergedMapsNum
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
//				System.out.println("no getRunningReducesMergedMapsNum!");
				getRunningReducesMergedMapsNum.add(0);
			}
		}
		return getRunningReducesMergedMapsNum;
	}

	public static List<Integer> getSucceededReducesMergedMapsNum(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededReducesName = getSucceededReducesName(jobId);

		int succeededReducesNum = 0;
		if (!(succeededReducesName == null)) {
			succeededReducesNum = succeededReducesName.size();
		}
		List<Integer> getSucceededReducesMergedMapsNum = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < succeededReducesNum; k++) {

			try {
				query1.setCustomQuery("select mergedMaps from reduceDetails where reduceId='"
						+ succeededReducesName.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getSucceededReducesMergedMapsNum
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
//				System.out.println("no getSucceededReducesMergedMapsNum!");
				getSucceededReducesMergedMapsNum.add(0);
			}
		}
		return getSucceededReducesMergedMapsNum;
	}

	// return the number of completed maps.
	public static int getSucceededReducesNum(String jobId) throws IOException, URISyntaxException, ParseException {
		int completedReducesNum = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select reducesCompleted from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			completedReducesNum = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));

		} catch (Exception e) {
			System.out.println("no completedReducesNum!");
			completedReducesNum = 0;
		}
		return completedReducesNum;
	}

	public static List<String> getSucceededReducesName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> succeededReducesName = new ArrayList<String>();
		int getSucceededReducesNum = getSucceededReducesNum(jobId);

		if (getSucceededReducesNum > 0) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select reduceId from reduce where jobId='" + jobId
						+ "' and state='SUCCEEDED' order by desc limit " + getSucceededReducesNum);
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					succeededReducesName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			} catch (Exception e) {
				System.out.println("no succeededReducesName!");
				succeededReducesName = null;
			}
		}
		return succeededReducesName;
	}

	public static String getMapBlockId(String jobId, String mapId)
			throws IOException, URISyntaxException, ParseException {

		String mapBlockId = "";
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select blockId from map where mapId='" + mapId + "' and jobId='" + jobId
					+ "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			mapBlockId = (values[1].replace("]", "").replace("\"", ""));
//		System.out.println("mapHost= " + mapHost);
		} catch (Exception e) {
			System.out.println("no mapBlockId!");
			mapBlockId = "";
		}
		return mapBlockId;
	}

	public static String getMapHost(String jobId, String mapId) throws IOException, URISyntaxException, ParseException {

		String mapHost = "";
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select host from map where mapId='" + mapId + "' and jobId='" + jobId + "' limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			mapHost = (values[1].replace("]", "").replace("\"", ""));
//		System.out.println("mapHost= " + mapHost);
		} catch (Exception e) {
			System.out.println("no mapHost!");
			mapHost = "";
		}
		return mapHost;
	}

	public static String getMasterNodeName(String jobId) throws IOException, URISyntaxException, ParseException {

		String masterNodeName = "";
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select nodeHostName from nodesName where nodeType='nameNode' and jobId='" + jobId + "' limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			masterNodeName = (values[1].replace("]", "").replace("\"", ""));
//		System.out.println("masterNodeName= " + masterNodeName);
		} catch (Exception e) {
			System.out.println("no masterNodeName!");
			masterNodeName = "";
		}
		return masterNodeName;
	}

	public static int getClusterTotalDataNodes(String jobId) throws IOException, URISyntaxException, ParseException {

		int clusterTotalDataNodes = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select activeNodes from cluster where jobId='" + jobId + "' limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			clusterTotalDataNodes = Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
//			System.out.println("clusterTotalDataNodes= " + clusterTotalDataNodes);
		} catch (Exception e) {
			System.out.println("no clusterTotalDataNodes!");
			clusterTotalDataNodes = 0;
		}
		return clusterTotalDataNodes;
	}

	public static int getTotalTasksNumOLD(String jobId) throws IOException, URISyntaxException, ParseException {

		// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
		// NOT USING
		// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
		int clusterTotalDataNodes = getDataNodesNames(jobId).size();
		List<String> getDataNodesNames = getDataNodesNames(jobId);
		int getTotalTasksNum = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			for (int i = 0; i < clusterTotalDataNodes; i++) {
				query1.setCustomQuery("select taskNum from resourceProc where hostName='" + getDataNodesNames.get(i)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");
				String[] values = (obj6.get(0).toString()).split(",");
				getTotalTasksNum += Integer.parseInt(values[1].replace("]", "").replace("\"", ""));
				System.out.println(getDataNodesNames.get(i) + " "
						+ Integer.parseInt(values[1].replace("]", "").replace("\"", "")));
			}
		} catch (Exception e) {
			System.out.println("no getTotalTasksNum!");
			getTotalTasksNum = 0;
		}
		return getTotalTasksNum;
	}

	public static int getRunningContainersNum(String jobId) throws IOException, URISyntaxException, ParseException {

		List<Integer> getNodeTotalNumRunningContainers = getNodeTotalNumRunningContainers(jobId);

//		System.out.println("getNodeTotalNumRunningContainers = " + getNodeTotalNumRunningContainers);

		int nodeTotalNumRunningContainers = 0;
		if (!(getNodeTotalNumRunningContainers == null)) {
			nodeTotalNumRunningContainers = getNodeTotalNumRunningContainers(jobId).size();
		}

		int getRunningContainersNum = 0;

		for (int i = 0; i < nodeTotalNumRunningContainers; i++) {
			getRunningContainersNum += getNodeTotalNumRunningContainers.get(i);
		}
		return getRunningContainersNum;
	}

	public static List<String> getDataNodesNames(String jobId) throws IOException, URISyntaxException, ParseException {

		List<String> dataNodesNames = new ArrayList<String>();
		int totalNodes = getClusterTotalDataNodes(jobId);
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");

			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select nodeHostName from nodesName where nodeType='dataNode' and jobId='" + jobId
					+ "' limit " + totalNodes);
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			for (int i = 0; i < obj6.size(); i++) {
				String[] values = (obj6.get(i).toString()).split(",");
				dataNodesNames.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
			}
		} catch (Exception e) {
			System.out.println("no dataNodesNames!");
			dataNodesNames = null;
		}
		return dataNodesNames;
	}
	
	

	public static boolean checkMasterNodeType(String jobId) throws IOException, URISyntaxException, ParseException {
		String masterNode = getMasterNodeName(jobId);
		List<String> dataNodes = getDataNodesNames(jobId);
//		System.out.println("datanodes: " + dataNodes);

		try {
			if (dataNodes.contains(masterNode)) {
//			System.out.println("datanode");
				return false;
			} else {
//			System.out.println("not a datanode");
				return true;
			}
		} catch (Exception e) {
			System.out.println("no checkMasterNodeType!");

		}
		return false;
	}

	public static double getMasterNodeCpuUsage() throws IOException, URISyntaxException, ParseException {

		double masterNodeCpuUsage = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select cpuUsage from resource where hostName='master' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			masterNodeCpuUsage = Double.valueOf(values[1].replace("]", "").replace("\"", ""));
//		System.out.println("masterNodeCpuUsage= " + masterNodeCpuUsage);
		} catch (Exception e) {
			System.out.println("no masterNodeCpuUsage!");
			masterNodeCpuUsage = 0;
		}
		return masterNodeCpuUsage;
	}

	public static double getMasterNodeMemoryUsage() throws IOException, URISyntaxException, ParseException {
		double masterNodeMemoryUsage = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select memoryUsage from resource where hostName='master' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			masterNodeMemoryUsage = Double.valueOf(values[1].replace("]", "").replace("\"", ""));
//			System.out.println("masterNodeMemoryUsage= " + masterNodeMemoryUsage);
		} catch (Exception e) {
			System.out.println("no masterNodeMemoryUsage!");
			masterNodeMemoryUsage = 0;
		}
		return masterNodeMemoryUsage;
	}

	public static List<Double> getDataNodesCpuUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		int totalNodes = getClusterTotalDataNodes(jobId);
		List<String> dataNodesName = getDataNodesNames(jobId);
		List<Double> dataNodesCpuUsage = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select cpuUsage from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				dataNodesCpuUsage
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no dataNodesCpuUsage!");
				dataNodesCpuUsage = null;
			}
		}
		return dataNodesCpuUsage;
	}

	public static double getDataNodesAverageCpuUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		double averageCpuUsage = 0.0;
		double sum = 0.0;
		List<Double> dataNodesCpuUsage = new ArrayList<Double>();

		try {
			dataNodesCpuUsage = getDataNodesCpuUsage(jobId);

			for (int i = 0; i < dataNodesCpuUsage.size(); i++) {
				sum += dataNodesCpuUsage.get(i);
			}

			averageCpuUsage = sum / dataNodesCpuUsage.size();

			averageCpuUsage = (Double.parseDouble(new DecimalFormat("##.##").format(averageCpuUsage)));

		} catch (Exception e) {
			System.out.println("no averageCpuUsage!");
			averageCpuUsage = 0.0;
		}

		return averageCpuUsage;
	}

	public static double getDataNodesAverageMemoryUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		double averageMemoryUsage = 0.0;
		double sum = 0.0;
		List<Double> dataNodesMemoryUsage = new ArrayList<Double>();

		try {
			dataNodesMemoryUsage = getDataNodesMemoryUsage(jobId);

			for (int i = 0; i < dataNodesMemoryUsage.size(); i++) {
				sum += dataNodesMemoryUsage.get(i);
			}

			averageMemoryUsage = sum / dataNodesMemoryUsage.size();
			averageMemoryUsage = (Double.parseDouble(new DecimalFormat("##.##").format(averageMemoryUsage)));

		} catch (Exception e) {
			System.out.println("no averageMemoryUsage!");
			averageMemoryUsage = 0.0;
		}

		return averageMemoryUsage;
	}

	public static List<Double> getNodesHostSpeculativeMapsCpuUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nodesHostSpeculativeMaps = getNodesHostRunningSpeculativeMaps(jobId);
		List<Double> getNodesHostSpeculativeMapsCpuUsage = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < nodesHostSpeculativeMaps.size(); k++) {
			try {
				query1.setCustomQuery("select cpuUsage from resource where hostName='" + nodesHostSpeculativeMaps.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getNodesHostSpeculativeMapsCpuUsage
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getNodesHostSpeculativeMapsCpuUsage!");
				getNodesHostSpeculativeMapsCpuUsage = null;
			}
		}
		return getNodesHostSpeculativeMapsCpuUsage;
	}

	public static List<Double> getNodesCommonCpuUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nodesCommon = new ArrayList<String>();
		for (int i = 0; i < getNodesCommon1.size(); i++) {
			nodesCommon.add(getNodesCommon1.get(i));
		}

		List<Double> getNodesCommonCpuUsage = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < nodesCommon.size(); k++) {
			try {
				query1.setCustomQuery("select cpuUsage from resource where hostName='" + nodesCommon.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getNodesCommonCpuUsage
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getNodesCommonCpuUsage!");
				getNodesCommonCpuUsage = null;
			}
		}
		getNodesCommonCpuUsage1 = getNodesCommonCpuUsage;
		return getNodesCommonCpuUsage;
	}

	public static List<Double> getNodesCommonMemoryUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nodesCommon = new ArrayList<String>();
		for (int i = 0; i < getNodesCommon1.size(); i++) {
			nodesCommon.add(getNodesCommon1.get(i));
		}

		List<Double> getNodesCommonMemoryUsage = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < nodesCommon.size(); k++) {
			try {
				query1.setCustomQuery("select memoryUsage from resource where hostName='" + nodesCommon.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getNodesCommonMemoryUsage
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getNodesCommonMemoryUsage!");
				getNodesCommonMemoryUsage = null;
			}
		}
		getNodesCommonMemoryUsage1 = getNodesCommonMemoryUsage;
		return getNodesCommonMemoryUsage;
	}

	public static List<Double> getNodesHostSpeculativeMapsMemoryUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> nodesHostSpeculativeMaps = getNodesHostRunningSpeculativeMaps(jobId);
		List<Double> getNodesHostSpeculativeMapsMemoryUsage = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < nodesHostSpeculativeMaps.size(); k++) {
			try {
				query1.setCustomQuery("select memoryUsage from resource where hostName='"
						+ nodesHostSpeculativeMaps.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getNodesHostSpeculativeMapsMemoryUsage
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no getNodesHostSpeculativeMapsMemoryUsage!");
				getNodesHostSpeculativeMapsMemoryUsage = null;
			}
		}
		return getNodesHostSpeculativeMapsMemoryUsage;
	}

	public static List<Double> getDataNodesMemoryUsage(String jobId)
			throws IOException, URISyntaxException, ParseException {

		int totalNodes = getClusterTotalDataNodes(jobId);
		List<String> dataNodesName = getDataNodesNames(jobId);
		List<Double> dataNodesMemoryUsage = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select memoryUsage from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				dataNodesMemoryUsage
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no dataNodesMemoryUsage!");
				dataNodesMemoryUsage = null;
			}
		}
		return dataNodesMemoryUsage;
	}

	public static List<Integer> getDataNodesVCoresNum(String jobId)
			throws IOException, URISyntaxException, ParseException {

		int totalNodes = getClusterTotalDataNodes(jobId);
		List<String> dataNodesName = getDataNodesNames(jobId);
		List<Integer> dataNodesVCoresNum = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select VCoresNum from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				dataNodesVCoresNum
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no dataNodesVCoresNum!");
				dataNodesVCoresNum = null;
			}
		}
		return dataNodesVCoresNum;
	}

	public static List<String> getDataNodesNameHavingMaxVCoresNum(String jobId)
			throws IOException, URISyntaxException, ParseException {

//		int totalNodes = getClusterTotalDataNodes(jobId);
//		List<String> dataNodesName = getDataNodesNames(jobId);
		List<String> dataNodesName = getNodesHostRunningMaps(jobId);

		List<String> getDataNodesNameHavingMaxVCoresNum = new ArrayList<String>();

		int maxNum = 0;
		int tempNum = 0;

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < dataNodesName.size(); k++) {
			try {
				query1.setCustomQuery("select VCoresNum from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				tempNum = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				if ((tempNum > maxNum)) {
					maxNum = tempNum;
				}

			} catch (Exception e) {
				System.out.println("no getDataNodesNameHavingMaxVCoresNum1!");
				getDataNodesNameHavingMaxVCoresNum = null;
			}
		}

		for (int k = 0; k < dataNodesName.size(); k++) {
			try {
				query1.setCustomQuery("select VCoresNum from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				tempNum = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				if ((tempNum == maxNum)) {
					getDataNodesNameHavingMaxVCoresNum.add(dataNodesName.get(k));
				}

			} catch (Exception e) {
				System.out.println("no getDataNodesNameHavingMaxVCoresNum2!");
				getDataNodesNameHavingMaxVCoresNum = null;
			}
		}

		getDataNodesNameHavingMaxVCoresNum1.clear();
		getDataNodesNameHavingMaxVCoresNum1 = getDataNodesNameHavingMaxVCoresNum;
		return getDataNodesNameHavingMaxVCoresNum;
	}

	public static List<String> getHighDataNodesName(String jobId)
			throws IOException, URISyntaxException, ParseException {

//		int totalNodes = getClusterTotalDataNodes(jobId);
//		List<String> dataNodesName = getDataNodesNames(jobId);
		List<String> dataNodesName = getDataNodesNames(jobId);

		List<String> getHighDataNodesName = new ArrayList<String>();

		int maxNum = 0;
		int tempNum = 0;

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < dataNodesName.size(); k++) {
			try {
				query1.setCustomQuery("select VCoresNum from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				tempNum = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				if ((tempNum > maxNum)) {
					maxNum = tempNum;
				}

			} catch (Exception e) {
				System.out.println("no getHighDataNodesName!");
				getHighDataNodesName = null;
			}
		}

		for (int k = 0; k < dataNodesName.size(); k++) {
			try {
				query1.setCustomQuery("select VCoresNum from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				tempNum = Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				if ((tempNum == maxNum)) {
					getHighDataNodesName.add(dataNodesName.get(k));
				}

			} catch (Exception e) {
				System.out.println("no getDataNodesNameHavingMaxVCoresNum2!");
				getHighDataNodesName = null;
			}
		}
		return getHighDataNodesName;
	}

	public static List<Integer> getDataNodesTotalMemory(String jobId)
			throws IOException, URISyntaxException, ParseException {

		int totalNodes = getClusterTotalDataNodes(jobId);
		List<String> dataNodesName = getDataNodesNames(jobId);
		List<Integer> dataNodesTotalMemory = new ArrayList<Integer>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select totalMemoryMb from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				dataNodesTotalMemory
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no dataNodesTotalMemory!");
				dataNodesTotalMemory = null;
			}
		}
		return dataNodesTotalMemory;
	}

	public static List<Double> getDataNodesDiskReadSpeed(String jobId)
			throws IOException, URISyntaxException, ParseException {

		int totalNodes = getClusterTotalDataNodes(jobId);
		List<String> dataNodesName = getDataNodesNames(jobId);
		List<Double> dataNodesDiskReadSpeed = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select diskReadSpeedMb from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				dataNodesDiskReadSpeed
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no dataNodesDiskReadSpeed!");
				dataNodesDiskReadSpeed = null;
			}
		}
		return dataNodesDiskReadSpeed;
	}

	public static List<Double> getDataNodesDiskWriteSpeed(String jobId)
			throws IOException, URISyntaxException, ParseException {

		int totalNodes = getClusterTotalDataNodes(jobId);
		List<String> dataNodesName = getDataNodesNames(jobId);
		List<Double> dataNodesDiskWriteSpeed = new ArrayList<Double>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int k = 0; k < totalNodes; k++) {
			try {
				query1.setCustomQuery("select diskWriteSpeedMb from resource where hostName='" + dataNodesName.get(k)
						+ "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				dataNodesDiskWriteSpeed
						.add(Double.parseDouble(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no dataNodesDiskWriteSpeed!");
				dataNodesDiskWriteSpeed = null;
			}
		}
		return dataNodesDiskWriteSpeed;
	}

	public static boolean checkClusterHomogeneity(String jobId) throws IOException, URISyntaxException, ParseException {

		boolean clusterHomogeneity = false;

		boolean allEqualVCores = false;
		boolean allEqualMemory = false;

		if (!(getDataNodesVCoresNum(jobId) == null)) {
			allEqualVCores = getDataNodesVCoresNum(jobId).stream().distinct().limit(2).count() <= 1;
		}

//		if (!(getDataNodesTotalMemory(jobId) == null)) {
//			allEqualMemory = getDataNodesTotalMemory(jobId).stream().distinct().limit(2).count() <= 1;
//		}
//		if (allEqualVCores && allEqualMemory) {
//			clusterHomogeneity = true;
//		}

		if (allEqualVCores) {
			clusterHomogeneity = true;
		}

		return clusterHomogeneity;
	}

//	public static List<Integer> checkMapsSynchronization(String jobId)
//			throws IOException, URISyntaxException, ParseException {
//
//		List<Integer> checkMapsSynchronization = new ArrayList<Integer>();
//		List<String> getLowPerformanceRunningMapsName = new ArrayList<String>();
//
//		List<Double> runningMapsCpuUsage = new ArrayList<Double>();
//		if (!(getRunningMapsCpuUsage(jobId) == null)) {
//			runningMapsCpuUsage = getRunningMapsCpuUsage(jobId);
//		}
//
//		List<String> runningMapsName = new ArrayList<String>();
//
//		if (!(getRunningMapsHostName(jobId) == null)) {
//			runningMapsName = getRunningMapsName(jobId);
//		}
//
//		List<String> runningMapsHostName = new ArrayList<String>();
//
//		if (!(getRunningMapsHostName(jobId) == null)) {
//			runningMapsHostName = getRunningMapsHostName(jobId);
//		}
//
//		List<String> getNodesHostLowPerformanceRunningMaps = new ArrayList<String>();
//
//		List<String> getDataNodesNameHavingMaxVCoresNum = new ArrayList<String>();
//
//		List<String> getLowPerformanceRunningMapsHostName = new ArrayList<String>();
//
//		int numMapsPending = getNumMapsPending(jobId);
//		double clusterCpuUsage = getClusterCpuUsage(jobId);
//		double clusterMemoryUsage = getClusterMemoryUsage(jobId);
//
//		int runningSpeculativeMapsNum = 0;
//		if (!(getRunningSpeculativeMapsName(jobId) == null)) {
//			runningSpeculativeMapsNum = getRunningSpeculativeMapsName(jobId).size();
//		}
//
//		int runningNormalMapsNum = 0;
//		if (!(getRunningNormalMapsName(jobId) == null)) {
//			runningNormalMapsNum = getRunningNormalMapsName(jobId).size();
//		}
//
////		numMapsPending = 1;
////		clusterUsage = 96;    // XXXXXXXXXXXXXXXXXXXXXXX   just for try. 
//
//		int slowestMapIndex = 0;
//		List<String> getNodesCommon = new ArrayList<String>();
//		List<Double> getNodesCommonCpuUsage = new ArrayList<Double>();
//		List<Double> getNodesCommonMemoryUsage = new ArrayList<Double>();
//
//		try {
//
//			if (numMapsPending > 0 && ((clusterCpuUsage > 95.0) || (clusterMemoryUsage > 95))) {
//				checkMapsSynchronization.add(1);// means; there is not resource enough.
//			}
//
//			if ((runningSpeculativeMapsNum > 0) && (runningNormalMapsNum > 0)) {
//				// get the common elements.
//				if (!(getNodesCommon(jobId) == null)) {
//					getNodesCommon = getNodesCommon(jobId);
//					getNodesCommonCpuUsage = getNodesCommonCpuUsage(jobId);
//					getNodesCommonMemoryUsage = getNodesCommonMemoryUsage(jobId);
//				}
//				for (int i = 0; i < getNodesCommon.size(); i++) {
//					if ((getNodesCommonCpuUsage.get(i) > 80) || (getNodesCommonMemoryUsage.get(i) > 80)) {
//						checkMapsSynchronization.add(2);// // means; there is unnecessary speculative executions.
//					}
//				}
//			}
//
//			if (!checkClusterHomogeneity(jobId)) {
//
////				if (!(getLowPerformanceRunningMapsName(jobId) == null)) {
////					getLowPerformanceRunningMapsName = getLowPerformanceRunningMapsName(jobId);
////				}
//////				System.out.println("getLowPerformanceRunningMapsName = " + getLowPerformanceRunningMapsName);
////
////				if (!(getLowPerformanceRunningMapsHostName(jobId) == null)) {
////					getLowPerformanceRunningMapsHostName = getLowPerformanceRunningMapsHostName(jobId);
////				}
////				System.out.println("getLowPerformanceRunningMapsHostName= " + getLowPerformanceRunningMapsHostName);
//
//				if (!(getNodesHostLowPerformanceRunningMaps(jobId) == null)) {
//					for (int i = 0; i < getNodesHostLowPerformanceRunningMaps1.size(); i++) {
//						getNodesHostLowPerformanceRunningMaps.add(getNodesHostLowPerformanceRunningMaps1.get(i));
//					}
//				}
//				System.out.println("getNodesHostLowPerformanceRunningMaps= " + getNodesHostLowPerformanceRunningMaps);
//
//				if (!(getDataNodesNameHavingMaxVCoresNum(jobId) == null)) {
//					for (int i = 0; i < getDataNodesNameHavingMaxVCoresNum1.size(); i++) {
//						getDataNodesNameHavingMaxVCoresNum.add(getDataNodesNameHavingMaxVCoresNum1.get(i));
//					}
//				}
//				System.out.println("getDataNodesNameHavingMaxVCoresNum= " + getDataNodesNameHavingMaxVCoresNum);
//
//				getNodesHostLowPerformanceRunningMaps.removeAll(getDataNodesNameHavingMaxVCoresNum);
//
//				if (getNodesHostLowPerformanceRunningMaps.size() > 0) {
//					checkMapsSynchronization.add(3);
//				}
//
//				// AFTER HERE, it is for finding the slowest one. (extra info)
//
////				List<Double> getRunningMapsPerformance = getRunningMapsPerformance(jobId);
////
////				System.out.println("getRunningMapsPerformance= " + getRunningMapsPerformance);
////
////				slowestMapIndex = getRunningMapsPerformance.indexOf(Collections.min(getRunningMapsPerformance));
////
////				System.out.println("worst performance mapName=  " + getRunningMapsName(jobId).get(slowestMapIndex));
////				System.out.println(
////						"worst performance MapHostName= " + getRunningMapsHostName(jobId).get(slowestMapIndex));
//
//				// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//
////				List<Double> runningMapsPerformance = new ArrayList<Double>(); 
////				if (!(getRunningMapsPerformance(jobId) == null)) {
////					runningMapsPerformance = getRunningMapsPerformance(jobId);
////				}
//
////				// to calculate the average value of all the list values.
////				double averageMapsPerformance = (runningMapsPerformance.stream().mapToDouble(Double::doubleValue).sum())
////						/ runningMapsPerformance.size();
////				for (int i = 0; i < runningMapsPerformance.size(); i++) {
////					if (runningMapsPerformance.get(i) < averageMapsPerformance) { // put and if bigger than 0
////
////						checkMapsSynchronization.add(3);
////						slowestMapIndex = runningMapsPerformance.indexOf(Collections.min(runningMapsPerformance));
////						System.out.println("worst performance mapName=  " + runningMapsName.get(slowestMapIndex));
////						System.out.println("worst performance MapHostName= " + runningMapsHostName.get(slowestMapIndex));
////						break;
////					}
////
////				} // for
//			}
//
//		} catch (Exception e) {
//			System.out.println("no checkMapsSynchronization!");
//			checkMapsSynchronization = null;
//		}
//
//		return checkMapsSynchronization;
//		// if it includes 1, which means there is not resource enough.
//		// if it includes 2, which means there is unnecessary speculative executions.
//		// if it includes 3, which means the heterogeneous cluster has caused this
//		// unsynchronization
//
//	}

	public static boolean checkResourceShortage(String jobId) throws IOException, URISyntaxException, ParseException {

		boolean checkResourceShortage = false;

		int numMapsPending = getNumMapsPending(jobId);
		double clusterCpuUsage = getClusterCpuUsage(jobId);
		double clusterMemoryUsage = getClusterMemoryUsage(jobId);

		try {
			if (numMapsPending > 0 && ((clusterCpuUsage > 95.0) || (clusterMemoryUsage > 95))) {
				checkResourceShortage = true; // means; there is not resource enough.
			}

		} catch (Exception e) {
			System.out.println("no checkResourceShortage!");
			checkResourceShortage = false;
		}

		return checkResourceShortage;

	}

	public static boolean checkUnnecessarySpeculation(String jobId)
			throws IOException, URISyntaxException, ParseException {

		boolean checkUnnecessarySpeculation = false;
		boolean durum = false;

		int runningSpeculativeMapsNum = runningSpeculativeMapsName1.size();
		int lowPerformanceRunningMapsName = lowPerformanceRunningMapsName1.size();

		List<String> getNodesCommon = new ArrayList<String>();
		List<Double> getNodesCommonCpuUsage = new ArrayList<Double>();
		List<Double> getNodesCommonMemoryUsage = new ArrayList<Double>();

		for (int i = 0; i < lowPerformanceRunningMapsName1.size(); i++) {
			if (!lowPerformanceRunningMapsName1.get(i).contains("_")) {
				durum = true;
			}
		}

		if (durum) {
			try {
				if ((runningSpeculativeMapsNum > 0) && (lowPerformanceRunningMapsName > 0)) {
					// get the common elements.
					if (!(getNodesCommon(jobId) == null)) {
						for (int i = 0; i < getNodesCommon1.size(); i++) {
							getNodesCommon.add(getNodesCommon1.get(i));
						}
						getNodesCommonCpuUsage = getNodesCommonCpuUsage(jobId);
						getNodesCommonMemoryUsage = getNodesCommonMemoryUsage(jobId);
					}
					for (int i = 0; i < getNodesCommon.size(); i++) {
						if ((getNodesCommonCpuUsage.get(i) > 80) || (getNodesCommonMemoryUsage.get(i) > 80)) {
							checkUnnecessarySpeculation = true;// // means; there is unnecessary speculative executions.
						}
					}
				}
			} catch (Exception e) {
				System.out.println("no checkUnnecessarySpeculation!");
				checkUnnecessarySpeculation = false;
			}
		}
		return checkUnnecessarySpeculation;
	}

	public static boolean checkHeterogeneousCluster(String jobId)
			throws IOException, URISyntaxException, ParseException {

		boolean checkHeterogeneousCluster = false;

		List<String> getNodesHostLowPerformanceRunningMaps = new ArrayList<String>();

		List<String> getDataNodesNameHavingMaxVCoresNum = new ArrayList<String>();

		try {
			if (!(getNodesHostLowPerformanceRunningMaps(jobId) == null)) {
				for (int i = 0; i < getNodesHostLowPerformanceRunningMaps1.size(); i++) {
					getNodesHostLowPerformanceRunningMaps.add(getNodesHostLowPerformanceRunningMaps1.get(i));
				}
			}

//				System.out.println("getNodesHostLowPerformanceRunningMaps= " + getNodesHostLowPerformanceRunningMaps);

			if (!(getDataNodesNameHavingMaxVCoresNum(jobId) == null)) {
				for (int i = 0; i < getDataNodesNameHavingMaxVCoresNum1.size(); i++) {
					getDataNodesNameHavingMaxVCoresNum.add(getDataNodesNameHavingMaxVCoresNum1.get(i));
				}
			}
//				System.out.println("getDataNodesNameHavingMaxVCoresNum= " + getDataNodesNameHavingMaxVCoresNum);

			getNodesHostLowPerformanceRunningMaps.removeAll(getDataNodesNameHavingMaxVCoresNum);

			if (getNodesHostLowPerformanceRunningMaps.size() > 0) {
				checkHeterogeneousCluster = true;
			}

		} catch (Exception e) {
			System.out.println("no checkHeterogeneousCluster!");
			checkHeterogeneousCluster = false;
		}

		return checkHeterogeneousCluster;

	}

	public static int getNumJobs() throws IOException, URISyntaxException, ParseException {

		int numJobs = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select count(distinct(jobId)) from map");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			numJobs = new Double(values[1].replace("]", "").replace("\"", "")).intValue();
		} catch (Exception e) {
//			System.out.println("no numJobs!");
			numJobs = 0;
		}
		return numJobs;
	}

	public static String getLastJobNo() throws IOException, URISyntaxException, ParseException {

		String lastJobNo = "";
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select distinct(jobId) from cluster order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			lastJobNo = (values[1].replace("]", "").replace("\"", ""));
		} catch (Exception e) {
			System.out.println("no lastJobNo!");
			lastJobNo = "";
		}
		return lastJobNo;
	}

	public static double getLastJobMapProgress(String jobId) throws IOException, URISyntaxException, ParseException {

		double lastJobMapProgress = 0;
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");
			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select mapProgress from jobDetails where jobId='" + jobId + "' order by desc limit 1");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
			String[] values = (obj6.get(0).toString()).split(",");
			lastJobMapProgress = Double.parseDouble((values[1].replace("]", "").replace("\"", "")));
		} catch (Exception e) {
			System.out.println("no lastJobMapProgress!");
			lastJobMapProgress = 0;
		}
		return lastJobMapProgress;
	}

	public static List<String> getSucceededStragglersRunningSlowNodesHostName(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> stragglersRunningSlowNodes = new ArrayList<String>();

		List<String> succeededMapsName = new ArrayList<String>();

		if (!(getSucceededMapsName(jobId) == null)) {
			for (int i = 0; i < succeededMapsName1.size(); i++) {
				succeededMapsName.add(succeededMapsName1.get(i));
			}
		}

		for (int i = 0; i < HeterogeneousCluster.stragglersRunningSlowNodes.size(); i++) {
			stragglersRunningSlowNodes.add(HeterogeneousCluster.stragglersRunningSlowNodes.get(i));
		}

		stragglersRunningSlowNodes.retainAll(succeededMapsName);

		int stragglersRunningSlowNodesNum = stragglersRunningSlowNodes.size();

		getSucceededStragglersNameRunningSlowNodes1.clear();

		getSucceededStragglersNameRunningSlowNodes1 = stragglersRunningSlowNodes;

		List<String> stragglersRunningSlowNodesHostName = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < stragglersRunningSlowNodesNum; i++) {
			try {
				query1.setCustomQuery("select host from map where mapId='" + stragglersRunningSlowNodes.get(i)
						+ "' and jobId='" + jobId + "' and state='SUCCEEDED' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				stragglersRunningSlowNodesHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no stragglersRunningSlowNodesHostName!");
				stragglersRunningSlowNodesHostName = null;
			}
		}

		return stragglersRunningSlowNodesHostName;
	}

	public static List<String> getMapsOnSlowDataNodes(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> getMapsOnSlowDataNode = new ArrayList<String>();

		List<String> getSlowDataNodesName = new ArrayList<String>();

		for (int i = 0; i < HeterogeneousCalculator.getSlowDataNodesName.size(); i++) {
			getSlowDataNodesName.add(HeterogeneousCalculator.getSlowDataNodesName.get(i));
		}

		System.out.println();
		System.out.println("getSlowDataNodesName in SmartReader: " + getSlowDataNodesName);

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();
		String deger = "";

		for (int i = 0; i < getSlowDataNodesName.size(); i++) {

			try {
				query1.setCustomQuery("select distinct(mapId) from map where jobId='" + jobId + "' and host='" + getSlowDataNodesName.get(i) + "'");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");
//				System.out.println("obj6 in SmartReader = " + obj6);
				for (int j = 0; j < obj6.size(); j++) {
					String[] values = (obj6.get(j).toString()).split(",");
					deger = values[1].replace("]", "").replace("\"", "").replace("\\", "");
					if (!getMapsOnSlowDataNode.contains(deger)) {
						getMapsOnSlowDataNode.add(deger);
					}
				}
				System.out.println("getMapsOnSlowDataNode in SmartReader = " + getMapsOnSlowDataNode);
				System.out.println();
			} catch (Exception e) {
				System.out.println("no getMapsOnSlowDataNode!");
				System.out.println(e);
//				e.printStackTrace();
//				getMapsOnSlowDataNode = null;
			}
		}
		return getMapsOnSlowDataNode;
	}

	public static List<String> getSucceededMapsRunningHighNodesHostName(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> stragglersRunningSlowNodes = new ArrayList<String>();

		List<String> succeededMapsName = new ArrayList<String>();

		if (!(getSucceededMapsName(jobId) == null)) {
			for (int i = 0; i < succeededMapsName1.size(); i++) {
				succeededMapsName.add(succeededMapsName1.get(i));
			}
		}

		for (int i = 0; i < HeterogeneousCluster.stragglersRunningSlowNodes.size(); i++) {
			stragglersRunningSlowNodes.add(HeterogeneousCluster.stragglersRunningSlowNodes.get(i));
		}

		succeededMapsName.removeAll(stragglersRunningSlowNodes);

		int mapsRunningHighNodesNum = succeededMapsName.size();

		getSucceededMapsNameRunningHighNodes1.clear();

		getSucceededMapsNameRunningHighNodes1 = succeededMapsName;

		List<String> mapsRunningHighNodesHostName = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < mapsRunningHighNodesNum; i++) {
			try {
				query1.setCustomQuery("select host from map where mapId='" + succeededMapsName.get(i) + "' and jobId='"
						+ jobId + "' and state='SUCCEEDED' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				mapsRunningHighNodesHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no mapsRunningHighNodesHostName!");
//				mapsRunningHighNodesHostName = null;
			}
		}

		return mapsRunningHighNodesHostName;
	}

	public static List<Integer> getSucceededStragglersRunningSlowNodesExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> succeededStragglersRunningSlowNodesExecTime = new ArrayList<Integer>();
		List<String> succeededStragglersNameRunningSlowNodes = getSucceededStragglersNameRunningSlowNodes1;

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < succeededStragglersNameRunningSlowNodes.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ succeededStragglersNameRunningSlowNodes.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				succeededStragglersRunningSlowNodesExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no succeededStragglersRunningSlowNodesExecTime!");
				succeededStragglersRunningSlowNodesExecTime = null;
			}
		}
		return succeededStragglersRunningSlowNodesExecTime;
	}

	public static List<Integer> getSucceededMapsRunningHighNodesExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> succeededMapsRunningHighNodesExecTime = new ArrayList<Integer>();

		List<String> succeededMapsNameRunningHighNodes = new ArrayList<String>();

		for (int i = 0; i < getSucceededMapsNameRunningHighNodes1.size(); i++) {
			succeededMapsNameRunningHighNodes.add(getSucceededMapsNameRunningHighNodes1.get(i));
		}

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < succeededMapsNameRunningHighNodes.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ succeededMapsNameRunningHighNodes.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				succeededMapsRunningHighNodesExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no succeededMapsRunningHighNodesExecTime!");
				succeededMapsRunningHighNodesExecTime = null;
			}
		}
		return succeededMapsRunningHighNodesExecTime;
	}

	public static List<Integer> stragglersRunningSlowNodesExecTime(String jobId) // aaaaaaaaaaaa
			throws IOException, URISyntaxException, ParseException {

		List<Integer> stragglersRunningSlowNodesExecTime = new ArrayList<Integer>();

		List<String> stragglersRunningSlowNodes = new ArrayList<String>();
		for (int i = 0; i < HeterogeneousCluster.stragglersRunningSlowNodes.size(); i++) {
			stragglersRunningSlowNodes.add(HeterogeneousCluster.stragglersRunningSlowNodes.get(i));
		}

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < stragglersRunningSlowNodes.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ stragglersRunningSlowNodes.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				stragglersRunningSlowNodesExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no stragglersRunningSlowNodesExecTime!");
				stragglersRunningSlowNodesExecTime = null;
			}
		}
		return stragglersRunningSlowNodesExecTime;
	}

	// return the name of the succeeded maps
	public static List<String> getKilledMapsName(String jobId) throws IOException, URISyntaxException, ParseException {

		List<String> killedMapsName = new ArrayList<String>();
		int getkilledMapsNum = getKilledMapsNum(jobId);

		if (getkilledMapsNum != 0) {
			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery(
						"select mapId from map where jobId='" + jobId + "' and state='KILLED' order by desc");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					killedMapsName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
				}
			} catch (Exception e) {
				System.out.println("no killedMapsName!");
				killedMapsName = null;
			}
		}

		killedMapsName1 = killedMapsName;
		return killedMapsName;
	}

	public static List<String> getFinishedAllMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getSucceededMapsName = getSucceededMapsName(jobId);

		List<String> getKilledMapsName = getKilledMapsName(jobId);

		List<String> getFinishedAllMapsName = new ArrayList<String>(getSucceededMapsName);

//		getFinishedAllMapsName.addAll(getKilledMapsName); // wrong 

		for (int i = 0; i < getKilledMapsName.size(); i++) {
			if (!getFinishedAllMapsName.contains(getKilledMapsName.get(i))) {
				getFinishedAllMapsName.add(getKilledMapsName.get(i));
			}
		}

		getFinishedAllMapsName1.clear();
		getFinishedAllMapsName1 = getFinishedAllMapsName;
		return getFinishedAllMapsName;
	}

	public static List<String> getFinishedAllNonLocalMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> finishedAllMapsName = new ArrayList<String>();

		for (int i = 0; i < getFinishedAllMapsName1.size(); i++) {
			finishedAllMapsName.add(getFinishedAllMapsName1.get(i));
		}

		List<String> finishedAllNonLocalMapsName = new ArrayList<>();

		try {
			if (finishedAllMapsName.size() != 0) {
				String mapId = "";

				for (int i = 0; i < finishedAllMapsName.size(); i++) {
					mapId = finishedAllMapsName.get(i);

					if (checkMapDataLocality(jobId, mapId)) {
//					System.out.println(mapId + " has the data locally..");
					} else {
						finishedAllNonLocalMapsName.add(mapId);
					}
//					Thread.sleep(50);
				}
			}
		} catch (Exception e) {
			System.out.println("no finishedAllNonLocalMapsName!");
			finishedAllNonLocalMapsName = null;
		}

		finishedAllNonLocalMapsName1 = finishedAllNonLocalMapsName;
		return finishedAllNonLocalMapsName;
	}

	// return the name of all the normal maps (not including speculative)
	// ****************
	public static List<String> getFinishedAllNormalMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getFinishedAllNormalMapsName = new ArrayList<String>();

		List<String> getSucceededMapsName = getSucceededMapsName(jobId);

		List<String> getKilledMapsName = getKilledMapsName(jobId);

		for (int i = 0; i < getSucceededMapsName.size(); i++) {
			if (!getSucceededMapsName.get(i).contains("_")) {
				getFinishedAllNormalMapsName.add(getSucceededMapsName.get(i));
			}
		}

		for (int i = 0; i < getKilledMapsName.size(); i++) {
			if (!getKilledMapsName.get(i).contains("_")) {
				getFinishedAllNormalMapsName.add(getKilledMapsName.get(i));
			}
		}

		return getFinishedAllNormalMapsName;
	}

	public static List<Integer> getFinishedAllLocalMapsExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> finishedAllLocalMapsExecTime = new ArrayList<Integer>();
		List<String> finishedAllLocalMapsName = DataLocalityCalculator.finishedAllLocalMapsName;

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < finishedAllLocalMapsName.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ finishedAllLocalMapsName.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				finishedAllLocalMapsExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no finishedAllLocalMapsExecTime!");
				finishedAllLocalMapsExecTime = null;
			}
		}
		return finishedAllLocalMapsExecTime;
	}

	public static List<String> getFinishedAllNonLocalNormalMapsName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> finishedAllMapsName = getFinishedAllNormalMapsName(jobId);

		List<String> finishedAllNonLocalNormalMapsName = new ArrayList<>();

		try {
			if (finishedAllMapsName.size() != 0) {
				String mapId = "";

				for (int i = 0; i < finishedAllMapsName.size(); i++) {
					mapId = finishedAllMapsName.get(i);

					if (checkMapDataLocality(jobId, mapId)) {
//					System.out.println(mapId + " has the data locally..");
					} else {
						finishedAllNonLocalNormalMapsName.add(mapId);
					}
//					Thread.sleep(50);
				}
			}
		} catch (Exception e) {
			System.out.println("no finishedAllNonLocalNormalMapsName!");
			finishedAllNonLocalNormalMapsName = null;
		}

		return finishedAllNonLocalNormalMapsName;
	}

	public static List<Integer> getFinishedAllNonLocalMapsExecTime(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<Integer> finishedAllNonLocalMapsExecTime = new ArrayList<Integer>();
		List<String> finishedAllNonLocalMapsName = getFinishedAllNonLocalNormalMapsName(jobId);

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < finishedAllNonLocalMapsName.size(); i++) {
			try {
				query1.setCustomQuery("select executionTimeSec from map where jobId='" + jobId + "' and mapId='"
						+ finishedAllNonLocalMapsName.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				finishedAllNonLocalMapsExecTime
						.add(Integer.parseInt(values[1].replace("]", "").replace("\"", "").replace("\\", "")));

			} catch (Exception e) {
				System.out.println("no finishedAllNonLocalMapsExecTime!");
				finishedAllNonLocalMapsExecTime = null;
			}
		}
		return finishedAllNonLocalMapsExecTime;
	}

	public static List<String> getFinishedNonLocalStragglersHostName(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> nonLocalStragglersName = new ArrayList<String>();

		for (int i = 0; i < DataLocalityCalculator.finishedAllNonLocalMapsNameYEDEKxxx.size(); i++) {
			nonLocalStragglersName.add(DataLocalityCalculator.finishedAllNonLocalMapsNameYEDEKxxx.get(i));
		}

		int nonLocalStragglersNum = nonLocalStragglersName.size();

		List<String> finishedNonLocalStragglersHostName = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < nonLocalStragglersNum; i++) {
			try {
				query1.setCustomQuery("select host from map where mapId='" + nonLocalStragglersName.get(i)
						+ "' and state='RUNNING' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				finishedNonLocalStragglersHostName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no nonLocalStragglersHostName!");
				finishedNonLocalStragglersHostName = null;
			}
		}

		return finishedNonLocalStragglersHostName;
	}

	public static List<String> getStragglersRunningSlowNodesHostName(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> getStragglersRunningSlowNodesHostName = new ArrayList<String>();

		List<String> stragglersRunningSlowNodes = new ArrayList<String>();
		for (int i = 0; i < HeterogeneousCluster.stragglersRunningSlowNodes.size(); i++) {
			stragglersRunningSlowNodes.add(HeterogeneousCluster.stragglersRunningSlowNodes.get(i));
		}

		int lowPerformanceRunningMapsNum = stragglersRunningSlowNodes.size();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();

		for (int i = 0; i < lowPerformanceRunningMapsNum; i++) {
			try {
				query1.setCustomQuery("select host from map where jobId='" + jobId + "' and mapId='"
						+ stragglersRunningSlowNodes.get(i) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				String[] values = (obj6.get(0).toString()).split(",");
				getStragglersRunningSlowNodesHostName
						.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));

			} catch (Exception e) {
				System.out.println("no getStragglersRunningSlowNodesHostName!");
				getStragglersRunningSlowNodesHostName = null;
			}
		}
		return getStragglersRunningSlowNodesHostName;
	}

	// NETWORK ISSUES
	// *****************************************************************************************************

	public static List<String> getDisconnectedNodesName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getDisconnectedNodesName = new ArrayList<String>();
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");

			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery(
					"select distinct(nodeHostName) from nodesName where jobId='" + jobId + "' and state='SHUTDOWN'");

			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			for (int i = 0; i < obj6.size(); i++) {
				String[] values = (obj6.get(i).toString()).split(",");
				getDisconnectedNodesName.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
			}
		} catch (Exception e) {
//			System.out.println("no getDisconnectedNodesName!");
			getDisconnectedNodesName = null;
		}
		disconnectedNodes = null;
		disconnectedNodes = getDisconnectedNodesName;
		return getDisconnectedNodesName;
	}

	// ANOTHER WAY to get the maps on disconnected nodes
//	public static List<String> getMapsOnDisconnectedNodes(String jobId)
//			throws IOException, URISyntaxException, ParseException, InterruptedException {
//
//		List<String> getMapsOnDisconnectedNodes = new ArrayList<String>();
//
//		List<String> disconnectedNodesNames = new ArrayList<String>();
//
//		for (int i = 0; i < disconnectedNodes.size(); i++) {
//			disconnectedNodesNames.add(disconnectedNodes.get(i));
//		}
//
////		System.out.println();
////		System.out.println("disconnectedNodesNames in SmartReader: " + disconnectedNodesNames);
//
//
//		query.setMeasurement("sigar");
//		query.setLimit(1000);
//		query.fillNullValues("0");
//		DataReader dataReader = new DataReader(query, configuration);
//		ResultSet resultSet = dataReader.getResult();
//		Query query1 = new Query();
//		String deger = "";
//			
//
//		for (int i = 0; i < disconnectedNodesNames.size(); i++) {
//
//			try {
//				query1.setCustomQuery("select distinct(mapId) from map where jobId='" + jobId + "' and host='" + disconnectedNodesNames.get(i) + "'");
//				dataReader.setQuery(query1);
//				resultSet = dataReader.getResult();
//				Gson gson = new Gson();
//				String jsonStr = gson.toJson(resultSet);
//				JSONParser parser = new JSONParser();
//				JSONObject obj = (JSONObject) parser.parse(jsonStr);
//				JSONArray obj2 = (JSONArray) obj.get("results");
//				JSONObject obj3 = (JSONObject) obj2.get(0);
//				JSONArray obj4 = (JSONArray) obj3.get("series");
//				JSONObject obj5 = (JSONObject) obj4.get(0);
//				JSONArray obj6 = (JSONArray) obj5.get("values");
////				System.out.println("obj6 in SmartReader = " + obj6);
//				for (int j = 0; j < obj6.size(); j++) {
//					String[] values = (obj6.get(j).toString()).split(",");
//					deger = values[1].replace("]", "").replace("\"", "").replace("\\", "");
//					if (!getMapsOnDisconnectedNodes.contains(deger)) {
//						getMapsOnDisconnectedNodes.add(deger);
//					}
//				}
////				System.out.println("mapsOnDisconnectedNodes in SmartReader = " + getMapsOnDisconnectedNodes);
////				System.out.println();
//			} catch (Exception e) {
//				System.out.println("no mapsOnDisconnectedNodes!");
//				System.out.println(e);
////				e.printStackTrace();
//				getMapsOnDisconnectedNodes = null;
//			}
//		}
//		mapsOnDisconnectedNodes = null;
//		mapsOnDisconnectedNodes = getMapsOnDisconnectedNodes;
//		return getMapsOnDisconnectedNodes;
//	}

	public static List<String> getMapsOnDisconnectedNodes(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> getMapsOnDisconnectedNodes = new ArrayList<String>();

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();
		String deger = "";

		try {
			query1.setCustomQuery(
					"select distinct(mapId) from map where jobId='" + jobId + "' and diagnostic='DisconnectedNode'");
			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");
//				System.out.println("obj6 in SmartReader = " + obj6);
			for (int j = 0; j < obj6.size(); j++) {
				String[] values = (obj6.get(j).toString()).split(",");
				deger = values[1].replace("]", "").replace("\"", "").replace("\\", "");
				if (!getMapsOnDisconnectedNodes.contains(deger)) {
					getMapsOnDisconnectedNodes.add(deger);
				}
			}
//				System.out.println("mapsOnDisconnectedNodes in SmartReader = " + getMapsOnDisconnectedNodes);
//				System.out.println();
		} catch (Exception e) {
			System.out.println("no mapsOnDisconnectedNodes!");
			System.out.println(e);
//				e.printStackTrace();
			getMapsOnDisconnectedNodes = null;
		}

		mapsOnDisconnectedNodes = null;
		mapsOnDisconnectedNodes = getMapsOnDisconnectedNodes;
		return getMapsOnDisconnectedNodes;
	}

	// NOT CORRECT EXACTLY as it should check 
	public static List<String> getRunningMapsRestarted(String jobId) throws IOException, URISyntaxException, ParseException {

		List<String> getRunningMapsRestarted = new ArrayList<String>();
//		int totalNodes = getClusterTotalDataNodes(jobId);
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");

			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select distinct(mapId) from map where jobId='" + jobId + "' and mapId=~/_/ and state='RUNNING'");
//			query1.setCustomQuery("select mapId from map where jobId='" + jobId + "' and state='RUNNING' and blockId <> '-1' and caseId='" + runningMapsCaseId + "' order by desc");

			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			for (int i = 0; i < obj6.size(); i++) {
				String[] values = (obj6.get(i).toString()).split(",");
				getRunningMapsRestarted.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
			}
		} catch (Exception e) {
			System.out.println("no getRunningMapsRestarted!");
			getRunningMapsRestarted = null;
		}
		runningMapsRestarted = null;
		runningMapsRestarted = getRunningMapsRestarted;
		return getRunningMapsRestarted;
	}
	
	public static List<String> getAllMapsRestarted(String jobId) throws IOException, URISyntaxException, ParseException {

		List<String> getAllMapsRestarted = new ArrayList<String>();
//		int totalNodes = getClusterTotalDataNodes(jobId);
		try {
			query.setMeasurement("sigar");
			query.setLimit(1000);
			query.fillNullValues("0");

			DataReader dataReader = new DataReader(query, configuration);
			ResultSet resultSet = dataReader.getResult();
			Query query1 = new Query();
			query1.setCustomQuery("select distinct(mapId) from map where jobId='" + jobId + "' and mapId=~/_/");

			dataReader.setQuery(query1);
			resultSet = dataReader.getResult();
			Gson gson = new Gson();
			String jsonStr = gson.toJson(resultSet);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(jsonStr);
			JSONArray obj2 = (JSONArray) obj.get("results");
			JSONObject obj3 = (JSONObject) obj2.get(0);
			JSONArray obj4 = (JSONArray) obj3.get("series");
			JSONObject obj5 = (JSONObject) obj4.get(0);
			JSONArray obj6 = (JSONArray) obj5.get("values");

			for (int i = 0; i < obj6.size(); i++) {
				String[] values = (obj6.get(i).toString()).split(",");
				getAllMapsRestarted.add(values[1].replace("]", "").replace("\"", "").replace("\\", ""));
			}
		} catch (Exception e) {
			System.out.println("no getAllMapsRestarted!");
			getAllMapsRestarted = null;
		}
//		runningMapsRestarted = null;
//		runningMapsRestarted = getAllMapsRestarted;
		return getAllMapsRestarted;
	}

	// --------------------------------- Network latency

	public static List<String> getLowDownloadNodesName(String jobId)
			throws IOException, URISyntaxException, ParseException {

		List<String> getLowDownloadNodesName = new ArrayList<String>();
		List<String> dataNodesNames = new ArrayList<String>();
		dataNodesNames = getDataNodesNames(jobId);
		
		List<Double> getDataNodesDownloadSpeeds = new ArrayList<Double>();
		List<String> getDataNodesNamesForBandwidth = new ArrayList<String>();
		HashMap<String, Double> info = new HashMap<String, Double>();
//		dataNodesNames.add("slave1");
//		dataNodesNames.add("slave2");
		String nodeName = "";
		double download = 0;
		double total = 0;
		double average = 0;

		for (int k = 0; k < dataNodesNames.size(); k++) {

			try {
				query.setMeasurement("sigar");
				query.setLimit(1000);
				query.fillNullValues("0");

				DataReader dataReader = new DataReader(query, configuration);
				ResultSet resultSet = dataReader.getResult();
				Query query1 = new Query();
				query1.setCustomQuery("select hostName, downloadMbps from resource where hostName='"
						+ dataNodesNames.get(k) + "' order by desc limit 1");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");

				for (int i = 0; i < obj6.size(); i++) {
					String[] values = (obj6.get(i).toString()).split(",");
					nodeName = (values[1].replace("]", "").replace("\"", "").replace("\\", ""));
					download = Double.parseDouble(values[2].replace("]", "").replace("\"", "").replace("\\", ""));
					getDataNodesNamesForBandwidth.add(nodeName);
					getDataNodesDownloadSpeeds.add(download);
					total += download;
					info.put(nodeName, download);
				}
			} catch (Exception e) {
				System.out.println("no getLowDownloadNodesName!");
				getLowDownloadNodesName = null;
			}
		}

		average = total / dataNodesNames.size();
//		System.out.println("all info = " + info);
//		System.out.println("average = " + average); 
//		System.out.println();
		
		for (Entry<String, Double> entry : info.entrySet()) {
		    if (entry.getValue() < average) {
		    	getLowDownloadNodesName.add(entry.getKey());
			}
		}
		
//		System.out.println("getLowDownloadNodesName = " + getLowDownloadNodesName);
		lowDownloadNodes = null;
		dataNodesDownloadSpeeds = null;
		dataNodesNamesForBandwidth = null;
		lowDownloadNodes = getLowDownloadNodesName;
		dataNodesDownloadSpeeds = getDataNodesDownloadSpeeds;
		dataNodesNamesForBandwidth = getDataNodesNamesForBandwidth;
		return getLowDownloadNodesName;
	}

	public static List<String> getMapsOnLowDownloadNodes(String jobId)
			throws IOException, URISyntaxException, ParseException, InterruptedException {

		List<String> getMapsOnLowDownloadNodes = new ArrayList<String>();
		List<String> getLowDownloadNodes = new ArrayList<String>();
		
		for (int i = 0; i < lowDownloadNodes.size(); i++) {
			getLowDownloadNodes.add(lowDownloadNodes.get(i));
		}

		query.setMeasurement("sigar");
		query.setLimit(1000);
		query.fillNullValues("0");
		DataReader dataReader = new DataReader(query, configuration);
		ResultSet resultSet = dataReader.getResult();
		Query query1 = new Query();
		String deger = "";

		for (int i = 0; i < getLowDownloadNodes.size(); i++) {

			try {
				query1.setCustomQuery("select distinct(mapId) from map where jobId='" + jobId + "' and host='" + getLowDownloadNodes.get(i) + "'");
				dataReader.setQuery(query1);
				resultSet = dataReader.getResult();
				Gson gson = new Gson();
				String jsonStr = gson.toJson(resultSet);
				JSONParser parser = new JSONParser();
				JSONObject obj = (JSONObject) parser.parse(jsonStr);
				JSONArray obj2 = (JSONArray) obj.get("results");
				JSONObject obj3 = (JSONObject) obj2.get(0);
				JSONArray obj4 = (JSONArray) obj3.get("series");
				JSONObject obj5 = (JSONObject) obj4.get(0);
				JSONArray obj6 = (JSONArray) obj5.get("values");
//				System.out.println("obj6 in SmartReader = " + obj6);
				for (int j = 0; j < obj6.size(); j++) {
					String[] values = (obj6.get(j).toString()).split(",");
					deger = values[1].replace("]", "").replace("\"", "").replace("\\", "");
					if (!getMapsOnLowDownloadNodes.contains(deger)) {
						getMapsOnLowDownloadNodes.add(deger);
					}
				}
//				System.out.println("mapsOnDisconnectedNodes in SmartReader = " + getMapsOnDisconnectedNodes);
//				System.out.println();
			} catch (Exception e) {
				System.out.println("no getMapsOnLowDownloadNodes!");
				System.out.println(e);
//				e.printStackTrace();
				getMapsOnLowDownloadNodes = null;
			}
		}

		mapsOnLowDownloadNodes = null;
		mapsOnLowDownloadNodes = getMapsOnLowDownloadNodes;
		return getMapsOnLowDownloadNodes;
	}

}
