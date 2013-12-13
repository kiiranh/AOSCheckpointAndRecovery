/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package startup;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Properties;

import model.Connection;
import model.NodeInfo;
import service.KooToueg;
import application.DistributedApplication;

public class Node {

	private int myId;
	private int leaderId;
	private NodeInfo myNodeInfo;
	private int nodeCount;
	private HashMap<Integer, NodeInfo> neighbors;
	private String configFileLocation;
	private String outputDir;
	private Connection connection;

	public Node(int nodeId, String configFileLocation, String outputDir) {
		this.myId = nodeId;
		this.neighbors = new HashMap<Integer, NodeInfo>();
		this.configFileLocation = configFileLocation;
		this.outputDir = outputDir;
		this.connection = new Connection();
		this.leaderId = 0;
	}

	private void cleanUpLogs(String logDir) {
		Path path = null;
		try {
			path = FileSystems.getDefault().getPath(logDir,
					"/node" + myId + ".log");
			Files.deleteIfExists(path);

			path = FileSystems.getDefault().getPath(logDir,
					"/" + myId + "_1.ckpt");
			Files.deleteIfExists(path);

			path = FileSystems.getDefault().getPath(logDir,
					"/" + myId + "_2.ckpt");
			Files.deleteIfExists(path);
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("\n <<< CLEARED PREVIOUS CHECKPOINTS >>>");
	}

	private void readConfigFiles() throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.load(new FileInputStream(configFileLocation));

		// Populate my info
		String myInfo = props.getProperty(myId + "");
		myNodeInfo = new NodeInfo(myId, myInfo.split(",")[0],
				Integer.parseInt(myInfo.split(",")[1]), Integer.parseInt(myInfo
						.split(",")[2]), Integer.parseInt(myInfo.split(",")[3]));

		this.nodeCount = Integer.parseInt(props.getProperty("numberOfNodes"));

		// Get neighbor list
		String neighborStr = props.getProperty("t" + myId);

		if(neighborStr == null || neighborStr.trim().isEmpty()){
			System.out.println("[NODE] NO NEIGHBORS");
		}else{
			for (String id : neighborStr.split(",")) {
				// Get Host Info for this neighbor
				String nodeInfo = props.getProperty(id);
				neighbors.put(
						Integer.parseInt(id),
						new NodeInfo(Integer.parseInt(id), nodeInfo.split(",")[0],
								Integer.parseInt(nodeInfo.split(",")[1]), Integer
										.parseInt(nodeInfo.split(",")[2]), Integer
										.parseInt(nodeInfo.split(",")[3])));
				System.out.println("[NODE] Neighbor:" + id);
			}
		}
		
		System.out.println("\n <<< CONFIGURATION FILE READ >>>");
	}

	private void verifyConsistency() {
		// TODO IMPLEMENT
	}

	public void start() throws Exception {
		// Read configfile
		readConfigFiles();

		// Clean previous log files
		cleanUpLogs(outputDir);

		// Setup sctp connections
		System.out.println("\n <<< CONNECTION SETUP STARTED >>>");
		connection.setUp(neighbors, myId, myNodeInfo);
		System.out.println("\n <<< CONNECTION SETUP DONE >>>");
		Thread.sleep(2000);

		// Elect Leader (Node 0 default)
		boolean amILeader = false;

		// Send leader info to all (Or assume Node 0)
//		if (leaderId == myId) {
//			// I'm leader. I will start off the program on all nodes.
//			System.out.println("\n <<< LEADER NODE ID: " + leaderId
//					+ " (ME) >>>");
//			amILeader = true;
//			// FIXME This won't work because leader not connected to all nodes
//			// connection.leaderNotifyStart(leaderId);
//		} else {
//			// I'm not leader. I will wait for "START" from leader.
//			System.out.println("\n <<< LEADER NODE ID: " + leaderId + " >>>");
//			// FIXME This won't work because leader not connected to all nodes
//			// connection.awaitStartFromLeader();
//		}

		// FIXME TEMP HACK TO WAIT FOR 10 sec so all connections are done
		Thread.sleep(10000);

		String originalString = "INITIAL";

		// Create and start checkpointing and recovery service
		KooToueg service = new KooToueg(myId,
				myNodeInfo.getCheckpointInterval(),
				myNodeInfo.getFailureInterval(), connection, originalString);
		service.start();

		// Create and Start Application
		DistributedApplication application = new DistributedApplication(
				nodeCount, amILeader, service);
		application.start();

		// Wait for application to end(join)
		application.join();

		// Wait for service to end(join)
		service.join();

		// Halt
		connection.tearDown();
		System.out.println("\n <<< CONNECTION TERMINATION DONE >>>");

		// VERIFY THAT THE CHECKPOINTS AND LAST STATES ARE CONSISTENT
//		if (leaderId == myId) {
//			Thread.sleep(2000);
//			System.out
//					.println("\n <<< [LEADER] VERIFYING MESSAGE ORDER ON ALL NODES >>>");
//			verifyConsistency();
//		}

		System.out.println("\n******* HALTING ******");
	}

	public static void main(String[] args) throws Exception {
		// Get my Node ID and config file path from CLI
		if (args.length < 3) {
			throw new Exception(
					"Invalid command: Please specify node id, absolute configuration file path & absolute log directory path"
							+ "\n\t Command: java Node <nodeId> <config File Path> <log dir path>\n\t "
							+ "Eg: java startup.Node 0 \"/home/kiiranh/workspace/AOSCheckPointAndRecovery/src/config.txt\" \"/home/kiiranh/workspace/AOSCheckPointAndRecovery/bin/\"");
		}

		String logDir = args[2];
		if (!logDir.endsWith("/")) {
			logDir = logDir + "/";
		}

		new Node(Integer.parseInt(args[0]), args[1], args[2]).start();
	}
}
