/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package model;

public class NodeInfo {
	final private int nodeId;
	final private String hostAddress;
	final private int port;
	final private int checkpointInterval;
	final private int failureInterval;

	public NodeInfo(int nodeId, String hostAddress, int port,
			int checkpointInterval, int failureInterval) {
		this.nodeId = nodeId;
		this.hostAddress = hostAddress;
		this.port = port;
		this.checkpointInterval = checkpointInterval;
		this.failureInterval = failureInterval;
	}

	public int getNodeId() {
		return nodeId;
	}

	public String getHostAddress() {
		return hostAddress;
	}

	public int getPort() {
		return port;
	}

	public int getCheckpointInterval() {
		return checkpointInterval;
	}

	public int getFailureInterval() {
		return failureInterval;
	}
	
	

}
