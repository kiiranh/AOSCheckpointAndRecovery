/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package application;

import java.util.ArrayList;
import java.util.Random;

import model.Message;
import service.KooToueg;

public class DistributedApplication extends Thread {
	// private String localString;
	private ArrayList<String> actions;
	private int nodeCount;
	private static boolean amILeader;
	private ArrayList<String> resultList = new ArrayList<String>();
	private int pendingResultCount;
	private static KooToueg service;
	private static final int MY_COMPUTATION_COUNT = 10;

	public DistributedApplication(int nodeCount, boolean amILeader,
			KooToueg service) {
		// this.localString = original;
		actions = new ArrayList<String>();
		actions.add("Upper Case"); // Upper case
		actions.add("Lower Case"); // Lower case
		actions.add("Remove Last Element"); // Remove the last element
		actions.add("Append \"Z\""); // Append Z to tail
		actions.add("Remove \"Z\" from the end"); // Remove Z from tail
		actions.add("Append \"*\""); // Append *
		actions.add("Replace [Ii] with 1"); // Replace I/i with 1
		actions.add("Replace [Aa] with 4"); // Replace A/a with 4
		actions.add("Reverse"); // Reverse the string

		this.nodeCount = nodeCount;
		this.pendingResultCount = nodeCount - 1;
		DistributedApplication.amILeader = amILeader;
		DistributedApplication.service = service;
	}

	private void processResults() {
		// At this point, App processing is done
		// FIXME For sufficiently high number of executions the string length
		// could outgrow buffer size. So we limit the Final String to 400 bytes
		// Buffer size = 512 bytes
		String localString = service.getCurrentState().getStringObject();
		if (localString.length() > 400) {
			service.getCurrentState().setStringObject(
					localString.substring(0, 400));
		}

		System.out.println("\n <<< [APPLICATION] MY FINAL OBJECT: "
				+ localString + " >>> ");
		if (amILeader) {
			// Leader:
			// Wait for result from all nodes
			// Verify results and output
			while (pendingResultCount > 0) {
				// Sleep a while
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				ArrayList<String> msgs = service.deliver();

				for (String msg : msgs) {
					if (msg.startsWith("=")) {
						resultList.add(msg.substring(1, msg.length()));
						--pendingResultCount;
					}
				}
			}

			// Got all Results. Verify
			for (String result : resultList) {
				if (!result.equals(localString)) {
					System.out
							.println("\n <<< [VERIFICATION] DISTRIBUTED COMPUTATION INCONSISTENT => FAILED :) >>>");
					return;
				}
			}

			System.out
					.println("\n <<< [VERIFICATION] DISTRIBUTED COMPUTATION CONSISTENT => SUCCESS :) >>>");
			// service.aBroadCast("DONE");

		} else {
			// Worker:
			// Send result to Leader Node
			service.broadCast("=" + localString);
			System.out
					.println("\n <<< [APPLICATION] SENT RESULT TO LEADER >>>");
		}
	}

	class SendThread implements Runnable {
		@Override
		public void run() {
			int msgToPropose = MY_COMPUTATION_COUNT;
			while (msgToPropose > 0) {
				// Sleep random time
				try {
					Thread.sleep(1000);
				} catch (Exception e) {

				}

				// Propose my action (Selected Randomly from the available set)
				int act = new Random().nextInt(actions.size());
				// System.out.println("\nPROPOSE ACTION: " + actions.get(act));
				service.broadCast(Integer.toString(act));
				--msgToPropose;
			}
		}
	}

	class ReceiveThread implements Runnable {
		@Override
		public void run() {
			int msgToProcess = MY_COMPUTATION_COUNT * nodeCount;

			while (msgToProcess > 0) {
				try {
					Thread.sleep(new Random().nextInt(1000));
				} catch (Exception e) {
				}

				ArrayList<String> msgs = service.deliver();

				for (String msg : msgs) {
					if (msg.startsWith("=")) {
						// Result msg
						resultList.add(msg.substring(1, msg.length()));
						--pendingResultCount;
					} else {
						// ACTION MESSAGE
						String localString = service.getCurrentState()
								.getStringObject();
						System.out.println("\n[DELIVER] ACTION: "
								+ actions.get(Integer.valueOf(msg)));
						System.out.println("BEFORE: " + localString);
						switch (Integer.valueOf(msg)) {
						case 0:
							localString = localString.toUpperCase();
							break;

						case 1:
							localString = localString.toLowerCase();
							break;

						case 2:
							if (localString.length() > 0) {
								localString = localString.substring(0,
										localString.length() - 1);
							}
							break;

						case 3:
							localString = localString + "Z";
							break;

						case 4:
							if (localString.endsWith("Z")) {
								localString = localString.substring(0,
										localString.length() - 1);
							}
							break;

						case 5:
							localString = localString.concat("*");
							break;

						case 6:
							localString = localString.replaceAll("[Ii]", "1");
							break;

						case 7:
							localString = localString.replaceAll("[Aa]", "4");
							break;

						case 8:
							localString = new StringBuilder(localString)
									.reverse().toString();
							break;

						default:
							break;
						}

						System.out.println("AFTER: " + localString);
						--msgToProcess;
						service.getCurrentState().setStringObject(localString);
					}
				}
			}
		}
	}

	@Override
	public void run() {
		System.out.println("\n <<< APPLICATION STARTED >>> ");
		System.out.println("\n <<< [APPLICATION] INITIAL OBJECT: "
				+ service.getCurrentState().getStringObject() + " >>> \n");

		Thread sendThread = new Thread(new SendThread());
		Thread receiveThread = new Thread(new ReceiveThread());

		// System.out.println("[APPLICATION] Starting Send Thread...");
		sendThread.start();
		// System.out.println("[APPLICATION] Starting Receive Thread...");
		receiveThread.start();

		// Wait for them to end
		try {
			sendThread.join();
			receiveThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		processResults();
		service.broadCast(Message.APPLICATION_DONE);
		System.out.println("\n <<< APPLICATION STOPPED >>>");
	}
}
