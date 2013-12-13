/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import model.AppState;
import model.Connection;
import model.Message;
import model.Message.MessageType;

import common.Backoff;
import common.Utility;

public class KooToueg extends Thread {
	private int myId;
	private Connection connection;
	private AppState currentState;
	private AppState tentativeCheckpoint;
	private int currentCheckPointTime;
	private AtomicBoolean inCheckPointOrRecovery = new AtomicBoolean();
	private int logicalClock;
	private List<Integer> cohorts;
	private int messageLabel;
	private int checkpointInterval;
	private long lastCheckpointTime;
	private int initiatorId = -1;
	private int instanceId = -1;
	private int cohortCount = -1;
	private final int DEFAULT_LABEL = 0;
	private Integer willingToCheckpoint = 1; // IF I'm going to CP
	private Backoff backoff;

	/**
	 * 0 = IN RECOVERY INSTANCE, CANNOT PARTICIPATE IN OTHER CP/RR 1 = NOT IN
	 * RECOVERY INSTANCE, CAN PARTICIPATE IN OTHER CP/RR
	 */
	private Integer readyToRoll = 0;

	/**
	 * True if I'm trying to recover after my failure
	 */
	private boolean tryingToRecover = false;

	private final Lock resourcesLock = new ReentrantLock(true);
	private final Lock applicationQueueLock = new ReentrantLock(true);
	private final Condition unfreezeSend = applicationQueueLock.newCondition();
	private final Condition unfreezeReceive = applicationQueueLock
			.newCondition();

	private long lastFailureTime;
	private long failureInterval;
	private Integer failedNeighbor;

	/* Holds the Actual Message Payload to be broadcast */
	private static Queue<String> sendMessageQueue;

	/* Holds the messages which are delivered to the application */
	private static Queue<String> deliveredMessageQueue;

	/* To denote when a thread should stop */
	private static AtomicBoolean keepWorking = new AtomicBoolean(true);

	public KooToueg(int nodeId, int checkpointInterval, int failureInterval,
			Connection connection, String initialObject) throws Exception {
		this.myId = nodeId;
		this.connection = connection;
		this.currentState = new AppState(connection.getNeighbors(),
				initialObject);
		this.inCheckPointOrRecovery.set(false);
		this.logicalClock = 0;
		this.messageLabel = 0;
		this.checkpointInterval = checkpointInterval;
		this.failureInterval = failureInterval;
		this.lastCheckpointTime = currentCheckPointTime = -1;
		this.cohorts = new ArrayList<>();
		this.failedNeighbor = -1;
		this.lastFailureTime = System.currentTimeMillis();

		backoff = new Backoff(64, 2048);

		KooToueg.sendMessageQueue = new LinkedList<String>();
		KooToueg.deliveredMessageQueue = new LinkedList<String>();

		System.out.println("\n[KOO TOUEG] INITIAL CHECKPOINT START");
		// Take Initial Checkpoint
		takeLocalCheckpoint();
		System.out.println("\n[KOO TOUEG] INITIAL CHECKPOINT COMPLETE");
	}

	public AppState getCurrentState() {
		return currentState;
	}

	/**
	 * This method will initiate a CP instance for ME
	 * 
	 * @throws Exception
	 */
	private void takeLocalCheckpoint() throws Exception {
		if (failedNeighbor > -1) {
			System.out.println("\n[KOO TOUEG] NEIGHBOR " + failedNeighbor
					+ " FAILED. NOT TAKING CHECKPOINT");
			return;
		}

		if (!inCheckPointOrRecovery.get()) {
			try {
				resourcesLock.lock();
				applicationQueueLock.lock();

				System.out.println("\n LOCAL CHECKPOINT START");

				// Set to BUSY
				inCheckPointOrRecovery.set(true);
				willingToCheckpoint = 1;

				// Clear the send message queue.
				sendMessageQueue.clear();

				// Save CP Initiator ID and Instance ID
				this.initiatorId = myId;
				this.instanceId = myId;
				this.currentCheckPointTime = ++logicalClock;

				takeTentativeCheckpoint();

				// Check if I have any cohorts
				if (cohortCount < 1) {
					System.out.println("\n[KOO TOUEG] NO COHORTS");

					if (lastCheckpointTime < 0) {
						// Make Checkpoint Permanent
						makeCheckpointPermanent();
					} else {
						System.out
								.println("\n[KOO TOUEG] NOT TAKING CHECKPOINT");
						// NO cohorts. Not saving
						discardTentativeCheckpoint();
					}

				} else {
					// Send ChkPoint request to all cohorts
					for (Integer cohort : cohorts) {
						Message cohortsReq = new Message(myId, DEFAULT_LABEL,
								Message.MessageType.CPREQ,
								currentCheckPointTime, tentativeCheckpoint
										.getLastLabelReceived().get(cohort)
										.toString());
						cohortsReq.setInstanceId(this.instanceId);
						connection.unicast(cohort, cohortsReq);
						System.out.println("\n[KOO TOUEG] LOCAL: SENT CPREQ "
								+ instanceId + " TO COHORT " + cohort);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				applicationQueueLock.unlock();
				resourcesLock.unlock();
			}
		} else {
			System.out.println("\n [KOO TOUEG] LOCAL CHECKPOINT NOT TAKEN.");

		}
	}

	private void processCheckpointRequestMessage(Message recvMsg)
			throws Exception {
		// THIS METHOD NEEDS TO BE SYNCHRONIZED. ALL STATE CHANGES WILL BE DONE
		// HERE

		if (failedNeighbor > -1) {
			// SEND NO
			Message repMsg = new Message(myId, DEFAULT_LABEL,
					Message.MessageType.CPREP, ++logicalClock, "0");
			repMsg.setInstanceId(instanceId);
			connection
					.unicast(recvMsg.getMessageId().getOriginatorId(), repMsg);
			System.out.println("\n[KOO TOUEG] SENT CPREP" + instanceId
					+ " NO TO INITIATOR BECAUSE NEIGHBOR FAILED"
					+ recvMsg.getMessageId().getOriginatorId());
			return;

		}

		// Handle CPREQ
		// 0. Check if I'm busy
		// - If not 1. onwards
		if (!inCheckPointOrRecovery.get()) {
			// Set to BUSY
			inCheckPointOrRecovery.set(true);
			willingToCheckpoint = 1;

			// Clear send message queue
			sendMessageQueue.clear();

			// Save CP Initiator ID and Instance ID
			this.initiatorId = recvMsg.getMessageId().getOriginatorId();
			this.instanceId = recvMsg.getInstanceId();
			this.currentCheckPointTime = recvMsg.getTimeStamp();

			// 1. Check if I need to take CP
			if (Integer.parseInt(recvMsg.getPayload()) >= currentState
					.getFirstLabelSent().get(
							recvMsg.getMessageId().getOriginatorId())
					&& currentState.getFirstLabelSent().get(
							recvMsg.getMessageId().getOriginatorId()) > Integer.MIN_VALUE) {
				// I NEED TO TAKE CP
				// If yes: 2. onwards

				// 2. Take tentative cp and populate cohorts
				takeTentativeCheckpoint();

				// 3. If there are no cohorts, send YES to Initiator
				if (cohortCount < 1) {
					// Send CPREP with YES TO INTIATOR
					Message repMsg = new Message(myId, DEFAULT_LABEL,
							Message.MessageType.CPREP, ++logicalClock,
							willingToCheckpoint.toString());
					repMsg.setInstanceId(instanceId);
					connection.unicast(
							recvMsg.getMessageId().getOriginatorId(), repMsg);
					System.out.println("\n SENT CPREP YES TO INITIATOR "
							+ recvMsg.getMessageId().getOriginatorId()
							+ " FOR CP INSTANCE " + instanceId);
				} else {
					// Else
					// 4. Send ChkPoint request to all cohorts
					for (Integer cohort : cohorts) {
						Message cohortsReq = new Message(myId, DEFAULT_LABEL,
								Message.MessageType.CPREQ,
								currentCheckPointTime, tentativeCheckpoint
										.getLastLabelReceived().get(cohort)
										.toString());
						cohortsReq.setInstanceId(this.instanceId);
						connection.unicast(cohort, cohortsReq);
						System.out.println("\nSENT CPREQ " + instanceId
								+ " TO COHORT " + cohort);
					}
				}

			} else {
				// I'M GOOD. JUST SEND CPREP WITH YES TO INITIATOR
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.CPREP, ++logicalClock,
						willingToCheckpoint.toString());
				repMsg.setInstanceId(instanceId);
				connection.unicast(recvMsg.getMessageId().getOriginatorId(),
						repMsg);
				System.out.println("\n[KOO TOUEG] SENT CPREP " + instanceId
						+ " YES TO INITIATOR "
						+ recvMsg.getMessageId().getOriginatorId());
			}

		} else {
			// I'm BUSY
			// CHECK IF THIS REQUEST IS FOR THE SAME INSTANCE
			if (this.instanceId == recvMsg.getInstanceId()) {
				// YES: SEND CPREP WITH YES TO INITIATOR
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.CPREP, ++logicalClock, "1");
				repMsg.setInstanceId(instanceId);
				connection.unicast(recvMsg.getMessageId().getOriginatorId(),
						repMsg);
				System.out.println("\nSENT CPREP" + instanceId
						+ " YES TO INITIATOR "
						+ recvMsg.getMessageId().getOriginatorId());
			} else {
				// SEND CPREP WITH NO TO INITIATOR
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.CPREP, ++logicalClock, "0");
				repMsg.setInstanceId(instanceId);
				connection.unicast(recvMsg.getMessageId().getOriginatorId(),
						repMsg);
				System.out.println("\nSENT CPREP" + instanceId
						+ " NO TO INITIATOR "
						+ recvMsg.getMessageId().getOriginatorId());
			}
		}
	}

	private void processCheckpointReplyMessage(Message recvMsg)
			throws Exception {
		// I will receive REPLY messages only from my cohorts
		// I DONT HAVE TO CHECK THE INSTANCE ID
		// Decrement the cohortCount for whom I'm waiting
		--cohortCount;

		// Fetch the decision from my cohort
		willingToCheckpoint = willingToCheckpoint
				& Integer.parseInt(recvMsg.getPayload());

		// Check if I have received reply from all messages.
		if (cohortCount == 0) {
			// I have received all replies.
			// IMP: If I STARTED THIS CHECKPOINT INSTANCE, PROCEED TO PHASE 2

			if (myId == this.instanceId) {
				// SEND DECISION TO MY COHORTS
				System.out.println("\n[KOO TOUEG] SENDING DECISION "
						+ willingToCheckpoint + " TO COHORTS");
				for (Integer cohort : cohorts) {
					Message decMsg = new Message(myId, DEFAULT_LABEL,
							Message.MessageType.CPFIN, ++logicalClock,
							willingToCheckpoint.toString());
					decMsg.setInstanceId(instanceId);
					connection.unicast(cohort, decMsg);
					System.out.println("NODE " + myId
							+ ": SENT CPFIN TO COHORT " + cohort);
				}

				// If ALL SENT YES: MAKE CHECKPOINT PERMANENT
				if (willingToCheckpoint == 1) {
					makeCheckpointPermanent();
				} else {
					// MERGE CURRENT STATE AND TENTATIVE CHECKPOINT
					discardTentativeCheckpoint();
					System.out
							.println("\n[KOO TOUEG] CHECKPOINT INSTANCE FAILED. BACKING OFF...");
				}

			} else {
				// Send REPLY to initiator
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.CPREP, ++logicalClock,
						willingToCheckpoint.toString());
				repMsg.setInstanceId(instanceId);
				connection.unicast(initiatorId, repMsg);
				System.out.println("\nNODE " + myId
						+ ": SENT CPREP TO INITIATOR " + initiatorId);
			}
		}
	}

	private void processCheckpointFinalMessage(Message recvMsg)
			throws Exception {
		// PROCESS THE DECISION IF ONLY FROM MY INITIATOR
		System.out.println("[KOO TOUEG] PROCESSING FINAL MESSAGE "
				+ recvMsg.toString());

		if (recvMsg.getMessageId().getOriginatorId() != initiatorId) {
			System.out.println("MY INITIATOR ID " + initiatorId
					+ " NOT MATCHED. DISCARDING");
			return;
		}

		// IF DECISION IS TO MAKE CHECKPOINT PERMANENT ---
		willingToCheckpoint = Integer.parseInt(recvMsg.getPayload());

		// SEND DECISION TO COHORTS IF I HAVE ANY
		for (Integer cohort : cohorts) {
			Message decToCohortMsg = new Message(myId, DEFAULT_LABEL,
					Message.MessageType.CPFIN, ++logicalClock,
					willingToCheckpoint.toString());
			decToCohortMsg.setInstanceId(this.instanceId);
			connection.unicast(cohort, decToCohortMsg);
			System.out.println("NODE " + myId + ": SENT CPFIN TO COHORT "
					+ cohort);
		}

		if (willingToCheckpoint == 1) {
			// MAKE CHECKPOINT PERMANENT
			makeCheckpointPermanent();
		} else {
			// ESLE DECISION IS TO DISCARD TENATIVE CHECKPOINT ---
			// DISCARD CHECKPOINT
			discardTentativeCheckpoint();
		}

	}

	private void takeTentativeCheckpoint() {
		// Take tentative CP
		tentativeCheckpoint = new AppState(currentState);
		// Reset Current State
		currentState = new AppState(connection.getNeighbors(),
				tentativeCheckpoint.getStringObject());

		// Find checkpoint cohorts
		for (Integer neighbor : tentativeCheckpoint.getLastLabelReceived()
				.keySet()) {
			if (tentativeCheckpoint.getLastLabelReceived().get(neighbor) > Integer.MIN_VALUE
					&& neighbor != initiatorId) {
				cohorts.add(neighbor);
			}
		}
		cohortCount = cohorts.size();

		tentativeCheckpoint.setTimeStamp(currentCheckPointTime);
		System.out.println("NODE " + myId + ": TOOK TENTATIVE CHECKPOINT "
				+ instanceId);
	}

	/**
	 * Write the tentative checkpoint to stable storage
	 * 
	 * @throws IOException
	 */
	private void makeCheckpointPermanent() throws IOException {
		if (tentativeCheckpoint == null) {
			System.out
					.println("\n[KOO TOUEG] NO NEED TO TAKE PERMANENT CHECKPOINT");
		} else {

			// Make this CP permanent
			tentativeCheckpoint.setPermanent(true);

			// Write CP to file
			Utility.writePermanentCheckpoint(tentativeCheckpoint, myId);

			// Update last checkpoint time
			lastCheckpointTime = System.currentTimeMillis();
			System.out.println("\n[KOO TOUEG] NODE " + myId
					+ ": SAVED PERMANENT CHECKPOINT " + instanceId);
		}

		resetAfterCheckpoint();

		// Unfreeze Send
		unfreezeSend.signal();

	}

	private void discardTentativeCheckpoint() {
		// Merge Current and tentative
		currentState.setFirstLabelSent(tentativeCheckpoint.getFirstLabelSent());
		currentState.setLastLabelSent(tentativeCheckpoint.getLastLabelSent());

		for (Integer neighbor : currentState.getLastLabelReceived().keySet()) {
			if (currentState.getLastLabelReceived().get(neighbor)
					.equals(Integer.MIN_VALUE)) {
				currentState.getLastLabelReceived().put(
						neighbor,
						tentativeCheckpoint.getLastLabelReceived()
								.get(neighbor));
			}
		}

		// delete tentative
		tentativeCheckpoint = null;
		System.out.println("\n[KOO TOUEG] NODE " + myId
				+ ": DISCARDED TENTATIVE CHECKPOINT " + instanceId);

		// Update last checkpoint time: TO AVOID CONTINUOS FAILURE
		lastCheckpointTime = System.currentTimeMillis();
		resetAfterCheckpoint();
		// Unfreeze Send
		unfreezeSend.signal();
	}

	/**
	 * Clear tentative checkpoints, initiator ID and instance ID
	 */
	private void resetAfterCheckpoint() {
		initiatorId = currentCheckPointTime = -1;
		instanceId = -1;
		cohortCount = 0;
		cohorts.clear();
		inCheckPointOrRecovery.set(false);
	}

	/**
	 * Called to recover to the last permanent checkpoint after a failure
	 * 
	 * @throws Exception
	 */
	public void recover() throws Exception {
		System.out.println("\n <<< RECOVERING AFTER FAILURE >>> ");

		// TRY RECOVERY UNTIL SUCCESSFUL. FLAG WILL BE SET TO FALSE IN ROLLBACK
		// COMMIT
		// ON SUCCESSFUL RECOVER

		// Check if I'm in a CP/RR state
		if (!inCheckPointOrRecovery.get() || tryingToRecover) {
			tryingToRecover = true;

			try {
				resourcesLock.lock();
				applicationQueueLock.lock();

				// Not in CP or RR
				inCheckPointOrRecovery.set(true);
				readyToRoll = 1; // In Rollback

				// Save CP Initiator ID and Instance ID
				this.initiatorId = myId;
				this.instanceId = myId;

				rollbackTentative();

				if (cohortCount < 1) {
					System.out
							.println("\n[KOO TOUEG] NO COHORTS FOR ROLLBACK. COMMITTING ROLLBACK");
					rollbackCommit();
					readyToRoll = 0;
					inCheckPointOrRecovery.set(false);
					// Unfreeze SEND/RECEIVE
					unfreezeSend.signal();
					unfreezeReceive.signal();
				} else {
					// Send ROLLBACK request to all cohorts
					for (Integer cohort : cohorts) {
						Message cohortsReq = new Message(myId, DEFAULT_LABEL,
								Message.MessageType.RRREQ, ++logicalClock,
								tentativeCheckpoint.getLastLabelSent()
										.get(cohort).toString());
						cohortsReq.setInstanceId(this.instanceId);
						connection.unicast(cohort, cohortsReq);
						System.out.println("\n[KOO TOUEG] SENT RRREQ "
								+ instanceId + " TO COHORT " + cohort);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				applicationQueueLock.unlock();
				resourcesLock.unlock();
			}

		} // end of "if (!inCheckPointOrRecovery.get()) {"
	}

	/**
	 * Fetch the last permanent checkpoint and send rollback to cohorts if any
	 * 
	 * @throws Exception
	 */
	private void rollbackTentative() throws Exception {
		System.out.println("\n[KOO TOUEG] PROCESSING ROLLBACK REQUEST");
		// Fetch the last permanent checkpoint from stable storage.
		tentativeCheckpoint = Utility.fetchLastPermanentCheckpoint(myId);

		// Find rollback cohorts according to this checkpoint
		// Send Rollback request to the rollback cohorts: ALL neighbors except
		// the sender of this request
		for (Integer neighbor : connection.getNeighbors()) {
			if (neighbor != initiatorId) {
				cohorts.add(neighbor);
			}
		}
		cohortCount = cohorts.size();

		tentativeCheckpoint.setTimeStamp(currentCheckPointTime);
		System.out.println("NODE " + myId
				+ ": TRYING TO ROLLBACK FOR RECOVERY INSTANCE " + instanceId);
	}

	/**
	 * Change current state to last permanent checkpoint
	 * 
	 * @throws Exception
	 */
	private void rollbackCommit() throws Exception {

		System.out
				.println("\n[KOO TOUEG] ROLLING BACK TO LAST PERMANENT CHECKPOINT");

		// Clear the delivered message queue.
		deliveredMessageQueue.clear();

		// RESET STATE
		if (instanceId == myId) {
			lastFailureTime = System.currentTimeMillis();
		}

		initiatorId = currentCheckPointTime = -1;
		instanceId = -1;
		cohortCount = 0;
		cohorts.clear();
		tryingToRecover = false;
		backoff.reset();
		failedNeighbor = -1;

		// Set current state to last permanent checkpoint (held in tentative cp)
		if (tentativeCheckpoint != null) {
			System.out
					.println("\n[KOO TOUEG] ROLL BACK COMPLETE TO FOLLOWING STATE\n"
							+ tentativeCheckpoint.toString());
			currentState = new AppState(connection.getNeighbors(),
					tentativeCheckpoint.getStringObject());
			tentativeCheckpoint = null;
		} else {
			System.out
					.println("\n[KOO TOUEG] NO NEED TO ROLLBACK..HENCE RECOVERY DONE");
		}
		System.out.println("\n <<< SUCCESSFULLY RECOVERED >>>");

		// TODO Retransmit lost messages?
	}

	private void discardRollback() {
		System.out.println("\n[KOO TOUEG] ROLLBACK ABORTING");

		// RESET STATE
		initiatorId = currentCheckPointTime = -1;
		instanceId = -1;
		cohortCount = 0;
		cohorts.clear();
		tryingToRecover = false;
		tentativeCheckpoint = null;
		backoff.reset();
		readyToRoll = 0;
		failedNeighbor = -1;

		System.out.println("\n[KOO TOUEG] CONTINUIG WITH CURRENT STATE\n"
				+ currentState.toString());

	}

	private void processRollbackRequestMessage(Message recvMsg)
			throws Exception {
		// Check if I'm already in CP or Rollback
		if (!inCheckPointOrRecovery.get()) {
			// Not in CP or RR
			inCheckPointOrRecovery.set(true);
			tryingToRecover = true;
			readyToRoll = 1; // In Rollbackd

			// Save CP Initiator ID and Instance ID
			this.initiatorId = recvMsg.getMessageId().getOriginatorId();
			this.instanceId = recvMsg.getInstanceId();
			// this.currentCheckPointTime = recvMsg.getTimeStamp();

			// Check if I need to rollback
			if (Integer.parseInt(recvMsg.getPayload()) < currentState
					.getLastLabelReceived().get(
							recvMsg.getMessageId().getOriginatorId())) {
				// I have to rollback
				// Rollback
				rollbackTentative();

				// If there are no cohorts, send YES to Initiator
				if (cohortCount < 1) {
					// Send RRREP with YES TO INTIATOR
					Message repMsg = new Message(myId, DEFAULT_LABEL,
							Message.MessageType.RRREP, ++logicalClock,
							readyToRoll.toString());
					repMsg.setInstanceId(instanceId);
					connection.unicast(
							recvMsg.getMessageId().getOriginatorId(), repMsg);
					System.out
							.println("\n[KOO TOUEG] RECOVERY SENT RRREP YES TO INITIATOR "
									+ recvMsg.getMessageId().getOriginatorId()
									+ " FOR RECOVERY INSTANCE " + instanceId);
				} else {
					// Else
					// Send ROLLBACK request to all cohorts
					for (Integer cohort : cohorts) {
						Message cohortsReq = new Message(myId, DEFAULT_LABEL,
								Message.MessageType.RRREQ, ++logicalClock,
								tentativeCheckpoint.getLastLabelSent()
										.get(cohort).toString());
						cohortsReq.setInstanceId(this.instanceId);
						connection.unicast(cohort, cohortsReq);
						System.out.println("\n[KOO TOUEG] SENT RRREQ "
								+ instanceId + " TO COHORT " + cohort);
					}
				}

			} else {
				// I don't need to rollback. Just send OK to INITIATOR
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.RRREP, ++logicalClock,
						readyToRoll.toString());
				repMsg.setInstanceId(instanceId);
				connection.unicast(recvMsg.getMessageId().getOriginatorId(),
						repMsg);
				System.out.println("\n[KOO TOUEG] SENT RRREP " + instanceId
						+ " YES TO INITIATOR "
						+ recvMsg.getMessageId().getOriginatorId());
			}

		} else {
			// I'M BUSY.
			// CHECK IF THIS REQUEST IS FOR THE SAME INSTANCE
			if (this.instanceId == recvMsg.getInstanceId()) {
				// YES: SEND RRREP WITH YES TO INITIATOR
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.RRREP, ++logicalClock, "1");
				repMsg.setInstanceId(instanceId);
				connection.unicast(recvMsg.getMessageId().getOriginatorId(),
						repMsg);
				System.out.println("\n[KOO TOUEG] SENT RRREP" + instanceId
						+ " YES TO INITIATOR "
						+ recvMsg.getMessageId().getOriginatorId());
			} else {
				// SEND RRREP WITH NO TO INITIATOR
				readyToRoll = 0;
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.RRREP, ++logicalClock, "0");
				repMsg.setInstanceId(instanceId);
				connection.unicast(recvMsg.getMessageId().getOriginatorId(),
						repMsg);
				System.out.println("\n[KOO TOUEG] SENT RRREP" + instanceId
						+ " NO TO INITIATOR "
						+ recvMsg.getMessageId().getOriginatorId());
			}
		}
	}

	private void processRollbackReplyMessage(Message recvMsg) throws Exception {
		// I will receive REPLY messages only from my cohorts
		// I DONT HAVE TO CHECK THE INSTANCE ID
		// Decrement the cohortCount for whom I'm waiting
		--cohortCount;

		// Fetch the decision from my cohort
		readyToRoll = readyToRoll & Integer.parseInt(recvMsg.getPayload());

		// Check if I have received reply from all messages.
		if (cohortCount == 0) {
			// I have received all replies.
			// IMP: If I STARTED THIS RECOVERY INSTANCE, PROCEED TO PHASE 2

			if (myId == this.instanceId) {
				// SEND DECISION TO MY COHORTS
				System.out.println("\n[KOO TOUEG] SENDING DECISION "
						+ readyToRoll + " TO COHORTS");
				for (Integer cohort : cohorts) {
					Message decMsg = new Message(myId, DEFAULT_LABEL,
							Message.MessageType.RRFIN, ++logicalClock,
							readyToRoll.toString());
					decMsg.setInstanceId(instanceId);
					connection.unicast(cohort, decMsg);
					System.out.println("[KOO TOUEG] NODE " + myId
							+ ": SENT RRFIN TO COHORT " + cohort);
				}

				// If ALL SENT YES: ROLLBACK
				if (readyToRoll == 1) {
					rollbackCommit();

					readyToRoll = 0;
					inCheckPointOrRecovery.set(false);
					// Unfreeze SEND/RECEIVE
					unfreezeSend.signal();
					unfreezeReceive.signal();
				} else {
					// COULD NOT RECOVER. BACKOFF AND TRY AGAIN
					// NEED TO MAKE SURE I DO NOT ENTER OTHER CP/RR
					System.out.println("\n[KOO TOUEG] RECOVERY INSTANCE "
							+ instanceId + " FAILED. BACKING OFF...");
					backoff.backoff();

					// reset before trying again
					cohorts.clear();
					cohortCount = 0;
					readyToRoll = 0;
					tentativeCheckpoint = null;

					recover();
				}

			} else {
				// Send REPLY to initiator
				Message repMsg = new Message(myId, DEFAULT_LABEL,
						Message.MessageType.RRREP, ++logicalClock,
						readyToRoll.toString());
				repMsg.setInstanceId(instanceId);
				connection.unicast(initiatorId, repMsg);
				System.out.println("\n[KOO TOUEG] NODE " + myId
						+ ": SENT RRREP TO INITIATOR " + initiatorId);
			}
		}

	}

	private void processRollbackFinalMessage(Message recvMsg) throws Exception {

		// PROCESS THE DECISION IF ONLY FROM MY INITIATOR
		System.out.println("\n[KOO TOUEG] PROCESSING ROLLBACK FINAL MESSAGE "
				+ recvMsg.toString());

		if (recvMsg.getMessageId().getOriginatorId() != initiatorId) {
			System.out.println("MY INITIATOR ID " + initiatorId
					+ " NOT MATCHED. DISCARDING");
			return;
		}

		// IF DECISION IS TO MAKE ROLLBACK
		readyToRoll = Integer.parseInt(recvMsg.getPayload());

		// SEND DECISION TO COHORTS IF I HAVE ANY
		for (Integer cohort : cohorts) {
			Message decToCohortMsg = new Message(myId, DEFAULT_LABEL,
					Message.MessageType.RRFIN, ++logicalClock,
					readyToRoll.toString());
			decToCohortMsg.setInstanceId(this.instanceId);
			connection.unicast(cohort, decToCohortMsg);
			System.out.println("\n[KOO TOUEG] NODE " + myId
					+ ": SENT RRFIN TO COHORT " + cohort);
		}

		if (readyToRoll == 1) {
			// ROLLBACK TO THE LAST PERMANENT CHECKPOINT
			// failedNeighbor = -1;
			System.out.println(" STAAAAAAAAARSARTA");
			rollbackCommit();
			System.out.println(" ENDDDDDDDDDDDDDDDDDDD");
			readyToRoll = 0;
			inCheckPointOrRecovery.set(false);
			// Unfreeze SEND/RECEIVE
			unfreezeSend.signal();
			unfreezeReceive.signal();
		} else {
			// ROLLBACK INSTANCE TO BE DISCARDED.
			discardRollback();
			inCheckPointOrRecovery.set(false);
			// Unfreeze SEND/RECEIVE
			unfreezeSend.signal();
			unfreezeReceive.signal();
		}
	}

	class SendThread implements Runnable {
		@Override
		public void run() {
			// System.out.println("\n[SERVICE] Starting Send Service...");
			while (keepWorking.get()) {
				try {
					resourcesLock.lock();
					applicationQueueLock.lock();

					// CHECK IF I'M BUSY WITH CP OR RR. IF YES FREEZE SENDING
					// APP MESSAGES
					// EDIT: 12/04/2013 This is not required since we used
					// wait/notify for
					// send thread on condition variable sendUnfreeze.
					if (!inCheckPointOrRecovery.get()) {

						// Read sendQueue and send messages if any
						while (sendMessageQueue.peek() != null) {
							String payload = sendMessageQueue.poll().trim();
							try {
								// Check if this is Result/Done/APP message
								if (payload.startsWith("=")) {
									// This is result message. Send only to
									// leader
									// Message msg = new Message(logicalClock.i,
									// myId,
									// payload);
									// msg.setType(Message.Type.RESULT);
									// connection.sendToLeader(msg);
									// connection.broadcast(msg);
								} else if (payload
										.equals(Message.APPLICATION_DONE)) {
									// This will be sent only by leader's
									// application. Stop service
									keepWorking.set(false);
								} else {
									// CREATE MESSAGE for payload
									Message appMsg = new Message(myId,
											++messageLabel, MessageType.APP,
											++logicalClock, payload);
									System.out
											.println("[APPLICATION] Proposed: "
													+ appMsg.toString());
									connection.broadcast(appMsg);

									// UPDATE STATE
									for (Integer neighbor : currentState
											.getFirstLabelSent().keySet()) {
										if (currentState.getFirstLabelSent()
												.get(neighbor)
												.equals(Integer.MIN_VALUE)) {
											currentState
													.getFirstLabelSent()
													.put(neighbor, messageLabel);
										}

										currentState.getLastLabelSent().put(
												neighbor, messageLabel);
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						} // END OF "while (sendMessageQueue.peek() != null) {"
					}

					// CHECK ITS TIME TO FAIL
					if (!inCheckPointOrRecovery.get()) {
						fail();
					}

					// CHECK IF IT IS TIME TO TAKE A CHECKPOINT
					if ((System.currentTimeMillis() - lastCheckpointTime) >= checkpointInterval) {
						// INITIATE LOCAL CHECKPOINT.
						takeLocalCheckpoint();
					}

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					// Unfreeze Send
					// sendUnfreeze.signal();
					applicationQueueLock.unlock();
					resourcesLock.unlock();
				}

				// Sleep for a moment
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// System.out.println("[SERVICE] Stopping Send Service...");
		}
	}

	class ReceiveThread implements Runnable {
		@Override
		public void run() {
			// System.out.println("[SERVICE] Starting Receiving Messages...");
			while (keepWorking.get()) {
				try {
					// Acquire Resources and Queue Locks
					resourcesLock.lock();
					applicationQueueLock.lock();

					LinkedList<Message> receivedMessages = connection.receive();
					// Process the messages and Perform ACTIONS ACCORDINGLY

					for (Iterator<Message> iterator = receivedMessages
							.iterator(); iterator.hasNext();) {
						Message message = iterator.next();
						// Receipt of a message will be an event: Increment
						// logical clock
						++logicalClock;

						// Check type of message
						switch (message.getMessageType()) {
						case APP:
							// Application Message
							// Directly deliver the message

							System.out
									.println("\n [KOOTOUEG] RECEIVED APP MSG "
											+ message.toString());

							// UPDATE STATE
							currentState.getLastLabelReceived().put(
									message.getMessageId().getOriginatorId(),
									message.getMessageId().getLabel());

							deliveredMessageQueue.add(message.getPayload());

							break;

						case CPREQ:
							// Checkpoint request
							// Call checkpoint method
							System.out
									.println("\n [KOOTOUEG] RECEIVED CP REQUEST MSG "
											+ message.toString());
							processCheckpointRequestMessage(message);
							break;

						case CPREP:
							// Checkpoint reply
							System.out
									.println("\n [KOOTOUEG] RECEIVED CP REPLY MSG "
											+ message.toString());
							processCheckpointReplyMessage(message);
							break;

						case CPFIN:
							// Checkpoint decision
							System.out
									.println("\n [KOOTOUEG] RECEIVED DECISION "
											+ message.toString());
							processCheckpointFinalMessage(message);
							break;

						case RRREQ:
							// Rollback Recovery request
							System.out
									.println("\n[KOO TOUEG] RECEIVED ROLLBACK REQUEST MSG "
											+ message.toString());
							processRollbackRequestMessage(message);
							break;

						case RRREP:
							// Rollback Recovery resply
							System.out
									.println("\n[KOO TOUEG] RECEIVED ROLLBACK REPLY MSG "
											+ message.toString());
							processRollbackReplyMessage(message);
							break;

						case RRFIN:
							// Rollback recovery decision
							System.out
									.println("\n[KOO TOUEG] RECEIVED ROLLBACK DECISION MSG "
											+ message.toString());
							processRollbackFinalMessage(message);
							break;

						case RESULT:
							// TODO Deliver result to application
							break;

						case FAIL:
							failedNeighbor = message.getMessageId()
									.getOriginatorId();
							System.out
									.println("\n[KOO TOUEG] RECEIVE FAILED MESSAGE FROM NEIGHBOR "
											+ failedNeighbor);

							break;

						default:
							System.out.println("ERROR: UNKNOWN MESSAGE TYPE");
							break;
						}

					}

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					// Unfreeze Send
					// sendUnfreeze.signal();
					applicationQueueLock.unlock();
					resourcesLock.unlock();
				}

				// Sleep for a moment
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// System.out.println("[SERVICE] Stopping Receiving Messages...");
		}
	}

	public ArrayList<String> deliver() {
		ArrayList<String> messages = new ArrayList<String>();

		applicationQueueLock.lock();
		try {

			// Freeze receive until ROLLBACK IS DONE
			while (tryingToRecover) {
				// WAIT UNTIL RR is DONE
				System.out.println("\n[KOO TOUEG] NODE " + myId
						+ " RECEIVE FREEZED");
				unfreezeReceive.await();
			}

			/*
			 * System.out.println("\n[KOO TOUEG] NODE " + myId +
			 * " RECEIVE UNFREEZED");
			 */while (deliveredMessageQueue.peek() != null) {
				messages.add(deliveredMessageQueue.poll());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			applicationQueueLock.unlock();
		}
		return messages;
	}

	public void broadCast(String message) {
		// FREEZE SEND IF PARTICIPATING IN CHECKPOINT OR RECOVERY
		applicationQueueLock.lock();
		try {
			while (inCheckPointOrRecovery.get()) {
				// WAIT UNTIL CP/RR is DONE
				System.out.println("\n[KOO TOUEG] NODE " + myId
						+ " SEND FREEZED");
				unfreezeSend.await();
			}

			System.out
					.println("\n[KOO TOUEG] NODE " + myId + " SEND UNFREEZED");
			sendMessageQueue.add(message);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			applicationQueueLock.unlock();
		}
	}

	/**
	 * Called when its time to fail
	 * 
	 * @throws IOException
	 */
	private void fail() throws Exception {
		// Check if I'm supposed to fail
		if (failureInterval > 0) {
			// Check if its time to fail
			if ((System.currentTimeMillis() - lastFailureTime) >= failureInterval) {
				Message failureMsg = new Message(myId, DEFAULT_LABEL,
						MessageType.FAIL, ++logicalClock, "");

				// Notify neighbors of my failure
				connection.broadcast(failureMsg);

				// Enter Recovery
				recover();
			}
		}
	}

	@Override
	public void run() {
		System.out.println("\n <<< CHECKPOINT & RECOVERY SERVICE STARTED >>>");

		// Start Send and Receive Thread
		Thread sendThread = new Thread(new SendThread());
		Thread receiveThread = new Thread(new ReceiveThread());

		// System.out.println("[SERVICE] Starting Send Thread...");
		sendThread.start();
		// System.out.println("[SERVICE] Starting Receive Thread...");
		receiveThread.start();

		// Wait for them to end
		try {
			sendThread.join();
			receiveThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("\n <<< CHECKPOINT & RECOVERY SERVICE STOPPED >>>");
	}

}
