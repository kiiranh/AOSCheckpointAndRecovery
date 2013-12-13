/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import model.AppState;

/**
 * Utility methods
 * 
 * @author kiiranh
 */
public class Utility {

	public static final String CHECKPOINT_1_SUFFIX = "_1.ckpt";
	public static final String CHECKPOINT_2_SUFFIX = "_2.ckpt";

	/**
	 * Write checkpoint to permanent storage
	 * 
	 * @param state
	 * @param nodeId
	 * @throws IOException
	 */
	public static void writePermanentCheckpoint(AppState state, int nodeId)
			throws IOException {
		// By default files in this directory will be written

		// There will be 2 latest checkpoints: nodeId_1.ckpt and nodeId_2.ckpt
		// Check if a latest checkpoint exists
		File cp1 = new File(nodeId + CHECKPOINT_1_SUFFIX);
		File cp2 = new File(nodeId + CHECKPOINT_2_SUFFIX);
		File newCpFile = new File(nodeId + CHECKPOINT_2_SUFFIX);

		if (cp2.exists()) {
			// 2 Checkpoints exist: Copy ckpt2 to ckpt1 and write new state to
			// ckpt2
			cp1.delete();
			cp2.renameTo(cp1);
		} else {
			// Checkpoint 2 does not exist. CHeck if CP1 exist
			if (!cp1.exists()) {
				// Checkpoint 1 does not exist either. Write new state to nodeId
				// + "_1.ckpt"
				newCpFile = cp1;
			}
		}

		// Write State to newCpFile
		ObjectOutput output = null;
		try {
			OutputStream file = new FileOutputStream(
					newCpFile.getAbsolutePath());
			OutputStream buffer = new BufferedOutputStream(file);
			output = new ObjectOutputStream(buffer);
			output.writeObject(state);
			System.out.println("NODE " + nodeId
					+ ": WROTE PERMANENT CHECKPOINT" + newCpFile.getName());
		} catch (Exception e) {
			throw e;
		} finally {
			if (output != null) {
				output.close();
			}
		}
	}

	/**
	 * Get the last permanent checkpoint for the node
	 * 
	 * @param nodeId
	 * @return
	 * @throws Exception
	 */
	public static AppState fetchLastPermanentCheckpoint(int nodeId) throws Exception {
		AppState cp = null;
		File lastCheckpoint = new File(nodeId + CHECKPOINT_2_SUFFIX);
		String checkpointToFetch = null;

		// Check if Checkpoint 2 exists
		if (lastCheckpoint.exists()) {
			checkpointToFetch = lastCheckpoint.getName();
		} else {
			// Check if Checkpoint 1 exists
			lastCheckpoint = new File(nodeId + CHECKPOINT_1_SUFFIX);

			if (lastCheckpoint.exists()) {
				checkpointToFetch = lastCheckpoint.getName();
			} else {
				// Neither Checkpoint 1 nor Checkpoint 2 exist.
				System.out.println("NODE " + nodeId + ": NO CHECKPOINT FOUND");
			}
		}

		// At this point, a checkpoint exists and checkpointToFetch holds its
		// handle. Read it
		if (checkpointToFetch != null) {
			ObjectInput input = null;

			try {
				InputStream file = new FileInputStream(checkpointToFetch);
				InputStream buffer = new BufferedInputStream(file);
				input = new ObjectInputStream(buffer);
				cp = (AppState) input.readObject();
			} catch (Exception e) {
				throw e;
			} finally {
				if (input != null) {
					input.close();
				}
			}
		}

		return cp;
	}
	/**
	 * For testing only
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length < 1){
			// Expecting a nodeId
			throw new Exception("");
		}
		
		AppState lastState = Utility.fetchLastPermanentCheckpoint(Integer.parseInt(args[0]));
		System.out.println("LAST PERMANENT CHECKPOINT FOR NODE " + Integer.parseInt(args[0]));
		System.out.println(lastState.toString());
	}
}
