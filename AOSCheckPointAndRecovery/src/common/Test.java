/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package common;

import java.io.FileInputStream;
import java.util.Properties;

import model.AppState;

public class Test {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			throw new Exception(
					"Invalid Usage.\n Run: java Test <config file location>");
		}

		System.out.println("\n <<< VERIFICATION START >>>");

		String configFile = args[0];

		Properties props = new Properties();
		props.load(new FileInputStream(configFile));

		// Populate my info
		int numNodes = Integer.parseInt(props.getProperty("numberOfNodes"));
		boolean consistent = true;

		for (int id = 0; id < numNodes; ++id) {
			String neighborStr = props.getProperty("t" + id);
			AppState myCP = Utility.fetchLastPermanentCheckpoint(id);

			if (neighborStr == null || neighborStr.trim().isEmpty()) {
				System.out.println("[NODE] NO NEIGHBORS");
			} else {
				for (String neighborId : neighborStr.split(",")) {
					AppState neighborCP = Utility
							.fetchLastPermanentCheckpoint(Integer
									.parseInt(neighborId));

					if (myCP.getLastLabelReceived().get(
							Integer.parseInt(neighborId)) > neighborCP
							.getLastLabelSent().get(id)) {
						// Not consistent.
						consistent = false;
						System.out.println("[VERIFICATION] NODE " + id
								+ " NOT CONSISTENT WITH NODE " + neighborId);
					} else {
						System.out.println("[VERIFICATION] NODE " + id
								+ " CONSISTENT WITH NODE " + neighborId);
					}

				}
			}

		}

		if (consistent) {
			System.out
					.println("[VERIFICATION] TEST PASSED !!! ALL NODES PAIRWISE CONSISTENT");
		} else {
			System.out.println("[VERIFICATION] TEST FAILED !!!");
		}

		System.out.println("\n <<< VERIFICATION COMPLETE >>>");

	}
}
