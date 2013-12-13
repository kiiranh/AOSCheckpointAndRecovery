/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

public class AppState implements Serializable {

	private static final long serialVersionUID = -728711326615449846L;
	private HashMap<Integer, Integer> lastLabelSent = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> lastLabelReceived = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> firstLabelSent = new HashMap<Integer, Integer>();
	private String stringObject;

	private boolean isPermanent;
	private int timeStamp;

	public AppState(Set<Integer> neighborIds, String initialObject) {
		for (Integer neighborId : neighborIds) {
			lastLabelSent.put(neighborId, Integer.MIN_VALUE);
			lastLabelReceived.put(neighborId, Integer.MIN_VALUE);
			firstLabelSent.put(neighborId, Integer.MIN_VALUE);
		}

		timeStamp = -1;
		stringObject = initialObject;
		isPermanent = false;
	}

	public AppState(AppState other) {
		this.lastLabelSent.putAll(other.getLastLabelSent());
		this.lastLabelReceived.putAll(other.getLastLabelReceived());
		this.firstLabelSent.putAll(other.getFirstLabelSent());
		this.timeStamp = other.getTimeStamp();
		this.isPermanent = other.isPermanent();
		this.setStringObject(other.getStringObject());
	}

	public HashMap<Integer, Integer> getLastLabelSent() {
		return lastLabelSent;
	}

	public void setLastLabelSent(HashMap<Integer, Integer> lastLabelSent) {
		this.lastLabelSent = lastLabelSent;
	}

	public void setLastLabelSent(int neighbor, int label) {
		this.lastLabelSent.put(neighbor, label);
	}

	public HashMap<Integer, Integer> getLastLabelReceived() {
		return lastLabelReceived;
	}

	public void setLastLabelReceived(HashMap<Integer, Integer> lastLabelReceived) {
		this.lastLabelReceived = lastLabelReceived;
	}

	public void setLastLabelReceived(int neighbor, int label) {
		this.lastLabelReceived.put(neighbor, label);
	}

	public HashMap<Integer, Integer> getFirstLabelSent() {
		return firstLabelSent;
	}

	public void setFirstLabelSent(HashMap<Integer, Integer> firstLabelSent) {
		this.firstLabelSent = firstLabelSent;
	}

	public void setFirstLabelSent(int neighbor, int label) {
		this.firstLabelSent.put(neighbor, label);
	}

	public boolean isPermanent() {
		return isPermanent;
	}

	public void setPermanent(boolean isPermanent) {
		this.isPermanent = isPermanent;
	}

	public int getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(int timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	

	public String getStringObject() {
		return stringObject;
	}

	public void setStringObject(String stringObject) {
		this.stringObject = stringObject;
	}

	@Override
	public String toString() {
		StringBuilder state = new StringBuilder();
		state.append("TimeStamp: " + timeStamp);
		state.append("\nisPermanent: " + isPermanent);
		state.append("\n--- Last Label Received ---");
		for (Entry<Integer, Integer> ent : lastLabelReceived.entrySet()) {
			state.append("\nNode " + ent.getKey() + ": " + ent.getValue());
		}

		state.append("\n--- Last Label Sent ---");
		for (Entry<Integer, Integer> ent : lastLabelSent.entrySet()) {
			state.append("\nNode " + ent.getKey() + ": " + ent.getValue());
		}

		state.append("\n--- First Label Sent ---");
		for (Entry<Integer, Integer> ent : firstLabelSent.entrySet()) {
			state.append("\nNode " + ent.getKey() + ": " + ent.getValue());
		}
		
		state.append("\nApplication Object: " + stringObject);

		return state.toString();
	}

}
