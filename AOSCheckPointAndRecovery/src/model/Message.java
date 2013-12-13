/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package model;

public class Message implements Comparable<Message> {
	public static final char MESSAGE_FIELDS_SEPARATOR = ':';
	public static final char MESSAGE_ID_SEPARATOR = '-';
	public static final String APPLICATION_DONE = "DONE";

	public static enum MessageType {
		APP, CPREQ, CPREP, CPFIN, RRREQ, RRREP, RRFIN, RESULT, FAIL
	}

	public class MessageId implements Comparable<MessageId> {
		final private int originatorId;
		final private int label;

		public MessageId(int originatorId, int messageNumber) {
			this.originatorId = originatorId;
			this.label = messageNumber;
		}

		public int getOriginatorId() {
			return originatorId;
		}

		public int getLabel() {
			return label;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + label;
			result = prime * result + originatorId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MessageId other = (MessageId) obj;
			if (originatorId != other.originatorId)
				return false;
			if (label != other.label)
				return false;
			return true;
		}

		@Override
		public int compareTo(MessageId other) {
			return Integer.valueOf(this.label).compareTo(
					Integer.valueOf(other.label));
		}

		@Override
		public String toString() {
			return "" + originatorId + MESSAGE_ID_SEPARATOR + label;
		}
	}

	// Message fields
	final private MessageId messageId;
	private MessageType messageType;
	final private int timeStamp;

	/**
	 * To identify who initiated the checkpoint/rollback request
	 */
	private int instanceId;
	private String payload;

	public Message(int originatorId, int messageLabel, MessageType type,
			int timeStamp, String payload) {
		this.messageId = new MessageId(originatorId, messageLabel);
		this.messageType = type;
		this.timeStamp = timeStamp;
		this.payload = payload;
		this.instanceId = -1;
	}

	public MessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public MessageId getMessageId() {
		return messageId;
	}

	public int getTimeStamp() {
		return timeStamp;
	}

	public int getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(int instanceId) {
		this.instanceId = instanceId;
	}

	@Override
	public String toString() {
		// MsgID(originatorId-label):type:timestamp:payload:instanceid
		return messageId.toString() + MESSAGE_FIELDS_SEPARATOR + messageType
				+ MESSAGE_FIELDS_SEPARATOR + timeStamp
				+ MESSAGE_FIELDS_SEPARATOR + payload + MESSAGE_FIELDS_SEPARATOR
				+ instanceId;
	}

	public static Message parseMessage(String msgStr) {
		String[] msgParts = msgStr.split(MESSAGE_FIELDS_SEPARATOR + "");
		int originatorId = Integer.parseInt(msgParts[0]
				.split(MESSAGE_ID_SEPARATOR + "")[0]);
		int label = Integer.parseInt(msgParts[0].split(MESSAGE_ID_SEPARATOR
				+ "")[1]);

		Message msg = new Message(originatorId, label,
				MessageType.valueOf(msgParts[1]),
				Integer.parseInt(msgParts[2]), msgParts[3]);
		msg.setInstanceId(Integer.parseInt(msgParts[4]));
		return msg;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((messageId == null) ? 0 : messageId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		// Assuming MessageID is unique, compare only msgId
		if (messageId == null) {
			if (other.messageId != null)
				return false;
		} else if (!messageId.equals(other.messageId))
			return false;
		return true;
	}

	@Override
	public int compareTo(Message other) {
		if (timeStamp == other.timeStamp) {
			// Current timestamp equal - break tie using message id
			return messageId.compareTo(other.messageId);
		} else {
			// timestamp differ.
			return Integer.valueOf(timeStamp).compareTo(
					Integer.valueOf(other.timeStamp));
		}
	}
}
