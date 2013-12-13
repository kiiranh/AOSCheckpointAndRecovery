/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Koo Toeug Checkpoint and Recovery
 *
 * @author Kiran Gavali
 */

package model;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Set;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

public class Connection {

	public enum Stream {
		CONTROL(0), DATA(1);

		private final int value;

		private Stream(int val) {
			value = val;
		}
	}

	public enum Action {
		START, STOP;
	}

	private HashMap<Integer, SctpChannel> nodeChannelMap;
	// private ArrayList<SctpChannel> channelList;

	public static final int BUFFER_SIZE = 512;
	private final Charset charset = Charset.forName("ISO-8859-1");
	private CharsetEncoder encoder;
	private CharsetDecoder decoder;

	public Connection() {
		// channelList = new ArrayList<SctpChannel>();
		nodeChannelMap = new LinkedHashMap<Integer, SctpChannel>();
		encoder = charset.newEncoder();
		decoder = charset.newDecoder();
	}

	public Set<Integer> getNeighbors() {
		return nodeChannelMap.keySet();
	}

	public void setUp(HashMap<Integer, NodeInfo> neighbors, int myId,
			NodeInfo myNodeInfo) throws IOException {

		ArrayList<Integer> sortedNeighbors = new ArrayList<Integer>(
				neighbors.keySet());
		Collections.sort(sortedNeighbors);

		// Selectively create connections to the given nodes.
		int i = 0;
		System.out.println("[NODE] MyID: " + myId);

		// Connect to nodes with lower IDs
		for (Integer neighborId : sortedNeighbors) {
			if (neighborId < myId) {
				// lower id node. Connect
				System.out.println("[NODE] Trying to connect to Node Id: " + neighborId);
				InetSocketAddress serverAddr = new InetSocketAddress(neighbors
						.get(neighborId).getHostAddress(),
						neighbors.get(neighborId).getPort());
				SctpChannel sc = SctpChannel.open(serverAddr, 0, 0);
				// channelList.add(sc);
				sc.configureBlocking(false);
				nodeChannelMap.put(neighborId, sc);
				System.out.println("[NODE] Connected to Node Id: " + neighborId);
//				System.out.println("\tLocal Channel: "
//						+ sc.getAllLocalAddresses().iterator().next()
//						+ " Remote Channel: "
//						+ sc.getRemoteAddresses().iterator().next());
				++i;
			} else {
				// Higher ID node. break
				break;
			}
		}

		// i points to my first neighbor with id higher than me
		SctpServerChannel ssc = SctpServerChannel.open();
		InetSocketAddress serverAddr = new InetSocketAddress(
				myNodeInfo.getPort());
		ssc.bind(serverAddr);
		// We have already gone through nodes with lower IDs. Only nodes with
		// higher IDs remain.
		// Accept connections from nodes with higher IDs.
		while (neighbors.size() != i) {
			System.out.println("[NODE] Awaiting connection from Node Id: "
					+ sortedNeighbors.get(i));
			SctpChannel sc = ssc.accept();
			// channelList.add(sc);
			sc.configureBlocking(false);
			nodeChannelMap.put(sortedNeighbors.get(i), sc);
			System.out.println("[NODE] Accepted connection from Node Id: "
					+ sortedNeighbors.get(i));
			System.out.println("\tLocal Channel: "
					+ sc.getAllLocalAddresses().iterator().next()
					+ " Remote Channel: "
					+ sc.getRemoteAddresses().iterator().next());
			++i;
		}

	}

	/**
	 * Called by leader to start off the program/computation
	 * 
	 * @throws IOException
	 */
	public void leaderNotifyStart(int leaderId) throws IOException {
		MessageInfo messageInfo = null;
		ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
		CharBuffer cbuf = CharBuffer.allocate(BUFFER_SIZE);

		for (SctpChannel channel : nodeChannelMap.values()) {
			cbuf.clear();
			buf.clear();
			cbuf.put(leaderId + "," + Action.START).flip();
			encoder.encode(cbuf, buf, true);
			buf.flip();

			/* send the message on the US stream */
			messageInfo = MessageInfo
					.createOutgoing(null, Stream.CONTROL.value);
			channel.send(buf, messageInfo);
			// System.out.println("[LEADER NODE] Signalled START to Channel: " +
			// channel.getRemoteAddresses().iterator().next());
		}

		cbuf.clear();
		buf.clear();
	}

	/**
	 * Called by non-leaders to wait for START signal from Leader
	 * 
	 * @throws IOException
	 */
	public void awaitStartFromLeader() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);

		MessageInfo messageInfo = null;
		buf.clear();
		System.out.println("\n <<< Awaiting START signal from Leader >>>");

		while (true) {
			// *** ASSUMPTION *** Assuming the first node in the config file to
			// be the leader
			messageInfo = nodeChannelMap.get(0).receive(buf, System.out, null);
			buf.flip();
			if (buf.remaining() > 0
					&& messageInfo.streamNumber() == Stream.CONTROL.value) {
				String msg = decoder.decode(buf).toString();
				if (msg.contains("" + Action.START)) {
					System.out
							.println("\n <<< Received START Signal from Leader >>>");
					break;
				}
			}
			buf.clear();
			try {
				Thread.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void sendToLeader(Message resultMsg) throws Exception {
		// ASSUMING FIRST NODE IS LEADER
		SctpChannel channel = nodeChannelMap.get(0);
		if (channel == null) {
			throw new Exception("Channel for node 0 not found!");
		}

		ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
		CharBuffer cbuf = CharBuffer.allocate(BUFFER_SIZE);

		cbuf.put(resultMsg.toString()).flip();
		encoder.encode(cbuf, buf, true);
		buf.flip();

		/* send the message on the DATA stream */
		MessageInfo messageInfo = MessageInfo.createOutgoing(null,
				Stream.DATA.value);
		channel.send(buf, messageInfo);
		// System.out.println("Sent Result: " + resultMsg.toString());
	}

	public void unicast(int nodeId, Message msg) throws Exception {
		SctpChannel channel = nodeChannelMap.get(nodeId);
		if (channel == null) {
			throw new Exception("Channel for node " + nodeId + " not found!");
		}

		ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
		CharBuffer cbuf = CharBuffer.allocate(BUFFER_SIZE);

		cbuf.put(msg.toString()).flip();
		encoder.encode(cbuf, buf, true);
		buf.flip();

		/* send the message on the DATA stream */
		MessageInfo messageInfo = MessageInfo.createOutgoing(null,
				Stream.DATA.value);
		channel.send(buf, messageInfo);
		//System.out.println("[CHANNEL] Unicast Message: " + msg.toString());
	}

	public void broadcast(Message msg) throws IOException {
		// 1. Convert Message to String
		// 2. Broadcast over All Channels.
		ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
		CharBuffer cbuf = CharBuffer.allocate(BUFFER_SIZE);

		for (Integer node : nodeChannelMap.keySet()) {
			cbuf.clear();
			buf.clear();
			cbuf.put(msg.toString()).flip();
			encoder.encode(cbuf, buf, true);
			buf.flip();

			/* send the message on the DATA stream */
			MessageInfo messageInfo = MessageInfo.createOutgoing(null,
					Stream.DATA.value);
			nodeChannelMap.get(node).send(buf, messageInfo);
//			 System.out.println("[CHANNEL] Sent Msg " + msg.toString() + " to Node "
//			 + node);
		}
		// System.out.println("Broadcast Message: " + msg.toString());
	}

	public LinkedList<Message> receive() throws IOException {
		LinkedList<Message> receivedMessages = new LinkedList<Message>();
		ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);

		// 1. Read messages from channel
		MessageInfo messageInfo = null;
		// buf.clear();
		// System.out.println("Checking message on Channel...");

		for (Integer node : nodeChannelMap.keySet()) {
			// for (SctpChannel channel : channelList) {
			// Get all available messages from each channel
			// messageInfo = channel.receive(buf, System.out, null);
			//System.out.println("Calling receive on CHannel" + node);
			SctpChannel sc = nodeChannelMap.get(node);
			sc.configureBlocking(false);
			messageInfo = sc.receive(buf, null, null);
			//System.out.println("Got message from Channel " + node);
			buf.flip();
			if (buf.remaining() > 0
					&& messageInfo.streamNumber() == Stream.DATA.value) {
				String msgStr = decoder.decode(buf).toString();

				// 2. Parse message and add to queue
				Message msg = Message.parseMessage(msgStr);
				receivedMessages.add(msg);
//				 System.out.println("[CHANNEL] Received msg " + msg.toString()
//				 + " from Node " + node);
			}
			buf.clear();
			// try {
			// Thread.sleep(5);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
		}

		return receivedMessages;

	}

	public void tearDown() throws Exception {
		for (SctpChannel channel : nodeChannelMap.values()) {
			channel.close();
		}
	}
}
