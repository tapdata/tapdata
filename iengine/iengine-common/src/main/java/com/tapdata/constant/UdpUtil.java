package com.tapdata.constant;

import java.net.DatagramSocket;
import java.net.SocketException;

public class UdpUtil {

	public static DatagramSocket createUdpSocketServer(int port, int soTimeout) throws SocketException {
		DatagramSocket udpServer = null;
		if (port > 0) {
			udpServer = new DatagramSocket(port);
			udpServer.setSoTimeout(soTimeout);
		} else {
			udpServer = new DatagramSocket();
		}

		return udpServer;
	}
}
