package com.tapdata.constant;

import org.apache.commons.lang3.StringUtils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * @author samuel
 */
public class NetworkUtil {

	public final static String MAC = "mac";
	public final static String IP = "ip";

	/**
	 * get ip or mac from first network card
	 *
	 * @param addressType ip/mac
	 * @return
	 * @throws Exception
	 */
	public static String GetAddress(String addressType) throws Exception {
		String address = "";
		InetAddress lanIp = null;

		String ipAddress = null;
		Enumeration<NetworkInterface> net = null;
		net = NetworkInterface.getNetworkInterfaces();

		while (net.hasMoreElements()) {
			NetworkInterface element = net.nextElement();
			Enumeration<InetAddress> addresses = element.getInetAddresses();

			if (element.getHardwareAddress() == null) {
				continue;
			}

			while (addresses.hasMoreElements() && element.getHardwareAddress().length > 0 && !isVMMac(element.getHardwareAddress())) {
				InetAddress ip = addresses.nextElement();
				if (ip instanceof Inet4Address) {

					if (ip.isSiteLocalAddress()) {
						ipAddress = ip.getHostAddress();
						lanIp = InetAddress.getByName(ipAddress);
					}

				}

			}
		}

		if (lanIp == null) {
			return null;
		}

		if (addressType.equals(IP)) {

			address = lanIp.toString().replaceAll("^/+", "");

		} else if (addressType.equals(MAC)) {

			address = getMacAddress(lanIp);

		} else {

			throw new Exception("Specify \"ip\" or \"mac\"");

		}

		return address;

	}

	private static String getMacAddress(InetAddress ip) throws Exception {
		String address = null;

		NetworkInterface network = NetworkInterface.getByInetAddress(ip);
		byte[] mac = network.getHardwareAddress();

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < mac.length; i++) {
			sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
		}
		address = sb.toString();


		return address;
	}

	private static boolean isVMMac(byte[] mac) {
		if (null == mac) {
			return false;
		}
		byte invalidMacs[][] = {
				{0x00, 0x05, 0x69},             //VMWare
				{0x00, 0x1C, 0x14},             //VMWare
				{0x00, 0x0C, 0x29},             //VMWare
				{0x00, 0x50, 0x56},             //VMWare
				{0x08, 0x00, 0x27},             //Virtualbox
				{0x0A, 0x00, 0x27},             //Virtualbox
				{0x00, 0x03, (byte) 0xFF},       //Virtual-PC
				{0x00, 0x15, 0x5D}              //Hyper-V
		};

		for (byte[] invalid : invalidMacs) {
			if (invalid[0] == mac[0] && invalid[1] == mac[1] && invalid[2] == mac[2]) {
				return true;
			}
		}

		return false;
	}

	public static String hostname2IpAddress(String hostname) {
		if (StringUtils.isBlank(hostname)) {
			return null;
		}
		InetAddress inetAddress;
		try {
			inetAddress = InetAddress.getByName(hostname);
		} catch (UnknownHostException e) {
			return hostname;
		}
		String hostAddress = inetAddress.getHostAddress();
		return hostAddress;
	}
}
