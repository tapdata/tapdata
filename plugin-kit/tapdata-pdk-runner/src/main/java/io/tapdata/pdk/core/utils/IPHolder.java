package io.tapdata.pdk.core.utils;

import io.tapdata.entity.logger.TapLogger;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class IPHolder {
	private static final String TAG = IPHolder.class.getSimpleName();
	private String ipPrefix;
	private String ethPrefix;
	
	private String ip;

	public void init() {
		if(ipPrefix == null)
			ipPrefix = CommonUtils.getProperty("tapdata_net_interface_ip_prefix");
		if(ethPrefix == null)
			ethPrefix = CommonUtils.getProperty("tapdata_net_interface_eth_prefix");
		ip = getLocalHostIp(ipPrefix, ethPrefix);
		if(ip == null)
			ip = "127.0.0.1";
		TapLogger.info(TAG, "Server ip is " + ip + " by ipPrefix " + ipPrefix + " ethPrefix " + ethPrefix);
	}
	public String getIpPrefix() {
		return ipPrefix;
	}

	public void setIpPrefix(String ipPrefix) {
		this.ipPrefix = ipPrefix;
	}

	public String getEthPrefix() {
		return ethPrefix;
	}

	public void setEthPrefix(String ethPrefix) {
		this.ethPrefix = ethPrefix;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	private static String getLocalHostIpPrivate() {
		try {
			InetAddress addr = InetAddress.getLocalHost();
			return addr.getHostAddress();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getLocalHostIp() {
		return getLocalHostIp(null, null);
	}

	public static String getLocalHostIp(String ipStartWith, String faceStartWith) {
		NetworkInterface iface = null;
		String ethr;
		String myip = null;

		if (ipStartWith == null && faceStartWith == null) {
			myip = getLocalHostIpPrivate();
			if (myip != null)
				return myip;
		}
		try {
//			String anyIp = null;
			for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
				iface = ifaces.nextElement();
				ethr = iface.getDisplayName();

				if (faceStartWith == null || isStartWith(ethr, faceStartWith)) {
					InetAddress ia = null;
					for (Enumeration<InetAddress> ips = iface.getInetAddresses(); ips.hasMoreElements(); ) {
						ia = ips.nextElement();
						String anyIp = ia.getHostAddress();
						if (ipStartWith == null || anyIp.startsWith(ipStartWith)) {
							myip = ia.getHostAddress();
							return myip;
						}
					}
				}
			}
		} catch (SocketException e) {
		}
		return myip;
	}

	private static boolean isStartWith(String str, String faceStartWith) {
		if (faceStartWith != null && str != null) {
			if (faceStartWith.contains("\\|")) {
				String[] compares = faceStartWith.split("\\|");
				for (String compare : compares) {
					if (str.startsWith(compare)) {
						return true;
					}
				}
			} else {
				return str.startsWith(faceStartWith);
			}
		}
		return false;
	}
}
