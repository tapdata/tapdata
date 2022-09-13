package io.tapdata.pdk.core.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.logger.TapLogger;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class IPHolder {
	private static final String TAG = IPHolder.class.getSimpleName();
	private String ipPrefix;
	private String ethPrefix;
	
	private List<String> ips;

	public void init() {
		String ip = CommonUtils.getProperty("tapdata_net_interface_ip"); //specified fixed IP
		if(ip == null) {
			if(ipPrefix == null)
				ipPrefix = CommonUtils.getProperty("tapdata_net_interface_ip_prefix");
			if(ethPrefix == null)
				ethPrefix = CommonUtils.getProperty("tapdata_net_interface_eth_prefix");
		}
		List<String> ips = getAllIps();
		if(ip == null && ips.isEmpty()) {
			ip = getLocalHostIp(ipPrefix, ethPrefix);
		}
		if(ip != null) {
			this.ips = new ArrayList<>(Collections.singleton(ip));
		} else {
			this.ips = ips;
		}
		TapLogger.info(TAG, "Server ip is " + this.ips + " by ipPrefix " + ipPrefix + " ethPrefix " + ethPrefix);
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

	public void setIps(List<String> ips) {
		this.ips = ips;
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

	public static void main(String[] args) {
		System.out.println(IPHolder.getAllIps());
	}
	public static List<String> getAllIps() {
		return getAllIps(true);
	}
	public static List<String> getAllIps(boolean onlyIPv4) {
		List<String> allIps = new ArrayList<>();
		try {
			for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
				NetworkInterface iface = ifaces.nextElement();
//				String ethr = iface.getDisplayName();

				for (Enumeration<InetAddress> ips = iface.getInetAddresses(); ips.hasMoreElements(); ) {
					InetAddress ia = ips.nextElement();
					String anyIp = ia.getHostAddress();
					if(onlyIPv4) {
						if(ia instanceof Inet4Address)
							allIps.add(anyIp);
					} else {
						allIps.add(anyIp);
					}
				}
			}
		} catch (SocketException e) {
			throw new CoreException(TapAPIErrorCodes.ERROR_ALL_IPS_FAILED, "Get all ips failed, {}", e.getMessage());
		}
		return allIps;
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

	public List<String> getIps() {
		return ips;
	}
}
