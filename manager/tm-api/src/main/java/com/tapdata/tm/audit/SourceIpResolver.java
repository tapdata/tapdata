package com.tapdata.tm.audit;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 来源 IP 解析：默认使用 TCP 对端地址；只有对端属于已配置的可信代理时，才从 X-Forwarded-For 右侧向左
 * 取第一个非可信代理地址。客户端提交的转发头在任何情况下都不直接作为来源 IP（需求 10.4）。
 */
public class SourceIpResolver {
	public static final String NOTE_TRUSTED_PROXY = "audit_ip_note_trusted_proxy";
	public static final String NOTE_PROXY_WITHOUT_HEADER = "audit_ip_note_proxy_without_header";
	public static final String NOTE_UNAVAILABLE = "audit_ip_note_unavailable";

	private final List<Cidr> trusted = new ArrayList<>();

	public SourceIpResolver(Collection<String> trustedProxies) {
		if (trustedProxies != null) {
			for (String entry : trustedProxies) {
				Cidr cidr = Cidr.parse(entry);
				if (cidr != null) {
					trusted.add(cidr);
				}
			}
		}
	}

	public Result resolve(String remoteAddr, String forwardedFor) {
		String peer = normalize(remoteAddr);
		if (peer == null) {
			return new Result(AuditValues.UNAVAILABLE, NOTE_UNAVAILABLE);
		}
		if (!isTrusted(peer)) {
			return new Result(peer, null);
		}
		if (forwardedFor == null || forwardedFor.isBlank()) {
			return new Result(peer, NOTE_PROXY_WITHOUT_HEADER);
		}
		String[] hops = forwardedFor.split(",");
		for (int i = hops.length - 1; i >= 0; i--) {
			String hop = normalize(hops[i]);
			if (hop == null) {
				return new Result(AuditValues.UNAVAILABLE, NOTE_UNAVAILABLE);
			}
			if (!isTrusted(hop)) {
				return new Result(hop, NOTE_TRUSTED_PROXY);
			}
		}
		return new Result(peer, NOTE_PROXY_WITHOUT_HEADER);
	}

	public boolean isTrusted(String ip) {
		for (Cidr cidr : trusted) {
			if (cidr.contains(ip)) {
				return true;
			}
		}
		return false;
	}

	static String normalize(String value) {
		if (value == null) {
			return null;
		}
		String ip = value.trim();
		if (ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
			return null;
		}
		if (ip.startsWith("[") && ip.contains("]")) {
			ip = ip.substring(1, ip.indexOf(']'));
		} else if (ip.indexOf(':') > 0 && ip.indexOf(':') == ip.lastIndexOf(':') && ip.contains(".")) {
			ip = ip.substring(0, ip.indexOf(':'));
		}
		return isLiteral(ip) ? ip : null;
	}

	private static boolean isLiteral(String ip) {
		if (ip.matches("\\d{1,3}(\\.\\d{1,3}){3}")) {
			for (String part : ip.split("\\.")) {
				if (Integer.parseInt(part) > 255) {
					return false;
				}
			}
			return true;
		}
		return ip.contains(":") && ip.matches("[0-9a-fA-F:.]+");
	}

	public record Result(String ip, String note) {
	}

	private record Cidr(BigInteger network, BigInteger mask, int length) {
		static Cidr parse(String text) {
			if (text == null || text.isBlank()) {
				return null;
			}
			String[] parts = text.trim().split("/");
			String ip = normalize(parts[0]);
			if (ip == null) {
				return null;
			}
			try {
				byte[] bytes = InetAddress.getByName(ip).getAddress();
				int bits = bytes.length * 8;
				int prefix = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : bits;
				if (prefix < 0 || prefix > bits) {
					return null;
				}
				BigInteger mask = prefix == 0 ? BigInteger.ZERO
						: BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE).shiftRight(bits - prefix).shiftLeft(bits - prefix);
				return new Cidr(new BigInteger(1, bytes).and(mask), mask, bytes.length);
			} catch (Exception e) {
				return null;
			}
		}

		boolean contains(String ip) {
			try {
				byte[] bytes = InetAddress.getByName(ip).getAddress();
				return bytes.length == length && new BigInteger(1, bytes).and(mask).equals(network);
			} catch (Exception e) {
				return false;
			}
		}
	}
}
