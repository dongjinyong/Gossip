package com.dongjinyong.gossip.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class InetSocketAddressUtil {

	public static InetSocketAddress parseInetSocketAddress(String str)
			throws UnknownHostException {
		String[] strs = str.split(":");
		if (strs.length == 2) {
			return new InetSocketAddress(InetAddress.getByName(strs[0]),
					Integer.parseInt(strs[1]));
		} else {
			throw new UnknownHostException(str);
		}
	}
}
