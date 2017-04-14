package de.soderer.utilities;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sun.net.util.IPAddressUtil;

public class NetworkUtilities {
	private static final String SPECIAL_CHARS_REGEXP = "\\p{Cntrl}\\(\\)<>@,;:'\\\\\\\"\\.\\[\\]";
	private static final String VALID_CHARS_REGEXP = "[^\\s" + SPECIAL_CHARS_REGEXP + "]";
	private static final String QUOTED_USER_REGEXP = "(\"[^\"]*\")";
	private static final String WORD_REGEXP = "((" + VALID_CHARS_REGEXP + "|')+|" + QUOTED_USER_REGEXP + ")";
	
	private static final String DOMAIN_PART_REGEX = "\\p{Alnum}(?>[\\p{Alnum}-]*\\p{Alnum})*";
	private static final String TOP_DOMAIN_PART_REGEX = "\\p{Alpha}{2,}";
	private static final String DOMAIN_NAME_REGEX = "^(?:" + DOMAIN_PART_REGEX + "\\.)+" + "(" + TOP_DOMAIN_PART_REGEX + ")$";
	
	/** 
	 * Regular expression for parsing email addresses. 
	 * 
	 * Taken from Apache Commons Validator. 
	 * If this is not working, shame on Apache ;) 
	 */
    private static final String EMAIL_REGEX = "^\\s*?(.+)@(.+?)\\s*$";
    
    private static final String USER_REGEX = "^\\s*" + WORD_REGEXP + "(\\." + WORD_REGEXP + ")*$";
    
    /** Regular expression pattern for parsing email addresses. */
    private static final Pattern EMAIL_PATTERN = Pattern.compile(EMAIL_REGEX);

    private static final Pattern USER_PATTERN = Pattern.compile(USER_REGEX);
    
    private static final Pattern DOMAIN_NAME_PATTERN = Pattern.compile(DOMAIN_NAME_REGEX);
    
	public static boolean testConnection(String hostname, int port) throws Exception {
		Socket socket = null;
		try {
			socket = new Socket();
			InetSocketAddress endPoint = new InetSocketAddress(hostname, port);
			int timeout = 2000; // 2 Sekunden
			if (endPoint.isUnresolved()) {
				throw new Exception("Cannot resolve hostname '" + hostname + "'");
			} else {
				try {
					socket.connect(endPoint, timeout);
					return true;
				} catch (IOException ioe) {
					throw new Exception("Cannot connect to host '" + hostname + "' on port " + port + ": " + ioe.getClass().getSimpleName() + ": " + ioe.getMessage());
				}
			}
		} finally {
			Utilities.closeQuietly(socket);
		}
	}

	public static boolean ping(String ipOrHostname) {
		try {
			if (ipOrHostname.toLowerCase().trim().startsWith("http://")) {
				URL url = new URL("http://" + getHostnameFromRequestString(ipOrHostname));
				HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
	            httpURLConnection.setConnectTimeout(2000);
	            httpURLConnection.setReadTimeout(2000);
	            httpURLConnection.setAllowUserInteraction(false);
	            httpURLConnection.connect();
				return true;
			} else if (ipOrHostname.toLowerCase().trim().startsWith("https://")) {
				URL url = new URL("https://" + getHostnameFromRequestString(ipOrHostname));
				HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
	            httpURLConnection.setConnectTimeout(2000);
	            httpURLConnection.setReadTimeout(2000);
	            httpURLConnection.setAllowUserInteraction(false);
	            httpURLConnection.connect();
				return true;
			} else {
				return InetAddress.getByName(getHostnameFromRequestString(ipOrHostname)).isReachable(5000);
			}
		} catch (Exception e) {
			return false;
		}
	}

	public static byte[] getMacAddressBytes(String macAddress) throws IllegalArgumentException {
		if (Utilities.isEmpty(macAddress)) {
			throw new IllegalArgumentException("Invalid MAC address.");
		}

		String[] hexParts = macAddress.split("(\\:|\\-| )");
		if (hexParts.length != 6) {
			throw new IllegalArgumentException("Invalid MAC address.");
		}

		try {
			byte[] bytes = new byte[6];
			for (int i = 0; i < 6; i++) {
				bytes[i] = (byte) Integer.parseInt(hexParts[i], 16);
			}
			return bytes;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid hex digit in MAC address.");
		}
	}

	public static boolean wakeOnLanPing(String macAddress) {
		DatagramSocket socket = null;
		try {
			byte[] macBytes = getMacAddressBytes(macAddress);
			byte[] bytes = new byte[6 + 16 * macBytes.length];
			Arrays.fill(bytes, 0, 6, (byte) 0xFF);
			for (int i = 6; i < bytes.length; i += macBytes.length) {
				System.arraycopy(macBytes, 0, bytes, i, macBytes.length);
			}

			socket = new DatagramSocket();
			socket.send(new DatagramPacket(bytes, bytes.length, InetAddress.getByName("255.255.255.255"), 9));
			socket.close();

			return true;
		} catch (Exception e) {
			return false;
		} finally {
			if (socket != null) {
				socket.close();
			}
		}
	}

	public static boolean checkForNetworkConnection() {
		try {
			for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
				if (networkInterface.isUp() && !networkInterface.isLoopback()) {
					return true;
				}
			}
			return false;
		} catch (SocketException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static String getHostnameFromRequestString(String requestString) {
		if (requestString == null || !requestString.contains("/")) {
			return requestString;
		} else {
			if (requestString.toLowerCase().startsWith("http")) {
				requestString = requestString.substring(requestString.indexOf("//") + 2);

				if (!requestString.contains("/")) {
					return requestString;
				}
			}

			return requestString.substring(0, requestString.indexOf("/"));
		}
	}

	/**
	 * Get hostname of this machine
	 *
	 * @return
	 */
	public static String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return "unbekannter Rechnername";
		}
	}
    
    public static boolean isValidDomain(String domain) {
    	String asciiDomainName;
		try {
			asciiDomainName = java.net.IDN.toASCII(domain);
		} catch (Exception e) {
			// invalid domain name like abc@.ch
			return false;
		}

    	// Do not allow ".local" top level domain
    	if (asciiDomainName.toLowerCase().endsWith(".local")) {
    		return false;
    	}
    	
    	return DOMAIN_NAME_PATTERN.matcher(asciiDomainName).matches();
    }
    
    public static boolean isValidEmail(String emailAddress) {
		Matcher m = EMAIL_PATTERN.matcher(emailAddress);
				
		// Check, if email address matches outline structure
		if (!m.matches()) {
			return false;
		}
		
		// Check if user-part is valid
		if (!isValidUser(m.group(1))) {
			return false;
		}
		
		// Check if domain-part is valid
		if (!isValidDomain(m.group(2))) {
			return false;
		}
		
		return true;
	}
    
    public static boolean isValidUser(String user) {
    	return USER_PATTERN.matcher(user).matches();
    }

	public static boolean isValidHostname(String value) {
		return isValidDomain(value);
	}

	public static boolean isValidHostnameOnline(String value) {
		try {
			InetAddress.getByName(value);
			return true;
		} catch (UnknownHostException e) {
			return false;
		}
	}

	public static boolean isValidIpV4(String ipv4) {
		return IPAddressUtil.isIPv4LiteralAddress(ipv4);
	}

	public static boolean isValidIpV6(String ipv6) {
		return IPAddressUtil.isIPv6LiteralAddress(ipv6);
	}

	public static boolean isValidUri(String uri) {
		try {
			new URL(uri).toURI();
			return true;
		} catch (Exception e) {
			return false;
		}
	}
}
