package de.soderer.utilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;

import de.soderer.utilities.collection.CaseInsensitiveMap;

public class SystemUtilities {
	public static final String SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR = "process.jar";

	public static String getOsName() {
		return System.getProperty("os.name");
	}

	public static boolean isWindowsSystem() {
		return getOsName().toLowerCase().contains("windows");
	}

	public static boolean isLinuxSystem() {
		return getOsName().toLowerCase().contains("unix") || getOsName().toLowerCase().contains("linux");
	}

	public static int getProcessId() {
		String data = ManagementFactory.getRuntimeMXBean().getName();
		if (data.contains("@")) {
			return Integer.parseInt(data.substring(0, data.indexOf("@")));
		} else if (isLinuxSystem()) {
			return Integer.parseInt(data);
		} else {
			return -1;
		}
	}

	public static long getUptimeInMillis() {
		return ManagementFactory.getRuntimeMXBean().getUptime();
	}

	public static Date getStarttime() {
		return new Date(ManagementFactory.getRuntimeMXBean().getStartTime());
	}

	public static List<String> getJavaStartupArguments() {
		return ManagementFactory.getRuntimeMXBean().getInputArguments();
	}

	public static Map<String, String> getRuntimeProperties(boolean humanReadable) {
		Runtime runtime = Runtime.getRuntime();
		Map<String, String> resultMap = new CaseInsensitiveMap<String>();
		if (humanReadable) {
			resultMap.put("memory.total", Utilities.getHumanReadableNumber(runtime.totalMemory(), "B", false));
			resultMap.put("memory.free", Utilities.getHumanReadableNumber(runtime.freeMemory(), "B", false));
			resultMap.put("memory.used", Utilities.getHumanReadableNumber(runtime.totalMemory() - runtime.freeMemory(), "B", false));
			resultMap.put("memory.max", Utilities.getHumanReadableNumber(runtime.maxMemory(), "B", false));
		} else {
			resultMap.put("memory.total", Long.toString(runtime.totalMemory()));
			resultMap.put("memory.free", Long.toString(runtime.freeMemory()));
			resultMap.put("memory.used", Long.toString(runtime.totalMemory() - runtime.freeMemory()));
			resultMap.put("memory.max", Long.toString(runtime.maxMemory()));
		}
		resultMap.put("processors.available", Long.toString(runtime.availableProcessors()));
		resultMap.put("process.id", Integer.toString(SystemUtilities.getProcessId()));
		resultMap.put("process.uptime", Long.toString(getUptimeInMillis()));
		resultMap.put("process.starttime", getStarttime().toString());
		resultMap.put("process.javastartuparguments", getJavaStartupArguments().toString());
		resultMap.put("process.jar", System.getProperty(SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR) == null ? "<unknown>" : System.getProperty(SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR));

		resultMap.put("cryptography.unlimitedKeyStrength", isunlimitedKeyStrengthAllowed() ? "true" : "false");

		return resultMap;
	}

	public static List<String> getProcessListRaw() {
		try {
			List<String> data = new ArrayList<String>();
			Process p;
			if (getOsName().toLowerCase().contains("windows")) {
				p = Runtime.getRuntime().exec(System.getenv("windir") + "\\system32\\" + "tasklist.exe");
			} else {
				p = Runtime.getRuntime().exec("ps -e");
			}
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = input.readLine()) != null) {
				data.add(line);
				System.out.println(line);
			}
			input.close();
			return data;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static boolean isJavaWebstartApp() {
		return System.getProperty("javawebstart.version") != null;
	}

	public static boolean isunlimitedKeyStrengthAllowed() {
		try {
			return Cipher.getMaxAllowedKeyLength("AES") == Integer.MAX_VALUE;
		} catch (NoSuchAlgorithmException e) {
			return false;
		}
	}
}
