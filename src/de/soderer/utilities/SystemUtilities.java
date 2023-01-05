package de.soderer.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.crypto.Cipher;

import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.jarinjarloader.JarInJarLoader;

public class SystemUtilities {
	private static final boolean isWindows = getOsName().toLowerCase().contains("windows");
	private static final boolean isLinux = getOsName().toLowerCase().contains("unix") || getOsName().toLowerCase().contains("linux");

	public static String getOsName() {
		return System.getProperty("os.name");
	}

	public static boolean isWindowsSystem() {
		return isWindows;
	}

	public static boolean isLinuxSystem() {
		return isLinux;
	}

	public static int getProcessId() {
		final String data = ManagementFactory.getRuntimeMXBean().getName();
		if (data.contains("@")) {
			return Integer.parseInt(data.substring(0, data.indexOf("@")));
		} else if (isLinuxSystem()) {
			return Integer.parseInt(data);
		} else {
			return -1;
		}
	}

	public static String getJavaBinPath() {
		final String javaHomePath = System.getProperty("java.home");
		if (javaHomePath == null) {
			return null;
		} else {
			File javaBinFile;
			if (SystemUtilities.isLinuxSystem()) {
				javaBinFile = new File(javaHomePath + File.separator + "bin" + File.separator + "java");
			} else if (System.console() == null) {
				javaBinFile = new File(javaHomePath + File.separator + "bin" + File.separator + "java.exe");
			} else {
				javaBinFile = new File(javaHomePath + File.separator + "bin" + File.separator + "javaw.exe");
			}

			if (javaBinFile.exists() && !javaBinFile.isDirectory()) {
				return javaBinFile.getAbsolutePath();
			} else {
				return null;
			}
		}
	}

	public static long getUptimeInMillis() {
		return ManagementFactory.getRuntimeMXBean().getUptime();
	}

	public static LocalDateTime getStarttime() {
		return DateUtilities.getLocalDateTime(ManagementFactory.getRuntimeMXBean().getStartTime());
	}

	public static List<String> getJavaStartupArguments() {
		return ManagementFactory.getRuntimeMXBean().getInputArguments();
	}

	public static Map<String, String> getRuntimeProperties(final boolean humanReadable) {
		final Runtime runtime = Runtime.getRuntime();
		final Map<String, String> resultMap = new CaseInsensitiveMap<>();
		if (humanReadable) {
			resultMap.put("memory.total", Utilities.getHumanReadableNumber(runtime.totalMemory(), "Byte", false, 5, false, Locale.ENGLISH));
			resultMap.put("memory.free", Utilities.getHumanReadableNumber(runtime.freeMemory(), "Byte", false, 5, false, Locale.ENGLISH));
			resultMap.put("memory.used", Utilities.getHumanReadableNumber(runtime.totalMemory() - runtime.freeMemory(), "Byte", false, 5, false, Locale.ENGLISH));
			resultMap.put("memory.max", Utilities.getHumanReadableNumber(runtime.maxMemory(), "Byte", false, 5, false, Locale.ENGLISH));
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
		resultMap.put("process.jar", getCurrentlyRunningJarFilePath() == null ? "<unknown>" : getCurrentlyRunningJarFilePath());

		resultMap.put("cryptography.unlimitedKeyStrength", isUnlimitedKeyStrengthAllowed() ? "true" : "false");

		return resultMap;
	}

	public static String getCurrentlyRunningJarFilePath() {
		String jarFilePath = System.getProperty(JarInJarLoader.SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR);
		if (jarFilePath == null) {
			jarFilePath = System.getenv(JarInJarLoader.SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR);
		}
		return jarFilePath;
	}

	public static List<String> getProcessListRaw() {
		try {
			final List<String> data = new ArrayList<>();
			Process p;
			if (getOsName().toLowerCase().contains("windows")) {
				p = Runtime.getRuntime().exec(System.getenv("windir") + "\\system32\\" + "tasklist.exe");
			} else {
				p = Runtime.getRuntime().exec("ps -e");
			}
			try (BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
				String line;
				while ((line = input.readLine()) != null) {
					data.add(line);
				}
			}
			return data;
		} catch (final Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static boolean isUnlimitedKeyStrengthAllowed() {
		try {
			return Cipher.getMaxAllowedKeyLength("AES") == Integer.MAX_VALUE;
		} catch (@SuppressWarnings("unused") final NoSuchAlgorithmException e) {
			return false;
		}
	}
}
