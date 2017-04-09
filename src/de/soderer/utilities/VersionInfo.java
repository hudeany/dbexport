package de.soderer.utilities;

import java.io.InputStream;

public class VersionInfo {
	private static String APPLICATION_VERSION = null;

	public static String getApplicationVersion() {
		if (APPLICATION_VERSION == null) {
			APPLICATION_VERSION = Version.getFirstFullVersionNumberFromText(getVersionInfoText()).toString();
		}
		return APPLICATION_VERSION;
	}

	public static String getVersionInfoText() {
		InputStream inputStream = null;
		try {
			inputStream = VersionInfo.class.getClassLoader().getResourceAsStream("VersionInfo.txt");
			return new String(Utilities.toByteArray(inputStream), "UTF-8");
		} catch (Exception e) {
			return "VersionInfo not found";
		} finally {
			Utilities.closeQuietly(inputStream);
		}
	}
}
