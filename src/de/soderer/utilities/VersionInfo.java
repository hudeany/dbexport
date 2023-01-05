package de.soderer.utilities;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class VersionInfo {
	private static Version APPLICATION_VERSION = null;

	public static Version getApplicationVersion() {
		if (APPLICATION_VERSION == null) {
			APPLICATION_VERSION = Version.getFirstFullVersionNumberFromText(getVersionInfoText());
		}
		return APPLICATION_VERSION;
	}

	public static String getVersionInfoText() {
		try (InputStream inputStream = VersionInfo.class.getClassLoader().getResourceAsStream("VersionInfo.txt")) {
			if (inputStream == null) {
				return getVersionText();
			} else {
				return new String(IoUtilities.toByteArray(inputStream), StandardCharsets.UTF_8);
			}
		} catch (@SuppressWarnings("unused") final Exception e) {
			return "VersionInfo not found";
		}
	}

	private static String getVersionText() {
		try (InputStream inputStream = VersionInfo.class.getClassLoader().getResourceAsStream("version.txt")) {
			return new String(IoUtilities.toByteArray(inputStream), StandardCharsets.UTF_8);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return "VersionInfo not found";
		}
	}
}
