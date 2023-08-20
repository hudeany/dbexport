package de.soderer.utilities.kdbx.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Class Version.
 */
public class Version implements Comparable<Version> {

	/** The versionpattern. */
	private static Pattern VERSIONPATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)$");

	/** The major version number. */
	private int majorVersionNumber;

	/** The minor version number. */
	private int minorVersionNumber;

	/** The micro version number. */
	private int microVersionNumber;

	/**
	 * The Constructor.
	 *
	 * @param majorVersionNumber
	 *            the major version number
	 * @param minorVersionNumber
	 *            the minor version number
	 * @param microVersionNumber
	 *            the micro version number
	 */
	public Version(final int majorVersionNumber, final int minorVersionNumber, final int microVersionNumber) {
		this.majorVersionNumber = majorVersionNumber;
		this.minorVersionNumber = minorVersionNumber;
		this.microVersionNumber = microVersionNumber;
	}

	/**
	 * The Constructor.
	 *
	 * @param versionString
	 *            the version string
	 * @throws Exception
	 *             the exception
	 */
	public Version(final String versionString) throws Exception {
		final Matcher matcher = VERSIONPATTERN.matcher(versionString);
		if (matcher.find()) {
			majorVersionNumber = Integer.parseInt(matcher.group(1));
			minorVersionNumber = Integer.parseInt(matcher.group(2));
			microVersionNumber = Integer.parseInt(matcher.group(3));
		} else {
			throw new Exception("Invalid version number");
		}
	}

	/**
	 * Gets the major version number.
	 *
	 * @return the major version number
	 */
	public int getMajorVersionNumber() {
		return majorVersionNumber;
	}

	/**
	 * Gets the minor version number.
	 *
	 * @return the minor version number
	 */
	public int getMinorVersionNumber() {
		return minorVersionNumber;
	}

	/**
	 * Gets the micro version number.
	 *
	 * @return the micro version number
	 */
	public int getMicroVersionNumber() {
		return microVersionNumber;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return new StringBuilder().append(majorVersionNumber).append(".").append(minorVersionNumber).append(".").append(microVersionNumber).toString();
	}

	/**
	 * Gets the first full version from a text.
	 *
	 * @param data
	 *            the data
	 * @return the first full version from a text
	 */
	public static Version getFirstFullVersionNumberFromText(final String data) {
		if (Utilities.isBlank(data)) {
			return null;
		} else {
			final Pattern versionPattern = Pattern.compile("\\d+\\.\\d+\\.\\d+");
			final Matcher matcher = versionPattern.matcher(data);
			if (matcher.find()) {
				try {
					return new Version(matcher.group());
				} catch (@SuppressWarnings("unused") final Exception e) {
					return null;
				}
			} else {
				return null;
			}
		}
	}

	/**
	 * @returns +1: thisVersion > otherVersion<br />
	 *          0: thisVersion = otherVersion<br />
	 *          -1: thisVersion < otherVersion
	 */
	@Override
	public int compareTo(final Version otherVersion) {
		if (otherVersion == null || majorVersionNumber > otherVersion.getMajorVersionNumber()) {
			return 1;
		} else if (majorVersionNumber == otherVersion.getMajorVersionNumber()) {
			if (minorVersionNumber > otherVersion.getMinorVersionNumber()) {
				return 1;
			} else if (minorVersionNumber == otherVersion.getMinorVersionNumber()) {
				if (microVersionNumber > otherVersion.getMicroVersionNumber()) {
					return 1;
				} else if (microVersionNumber == otherVersion.getMicroVersionNumber()) {
					return 0;
				} else {
					return -1;
				}
			} else {
				return -1;
			}
		} else {
			return -1;
		}
	}
}
