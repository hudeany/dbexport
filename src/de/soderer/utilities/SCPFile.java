package de.soderer.utilities;

public class SCPFile {
	public static final String PRELUDE = "\\\\";
	private static final String UNIX_SEPARATOR = "/";
	private static final String DOS_SEPARATOR = "\\";

	private String hostname = null;
	private String path = null; // always ends with a Separator
	private String filename = null; // also allows wildcards like '*'

	/**
	 * This class encapsulates file descriptors and shares their parts. It works explicitly with non system file separators, which are not changed while working with them. Class java.io.File would
	 * change them.
	 *
	 * Allowed formats: "\\hostname\path1\path2\filename" => DOS-file "\\hostname/path1/path2/filename" => Unix-file "\\hostname/path1/path2/" => directory "\\hostname/filename" => root-directory-file
	 *
	 * Filename also allows wildcards like '*'
	 *
	 * @param hostnamePathAndFileName
	 * @throws Exception
	 */
	public SCPFile(String hostnamePathAndFileName) throws Exception {
		if (Utilities.isEmpty(hostnamePathAndFileName)) {
			throw new Exception("SCPFile: Empty filespecifier");
		}
		if (!hostnamePathAndFileName.startsWith(PRELUDE)) {
			throw new Exception("SCPFile: Invalid prelude in filespecifier: " + hostnamePathAndFileName);
		}

		int firstIndexOfUnixPathSeparator = hostnamePathAndFileName.indexOf(UNIX_SEPARATOR, PRELUDE.length());
		int firstIndexOfWindowsPathSeparator = hostnamePathAndFileName.indexOf(DOS_SEPARATOR, PRELUDE.length());
		int firstIndexOfPathSeparator = -1;
		if (firstIndexOfUnixPathSeparator != -1 && firstIndexOfWindowsPathSeparator != -1) {
			firstIndexOfPathSeparator = Math.min(firstIndexOfUnixPathSeparator, firstIndexOfWindowsPathSeparator);
		} else if (firstIndexOfUnixPathSeparator != -1) {
			firstIndexOfPathSeparator = firstIndexOfUnixPathSeparator;
		} else {
			firstIndexOfPathSeparator = firstIndexOfWindowsPathSeparator;
		}

		int lastIndexOfPathSeparator = Math.max(hostnamePathAndFileName.lastIndexOf(UNIX_SEPARATOR), hostnamePathAndFileName.lastIndexOf(DOS_SEPARATOR));
		if (lastIndexOfPathSeparator < PRELUDE.length()) {
			lastIndexOfPathSeparator = -1;
		}

		if (firstIndexOfPathSeparator > -1 && lastIndexOfPathSeparator > -1) {
			hostname = hostnamePathAndFileName.substring(PRELUDE.length(), firstIndexOfPathSeparator);
			path = hostnamePathAndFileName.substring(firstIndexOfPathSeparator, lastIndexOfPathSeparator + 1);
			filename = hostnamePathAndFileName.substring(lastIndexOfPathSeparator + 1);
		} else {
			throw new Exception("SCPFile: Missing path- or filedescription in filespecifier: " + hostnamePathAndFileName);
		}

		if (Utilities.isEmpty(hostname)) {
			throw new Exception("SCPFile: Empty hostname: " + hostnamePathAndFileName);
		}
	}

	public SCPFile(String hostname, String path, String filename) throws Exception {
		if (Utilities.isEmpty(hostname)) {
			throw new Exception("SCPFile: Empty hostname");
		}
		if (Utilities.isEmpty(path)) {
			throw new Exception("SCPFile: Empty path");
		}
		if (filename != null && (filename.contains(UNIX_SEPARATOR) || filename.contains(DOS_SEPARATOR))) {
			throw new Exception("SCPFile: Invalid filename: " + filename);
		}

		this.hostname = hostname;
		setPath(path);
		this.filename = filename;
	}

	public SCPFile(String hostname, String path) throws Exception {
		this(hostname, path, null);
	}

	public String getHostname() {
		return hostname;
	}

	public SCPFile setHostname(String hostname) {
		this.hostname = hostname;
		return this;
	}

	public String getPath() {
		return path;
	}

	public SCPFile setPath(String path) {
		if (path.endsWith(DOS_SEPARATOR) || path.endsWith(UNIX_SEPARATOR)) {
			this.path = path;
		} else if (path.contains(DOS_SEPARATOR)) {
			this.path = path + DOS_SEPARATOR;
		} else {
			this.path = path + UNIX_SEPARATOR;
		}

		return this;
	}

	/**
	 * Filename also allows wildcards like '*'
	 *
	 * @return
	 */
	public String getFilename() {
		return filename;
	}

	@Override
	public String toString() {
		if (Utilities.isEmpty(filename)) {
			return PRELUDE + hostname + path;
		} else {
			return PRELUDE + hostname + path + filename;
		}
	}

	public String getPathAndFilename() {
		if (Utilities.isEmpty(filename)) {
			return path;
		} else {
			return path + filename;
		}
	}

	/**
	 * Also allows wildcards like '*' Put value to null => Directory of original file Put value from null to "test" => new File in original directory
	 *
	 * @param filename
	 * @throws Exception
	 */
	public SCPFile setFilename(String filename) throws Exception {
		if (filename != null && (filename.contains(UNIX_SEPARATOR) || filename.contains(DOS_SEPARATOR))) {
			throw new Exception("SCPFile: Invalid new filename: " + filename);
		}
		this.filename = filename;
		return this;
	}

	public SCPFile getDirectory() throws Exception {
		return new SCPFile(hostname, path);
	}

	public boolean isDirectory() {
		return Utilities.isEmpty(filename);
	}

	public boolean isFile() {
		return !isDirectory();
	}

	public SCPFile getParent() throws Exception {
		if (isDirectory()) {
			String newPath = path.substring(0, path.length() - 1);
			if (newPath.length() == 0) {
				throw new Exception("SCPFile: No parentpath available for: " + path);
			}
			int lastIndexOfPathSeparator = Math.max(newPath.lastIndexOf(UNIX_SEPARATOR), newPath.lastIndexOf(DOS_SEPARATOR));
			return new SCPFile(hostname, newPath.substring(0, lastIndexOfPathSeparator + 1));
		} else {
			return new SCPFile(hostname, path); // ohne Dateiname => Verzeichnis
		}
	}
}
