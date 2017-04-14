package de.soderer.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import de.soderer.utilities.UserError.Reason;
import de.soderer.utilities.xml.XmlUtilities;

public class ApplicationUpdateHelper implements WorkerParentSimple {
	private String applicationName;
	private Version applicationVersion;
	private String versionIndexFileLocation;
	private UpdateParent updateParent;
	private FileDownloadWorker fileDownloadWorker;
	private String updateFileLocation;
	private String updateFileMd5Checksum = null;
	private String restartParameter;
	private String trustedCaCertificateFileName = null;
	private String username;
	private char[] password;

	/**
	 * @param versionIndexFileLocation
	 *            may include placeholders:<br />
	 *            &lt;system> : replaced by "windows" or "linux"<br />
	 *            &lt;bitmode> : replaced by "32" or "64"<br />
	 *            &lt;username> : interactive username dialog<br />
	 *            &lt;password> : interactive password dialog
	 *            &lt;time_seconds> : current timestamp in seconds (used by sourceforge.net)
	 * @throws Exception
	 */
	public ApplicationUpdateHelper(String applicationName, String applicationVersion, String versionIndexFileLocation, UpdateParent updateParent, String restartParameter, String trustedCaCertificateFileName) throws Exception {
		if (Utilities.isEmpty(versionIndexFileLocation)) {
			throw new Exception("Invalid version index location");
		}

		this.applicationName = applicationName;
		this.applicationVersion = new Version(applicationVersion);
		this.versionIndexFileLocation = replacePlaceholders(versionIndexFileLocation);
		this.updateParent = updateParent;
		this.restartParameter = restartParameter;
		this.trustedCaCertificateFileName = trustedCaCertificateFileName;
	}

	public void executeUpdate() {
		String newVersionAvailable = checkForNewVersionAvailable();
		if (askForUpdate(newVersionAvailable)) {
			updateApplication();
		}
	}

	private String checkForNewVersionAvailable() {
		try {
			if (!NetworkUtilities.checkForNetworkConnection()) {
				throw new Exception("error.missingNetworkConnection");
			} else if (!NetworkUtilities.ping(versionIndexFileLocation)) {
				throw new Exception("error.missingInternetConnection");
			}

			Document versionsDocument = null;
			if (versionIndexFileLocation.toLowerCase().startsWith("http")) {
				versionsDocument = XmlUtilities.downloadAndParseXmlFile(versionIndexFileLocation);
			} else if (new File(versionIndexFileLocation).exists()) {
				versionsDocument = XmlUtilities.parseXmlFile(new File(versionIndexFileLocation));
			}

			if (versionsDocument == null) {
				throw new Exception("Version index not found at location '" + versionIndexFileLocation + "'");
			}

			Node startFileNameNode = XmlUtilities.getSingleXPathNode(versionsDocument, "ApplicationVersions/" + applicationName);
			if (startFileNameNode == null) {
				throw new Exception("error.cannotFindUpdateVersionData");
			}
			Node versionNode = startFileNameNode.getAttributes().getNamedItem("version");
			String version = versionNode.getNodeValue();

			Node md5ChecksumNode = startFileNameNode.getAttributes().getNamedItem("md5CheckSum");
			if (md5ChecksumNode != null) {
				updateFileMd5Checksum = md5ChecksumNode.getNodeValue();
			}

			updateFileLocation = startFileNameNode.getFirstChild().getNodeValue();

			Version availableVersion = new Version(version);
			if (applicationVersion.compareTo(availableVersion) < 0 && updateFileLocation != null && updateFileLocation.trim().length() > 0) {
				updateFileLocation = replacePlaceholders(updateFileLocation);
				return version;
			} else {
				updateFileLocation = null;
				return null;
			}
		} catch (Exception e) {
			showUpdateError("Update error while checking for new version:\n" + e.getMessage());
			return null;
		}
	}

	private boolean askForUpdate(String availableNewVersion) {
		if (updateParent != null) {
			try {
				return updateParent.askForUpdate(availableNewVersion);
			} catch (Exception e) {
				updateParent.showUpdateError("Update error :\n" + e.getMessage());
				return false;
			}
		} else {
			return false;
		}
	}

	private void updateApplication() {
		try {
			String jarFilePath = System.getProperty(SystemUtilities.SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR);

			if (jarFilePath == null) {
				jarFilePath = System.getenv(SystemUtilities.SYSTEM_PARAMETER_NAME_CURRENT_RUNNING_JAR);
			}

			if (jarFilePath == null) {
				try {
					String currentJarUrlPath = getClass().getResource(getClass().getSimpleName() + ".class").toString();
					jarFilePath = currentJarUrlPath.substring(0, currentJarUrlPath.lastIndexOf("!")).replaceFirst("jar:file:", "");
				} catch (Exception e) {
					jarFilePath = null;
				}
			}

			jarFilePath = Utilities.replaceHomeTilde(jarFilePath);

			if (jarFilePath == null || !new File(jarFilePath).exists()) {
				throw new Exception("Current running jar file was not found");
			}

			File downloadTempFile = new File(new File(jarFilePath).getParent() + File.separator + "temp_" + new File(jarFilePath).getName());

			boolean downloadSuccess = getNewApplicationVersionFile(downloadTempFile);
			if (downloadSuccess) {
				if (Utilities.isNotEmpty(trustedCaCertificateFileName)) {
					showUpdateError("Update error:\n" + "CA certificate check not available");
					return;
				} else if (Utilities.isNotEmpty(updateFileMd5Checksum) && !updateFileMd5Checksum.equalsIgnoreCase("NONE")) {
					String downloadTempFileMd5Checksum = createMd5Checksum(downloadTempFile);
					if (!updateFileMd5Checksum.equalsIgnoreCase(downloadTempFileMd5Checksum)) {
						showUpdateError("Update error:\n" + "MD5-Checksum of updatefile is invalid (expected: " + updateFileMd5Checksum + ", actual: " + downloadTempFileMd5Checksum + ")");
						return;
					}
				}
	
				String restartCommand = createUpdateBatchFile(jarFilePath, downloadTempFile);
				
				if (updateParent != null) {
					updateParent.showUpdateDone();
				}
				
				if (restartCommand != null) {
					Runtime.getRuntime().exec(restartCommand);
					Runtime.getRuntime().exit(0);
				}
			}
		} catch (Exception e) {
			showUpdateError("Update error while updating to new version:\n" + e.getMessage());
			return;
		}
	}

	private boolean getNewApplicationVersionFile(File downloadTempFile) throws Exception {
		if (updateFileLocation.toLowerCase().startsWith("http")) {
			// Download file
			boolean retryDownload = true;
			while (retryDownload) {
				String downloadUrlWithCredentials = updateFileLocation;
				
				if (username != null) {
					downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<username>", username);
					// Only use preconfigured username in first try
					username = null;
				}
				if (password != null) {
					downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<password>", new String(password));
					// Only use preconfigured password in first try
					password = null;
				}
				
				if (downloadUrlWithCredentials.contains("<username>") && downloadUrlWithCredentials.contains("<password>")) {
					if (updateParent != null) {
						Credentials credentials = updateParent.aquireCredentials("Please enter update credentials", true, true);
						if (credentials == null || Utilities.isEmpty(credentials.getUsername()) || Utilities.isEmpty(credentials.getPassword())) {
							updateParent.showUpdateError("Update error:\nusername and password required");
							return false;
						} else {
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<username>", credentials.getUsername());
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<password>", new String(credentials.getPassword()));
						}
					} else {
						showUpdateError("Update error:\n" + "username required");
						return false;
					}
				} else if (downloadUrlWithCredentials.contains("<username>")) {
					if (updateParent != null) {
						Credentials credentials = updateParent.aquireCredentials("Please enter update credentials", true, false);
						if (credentials == null || Utilities.isEmpty(credentials.getUsername())) {
							updateParent.showUpdateError("Update error:\nusername required");
							return false;
						} else {
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<username>", credentials.getUsername());
						}
					} else {
						showUpdateError("Update error:\n" + "username required");
						return false;
					}
				} else if (downloadUrlWithCredentials.contains("<password>")) {
					if (updateParent != null) {
						Credentials credentials = updateParent.aquireCredentials("Please enter update credentials", false, true);
						if (credentials == null || Utilities.isEmpty(credentials.getPassword())) {
							updateParent.showUpdateError("Update error:\npassword required");
							return false;
						} else {
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<password>", new String(credentials.getPassword()));
						}
					} else {
						showUpdateError("Update error:\n" + "password required");
						return false;
					}
				}

				retryDownload = false;
				try {
					if (downloadTempFile.exists()) {
						downloadTempFile.delete();
					}
					boolean success = downloadUpdateFile(downloadTempFile, downloadUrlWithCredentials);
					if (!success) {
						// download stopped by user
						updateParent.showUpdateError("Canceled by user");
						return false;
					}
				} catch (UserError e) {
					if (updateParent != null) {
						updateParent.showUpdateError("Update error:\n" + e.getMessage());

						if (e.getReason() == Reason.UnauthenticatedOrUnauthorized) {
							if (updateFileLocation.contains("<username>") || updateFileLocation.contains("<password>")) {
								retryDownload = true;
							} else {
								System.err.println("Update error:\nAuthentication error");
								return false;
							}
						} else {
							return false;
						}
					} else {
						System.err.println("Update error while downloading new version:\n" + e.getMessage());
						return false;
					}
				} catch (Exception e) {
					if (e instanceof ExecutionException && e.getCause() instanceof UserError) {
						if (updateParent != null) {
							updateParent.showUpdateError("Update error:\n" + ((UserError) e.getCause()).getMessage());

							if (((UserError) e.getCause()).getReason() == Reason.UnauthenticatedOrUnauthorized) {
								if (updateFileLocation.contains("<username>") || updateFileLocation.contains("<password>")) {
									retryDownload = true;
								} else {
									System.err.println("Update error:\nAuthentication error");
									return false;
								}
							} else {
								return false;
							}
						} else {
							System.err.println("Update error while downloading new version:\n" + ((UserError) e.getCause()).getMessage());
							return false;
						}
					} else {
						showUpdateError("Update error:\n" + e.getMessage());
						return false;
					}
				}
			}
		} else {
			// Copy file
			Files.copy(new File(updateFileLocation).toPath(), downloadTempFile.toPath());
		}
		
		return downloadTempFile.exists();
	}

	private String createUpdateBatchFile(String jarFilePath, File downloadTempFile) throws Exception {
		if (SystemUtilities.isWindowsSystem()) {
			File batchFile = new File(new File(jarFilePath).getParent() + File.separator + "batchUpdate_" + new File(jarFilePath).getName() + ".cmd");
			writeFile(batchFile,
				"@echo off\r\n"
				+ "del \"" + jarFilePath + "\"\r\n"
				+ "if exist \"" + jarFilePath + "\" (\r\n"
					+ "ping -n 3 127.0.0.1 >nul\r\n"
					+ "del \"" + jarFilePath + "\"\r\n"
					+ "if exist \"" + jarFilePath + "\" (\r\n"
						+ "ping -n 3 127.0.0.1 >nul\r\n"
						+ "del \"" + jarFilePath + "\"\r\n"
						+ "if exist \"" + jarFilePath + "\" (\r\n"
							+ "ping -n 3 127.0.0.1 >nul\r\n"
							+ "del \"" + jarFilePath + "\"\r\n"
						+ ")\r\n"
					+ ")\r\n"
				+ ")\r\n"
				+ "ren \"" + downloadTempFile.getAbsolutePath() + "\" \"" + new File(jarFilePath).getName() + "\"\r\n"
				+ "if not exist \"" + downloadTempFile.getAbsolutePath() + "\" (\r\n"
					+ "java -jar \"" + jarFilePath + "\"" + (restartParameter != null ? " " + restartParameter : "") + "\r\n"
					+ "del \"" + batchFile.getAbsolutePath() + "\"\r\n"
				+ ")\r\n");

			return "cmd /c start /B " + batchFile.getAbsolutePath();
		} else if (SystemUtilities.isLinuxSystem()) {
			File batchFile = new File(new File(jarFilePath).getParent() + File.separator + "batchUpdate_" + new File(jarFilePath).getName() + ".sh");
			writeFile(batchFile,
				"#!/bin/bash\n"
				+ "rm \"" + jarFilePath + "\"\n"
				+ "if [ -f \"" + jarFilePath + "\" ]; then\n"
					+ "sleep 3\n"
					+ "rm \"" + jarFilePath + "\"\n"
					+ "if [ -f \"" + jarFilePath + "\" ]; then\n"
						+ "sleep 3\n"
						+ "rm \"" + jarFilePath + "\"\n"
						+ "if [ -f \"" + jarFilePath + "\" ]; then\n"
							+ "sleep 3\n"
							+ "rm \"" + jarFilePath + "\"\n"
						+ "fi\n"
					+ "fi\n"
				+ "fi\n"
				+ "mv \"" + downloadTempFile.getAbsolutePath() + "\" \"" + jarFilePath + "\"\n"
				+ "if ! [ -f \"" + downloadTempFile.getAbsolutePath() + "\" ]; then\n"
					+ "java -jar \"" + jarFilePath + "\"" + (restartParameter != null ? " " + restartParameter : "") + "\n"
					+ "rm \"" + batchFile.getAbsolutePath() + "\"\n"
				+ "fi\n");
				
			return "sh " + batchFile.getAbsolutePath();
		} else {
			return null;
		}
	}

	private String replacePlaceholders(String value) {
		if (value == null) {
			return null;
		} else {
			if (value.contains("<system>")) {
				if (SystemUtilities.isWindowsSystem()) {
					value = value.replace("<system>", "windows");
				} else if (SystemUtilities.isLinuxSystem()) {
					value = value.replace("<system>", "linux");
				}
			}

			if (value.contains("<bitmode>")) {
				if (System.getProperty("os.arch") != null && System.getProperty("os.arch").contains("64")) {
					value = value.replace("<bitmode>", "64");
				} else if (SystemUtilities.isLinuxSystem()) {
					value = value.replace("<bitmode>", "32");
				}
			}
			
			if (value.contains("<time_seconds>")) {
				value = value.replace("<time_seconds>", "" + (new Date().getTime() / 1000));
			}

			return value;
		}
	}

	private void writeFile(File file, String string) throws Exception {
		try (FileOutputStream out = new FileOutputStream(file)) {
			out.write(string.getBytes("UTF-8"));
		} catch (Exception e) {
			throw new Exception("Cannot write file " + file.getAbsolutePath(), e);
		}
	}

	private boolean downloadUpdateFile(File downloadTempFile, String downloadUrl) throws Exception {
		try {
			fileDownloadWorker = new FileDownloadWorker(this, downloadUrl, downloadTempFile);
			new Thread(fileDownloadWorker).start();

			if (updateParent != null) {
				updateParent.showUpdateDownloadStart();
			}
			
			while (!fileDownloadWorker.isDone()) {
				// Wait for download process
				Thread.sleep(1000);
			}

			if (!fileDownloadWorker.get()) {
				throw new Exception("Download was not successful");
			}

			return !fileDownloadWorker.isCancelled();
		} catch (Exception e) {
			if (e instanceof ExecutionException && e.getCause() instanceof UserError) {
				throw (UserError) e.getCause();
			} else {
				throw e;
			}
		}
	}

	private String createMd5Checksum(File file) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			try (InputStream inputStream = new FileInputStream(file)) {
				byte[] buffer = new byte[4096];
				int bytesRead;
				while ((bytesRead = inputStream.read(buffer)) >= 0) {
					messageDigest.update(buffer, 0, bytesRead);
				}
			}
			return bytesToHexString(messageDigest.digest());
		} catch (Exception e) {
			return null;
		}
	}

	private static String bytesToHexString(byte[] bytes) {
		char[] hexArray = "0123456789ABCDEF".toCharArray();

		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	@Override
	public void showUnlimitedProgress() {
		// Do nothing
	}

	@Override
	public void showProgress(Date start, long itemsToDo, long itemsDone) {
		if (updateParent != null) {
			updateParent.showUpdateProgress(start, itemsToDo, itemsDone);
		}
	}

	@Override
	public void showDone(Date start, Date end, long itemsDone) {
		if (updateParent != null) {
			updateParent.showUpdateDownloadEnd();
		}
	}

	@Override
	public void cancel() {
		// Do nothing
	}
	
	private void showUpdateError(String errorMessage) {
		if (updateParent != null) {
			updateParent.showUpdateError(errorMessage);
		} else {
			// Linebreak to end progress display
			System.err.println("");
			System.err.println(errorMessage);
		}
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	@Override
	public void changeTitle(String text) {
		// do nothing	
	}
}
