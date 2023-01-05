package de.soderer.utilities.appupdate;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.cert.CertPathBuilder;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXCertPathBuilderResult;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import de.soderer.utilities.Credentials;
import de.soderer.utilities.FileDownloadWorker;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.NetworkUtilities;
import de.soderer.utilities.SystemUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.UserError;
import de.soderer.utilities.UserError.Reason;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.http.HttpUtilities;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.xml.XmlUtilities;

/**
 * versionIndexFileLocation
 * 	may include placeholders:<br />
 * 		&lt;system> : replaced by "windows" or "linux"<br />
 *  	&lt;bitmode> : replaced by "32" or "64"<br />
 * 		&lt;username> : interactive username dialog<br />
 * 		&lt;password> : interactive password dialog<br />
 * 		&lt;time_seconds> : current timestamp in seconds (used by sourceforge.net)
 */
public class ApplicationUpdateUtilities {
	public static final String CONFIG_DAILY_UPDATE_CHECK = "Application.DailyUpdateCheck";
	public static final String CONFIG_NEXT_DAILY_UPDATE_CHECK = "NextDailyUpdateCheck";

	public static void executeUpdate(final ApplicationUpdateParent updateParent, final String versionIndexFileLocation, final String applicationName, final Version applicationVersion, final String trustedCaCertificateFileName, final String username, final char[] password, final String restartParameter, final boolean restartAfterUpdate) throws Exception {
		try {
			if (Utilities.isEmpty(versionIndexFileLocation)) {
				throw new Exception("Invalid version index location");
			}

			final Tuple<Version, String> newVersionAvailable = checkForNewVersionAvailable(updateParent, versionIndexFileLocation, applicationName, applicationVersion);
			if (askForUpdate(updateParent, newVersionAvailable != null ? newVersionAvailable.getFirst() : null)) {
				updateApplication(updateParent, newVersionAvailable != null ? newVersionAvailable.getSecond() : null, trustedCaCertificateFileName, username, password, restartParameter, restartAfterUpdate);
			}
		} catch (final Exception e) {
			if (updateParent != null) {
				showUpdateError(updateParent, "Update error while executing update:\n" + e.getMessage());
			} else {
				throw e;
			}
		}
	}

	public static Tuple<Version, String> checkForNewVersionAvailable(final ApplicationUpdateParent updateParent, final String versionIndexFileLocation, final String applicationName, final Version applicationVersion) throws Exception {
		try {
			if (Utilities.isEmpty(versionIndexFileLocation)) {
				throw new Exception("Invalid version index location");
			} else if (!NetworkUtilities.checkForNetworkConnection()) {
				throw new Exception("error.missingNetworkConnection");
			} else if (!NetworkUtilities.ping(versionIndexFileLocation)) {
				throw new Exception("error.missingInternetConnection");
			}

			Version availableVersion = null;
			String updateFileLocation = null;

			if (versionIndexFileLocation.toLowerCase().contains("json")) {
				try (JsonReader jsonReader = new JsonReader(new BufferedInputStream(new URL(versionIndexFileLocation).openStream()))) {
					final JsonNode jsonNode = jsonReader.read();
					if (!jsonNode.isJsonObject()) {
						throw new Exception("Invalid version index found at location '" + versionIndexFileLocation + "'");
					} else {
						final JsonObject jsonObject = (JsonObject) jsonNode.getValue();
						final JsonObject applicationJsonObject = (JsonObject) jsonObject.get(applicationName);
						if (applicationJsonObject == null) {
							throw new Exception("Version index not found at location '" + versionIndexFileLocation + "'");
						} else {
							try {
								availableVersion = new Version((String) applicationJsonObject.get("version"));
								updateFileLocation = (String) applicationJsonObject.get("downloadUrl");
							} catch (final Exception e) {
								throw new Exception("Invalid application version entry at location '" + versionIndexFileLocation + "'", e);
							}
						}
					}
				} catch (final Exception e) {
					throw new Exception("Invalid version index found at location '" + versionIndexFileLocation + "':\n" + e.getMessage(), e);
				}
			} else {
				Document versionsDocument = null;
				if (versionIndexFileLocation.toLowerCase().startsWith("http")) {
					versionsDocument = XmlUtilities.downloadAndParseXmlFile(versionIndexFileLocation);
				} else if (new File(versionIndexFileLocation).exists()) {
					versionsDocument = XmlUtilities.parseXmlFile(new File(versionIndexFileLocation));
				}

				if (versionsDocument == null) {
					throw new Exception("Version index not found at location '" + versionIndexFileLocation + "'");
				}

				final Node startFileNameNode = XmlUtilities.getSingleXPathNode(versionsDocument, "ApplicationVersions/" + applicationName);
				if (startFileNameNode == null) {
					throw new Exception("error.cannotFindUpdateVersionData");
				}

				final Node versionNode = startFileNameNode.getAttributes().getNamedItem("version");
				if (versionNode != null && Utilities.isNotBlank(versionNode.getNodeValue())) {
					availableVersion = new Version(versionNode.getNodeValue());
				}

				if (startFileNameNode.getFirstChild() != null && Utilities.isNotBlank(startFileNameNode.getFirstChild().getNodeValue())) {
					updateFileLocation = startFileNameNode.getFirstChild().getNodeValue();
				}
			}

			if (applicationVersion.compareTo(availableVersion) < 0 && updateFileLocation != null && updateFileLocation.trim().length() > 0) {
				updateFileLocation = replacePlaceholders(updateFileLocation);
				return new Tuple<>(availableVersion, updateFileLocation);
			} else {
				updateFileLocation = null;
				return null;
			}
		} catch (final Exception e) {
			if (updateParent != null) {
				showUpdateError(updateParent, "Update error while checking for new version:\n" + e.getMessage());
				return null;
			} else {
				throw e;
			}
		}
	}

	private static boolean askForUpdate(final ApplicationUpdateParent updateParent, final Version availableNewVersion) {
		if (updateParent != null) {
			try {
				return updateParent.askForUpdate(availableNewVersion);
			} catch (final Exception e) {
				updateParent.showUpdateError("Update error:\n" + e.getMessage());
				return false;
			}
		} else {
			return false;
		}
	}

	private static void updateApplication(final ApplicationUpdateParent updateParent, final String updateFileLocation, final String trustedCaCertificateFileName, final String username, final char[] password, final String restartParameter, final boolean restartAfterUpdate) {
		try {
			if (Utilities.isBlank(updateFileLocation)) {
				throw new Exception("Mandatory updateFileLocation is empty");
			}

			String jarFilePath = SystemUtilities.getCurrentlyRunningJarFilePath();

			if (jarFilePath == null) {
				try {
					final String currentJarUrlPath = updateParent.getClass().getResource(updateParent.getClass().getSimpleName() + ".class").toString();
					jarFilePath = currentJarUrlPath.substring(0, currentJarUrlPath.lastIndexOf("!")).replaceFirst("jar:file:", "");
				} catch (@SuppressWarnings("unused") final Exception e) {
					jarFilePath = null;
				}
			}

			jarFilePath = Utilities.replaceUsersHome(jarFilePath);

			if (jarFilePath == null || !new File(jarFilePath).exists()) {
				throw new Exception("Current running jar file was not found");
			}

			final File downloadTempFile = new File(new File(jarFilePath).getParent() + File.separator + "temp_" + new File(jarFilePath).getName());

			final boolean downloadSuccess = getNewApplicationVersionFile(updateParent, username, password, updateFileLocation, downloadTempFile);
			if (downloadSuccess) {
				if (Utilities.isNotEmpty(trustedCaCertificateFileName)) {
					Collection<? extends Certificate> trustedCertificates = null;
					final ClassLoader applicationClassLoader = updateParent.getClass().getClassLoader();
					if (applicationClassLoader == null) {
						showUpdateError(updateParent, "Update error:\nApplications classloader is not readable");
						return;
					}
					try (InputStream trustedUpdateCertificatesStream = applicationClassLoader.getResourceAsStream(trustedCaCertificateFileName)) {
						trustedCertificates = CertificateFactory.getInstance("X.509").generateCertificates(new BufferedInputStream(trustedUpdateCertificatesStream));
					} catch (final Exception e) {
						showUpdateError(updateParent, "Update error:\nTrusted CA certificate '" + trustedCaCertificateFileName + "' is not readable: " + e.getMessage());
						return;
					}
					if (trustedCertificates == null || trustedCertificates.size() == 0) {
						showUpdateError(updateParent, "Update error:\nTrusted CA certificate is missing");
						return;
					} else if (!verifyJarSignature(downloadTempFile, trustedCertificates)) {
						showUpdateError(updateParent, "Update error:\nSignature of updatefile is invalid");
						return;
					}
				} else {
					showUpdateError(updateParent, "Update error:\nTrusted CA certificate file name is missing");
				}

				if (updateParent != null) {
					updateParent.showUpdateDone(null, null, 0);
				}

				final String restartCommand = createUpdateBatchFile(jarFilePath, downloadTempFile, restartAfterUpdate, restartParameter);
				if (restartCommand != null) {
					Runtime.getRuntime().exec(restartCommand);
					Runtime.getRuntime().exit(0);
				}
			}
		} catch (final Exception e) {
			showUpdateError(updateParent, "Update error while updating to new version:\n" + e.getMessage());
			return;
		}
	}

	private static boolean getNewApplicationVersionFile(final ApplicationUpdateParent updateParent, final String username, final char[] password, final String updateFileLocation, final File downloadTempFile) throws Exception {
		if (updateFileLocation.toLowerCase().startsWith("http")) {
			// Download file
			boolean retryDownload = true;
			boolean firstRequest = true;
			while (retryDownload) {
				String downloadUrlWithCredentials = updateFileLocation;

				// Only use preconfigured credentials in first try only
				if (firstRequest) {
					if (username != null) {
						downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<username>", HttpUtilities.urlEncode(username, StandardCharsets.UTF_8));
					}
					if (password != null) {
						downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<password>", HttpUtilities.urlEncode(new String(password), StandardCharsets.UTF_8));
					}
				}

				if (downloadUrlWithCredentials.contains("<username>") && downloadUrlWithCredentials.contains("<password>")) {
					if (updateParent != null) {
						final Credentials credentials = updateParent.aquireCredentials(getI18NString("enterDownloadCredentials"), true, true, firstRequest);
						if (credentials == null) {
							updateParent.showUpdateError(getI18NString("canceledByUser"));
							return false;
						} else if (Utilities.isEmpty(credentials.getUsername()) || Utilities.isEmpty(credentials.getPassword())) {
							updateParent.showUpdateError(getI18NString("usernameAndPasswordRequired"));
							return false;
						} else {
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<username>", HttpUtilities.urlEncode(credentials.getUsername(), StandardCharsets.UTF_8));
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<password>", HttpUtilities.urlEncode(new String(credentials.getPassword()), StandardCharsets.UTF_8));
						}
					} else {
						showUpdateError(updateParent, getI18NString("usernameAndPasswordRequired"));
						return false;
					}
				} else if (downloadUrlWithCredentials.contains("<username>")) {
					if (updateParent != null) {
						final Credentials credentials = updateParent.aquireCredentials(getI18NString("enterDownloadUserName"), true, false, firstRequest);
						if (credentials == null) {
							updateParent.showUpdateError(getI18NString("canceledByUser"));
							return false;
						} else if (Utilities.isEmpty(credentials.getUsername())) {
							updateParent.showUpdateError(getI18NString("usernameRequired"));
							return false;
						} else {
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<username>", HttpUtilities.urlEncode(credentials.getUsername(), StandardCharsets.UTF_8));
						}
					} else {
						showUpdateError(updateParent, getI18NString("usernameRequired"));
						return false;
					}
				} else if (downloadUrlWithCredentials.contains("<password>")) {
					if (updateParent != null) {
						final Credentials credentials = updateParent.aquireCredentials(getI18NString("enterDownloadPassword"), false, true, firstRequest);
						if (credentials == null) {
							updateParent.showUpdateError(getI18NString("canceledByUser"));
							return false;
						} else if (Utilities.isEmpty(credentials.getPassword())) {
							updateParent.showUpdateError(getI18NString("passwordRequired"));
							return false;
						} else {
							downloadUrlWithCredentials = downloadUrlWithCredentials.replace("<password>", HttpUtilities.urlEncode(new String(credentials.getPassword()), StandardCharsets.UTF_8));
						}
					} else {
						showUpdateError(updateParent, getI18NString("passwordRequired"));
						return false;
					}
				}

				retryDownload = false;
				try {
					if (downloadTempFile.exists()) {
						downloadTempFile.delete();
					}
					final boolean success = downloadUpdateFile(updateParent, downloadTempFile, downloadUrlWithCredentials);
					if (!success) {
						// download stopped by user
						updateParent.showUpdateError(getI18NString("canceledByUser"));
						return false;
					}
				} catch (final UserError e) {
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
				} catch (final Exception e) {
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
						showUpdateError(updateParent, "Update error:\n" + e.getMessage());
						return false;
					}
				}

				firstRequest = false;
			}
		} else {
			// Copy file
			Files.copy(new File(updateFileLocation).toPath(), downloadTempFile.toPath());
		}

		return downloadTempFile.exists();
	}

	private static String createUpdateBatchFile(final String jarFilePath, final File downloadTempFile, final boolean restartApplicationAfterUpdate, final String restartParameter) throws Exception {
		String javaBinPath = SystemUtilities.getJavaBinPath();
		if (Utilities.isBlank(javaBinPath)) {
			javaBinPath = "java";
		}
		if (SystemUtilities.isWindowsSystem()) {
			final File batchFile = new File(new File(jarFilePath).getParent() + File.separator + "batchUpdate_" + new File(jarFilePath).getName() + ".cmd");
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
							+ (restartApplicationAfterUpdate ? "\"" + javaBinPath + "\" -jar \"" + jarFilePath + "\"" + (restartParameter != null ? " " + restartParameter : "") + "\r\n" : "")
							+ "del \"" + batchFile.getAbsolutePath() + "\"\r\n"
							+ ")\r\n");

			return "cmd /c start /B " + batchFile.getAbsolutePath();
		} else if (SystemUtilities.isLinuxSystem()) {
			final File batchFile = new File(new File(jarFilePath).getParent() + File.separator + "batchUpdate_" + new File(jarFilePath).getName() + ".sh");
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
							+ (restartApplicationAfterUpdate ? "\"" + javaBinPath + "\" -jar \"" + jarFilePath + "\"" + (restartParameter != null ? " " + restartParameter : "") + "\n": "")
							+ "rm \"" + batchFile.getAbsolutePath() + "\"\n"
							+ "fi\n");

			return "sh " + batchFile.getAbsolutePath();
		} else {
			return null;
		}
	}

	private static String replacePlaceholders(String value) {
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
				value = value.replace("<time_seconds>", "" + (LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)));
			}

			return value;
		}
	}

	private static void writeFile(final File file, final String string) throws Exception {
		try (FileOutputStream out = new FileOutputStream(file)) {
			out.write(string.getBytes(StandardCharsets.UTF_8));
		} catch (final Exception e) {
			throw new Exception("Cannot write file " + file.getAbsolutePath(), e);
		}
	}

	private static boolean downloadUpdateFile(final ApplicationUpdateParent updateParent, final File downloadTempFile, final String downloadUrl) throws Exception {
		try {
			final WorkerParentSimple workerParentAdapter = new WorkerParentSimple() {
				@Override
				public void showUnlimitedProgress() {
					// Do nothing
				}

				@Override
				public void showProgress(final LocalDateTime startTime, final long itemsToDo, final long itemsDone) {
					if (updateParent != null) {
						updateParent.showUpdateProgress(startTime, itemsToDo, itemsDone);
					}
				}

				@Override
				public void showDone(final LocalDateTime startTime, final LocalDateTime endTime, final long itemsDone) {
					if (updateParent != null) {
						updateParent.showUpdateDownloadEnd(startTime, endTime, itemsDone);
					}
				}

				@Override
				public boolean cancel() {
					// Do nothing
					return true;
				}

				@Override
				public void changeTitle(final String text) {
					// do nothing
				}
			};
			final FileDownloadWorker fileDownloadWorker = new FileDownloadWorker(workerParentAdapter, downloadUrl, downloadTempFile);
			new Thread(fileDownloadWorker).start();

			if (updateParent != null) {
				updateParent.showUpdateDownloadStart(fileDownloadWorker);
			}

			while (!fileDownloadWorker.isDone()) {
				// Wait for download process
				Thread.sleep(1000);
			}

			if (fileDownloadWorker.isCancelled()) {
				return false;
			}

			if (!fileDownloadWorker.get()) {
				throw new Exception("Download was not successful");
			}

			return true;
		} catch (final Exception e) {
			if (e instanceof ExecutionException && e.getCause() instanceof UserError) {
				throw (UserError) e.getCause();
			} else {
				throw e;
			}
		}
	}

	private static void showUpdateError(final ApplicationUpdateParent updateParent, final String errorMessage) {
		if (updateParent != null) {
			updateParent.showUpdateError(errorMessage);
		} else {
			// Linebreak to end progress display
			System.err.println("");
			System.err.println(errorMessage);
		}
	}

	private static boolean verifyJarSignature(final File jarFile, final Collection<? extends Certificate> trustedCertificates) throws Exception {
		if (trustedCertificates == null || trustedCertificates.size() == 0) {
			return false;
		}

		try (JarFile jar = new JarFile(jarFile)) {
			final Manifest manifest = jar.getManifest();
			if (manifest == null) {
				throw new SecurityException("The jar file has no manifest, which contains the file signatures");
			}

			final byte[] buffer = new byte[4096];
			final Enumeration<JarEntry> jarEntriesEnumerator = jar.entries();
			final List<JarEntry> jarEntries = new ArrayList<>();

			while (jarEntriesEnumerator.hasMoreElements()) {
				final JarEntry jarEntry = jarEntriesEnumerator.nextElement();
				jarEntries.add(jarEntry);

				try (InputStream jarEntryInputStream = jar.getInputStream(jarEntry))  {
					// Reading the jarEntry throws a SecurityException if signature/digest check fails.
					while (jarEntryInputStream.read(buffer, 0, buffer.length) != -1) {
						// Do nothing
					}
				}
			}

			for (final JarEntry jarEntry : jarEntries) {
				if (!jarEntry.isDirectory()) {
					// Every file must be signed, except for files in META-INF
					final Certificate[] certificates = jarEntry.getCertificates();
					if ((certificates == null) || (certificates.length == 0)) {
						if (!jarEntry.getName().startsWith("META-INF")) {
							throw new SecurityException("The jar file contains unsigned files.");
						}
					} else {
						boolean isSignedByTrustedCert = false;

						for (final Certificate chainRootCertificate : certificates) {
							if (chainRootCertificate instanceof X509Certificate && verifyChainOfTrust((X509Certificate) chainRootCertificate, trustedCertificates)) {
								isSignedByTrustedCert = true;
								break;
							}
						}

						if (!isSignedByTrustedCert) {
							throw new SecurityException("The jar file contains untrusted signed files");
						}
					}
				}
			}

			return true;
		} catch (@SuppressWarnings("unused") final Exception e) {
			return false;
		}
	}

	private static boolean verifyChainOfTrust(final X509Certificate cert, final Collection<? extends Certificate> trustedCertificates) throws Exception {
		final CertPathBuilder certifier = CertPathBuilder.getInstance("PKIX");
		final X509CertSelector targetConstraints = new X509CertSelector();
		targetConstraints.setCertificate(cert);

		final Set<TrustAnchor> trustAnchors = new HashSet<>();
		for (final Certificate trustedRootCert : trustedCertificates) {
			trustAnchors.add(new TrustAnchor((X509Certificate) trustedRootCert, null));
		}

		final PKIXBuilderParameters params = new PKIXBuilderParameters(trustAnchors, targetConstraints);
		params.setRevocationEnabled(false);
		try {
			final PKIXCertPathBuilderResult result = (PKIXCertPathBuilderResult) certifier.build(params);
			return result != null;
		} catch (@SuppressWarnings("unused") final Exception cpbe) {
			return false;
		}
	}

	private static String getI18NString(final String resourceKey, final Object... arguments) {
		if (LangResources.existsKey(resourceKey)) {
			return LangResources.get(resourceKey, arguments);
		} else if ("de".equalsIgnoreCase(Locale.getDefault().getLanguage())) {
			String pattern;
			switch(resourceKey) {
				case "canceledByUser": pattern = "Abbruch durch Benutzer"; break;
				case "usernameAndPasswordRequired": pattern = "Fehler:\nUsername und Passwort sind notwendig"; break;
				case "usernameRequired": pattern = "Fehler:\nUsername ist notwendig"; break;
				case "passwordRequired": pattern = "Fehler:\nPasswort ist notwendig"; break;
				case "enterDownloadCredentials": pattern = "Bitte Userdaten für den Download eingeben"; break;
				case "enterDownloadUserName": pattern = "Bitte Passwort für Download eingeben"; break;
				case "enterDownloadPassword": pattern = "Bitte Usernamen für Download eingeben"; break;
				default: pattern = "MessageKey unbekannt: " + resourceKey + (arguments != null && arguments.length > 0 ? " Argumente: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		} else {
			String pattern;
			switch(resourceKey) {
				case "canceledByUser": pattern = "Canceled by user"; break;
				case "usernameAndPasswordRequired": pattern = "Error:\nUsername and password are required"; break;
				case "usernameRequired": pattern = "Error:\nUsername is required"; break;
				case "passwordRequired": pattern = "Error:\nPassword is required"; break;
				case "enterDownloadCredentials": pattern = "Enter credentials for download"; break;
				case "enterDownloadUserName": pattern = "Enter password for download"; break;
				case "enterDownloadPassword": pattern = "Enter username for download"; break;
				default: pattern = "MessageKey unknown: " + resourceKey + (arguments != null && arguments.length > 0 ? " arguments: " + Utilities.join(arguments, ", ") : "");
			}
			return MessageFormat.format(pattern, arguments);
		}
	}
}
