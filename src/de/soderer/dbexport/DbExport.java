package de.soderer.dbexport;

import java.awt.GraphicsEnvironment;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;

import de.soderer.dbexport.console.ConnectionTestMenu;
import de.soderer.dbexport.console.CreateTrustStoreMenu;
import de.soderer.dbexport.console.ExportMenu;
import de.soderer.dbexport.console.HelpMenu;
import de.soderer.dbexport.console.PreferencesMenu;
import de.soderer.dbexport.console.UpdateMenu;
import de.soderer.dbexport.worker.AbstractDbExportWorker;
import de.soderer.network.TrustManagerUtilities;
import de.soderer.pac.PacScriptParser;
import de.soderer.pac.utilities.ProxyConfiguration;
import de.soderer.pac.utilities.ProxyConfiguration.ProxyConfigurationType;
import de.soderer.utilities.ConfigurationProperties;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.ParameterException;
import de.soderer.utilities.UpdateableConsoleApplication;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.appupdate.ApplicationUpdateUtilities;
import de.soderer.utilities.console.ConsoleMenu;
import de.soderer.utilities.console.ConsoleType;
import de.soderer.utilities.console.ConsoleUtilities;
import de.soderer.utilities.console.PasswordConsoleInput;
import de.soderer.utilities.db.DbNotExistsException;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.swing.ApplicationConfigurationDialog;
import de.soderer.utilities.worker.WorkerParentDual;

// TODO: Export/Import KDBX entry path
/**
 * The Main-Class of DbExport.
 */
public class DbExport extends UpdateableConsoleApplication implements WorkerParentDual {
	/** The Constant APPLICATION_NAME. */
	public static final String APPLICATION_NAME = "DbExport";
	public static final String APPLICATION_STARTUPCLASS_NAME = "de-soderer-DbExport";

	/** The Constant VERSION_RESOURCE_FILE, which contains version number and versioninfo download url. */
	public static final String VERSION_RESOURCE_FILE = "/version.txt";

	public static final String HELP_RESOURCE_FILE = "/help.txt";

	/** The Constant CONFIGURATION_FILE. */
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + "." + APPLICATION_NAME + File.separator + "." + APPLICATION_NAME + ".config");
	public static final String CONFIGURATION_DRIVERLOCATIONPROPERTYNAME = "driver_location";

	/** The Constant SECURE_PREFERENCES_FILE. */
	public static final File SECURE_PREFERENCES_FILE = new File(System.getProperty("user.home") + File.separator + "." + APPLICATION_NAME + File.separator + "." + APPLICATION_NAME + ".secpref");

	/** The version is filled in at application start from the version.txt file. */
	public static Version VERSION = null;

	/** The version build time is filled in at application start from the version.txt file */
	public static LocalDateTime VERSION_BUILDTIME = null;

	/** The versioninfo download url is filled in at application start from the version.txt file. */
	public static String VERSIONINFO_DOWNLOAD_URL = null;

	/** Trusted CA certificate for updates **/
	public static String TRUSTED_UPDATE_CA_CERTIFICATES = null;

	/** The usage message. **/
	private static String getUsageMessage() {
		try (InputStream helpInputStream = DbExport.class.getResourceAsStream(HELP_RESOURCE_FILE)) {
			return "DbExport (by Andreas Soderer, mail: dbexport@soderer.de)\n"
					+ "VERSION: " + VERSION.toString() + " (" + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, VERSION_BUILDTIME) + ")" + "\n\n"
					+ new String(IoUtilities.toByteArray(helpInputStream), StandardCharsets.UTF_8);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return "Help info is missing";
		}
	}

	/** The database csv export definition. */
	private DbExportDefinition dbExportDefinitionToExecute;

	private int previousTerminalWidth = 0;

	/** The worker. */
	private AbstractDbExportWorker worker;

	/**
	 * The main method.
	 *
	 * @param arguments the arguments
	 */
	public static void main(final String[] arguments) {
		final int returnCode = _main(arguments);
		if (returnCode >= 0) {
			System.exit(returnCode);
		}
	}

	/**
	 * Method used for main but with no System.exit call to make it junit testable
	 *
	 * @param arguments
	 * @return
	 */
	protected static int _main(final String[] args) {
		ApplicationUpdateUtilities.removeUpdateLeftovers();

		try (InputStream resourceStream = DbExport.class.getResourceAsStream(VERSION_RESOURCE_FILE)) {
			// Try to fill the version and versioninfo download url
			final List<String> versionInfoLines = Utilities.readLines(resourceStream, StandardCharsets.UTF_8);
			VERSION = new Version(versionInfoLines.get(0));
			if (versionInfoLines.size() >= 2) {
				VERSION_BUILDTIME = DateUtilities.parseLocalDateTime(DateUtilities.YYYY_MM_DD_HHMMSS, versionInfoLines.get(1));
			}
			if (versionInfoLines.size() >= 3) {
				VERSIONINFO_DOWNLOAD_URL = versionInfoLines.get(2);
			}
			if (versionInfoLines.size() >= 4) {
				TRUSTED_UPDATE_CA_CERTIFICATES = versionInfoLines.get(3);
			}
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Without the version.txt file we may not go on
			System.err.println("Invalid version.txt");
			return 1;
		}

		ConfigurationProperties applicationConfiguration;
		try {
			applicationConfiguration = new ConfigurationProperties(DbExport.APPLICATION_NAME, true);
			DbExportGui.setupDefaultConfig(applicationConfiguration);
		} catch (@SuppressWarnings("unused") final Exception e) {
			System.err.println("Invalid application configuration");
			return 1;
		}

		if (!applicationConfiguration.containsKey(ApplicationConfigurationDialog.CONFIG_PROXY_CONFIGURATION_TYPE)) {
			if (PacScriptParser.findPacFileUrlByWpad() != null) {
				applicationConfiguration.set(ApplicationConfigurationDialog.CONFIG_PROXY_CONFIGURATION_TYPE, ProxyConfigurationType.WPAD.name());
			} else {
				applicationConfiguration.set(ApplicationConfigurationDialog.CONFIG_PROXY_CONFIGURATION_TYPE, ProxyConfigurationType.None.name());
			}
			applicationConfiguration.save();
		}

		final ProxyConfigurationType proxyConfigurationType = ProxyConfigurationType.getFromString(applicationConfiguration.get(ApplicationConfigurationDialog.CONFIG_PROXY_CONFIGURATION_TYPE));
		final String proxyUrl = applicationConfiguration.get(ApplicationConfigurationDialog.CONFIG_PROXY_URL);
		final ProxyConfiguration proxyConfiguration = new ProxyConfiguration(proxyConfigurationType, proxyUrl);

		try {
			String[] arguments = args;

			boolean openGui = false;
			boolean openMenu = false;
			boolean connectionTest = false;
			boolean createTrustStore = false;

			if (arguments.length == 0) {
				// If started without any parameter we check for headless mode and show the console menu or the GUI
				if (GraphicsEnvironment.isHeadless()) {
					openMenu = true;
				} else {
					openGui = true;
				}
			} else {
				for (int i = 0; i < arguments.length; i++) {
					if ("help".equalsIgnoreCase(arguments[i]) || "-help".equalsIgnoreCase(arguments[i]) || "--help".equalsIgnoreCase(arguments[i]) || "-h".equalsIgnoreCase(arguments[i]) || "--h".equalsIgnoreCase(arguments[i])
							|| "-?".equalsIgnoreCase(arguments[i]) || "--?".equalsIgnoreCase(arguments[i])) {
						System.out.println(getUsageMessage());
						return 1;
					} else if ("ConsoleType".equalsIgnoreCase(arguments[i])) {
						System.out.println("ConsoleType: " + ConsoleUtilities.getConsoleType());
						return 1;
					} else if ("version".equalsIgnoreCase(arguments[i]) && arguments.length == 1) {
						System.out.println(VERSION);
						return 1;
					} else if ("update".equalsIgnoreCase(arguments[i]) && arguments.length == 1) {
						if (arguments.length > i + 2) {
							final DbExport dbExport = new DbExport();
							ApplicationUpdateUtilities.executeUpdate(dbExport, DbExport.VERSIONINFO_DOWNLOAD_URL, proxyConfiguration, DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, arguments[i + 1], arguments[i + 2].toCharArray(), null, false);
						} else if (arguments.length > i + 1) {
							final DbExport dbExport = new DbExport();
							ApplicationUpdateUtilities.executeUpdate(dbExport, DbExport.VERSIONINFO_DOWNLOAD_URL, proxyConfiguration, DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, arguments[i + 1], null, null, false);
						} else {
							final DbExport dbExport = new DbExport();
							ApplicationUpdateUtilities.executeUpdate(dbExport, DbExport.VERSIONINFO_DOWNLOAD_URL, proxyConfiguration, DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, null, null, null, false);
						}
						return 1;
					} else if ("gui".equalsIgnoreCase(arguments[i])) {
						if (GraphicsEnvironment.isHeadless()) {
							throw new DbExportException("GUI can only be shown on a non-headless environment");
						}
						openGui = true;
						if (openMenu) {
							throw new DbExportException("Only one of gui or menu can be opend at a time");
						} else if (connectionTest) {
							throw new DbExportException("Only one of gui or connection test can be used at a time");
						} else if (createTrustStore) {
							throw new DbExportException("Only one of gui or create truststore can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					} else if ("menu".equalsIgnoreCase(arguments[i])) {
						openMenu = true;
						if (openGui) {
							throw new DbExportException("Only one of menu or gui can be opend at a time");
						} else if (connectionTest) {
							throw new DbExportException("Only one of menu or connection test can be used at a time");
						} else if (createTrustStore) {
							throw new DbExportException("Only one of menu or create truststore can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					} else if ("connectiontest".equalsIgnoreCase(arguments[i])) {
						connectionTest = true;
						if (openGui) {
							throw new DbExportException("Only one of connection test or gui can be used at a time");
						} else if (openMenu) {
							throw new DbExportException("Only one of connection test or menu can be used at a time");
						} else if (createTrustStore) {
							throw new DbExportException("Only one of connection test or create truststore can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					} else if ("createtruststore".equalsIgnoreCase(arguments[i])) {
						createTrustStore = true;
						if (openGui) {
							throw new DbExportException("Only one of create truststore or gui can be used at a time");
						} else if (connectionTest) {
							throw new DbExportException("Only one of create truststore or connection test can be used at a time");
						} else if (openMenu) {
							throw new DbExportException("Only one of create truststore or menu can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					}
				}
			}

			final DbExportDefinition dbExportDefinition = new DbExportDefinition();
			final ConnectionTestDefinition connectionTestDefinition = new ConnectionTestDefinition();

			// Read the parameters
			for (int i = 0; i < arguments.length; i++) {
				boolean wasAllowedParam = false;

				if (!connectionTest) {
					if ("-x".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for export format");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for export format");
						} else {
							dbExportDefinition.setDataType(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-n".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter null value string");
						} else {
							dbExportDefinition.setNullValueString(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-file".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setStatementFile(true);
						wasAllowedParam = true;
					} else if ("-l".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setLog(true);
						wasAllowedParam = true;
					} else if ("-v".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setVerbose(true);
						wasAllowedParam = true;
					} else if ("-z".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setCompression(FileCompressionType.ZIP);
						wasAllowedParam = true;
					} else if ("-compress".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for compress type");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for compress type");
						} else {
							dbExportDefinition.setCompression(FileCompressionType.getFromString(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-zippassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter zippassword");
						} else {
							String zipPassword = arguments[i];
							if ((zipPassword.startsWith("\"") && zipPassword.endsWith("\"")) || (zipPassword.startsWith("'") && zipPassword.endsWith("'"))) {
								zipPassword = zipPassword.substring(1, zipPassword.length() - 1);
							}
							dbExportDefinition.setZipPassword(zipPassword.toCharArray());
						}
						wasAllowedParam = true;
					} else if ("-kdbxpassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter kdbxpassword");
						} else {
							String kdbxPassword = arguments[i];
							if ((kdbxPassword.startsWith("\"") && kdbxPassword.endsWith("\"")) || (kdbxPassword.startsWith("'") && kdbxPassword.endsWith("'"))) {
								kdbxPassword = kdbxPassword.substring(1, kdbxPassword.length() - 1);
							}
							dbExportDefinition.setKdbxPassword(kdbxPassword.toCharArray());
						}
						wasAllowedParam = true;
					} else if ("-useZipCrypto".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setUseZipCrypto(true);
						wasAllowedParam = true;
					} else if ("-dbtz".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter database timezone");
						} else {
							dbExportDefinition.setDatabaseTimeZone(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-edtz".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter export data timezone");
						} else {
							dbExportDefinition.setExportDataTimeZone(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-e".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter encoding");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter encoding");
						} else {
							dbExportDefinition.setEncoding(Charset.forName(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-s".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter separator character");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter separator character");
						} else {
							dbExportDefinition.setSeparator(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-q".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter stringquote character");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote character");
						} else {
							dbExportDefinition.setStringQuote(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-qe".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter stringquote escape character");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote escape character");
						} else {
							dbExportDefinition.setStringQuoteEscapeCharacter(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-i".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter indentation string");
						} else if (arguments[i].length() == 0) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter indentation string");
						}
						if ("TAB".equalsIgnoreCase(arguments[i])) {
							dbExportDefinition.setIndentation("\t");
						} else if ("BLANK".equalsIgnoreCase(arguments[i])) {
							dbExportDefinition.setIndentation(" ");
						} else if ("DOUBLEBLANK".equalsIgnoreCase(arguments[i])) {
							dbExportDefinition.setIndentation("  ");
						} else {
							dbExportDefinition.setIndentation(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-a".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setAlwaysQuote(true);
						wasAllowedParam = true;
					} else if ("-f".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter format locale");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 2) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter format locale");
						} else {
							dbExportDefinition.setDateFormatLocale(new Locale(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-dateFormat".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter dateFormat");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter dateFormat");
						} else {
							dbExportDefinition.setDateFormat(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-dateTimeFormat".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter dateTimeFormat");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter dateTimeFormat");
						} else {
							dbExportDefinition.setDateTimeFormat(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-decimalSeparator".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter decimalSeparator");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter decimalSeparator");
						} else {
							dbExportDefinition.setDecimalSeparator(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-blobfiles".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setCreateBlobFiles(true);
						wasAllowedParam = true;
					} else if ("-clobfiles".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setCreateClobFiles(true);
						wasAllowedParam = true;
					} else if ("-beautify".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setBeautify(true);
						wasAllowedParam = true;
					} else if ("-noheaders".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setNoHeaders(true);
						wasAllowedParam = true;
					} else if ("-structure".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for structure");
						} else {
							dbExportDefinition.setExportStructureFilePath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-export".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for export");
						} else {
							dbExportDefinition.setSqlStatementOrTablelist(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-output".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for output");
						} else {
							dbExportDefinition.setOutputpath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-secure".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setSecureConnection(true);
						wasAllowedParam = true;
					} else if ("-truststore".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststore");
						} else {
							dbExportDefinition.setTrustStoreFile(new File(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-truststorepassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststorepassword");
						} else {
							dbExportDefinition.setTrustStorePassword(Utilities.isNotEmpty(arguments[i]) ? arguments[i].toCharArray() : null);
						}
						wasAllowedParam = true;
					} else {
						if (dbExportDefinition.getDbVendor() == null) {
							dbExportDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (dbExportDefinition.getHostnameAndPort() == null && dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.Derby) {
							dbExportDefinition.setHostnameAndPort(arguments[i]);
							wasAllowedParam = true;
						} else if (dbExportDefinition.getDbName() == null) {
							dbExportDefinition.setDbName(arguments[i]);
							wasAllowedParam = true;
						} else if (dbExportDefinition.getUsername() == null && dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.Derby) {
							dbExportDefinition.setUsername(arguments[i]);
							wasAllowedParam = true;
						} else if (dbExportDefinition.getPassword() == null && dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.Derby) {
							dbExportDefinition.setPassword(arguments[i] == null ? null : arguments[i].toCharArray());
							wasAllowedParam = true;
						}
					}
				}

				if (openMenu || connectionTest) {
					if ("-iter".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for connectiontest iterations");
						} else if (!NumberUtilities.isInteger(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for connectiontest iterations");
						} else {
							connectionTestDefinition.setIterations(Integer.parseInt(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-sleep".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for connectiontest sleep time");
						} else if (!NumberUtilities.isInteger(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for connectiontest sleep time");
						} else {
							connectionTestDefinition.setSleepTime(Integer.parseInt(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-check".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for check");
						} else {
							connectionTestDefinition.setCheckStatement(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-secure".equalsIgnoreCase(arguments[i])) {
						connectionTestDefinition.setSecureConnection(true);
						wasAllowedParam = true;
					} else if ("-truststore".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststore");
						} else {
							connectionTestDefinition.setTrustStoreFile(new File(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-truststorepassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststorepassword");
						} else {
							connectionTestDefinition.setTrustStorePassword(Utilities.isNotEmpty(arguments[i]) ? arguments[i].toCharArray() : null);
						}
						wasAllowedParam = true;
					} else {
						if (connectionTestDefinition.getDbVendor() == null) {
							connectionTestDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getHostnameAndPort() == null && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
							connectionTestDefinition.setHostnameAndPort(arguments[i]);
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getDbName() == null) {
							connectionTestDefinition.setDbName(arguments[i]);
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getUsername() == null && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
							connectionTestDefinition.setUsername(arguments[i]);
							wasAllowedParam = true;
						} else if (connectionTestDefinition.getPassword() == null && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby) {
							connectionTestDefinition.setPassword(arguments[i] == null ? null : arguments[i].toCharArray());
							wasAllowedParam = true;
						}
					}
				}

				if (createTrustStore) {
					TrustManagerUtilities.createTrustStoreFile(arguments[0], 443, new File(arguments[1]), Utilities.isNotEmpty(arguments[2]) ? arguments[2].toCharArray() : null, null);
				}

				if (!wasAllowedParam) {
					throw new ParameterException(arguments[i], "Invalid parameter");
				}
			}

			if (openMenu) {
				if (System.console() == null) {
					System.err.println("Couldn't get Console instance for menu");
					return 1;
				}

				final ConsoleMenu mainMenu = new ConsoleMenu(APPLICATION_NAME + " (v" + VERSION.toString() + ")");
				final ExportMenu exportMenu = new ExportMenu(mainMenu);
				exportMenu.setDbExportDefinition(dbExportDefinition);
				final ConnectionTestMenu connectionTestMenu = new ConnectionTestMenu(mainMenu, exportMenu.getDbExportDefinition());
				if (connectionTest) {
					connectionTestMenu.setConnectionTestDefinition(connectionTestDefinition);
				}

				@SuppressWarnings("unused")
				final PreferencesMenu preferencesMenu = new PreferencesMenu(mainMenu, exportMenu.getDbExportDefinition());

				final CreateTrustStoreMenu createTrustStoreMenu = new CreateTrustStoreMenu(mainMenu, exportMenu.getDbExportDefinition());
				if (createTrustStore) {
					createTrustStoreMenu.setConnectionTestDefinition(connectionTestDefinition);
				}
				@SuppressWarnings("unused")
				final UpdateMenu updateMenu = new UpdateMenu(mainMenu);
				@SuppressWarnings("unused")
				final HelpMenu helpMenu = new HelpMenu(mainMenu);

				final int consoleMenuExecutionCode = mainMenu.show();

				if (consoleMenuExecutionCode == -1) {
					// Validate all given parameters
					dbExportDefinition.checkParameters();

					// Start the export worker for terminal output
					try {
						new DbExport().export(dbExportDefinition);
						return 0;
					} catch (final DbExportException e) {
						System.err.println(e.getMessage());
						return 1;
					} catch (final Exception e) {
						e.printStackTrace();
						return 1;
					}
				} else if (consoleMenuExecutionCode == -2) {
					// Validate all given parameters
					connectionTestDefinition.checkParameters();

					return connectionTest(connectionTestDefinition);
				} else if (consoleMenuExecutionCode == -3) {
					// Update application
					final DbExport dbExport = new DbExport();
					ApplicationUpdateUtilities.executeUpdate(dbExport, DbExport.VERSIONINFO_DOWNLOAD_URL, proxyConfiguration, DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, null, null, null, false);
					return 0;
				} else if (consoleMenuExecutionCode == -5) {
					// Create TrustStore
					TrustManagerUtilities.createTrustStoreFile(connectionTestDefinition.getHostnameAndPort(), 443, connectionTestDefinition.getTrustStoreFile(), connectionTestDefinition.getTrustStorePassword(), null);
					System.out.println();
					System.out.println("Created TrustStore in file '" + connectionTestDefinition.getTrustStoreFile().getAbsolutePath() + "'");
					return 0;
				} else {
					System.out.println();
					System.out.println("Bye");
					System.out.println();
					return 0;
				}
			} else if (openGui) {
				// open the preconfigured GUI
				de.soderer.utilities.swing.SwingUtilities.setSystemLookAndFeel();
				try {
					final DbExportGui dbExportGui = new DbExportGui(dbExportDefinition);
					SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							dbExportGui.setVisible(true);
						}
					});
					return -1;
				} catch (final Exception e) {
					e.printStackTrace();
					return 1;
				}
			} else if (connectionTest) {
				// If started without GUI we may enter the missing password via the terminal
				if (Utilities.isNotBlank(connectionTestDefinition.getUsername()) && connectionTestDefinition.getPassword() == null
						&& connectionTestDefinition.getDbVendor() != DbVendor.SQLite
						&& connectionTestDefinition.getDbVendor() != DbVendor.Derby
						&& connectionTestDefinition.getDbVendor() != DbVendor.Cassandra) {
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					connectionTestDefinition.setPassword(passwordArray);
				}

				return connectionTest(connectionTestDefinition);
			} else {
				// If started without GUI we may enter the missing password via the terminal
				if (Utilities.isNotBlank(dbExportDefinition.getUsername()) && dbExportDefinition.getPassword() == null
						&& dbExportDefinition.getDbVendor() != DbVendor.SQLite
						&& dbExportDefinition.getDbVendor() != DbVendor.Derby
						&& dbExportDefinition.getDbVendor() != DbVendor.Cassandra) {
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					dbExportDefinition.setPassword(passwordArray);
				}

				// Validate all given parameters
				dbExportDefinition.checkParameters();

				// Start the export worker for terminal output
				try {
					new DbExport().export(dbExportDefinition);
					return 0;
				} catch (final DbExportException e) {
					System.err.println(e.getMessage());
					return 1;
				} catch (final Exception e) {
					e.printStackTrace();
					return 1;
				}
			}
		} catch (final ParameterException e) {
			System.err.println(e.getMessage());
			System.err.println();
			System.err.println(getUsageMessage());
			return 1;
		} catch (final Exception e) {
			System.err.println(e.getMessage());
			return 1;
		}
	}

	/**
	 * Instantiates a new database csv export.
	 *
	 * @throws Exception the exception
	 */
	public DbExport() throws Exception {
		super(APPLICATION_NAME, VERSION);
	}

	/**
	 * Export.
	 *
	 * @param dbExportDefinition the database csv export definition
	 * @throws Exception the exception
	 */
	private void export(final DbExportDefinition dbExportDefinition) throws Exception {
		dbExportDefinitionToExecute = dbExportDefinition;

		try {
			worker = dbExportDefinition.getConfiguredWorker(this);

			if (dbExportDefinition.isVerbose()) {
				System.out.println(worker.getConfigurationLogString(new File(dbExportDefinition.getOutputpath()).getName(), dbExportDefinition.getSqlStatementOrTablelist())
						+ (Utilities.isNotBlank(dbExportDefinition.getDateFormat()) ? "DateFormatPattern: " + dbExportDefinition.getDateFormat() + "\n" : "")
						+ (Utilities.isNotBlank(dbExportDefinition.getDateTimeFormat()) ? "DateTimeFormatPattern: " + dbExportDefinition.getDateTimeFormat() + "\n" : "")
						+ (dbExportDefinition.getDatabaseTimeZone() != null && !dbExportDefinition.getDatabaseTimeZone().equals(dbExportDefinition.getExportDataTimeZone()) ? "DatabaseZoneId: " + dbExportDefinition.getDatabaseTimeZone() + "\nExportDataZoneId: " + dbExportDefinition.getExportDataTimeZone() + "\n" : ""));
				System.out.println();
			}

			worker.setProgressDisplayDelayMilliseconds(2000);
			worker.run();

			if (dbExportDefinition.isVerbose()) {
				System.out.println(LangResources.get("exportedlines") + ": " + worker.getOverallExportedLines());
				System.out.println(LangResources.get("exporteddataamount") + ": " + worker.getOverallExportedDataAmountRaw());
				if (dbExportDefinition.getCompression() != null) {
					System.out.println(LangResources.get("exporteddataamountcompressed") + ": " + worker.getOverallExportedDataAmountCompressed());
				}
				System.out.println(LangResources.get("exportSpeed") + ": " + Utilities.getHumanReadableSpeed(worker.getStartTime(), worker.getEndTime(), worker.getOverallExportedDataAmountRaw() * 8, "Bit", true, Locale.getDefault()));
				System.out.println();
			}

			// Get result to trigger possible Exception
			worker.get();
		} catch (final ExecutionException e) {
			if (e.getCause() instanceof Exception) {
				throw (Exception) e.getCause();
			} else {
				throw e;
			}
		} catch (final Exception e) {
			throw e;
		}
	}

	private static int connectionTest(final ConnectionTestDefinition connectionTestDefinition) {
		int returnCode = 0;
		int connectionCheckCount = 0;
		int successfulConnectionCount = 0;
		for (int i = 1; i <= connectionTestDefinition.getIterations() || connectionTestDefinition.getIterations() == 0; i++) {
			connectionCheckCount++;
			System.out.println("Connection test " + i + (connectionTestDefinition.getIterations() > 0 ? " / " + connectionTestDefinition.getIterations() : ""));
			Connection testConnection = null;
			try {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Creating database connection");
				if (connectionTestDefinition.getDbVendor() == DbVendor.Derby || (connectionTestDefinition.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(connectionTestDefinition.getHostnameAndPort())) || connectionTestDefinition.getDbVendor() == DbVendor.SQLite) {
					try {
						testConnection = DbUtilities.createConnection(connectionTestDefinition, false);
					} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
						testConnection = DbUtilities.createNewDatabase(connectionTestDefinition.getDbVendor(), connectionTestDefinition.getDbName());
					}
				} else {
					testConnection = DbUtilities.createConnection(connectionTestDefinition, false);
				}

				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Successfully created database connection");

				if (Utilities.isNotBlank(connectionTestDefinition.getCheckStatement())) {
					try (Statement statement = testConnection.createStatement()) {
						String statementString = connectionTestDefinition.getCheckStatement();
						if ("vendor".equalsIgnoreCase(statementString)) {
							statementString = connectionTestDefinition.getDbVendor().getTestStatement();
						}

						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Executing \"" + statementString + "\"");
						try (ResultSet resultSet = statement.executeQuery(statementString)) {
							// do nothing
						}
					}
				}

				successfulConnectionCount++;
			} catch (final SQLException sqle) {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": SQL-Error creating database connection: " + sqle.getMessage() + " (" + sqle.getErrorCode() + " / " + sqle.getSQLState() + ")");
				returnCode = 1;
			} catch (final Exception e) {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Error creating database connection: " + e.getClass().getSimpleName() + ":" + e.getMessage());
				e.printStackTrace();
				returnCode = 1;
			} finally {
				if (testConnection != null) {
					try {
						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Closing database connection");
						if (!testConnection.isClosed()) {
							testConnection.close();
						}
					} catch (final SQLException e) {
						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Error closing database connection: " + e.getMessage());
						returnCode = 1;
					}
				}
				if (connectionTestDefinition.getDbVendor() == DbVendor.Derby) {
					try {
						DbUtilities.shutDownDerbyDb(connectionTestDefinition.getDbName());
					} catch (final Exception e) {
						System.err.println(e.getMessage());
					}
				}
			}

			if (connectionTestDefinition.getIterations() == 0) {
				final int successPercentage = successfulConnectionCount * 100 / connectionCheckCount;
				System.out.println("Successful connection checks: " + successfulConnectionCount + " / " + connectionCheckCount + " (" + successPercentage + "%)");
			}

			if ((connectionCheckCount < connectionTestDefinition.getIterations() || connectionTestDefinition.getIterations() == 0) && connectionTestDefinition.getSleepTime() > 0) {
				try {
					System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Sleeping for " + connectionTestDefinition.getSleepTime() + " seconds");
					Thread.sleep(connectionTestDefinition.getSleepTime() * 1000);
				} catch (@SuppressWarnings("unused") final InterruptedException e) {
					// do nothing
				}
			}
		}

		final int successPercentage = successfulConnectionCount * 100 / connectionCheckCount;
		System.out.println("Successful connection checks: " + successfulConnectionCount + " / " + connectionCheckCount + " (" + successPercentage + "%)");

		return returnCode;
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showUnlimitedProgress()
	 */
	@Override
	public void receiveUnlimitedProgressSignal() {
		// Do nothing
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentDual#showUnlimitedSubProgress()
	 */
	@Override
	public void receiveUnlimitedSubProgressSignal() {
		// Do nothing
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showProgress(java.util.Date, long, long)
	 */
	@Override
	public void receiveProgressSignal(final LocalDateTime start, final long itemsToDo, final long itemsDone, final String itemsUnitSign) {
		if (dbExportDefinitionToExecute.isVerbose()) {
			if (dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")
					|| dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select\t")
					|| dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select\n")
					|| dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select\r")) {
				if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
					int currentTerminalWidth;
					try {
						currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
					} catch (@SuppressWarnings("unused") final Exception e) {
						currentTerminalWidth = 80;
					}

					ConsoleUtilities.saveCurrentCursorPosition();

					if (currentTerminalWidth < previousTerminalWidth) {
						System.out.print("\033[F" + Utilities.repeat(" ", currentTerminalWidth));
					}
					previousTerminalWidth = currentTerminalWidth;

					ConsoleUtilities.moveCursorToSavedPosition();

					System.out.print(ConsoleUtilities.getConsoleProgressString(currentTerminalWidth - 1, start, itemsToDo, itemsDone, itemsUnitSign));

					ConsoleUtilities.moveCursorToSavedPosition();
				} else {
					System.out.print("\r" + ConsoleUtilities.getConsoleProgressString(80 - 1, start, itemsToDo, itemsDone, itemsUnitSign) + "\r");
				}
			} else if (ConsoleUtilities.getConsoleType() == ConsoleType.TEST) {
				System.out.println();
				System.out.println("Exporting table " + (itemsDone + 1) + " of " + itemsToDo);
			} else {
				System.out.println();
				System.out.println("Exporting table " + (itemsDone + 1) + " of " + itemsToDo);
			}
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentDual#showItemStart(java.lang.String)
	 */
	@Override
	public void receiveItemStartSignal(final String itemName, final String description) {
		if ("Scanning tables ...".equals(itemName)) {
			System.out.println(itemName);
		} else {
			System.out.println("Table " + itemName);
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentDual#showItemProgress(java.util.Date, long, long)
	 */
	@Override
	public void receiveItemProgressSignal(final LocalDateTime itemStart, final long subItemToDo, final long subItemDone, final String itemsUnitSign) {
		if (dbExportDefinitionToExecute.isVerbose()) {
			if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
				int currentTerminalWidth;
				try {
					currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
				} catch (@SuppressWarnings("unused") final Exception e) {
					currentTerminalWidth = 80;
				}

				ConsoleUtilities.saveCurrentCursorPosition();

				if (currentTerminalWidth < previousTerminalWidth) {
					System.out.print(Utilities.repeat(" ", currentTerminalWidth));
				}
				previousTerminalWidth = currentTerminalWidth;

				ConsoleUtilities.moveCursorToSavedPosition();

				System.out.print(ConsoleUtilities.getConsoleProgressString(currentTerminalWidth - 1, itemStart, subItemToDo, subItemDone, itemsUnitSign));

				ConsoleUtilities.moveCursorToSavedPosition();
			} else if (ConsoleUtilities.getConsoleType() == ConsoleType.TEST) {
				System.out.print(ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, subItemToDo, subItemDone, itemsUnitSign) + "\n");
			} else {
				System.out.print("\r" + ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, subItemToDo, subItemDone, itemsUnitSign) + "\r");
			}
		}
	}

	@Override
	public void receiveItemDoneSignal(final LocalDateTime itemStart, final LocalDateTime itemEnd, final long subItemsDone, final String itemsUnitSign, final String resultText) {
		if (dbExportDefinitionToExecute.isVerbose()) {
			int currentTerminalWidth;
			try {
				currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
			} catch (@SuppressWarnings("unused") final Exception e) {
				currentTerminalWidth = 80;
			}
			System.out.println(Utilities.rightPad("Exported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(subItemsDone) + " lines in " + DateUtilities.getShortHumanReadableTimespan(Duration.between(itemStart, itemEnd), false, false), currentTerminalWidth));
			System.out.println();
		}
	}

	@Override
	public void receiveDoneSignal(final LocalDateTime start, final LocalDateTime end, final long itemsDone, final String itemsUnitSign, final String resultText) {
		if (dbExportDefinitionToExecute.isVerbose()) {
			if (dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")
					|| dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select\t")
					|| dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select\n")
					|| dbExportDefinitionToExecute.getSqlStatementOrTablelist().toLowerCase().startsWith("select\r")) {
				int currentTerminalWidth;
				try {
					currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
				} catch (@SuppressWarnings("unused") final Exception e) {
					currentTerminalWidth = 80;
				}
				System.out.println(Utilities.rightPad("Exported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsDone) + " lines in " + DateUtilities.getShortHumanReadableTimespan(Duration.between(start, end), false, false), currentTerminalWidth));
				System.out.println();
				System.out.println();
			} else {
				System.out.println();
				System.out.println("Done after " + DateUtilities.getShortHumanReadableTimespan(Duration.between(start, end), false, false));
				System.out.println();
			}
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#cancel()
	 */
	@Override
	public boolean cancel() {
		System.out.println("Canceled");
		return true;
	}

	@Override
	public void changeTitle(final String text) {
		// Do nothing
	}
}
