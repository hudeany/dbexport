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
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;

import de.soderer.dbexport.console.ConnectionTestMenu;
import de.soderer.dbexport.console.CreateTrustStoreMenu;
import de.soderer.dbexport.console.ExportMenu;
import de.soderer.dbexport.console.HelpMenu;
import de.soderer.dbexport.console.PreferencesMenu;
import de.soderer.dbexport.console.UpdateMenu;
import de.soderer.dbexport.worker.AbstractDbExportWorker;
import de.soderer.network.trustmanager.TrustManagerUtilities;
import de.soderer.pac.PacScriptParser;
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
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbNotExistsException;
import de.soderer.utilities.worker.WorkerParentDual;

// TODO: Export/Import KDBX entry path
/**
 * The Main-Class of DbExport.
 */
public class DbExport extends UpdateableConsoleApplication implements WorkerParentDual {
	/** The Constant APPLICATION_NAME. */
	public static final String APPLICATION_NAME = "DbExport";
	/**
	 * Name of the startup class used for the application update.
	 */
	public static final String APPLICATION_STARTUPCLASS_NAME = "de-soderer-DbExport";

	/** The Constant VERSION_RESOURCE_FILE, which contains version number and versioninfo download url. */
	public static final String VERSION_RESOURCE_FILE = "/version.txt";

	/**
	 * Resource file containing the help text.
	 */
	public static final String HELP_RESOURCE_FILE = "/help.txt";

	/** The Constant CONFIGURATION_FILE. */
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + "." + APPLICATION_NAME + File.separator + "." + APPLICATION_NAME + ".config");
	/**
	 * Property name of the driver file path in the configuration file.
	 */
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
					+ "VERSION: " + VERSION.toString() + getBuildTimeText() + "\n\n"
					+ new String(IoUtilities.toByteArray(helpInputStream), StandardCharsets.UTF_8);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return "Help info is missing";
		}
	}

	/**
	 * Returns the build time for the version output, e.g. " (2026-10-07 10:50:48)".
	 *
	 * @return the formatted build time in brackets, or an empty string, if the version file contains no build time
	 */
	public static String getBuildTimeText() {
		// The build time is optional in version.txt, so it must not break the help output
		return VERSION_BUILDTIME == null ? "" : " (" + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, VERSION_BUILDTIME) + ")";
	}

	/**
	 * Checks whether a command line value for a HSQL database is the path of a file database instead of a hostname.
	 * A file database path contains a path separator or starts with "~" or ".", e.g. "./mydb" or "~/data/mydb".
	 *
	 * @param value the command line value
	 * @return true if the value is a file database path, false if it is a hostname (optionally with port)
	 */
	public static boolean isHsqlFileDatabasePath(final String value) {
		return value != null && (value.contains("/") || value.contains("\\") || value.startsWith("~") || value.startsWith("."));
	}

	/** Lower case keywords for the help text, only recognized as single argument */
	private static final Set<String> HELP_KEYWORDS = Set.of("help", "-help", "--help", "-h", "--h", "-?", "--?");

	/** Lower case flags only known by the connection test parameters (see menu mode parsing in _main) */
	private static final Set<String> CONNECTION_TEST_FLAGS = Set.of("-iter", "-sleep", "-check");

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
	 * Method used for main but with no System.exit call to make it junit testable.
	 *
	 * @param args the command line arguments
	 * @return the exit code (0 for success, 1 for errors), or a negative value if the application keeps running (GUI)
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

		if (!applicationConfiguration.containsKey(ConfigurationProperties.CONFIG_KEY_PROXY_CONFIGURATION_TYPE)) {
			if (PacScriptParser.findPacFileUrlByWpad() != null) {
				applicationConfiguration.set(ConfigurationProperties.CONFIG_KEY_PROXY_CONFIGURATION_TYPE, ProxyConfigurationType.WPAD.name());
			} else {
				applicationConfiguration.set(ConfigurationProperties.CONFIG_KEY_PROXY_CONFIGURATION_TYPE, ProxyConfigurationType.None.name());
			}
			applicationConfiguration.save();
		}

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
			} else if (arguments.length == 1 && HELP_KEYWORDS.contains(arguments[0].toLowerCase(Locale.ROOT))) {
				// Only as single argument, otherwise e.g. a table name, username or password "help" showed the help text
				System.out.println(getUsageMessage());
				return 1;
			} else if (arguments.length == 1 && "ConsoleType".equalsIgnoreCase(arguments[0])) {
				System.out.println("ConsoleType: " + ConsoleUtilities.getConsoleType());
				return 1;
			} else if (arguments.length == 1 && "version".equalsIgnoreCase(arguments[0])) {
				System.out.println(VERSION);
				return 1;
			} else {
				// The mode keywords are only recognized as first argument. Before, they were recognized at any position,
				// so e.g. a password, username or table name "menu" or "gui" changed the mode and was removed from the arguments.
				final String modeKeyword = arguments[0].toLowerCase(Locale.ROOT);
				if ("update".equals(modeKeyword) && arguments.length <= 3) {
					final DbExport dbExport = new DbExport();
					final String username = arguments.length > 1 ? arguments[1] : null;
					final char[] password = arguments.length > 2 ? arguments[2].toCharArray() : null;
					ApplicationUpdateUtilities.executeUpdate(dbExport, DbExport.VERSIONINFO_DOWNLOAD_URL, applicationConfiguration.getProxyConfiguration(), DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, username, password, null, false, false);
					return 1;
				} else if ("gui".equals(modeKeyword)) {
					if (GraphicsEnvironment.isHeadless()) {
						throw new DbExportException("GUI can only be shown on a non-headless environment");
					}
					openGui = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				} else if ("menu".equals(modeKeyword)) {
					openMenu = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				} else if ("connectiontest".equals(modeKeyword)) {
					connectionTest = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				} else if ("createtruststore".equals(modeKeyword)) {
					createTrustStore = true;
					arguments = Utilities.removeItemAtIndex(arguments, 0);
				}
			}

			final DbExportDefinition dbExportDefinition = new DbExportDefinition();
			final ConnectionTestDefinition connectionTestDefinition = new ConnectionTestDefinition();

			// Read the parameters
			for (int i = 0; i < arguments.length; i++) {
				boolean wasAllowedParam = createTrustStore;

				// In menu mode the export and connection test parameters are both read from the same arguments.
				// A flag handled by the export parameters (including its value, which advances "i") must not be taken
				// as a positional parameter (vendor, host, ...) of the connection test parameters and vice versa.
				final int startIndex = i;
				final boolean isConnectionTestFlag = CONNECTION_TEST_FLAGS.contains(arguments[i].toLowerCase(Locale.ROOT));
				boolean exportFlagHandled = false;

				if (!connectionTest && !createTrustStore) {
					boolean positionalParameter = false;
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
					} else if ("-noescapesequences".equalsIgnoreCase(arguments[i])) {
						dbExportDefinition.setInterpretEscapeSequences(false);
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
							dbExportDefinition.setDateFormatLocale(Locale.forLanguageTag(arguments[i]));
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
					} else if ("-createOutputDirectoyIfNotExists".equalsIgnoreCase(arguments[i])) {
						// Documented in help.txt and generated by toParamsString(), but was not accepted
						dbExportDefinition.setCreateOutputDirectoyIfNotExists(true);
						wasAllowedParam = true;
					} else if ("-replaceAlreadyExistingFiles".equalsIgnoreCase(arguments[i])) {
						// Documented in help.txt and generated by toParamsString(), but was not accepted
						dbExportDefinition.setReplaceAlreadyExistingFiles(true);
						wasAllowedParam = true;
					} else if (!(openMenu && isConnectionTestFlag)) {
						positionalParameter = true;
						if (dbExportDefinition.getDbVendor() == null) {
							dbExportDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (dbExportDefinition.getHostnameAndPort() == null && dbExportDefinition.getDbVendor() != DbVendor.SQLite && dbExportDefinition.getDbVendor() != DbVendor.Derby
								&& !(dbExportDefinition.getDbVendor() == DbVendor.HSQL && isHsqlFileDatabasePath(arguments[i]))) {
							// HSQL needs no hostname for a file database (before, its path was taken as hostname)
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
					exportFlagHandled = wasAllowedParam && !positionalParameter;
				}

				if ((openMenu || connectionTest) && i == startIndex && !exportFlagHandled) {
					if ("-iter".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for connectiontest iterations");
						} else if (!NumberUtilities.isInteger(arguments[i]) || Integer.parseInt(arguments[i]) < 0) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for connectiontest iterations");
						} else {
							connectionTestDefinition.setIterations(Integer.parseInt(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-sleep".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for connectiontest sleep time");
						} else if (!NumberUtilities.isInteger(arguments[i]) || Integer.parseInt(arguments[i]) < 0) {
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
						} else if (connectionTestDefinition.getHostnameAndPort() == null && connectionTestDefinition.getDbVendor() != DbVendor.SQLite && connectionTestDefinition.getDbVendor() != DbVendor.Derby
								&& !(connectionTestDefinition.getDbVendor() == DbVendor.HSQL && isHsqlFileDatabasePath(arguments[i]))) {
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

				if (!wasAllowedParam) {
					throw new ParameterException(arguments[i], "Invalid parameter");
				}
			}

			if (createTrustStore) {
				// The TrustStore password is optional (before, it failed with an ArrayIndexOutOfBoundsException without it)
				if (arguments.length < 2 || arguments.length > 3) {
					throw new ParameterException("createtruststore", "Expected parameters: hostname[:port] truststorefilePath [truststorepassword]");
				}
				final char[] trustStorePassword = arguments.length > 2 && Utilities.isNotEmpty(arguments[2]) ? arguments[2].toCharArray() : null;
				TrustManagerUtilities.createTrustStoreFile(arguments[0], 443, new File(arguments[1]), trustStorePassword, null);
				System.out.println();
				System.out.println("Created TrustStore in file '" + arguments[1] + "'");
				return 0;
			}

			if (openMenu) {
				if (System.console() == null) {
					System.err.println("Couldn't get Console instance for menu");
					return 1;
				}

				final ConsoleMenu mainMenu = new ConsoleMenu(APPLICATION_NAME + " (v" + VERSION.toString() + ")");
				final ExportMenu exportMenu = new ExportMenu(mainMenu);
				exportMenu.setDbExportDefinition(dbExportDefinition);
				// Both menus edit the same definition, which is used below for the connection test (-2) and the TrustStore
				// creation (-5). Before, it was only passed to them for "connectiontest"/"createtruststore", which cannot be
				// combined with "menu", so both actions were executed with an empty definition.
				final ConnectionTestMenu connectionTestMenu = new ConnectionTestMenu(mainMenu, exportMenu.getDbExportDefinition());
				connectionTestMenu.setConnectionTestDefinition(connectionTestDefinition);

				@SuppressWarnings("unused")
				final PreferencesMenu preferencesMenu = new PreferencesMenu(mainMenu, exportMenu.getDbExportDefinition());

				final CreateTrustStoreMenu createTrustStoreMenu = new CreateTrustStoreMenu(mainMenu, exportMenu.getDbExportDefinition());
				createTrustStoreMenu.setConnectionTestDefinition(connectionTestDefinition);
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
					ApplicationUpdateUtilities.executeUpdate(dbExport, DbExport.VERSIONINFO_DOWNLOAD_URL, applicationConfiguration.getProxyConfiguration(), DbExport.APPLICATION_NAME, DbExport.VERSION, DbExport.TRUSTED_UPDATE_CA_CERTIFICATES, null, null, null, false, false);
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
				// Not for a HSQL file database, which needs no password (see help)
				if (Utilities.isNotBlank(connectionTestDefinition.getUsername()) && connectionTestDefinition.getPassword() == null
						&& connectionTestDefinition.getDbVendor() != DbVendor.SQLite
						&& connectionTestDefinition.getDbVendor() != DbVendor.Derby
						&& connectionTestDefinition.getDbVendor() != DbVendor.Cassandra
						&& !(connectionTestDefinition.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(connectionTestDefinition.getHostnameAndPort()))) {
					final char[] passwordArray = new PasswordConsoleInput().withPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					connectionTestDefinition.setPassword(passwordArray);
				}

				return connectionTest(connectionTestDefinition);
			} else {
				// If started without GUI we may enter the missing password via the terminal
				// Not for a HSQL file database, which needs no password (see help)
				if (Utilities.isNotBlank(dbExportDefinition.getUsername()) && dbExportDefinition.getPassword() == null
						&& dbExportDefinition.getDbVendor() != DbVendor.SQLite
						&& dbExportDefinition.getDbVendor() != DbVendor.Derby
						&& dbExportDefinition.getDbVendor() != DbVendor.Cassandra
						&& !(dbExportDefinition.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(dbExportDefinition.getHostnameAndPort()))) {
					final char[] passwordArray = new PasswordConsoleInput().withPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
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
			System.err.println(e.getMessage() != null ? e.getMessage() : e.toString());
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
				final String outputFileName = dbExportDefinition.getOutputpath() == null ? "" : new File(dbExportDefinition.getOutputpath()).getName();
				System.out.println(worker.getConfigurationLogString(outputFileName, dbExportDefinition.getSqlStatementOrTablelist())
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

	/**
	 * Signals progress with an unknown total amount. Nothing is shown on the console.
	 */
	@Override
	public void receiveUnlimitedProgressSignal() {
		// Do nothing
	}

	/**
	 * Signals sub progress with an unknown total amount. Nothing is shown on the console.
	 */
	@Override
	public void receiveUnlimitedSubProgressSignal() {
		// Do nothing
	}

	/**
	 * Shows the progress of the export on the console (verbose mode only): a progress bar for a single statement,
	 * otherwise the number of the table being exported.
	 *
	 * @param start start time of the export
	 * @param itemsToDo amount of data lines (single statement) or tables to export
	 * @param itemsDone amount of data lines or tables exported so far
	 * @param itemsUnitSign unit sign of the amounts, or null for data items
	 */
	@Override
	public void receiveProgressSignal(final LocalDateTime start, final long itemsToDo, final long itemsDone, final String itemsUnitSign) {
		if (isVerbose()) {
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

	/**
	 * Shows the start of the export of a single table on the console.
	 *
	 * @param itemName name of the exported table
	 * @param description description of the export (unused)
	 */
	@Override
	public void receiveItemStartSignal(final String itemName, final String description) {
		if ("Scanning tables ...".equals(itemName)) {
			System.out.println(itemName);
		} else {
			System.out.println("Table " + itemName);
		}
	}

	/**
	 * Shows the progress bar of the export of a single table on the console (verbose mode only).
	 *
	 * @param itemStart start time of the export of the table
	 * @param subItemToDo amount of data lines to export from the table
	 * @param subItemDone amount of data lines exported so far
	 * @param itemsUnitSign unit sign of the amounts, or null for data lines
	 */
	@Override
	public void receiveItemProgressSignal(final LocalDateTime itemStart, final long subItemToDo, final long subItemDone, final String itemsUnitSign) {
		if (isVerbose()) {
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

	/**
	 * Shows the summary of the export of a single table on the console (verbose mode only).
	 *
	 * @param itemStart start time of the export of the table
	 * @param itemEnd end time of the export of the table
	 * @param subItemsDone amount of exported data lines
	 * @param itemsUnitSign unit sign of the amount, or null for data lines
	 * @param resultText result text of the export of the table (unused)
	 */
	@Override
	public void receiveItemDoneSignal(final LocalDateTime itemStart, final LocalDateTime itemEnd, final long subItemsDone, final String itemsUnitSign, final String resultText) {
		if (isVerbose()) {
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

	/**
	 * Shows the summary of the export on the console (verbose mode only).
	 *
	 * @param start start time of the export
	 * @param end end time of the export
	 * @param itemsDone amount of exported data lines (single statement) or tables
	 * @param itemsUnitSign unit sign of the amount, or null for data items
	 * @param resultText result text of the export (unused)
	 */
	@Override
	public void receiveDoneSignal(final LocalDateTime start, final LocalDateTime end, final long itemsDone, final String itemsUnitSign, final String resultText) {
		if (isVerbose()) {
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

	/**
	 * The worker signals are also received while no export runs (e.g. as parent of the application update),
	 * so the definition to execute may still be null.
	 */
	private boolean isVerbose() {
		return dbExportDefinitionToExecute != null && dbExportDefinitionToExecute.isVerbose();
	}

	/**
	 * Signals the cancellation of the export on the console.
	 *
	 * @return always true
	 */
	@Override
	public boolean cancel() {
		System.out.println("Canceled");
		return true;
	}

	/**
	 * Title changes are not shown on the console.
	 *
	 * @param text the new title
	 */
	@Override
	public void changeTitle(final String text) {
		// Do nothing
	}
}
