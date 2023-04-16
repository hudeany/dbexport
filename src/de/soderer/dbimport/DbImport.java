package de.soderer.dbimport;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;

import de.soderer.dbimport.DbImportDefinition.DataType;
import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.dbimport.DbImportDefinition.ImportMode;
import de.soderer.dbimport.console.BlobUpdateMenu;
import de.soderer.dbimport.console.ConnectionTestMenu;
import de.soderer.dbimport.console.CreateTrustStoreMenu;
import de.soderer.dbimport.console.HelpMenu;
import de.soderer.dbimport.console.ImportMenu;
import de.soderer.dbimport.console.UpdateMenu;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
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
import de.soderer.utilities.http.HttpUtilities;
import de.soderer.utilities.worker.WorkerParentDual;

/**
 * The Main-Class of DbImport.
 */
// TODO: Invalid null in notnull column => error message
// TODO: Missing mapping for not null column => error message
// TODO: Check for string too large
// TODO: Check for int too large
// TODO: Console menu: Manage preferences
// TODO: Errorhandling on createConnection (dbconnection error detection and help messages in errors)

public class DbImport extends UpdateableConsoleApplication implements WorkerParentDual {
	/** The Constant APPLICATION_NAME. */
	public static final String APPLICATION_NAME = "DbImport";
	public static final String APPLICATION_STARTUPCLASS_NAME = "de.soderer.DbImport";

	/** The Constant VERSION_RESOURCE_FILE, which contains version number and versioninfo download url. */
	public static final String VERSION_RESOURCE_FILE = "/version.txt";

	public static final String HELP_RESOURCE_FILE = "/help.txt";

	/** The Constant CONFIGURATION_FILE. */
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + "." + APPLICATION_NAME + ".config");
	public static final String CONFIGURATION_DRIVERLOCATIONPROPERTYNAME = "driver_location";

	/** The Constant SECURE_PREFERENCES_FILE. */
	public static final File SECURE_PREFERENCES_FILE = new File(System.getProperty("user.home") + File.separator + "." + APPLICATION_NAME + ".secpref");

	/** The version is filled in at application start from the version.txt file. */
	public static Version VERSION = null;

	/** The version build time is filled in at application start from the version.txt file */
	public static LocalDateTime VERSION_BUILDTIME = null;

	/** The versioninfo download url is filled in at application start from the version.txt file. */
	public static String VERSIONINFO_DOWNLOAD_URL = null;

	/** Trusted CA certificate for updates **/
	public static String TRUSTED_UPDATE_CA_CERTIFICATE = null;

	/** The usage message. */
	private static String getUsageMessage() {
		try (InputStream helpInputStream = DbImport.class.getResourceAsStream(HELP_RESOURCE_FILE)) {
			return "DbImport (by Andreas Soderer, mail: dbimport@soderer.de)\n"
					+ "VERSION: " + VERSION.toString() + " (" + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, VERSION_BUILDTIME) + ")" + "\n\n"
					+ new String(IoUtilities.toByteArray(helpInputStream), StandardCharsets.UTF_8);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return "Help info is missing";
		}
	}

	/** The db import definition. */
	private DbImportDefinition dbImportDefinitionToExecute;

	private int previousTerminalWidth = 0;

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
		try (InputStream resourceStream = DbImport.class.getResourceAsStream(VERSION_RESOURCE_FILE)) {
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
				TRUSTED_UPDATE_CA_CERTIFICATE = versionInfoLines.get(3);
			}
		} catch (@SuppressWarnings("unused") final Exception e) {
			// Without the version.txt file we may not go on
			System.err.println("Invalid version.txt");
			return 1;
		}

		try {
			String[] arguments = args;

			boolean openGui = false;
			boolean openMenu = false;
			boolean connectionTest = false;
			boolean blobImport = false;
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
					} else if ("version".equalsIgnoreCase(arguments[i]) && arguments.length == 1) {
						System.out.println(VERSION);
						return 1;
					} else if ("update".equalsIgnoreCase(arguments[i]) && arguments.length == 1) {
						if (arguments.length > i + 2) {
							final DbImport dbImport = new DbImport();
							ApplicationUpdateUtilities.executeUpdate(dbImport, DbImport.VERSIONINFO_DOWNLOAD_URL, DbImport.APPLICATION_NAME, DbImport.VERSION, DbImport.TRUSTED_UPDATE_CA_CERTIFICATE, arguments[i + 1], arguments[i + 2].toCharArray(), null, false);
						} else if (arguments.length > i + 1) {
							final DbImport dbImport = new DbImport();
							ApplicationUpdateUtilities.executeUpdate(dbImport, DbImport.VERSIONINFO_DOWNLOAD_URL, DbImport.APPLICATION_NAME, DbImport.VERSION, DbImport.TRUSTED_UPDATE_CA_CERTIFICATE, arguments[i + 1], null, null, false);
						} else {
							final DbImport dbImport = new DbImport();
							ApplicationUpdateUtilities.executeUpdate(dbImport, DbImport.VERSIONINFO_DOWNLOAD_URL, DbImport.APPLICATION_NAME, DbImport.VERSION, DbImport.TRUSTED_UPDATE_CA_CERTIFICATE, null, null, null, false);
						}
						return 1;
					} else if ("gui".equalsIgnoreCase(arguments[i])) {
						if (GraphicsEnvironment.isHeadless()) {
							throw new DbImportException("GUI can only be shown on a non-headless environment");
						}
						openGui = true;
						if (openMenu) {
							throw new DbImportException("Only one of gui or menu can be opend at a time");
						} else if (connectionTest) {
							throw new DbImportException("Only one of gui or connection test can be used at a time");
						} else if (blobImport) {
							throw new DbImportException("Only one of gui or blob import can be used at a time");
						} else if (createTrustStore) {
							throw new DbImportException("Only one of gui or create truststore can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					} else if ("menu".equalsIgnoreCase(arguments[i])) {
						openMenu = true;
						if (openGui) {
							throw new DbImportException("Only one of menu or gui can be opend at a time");
						} else if (connectionTest) {
							throw new DbImportException("Only one of menu or connection test can be used at a time");
						} else if (blobImport) {
							throw new DbImportException("Only one of menu or blob import can be used at a time");
						} else if (createTrustStore) {
							throw new DbImportException("Only one of menu or create truststore can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					} else if ("connectiontest".equalsIgnoreCase(arguments[i])) {
						connectionTest = true;
						if (openGui) {
							throw new DbImportException("Only one of connection test or gui can be used at a time");
						} else if (openMenu) {
							throw new DbImportException("Only one of connection test or menu can be used at a time");
						} else if (blobImport) {
							throw new DbImportException("Only one of connection test or blob import can be used at a time");
						} else if (createTrustStore) {
							throw new DbImportException("Only one of connection test or create truststore can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					} else if ("importblob".equalsIgnoreCase(arguments[i])) {
						blobImport = true;
						if (openGui) {
							throw new DbImportException("Only one of blob import or gui can be used at a time");
						} else if (connectionTest) {
							throw new DbImportException("Only one of blob import or connection test can be used at a time");
						} else if (openMenu) {
							throw new DbImportException("Only one of blob import or menu can be used at a time");
						} else if (createTrustStore) {
							throw new DbImportException("Only one of blob import or create truststore can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					} else if ("createtruststore".equalsIgnoreCase(arguments[i])) {
						createTrustStore = true;
						if (openGui) {
							throw new DbImportException("Only one of create truststore or gui can be used at a time");
						} else if (connectionTest) {
							throw new DbImportException("Only one of create truststore or connection test can be used at a time");
						} else if (openMenu) {
							throw new DbImportException("Only one of create truststore or menu can be used at a time");
						} else if (blobImport) {
							throw new DbImportException("Only one of create truststore or blobImport can be used at a time");
						}
						arguments = Utilities.removeItemAtIndex(arguments, i--);
					}
				}
			}

			final DbImportDefinition dbImportDefinition = new DbImportDefinition();
			final BlobImportDefinition blobImportDefinition = new BlobImportDefinition();
			final ConnectionTestDefinition connectionTestDefinition = new ConnectionTestDefinition();

			// Read the parameters
			for (int i = 0; i < arguments.length; i++) {
				boolean wasAllowedParam = false;

				if (openGui || openMenu || (!blobImport && !connectionTest)) {
					if ("-x".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for import format");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for import format");
						} else {
							dbImportDefinition.setDataType(DataType.getFromString(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-n".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter null value string");
						} else {
							dbImportDefinition.setNullValueString(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-m".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for mapping");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for mapping");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping is already defined");
						} else {
							dbImportDefinition.setMapping(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-mf".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for mapping file");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for mapping file");
						} else if (!new File(arguments[i]).exists()) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping file does not exist");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping is already defined");
						} else {
							dbImportDefinition.setMapping(new String(Utilities.readFileToByteArray(new File(arguments[i])), StandardCharsets.UTF_8));
						}
						wasAllowedParam = true;
					} else if ("-l".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setLog(true);
						wasAllowedParam = true;
					} else if ("-v".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setVerbose(true);
						wasAllowedParam = true;
					} else if ("-e".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter encoding");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter encoding");
						} else {
							dbImportDefinition.setEncoding(Charset.forName(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-s".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter separator character");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter separator character");
						} else {
							dbImportDefinition.setSeparator(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-q".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter stringquote character");
						} else if ("null".equalsIgnoreCase(arguments[i]) || "none".equalsIgnoreCase(arguments[i])) {
							dbImportDefinition.setStringQuote(null);
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote character (use 'null' or 'none' for empty)");
						} else {
							dbImportDefinition.setStringQuote(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-qe".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter stringquote escape character");
						} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote escape character");
						} else {
							dbImportDefinition.setEscapeStringQuote(arguments[i].charAt(0));
						}
						wasAllowedParam = true;
					} else if ("-a".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setAllowUnderfilledLines(true);
						wasAllowedParam = true;
					} else if ("-r".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setRemoveSurplusEmptyTrailingColumns(true);
						wasAllowedParam = true;
					} else if ("-t".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setTrimData(true);
						wasAllowedParam = true;
					} else if ("-i".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter importmode");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter importmode");
						} else {
							dbImportDefinition.setImportMode(ImportMode.getFromString(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-d".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter duplicatemode");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter duplicatemode");
						} else {
							dbImportDefinition.setDuplicateMode(DuplicateMode.getFromString(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-u".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setUpdateNullData(false);
						wasAllowedParam = true;
					} else if ("-k".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter keycolumnslist");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter keycolumnslist");
						} else {
							dbImportDefinition.setKeycolumns(Utilities.splitAndTrimList(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-insvalues".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter valueslist");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter valueslist");
						} else {
							dbImportDefinition.setAdditionalInsertValues(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-updvalues".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter valueslist");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter valueslist");
						} else {
							dbImportDefinition.setAdditionalUpdateValues(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-create".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setCreateTable(true);
						wasAllowedParam = true;
					} else if ("-structure".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for structure file path");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for structure file path");
						} else if (!new File(arguments[i]).exists()) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Structure file file does not exist");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Structure file path is already defined");
						} else {
							dbImportDefinition.setStructureFilePath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-logerrors".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setLogErroneousData(true);
						wasAllowedParam = true;
					} else if ("-noheaders".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setNoHeaders(true);
						wasAllowedParam = true;
					} else if ("-c".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setCompleteCommit(true);
						wasAllowedParam = true;
					} else if ("-nonewindex".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setCreateNewIndexIfNeeded(false);
						wasAllowedParam = true;
					} else if ("-deactivatefk".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setDeactivateForeignKeyConstraints(true);
						wasAllowedParam = true;
					} else if ("-data".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setInlineData(true);
						wasAllowedParam = true;
					} else if ("-dp".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for data path");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for data path");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Data path is already defined");
						} else {
							dbImportDefinition.setDataPath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-sp".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter for schema file path");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for schema file path");
						} else if (!new File(arguments[i]).exists()) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Schema file file does not exist");
						} else if (Utilities.isNotBlank(dbImportDefinition.getMapping())) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Schema file path is already defined");
						} else {
							dbImportDefinition.setSchemaFilePath(arguments[i]);
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
							dbImportDefinition.setZipPassword(zipPassword.toCharArray());
						}
						wasAllowedParam = true;
					} else if ("-dbtz".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter database timezone");
						} else {
							dbImportDefinition.setDatabaseTimeZone(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-idtz".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter import data timezone");
						} else {
							dbImportDefinition.setImportDataTimeZone(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-dateFormat".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter dateFormat");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter dateFormat");
						} else {
							dbImportDefinition.setDateFormat(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-dateTimeFormat".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter dateTimeFormat");
						} else if (Utilities.isBlank(arguments[i])) {
							throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter dateTimeFormat");
						} else {
							dbImportDefinition.setDateTimeFormat(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-table".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter import table name");
						} else {
							dbImportDefinition.setTableName(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-import".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter importfilepathOrData");
						} else {
							dbImportDefinition.setImportFilePathOrData(arguments[i], true);
						}
						wasAllowedParam = true;
					} else if ("-secure".equalsIgnoreCase(arguments[i])) {
						dbImportDefinition.setSecureConnection(true);
						wasAllowedParam = true;
					} else if ("-truststore".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststore");
						} else {
							dbImportDefinition.setTrustStoreFile(new File(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-truststorepassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststorepassword");
						} else {
							dbImportDefinition.setTrustStorePassword(Utilities.isNotEmpty(arguments[i]) ? arguments[i].toCharArray() : null);
						}
						wasAllowedParam = true;
					} else {
						if (dbImportDefinition.getDbVendor() == null) {
							dbImportDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (dbImportDefinition.getHostnameAndPort() == null && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
							dbImportDefinition.setHostnameAndPort(arguments[i]);
							wasAllowedParam = true;
						} else if (dbImportDefinition.getDbName() == null) {
							dbImportDefinition.setDbName(arguments[i]);
							wasAllowedParam = true;
						} else if (dbImportDefinition.getUsername() == null && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
							dbImportDefinition.setUsername(arguments[i]);
							wasAllowedParam = true;
						} else if (dbImportDefinition.getPassword() == null && dbImportDefinition.getDbVendor() != DbVendor.SQLite && dbImportDefinition.getDbVendor() != DbVendor.Derby) {
							dbImportDefinition.setPassword(arguments[i] == null ? null : arguments[i].toCharArray());
							wasAllowedParam = true;
						}
					}
				}

				if (openMenu || blobImport) {
					if ("-updatesql".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for updatesql");
						} else if (!arguments[i - 1].contains("?")) {
							throw new ParameterException(arguments[i - 1], "Updatesql does not contain mandatory '?' placeholder");
						} else {
							blobImportDefinition.setBlobImportStatement(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-blobfile".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing parameter value for blobfile");
						} else if (!new File(arguments[i - 1]).exists()) {
							throw new ParameterException(arguments[i - 1], "Blob import file does not exist");
						} else {
							blobImportDefinition.setImportFilePath(arguments[i]);
						}
						wasAllowedParam = true;
					} else if ("-secure".equalsIgnoreCase(arguments[i])) {
						blobImportDefinition.setSecureConnection(true);
						wasAllowedParam = true;
					} else if ("-truststore".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststore");
						} else {
							blobImportDefinition.setTrustStoreFile(new File(arguments[i]));
						}
						wasAllowedParam = true;
					} else if ("-truststorepassword".equalsIgnoreCase(arguments[i])) {
						i++;
						if (i >= arguments.length) {
							throw new ParameterException(arguments[i - 1], "Missing value for parameter truststorepassword");
						} else {
							blobImportDefinition.setTrustStorePassword(Utilities.isNotEmpty(arguments[i]) ? arguments[i].toCharArray() : null);
						}
						wasAllowedParam = true;
					} else {
						if (blobImportDefinition.getDbVendor() == null) {
							blobImportDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
							wasAllowedParam = true;
						} else if (blobImportDefinition.getHostnameAndPort() == null && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
							blobImportDefinition.setHostnameAndPort(arguments[i]);
							wasAllowedParam = true;
						} else if (blobImportDefinition.getDbName() == null) {
							blobImportDefinition.setDbName(arguments[i]);
							wasAllowedParam = true;
						} else if (blobImportDefinition.getUsername() == null && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
							blobImportDefinition.setUsername(arguments[i]);
							wasAllowedParam = true;
						} else if (blobImportDefinition.getPassword() == null && blobImportDefinition.getDbVendor() != DbVendor.SQLite && blobImportDefinition.getDbVendor() != DbVendor.Derby) {
							blobImportDefinition.setPassword(arguments[i] == null ? null : arguments[i].toCharArray());
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
					HttpUtilities.createTrustStoreFile(arguments[0], 443, new File(arguments[1]), Utilities.isNotEmpty(arguments[2]) ? arguments[2].toCharArray() : null);
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

				final int consoleMenuExecutionCode = openConsoleMenu(dbImportDefinition, blobImport ? blobImportDefinition : null, connectionTestDefinition);

				if (consoleMenuExecutionCode == -1) {
					// Validate all given parameters
					dbImportDefinition.checkParameters();

					// Start the import worker for terminal output
					try {
						new DbImport().importData(dbImportDefinition);
						return 0;
					} catch (final DbImportException e) {
						System.err.println(e.getMessage());
						return 1;
					} catch (final Exception e) {
						e.printStackTrace();
						return 1;
					}
				} else if (consoleMenuExecutionCode == -2) {
					// Validate all given parameters
					blobImportDefinition.checkParameters();

					DbUtilities.updateBlob(blobImportDefinition, blobImportDefinition.getBlobImportStatement(), blobImportDefinition.getImportFilePath());
					return 0;
				} else if (consoleMenuExecutionCode == -3) {
					// Validate all given parameters
					connectionTestDefinition.checkParameters();

					return connectionTest(connectionTestDefinition);
				} else if (consoleMenuExecutionCode == -4) {
					// Update application
					final DbImport dbImport = new DbImport();
					ApplicationUpdateUtilities.executeUpdate(dbImport, DbImport.VERSIONINFO_DOWNLOAD_URL, DbImport.APPLICATION_NAME, DbImport.VERSION, DbImport.TRUSTED_UPDATE_CA_CERTIFICATE, null, null, null, false);
					return 0;
				} else if (consoleMenuExecutionCode == -5) {
					// Create TrustStore
					HttpUtilities.createTrustStoreFile(connectionTestDefinition.getHostnameAndPort(), 443, connectionTestDefinition.getTrustStoreFile(), connectionTestDefinition.getTrustStorePassword());
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
					final DbImportGui dbImportGui = new DbImportGui(dbImportDefinition);
					SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							dbImportGui.setVisible(true);
						}
					});
					return -1;
				} catch (final Exception e) {
					e.printStackTrace();
					return 1;
				}
			} else if (blobImport) {
				// If started without GUI we may enter the missing password via the terminal
				if (Utilities.isNotBlank(blobImportDefinition.getUsername()) && blobImportDefinition.getPassword() == null
						&& blobImportDefinition.getDbVendor() != DbVendor.SQLite
						&& blobImportDefinition.getDbVendor() != DbVendor.Derby
						&& blobImportDefinition.getDbVendor() != DbVendor.Cassandra) {
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					blobImportDefinition.setPassword(passwordArray);
				}

				DbUtilities.updateBlob(blobImportDefinition, blobImportDefinition.getBlobImportStatement(), blobImportDefinition.getImportFilePath());
				return 0;
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
				if (Utilities.isNotBlank(dbImportDefinition.getUsername()) && dbImportDefinition.getPassword() == null
						&& dbImportDefinition.getDbVendor() != DbVendor.SQLite
						&& dbImportDefinition.getDbVendor() != DbVendor.Derby
						&& dbImportDefinition.getDbVendor() != DbVendor.Cassandra) {
					final char[] passwordArray = new PasswordConsoleInput().setPrompt(LangResources.get("enterDbPassword") + ": ").readInput();
					dbImportDefinition.setPassword(passwordArray);
				}

				// Validate all given parameters
				dbImportDefinition.checkParameters();

				// Start the import worker for terminal output
				try {
					new DbImport().importData(dbImportDefinition);
					return 0;
				} catch (final DbImportException e) {
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
	 * Instantiates a new db csv import.
	 *
	 * @throws Exception the exception
	 */
	public DbImport() throws Exception {
		super(APPLICATION_NAME, VERSION);
	}

	/**
	 * Import.
	 *
	 * @param dbImportDefinitionToExecute the db csv import definition
	 * @throws Exception the exception
	 */
	private void importData(final DbImportDefinition dbImportDefinition) throws Exception {
		dbImportDefinitionToExecute = dbImportDefinition;

		List<File> filesToImport;
		if (!dbImportDefinitionToExecute.isInlineData()) {
			if (dbImportDefinitionToExecute.getImportFilePathOrData().contains("?") || dbImportDefinitionToExecute.getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(dbImportDefinitionToExecute.getImportFilePathOrData().lastIndexOf("/"), dbImportDefinitionToExecute.getImportFilePathOrData().lastIndexOf("\\"));
				String directoryPath = dbImportDefinitionToExecute.getImportFilePathOrData().substring(0, lastSeparator);
				directoryPath = Utilities.replaceUsersHome(directoryPath);
				final String filePattern = dbImportDefinitionToExecute.getImportFilePathOrData().substring(lastSeparator + 1);
				if (directoryPath.contains("?") || directoryPath.contains("*")) {
					throw new DbImportException("Import directory path contains wildcards, but wildcards only allowed for filenames: " + (directoryPath));
				} else if (!new File(directoryPath).exists()) {
					throw new DbImportException("Import path does not exist: " + (directoryPath));
				} else if (!new File((directoryPath)).isDirectory()) {
					throw new DbImportException("Import path is not a directory: " + (directoryPath));
				} else {
					filesToImport = FileUtilities.getFilesByPattern(new File(directoryPath), filePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*"), false);
					if (filesToImport.size() == 0) {
						throw new DbImportException("Import file pattern has no matching files: " + (directoryPath));
					}
				}
			} else {
				if (!new File(dbImportDefinitionToExecute.getImportFilePathOrData()).exists()) {
					throw new DbImportException("Import file does not exist: " + (dbImportDefinitionToExecute.getImportFilePathOrData()));
				} else if (new File(dbImportDefinitionToExecute.getImportFilePathOrData()).isDirectory()) {
					throw new DbImportException("Import path is a directory: " + (dbImportDefinitionToExecute.getImportFilePathOrData()));
				} else {
					filesToImport = new ArrayList<>();
					filesToImport.add(new File(dbImportDefinitionToExecute.getImportFilePathOrData()));
				}
			}

			if (filesToImport.size() == 0) {
				throw new Exception("No files for import were found");
			} else if (filesToImport.size() == 1) {
				final File fileToImport = filesToImport.get(0);
				String tableName = dbImportDefinitionToExecute.getTableName();
				if ("*".equals(tableName)) {
					tableName = fileToImport.getName();
					if (tableName.toLowerCase().endsWith(".zip")) {
						tableName = tableName.substring(0, tableName.length() - 4);
					}
					if (tableName.contains(".")) {
						tableName = tableName.substring(0, tableName.indexOf("."));
					}
				}
				System.out.println("Importing file '" + fileToImport.getAbsolutePath() + "' into table '"+ tableName + "'");
				importFileOrData(dbImportDefinitionToExecute, tableName, fileToImport.getAbsolutePath());
			} else {
				multiImportFiles(dbImportDefinitionToExecute, filesToImport, dbImportDefinitionToExecute.getTableName());
			}
		} else {
			importFileOrData(dbImportDefinitionToExecute, dbImportDefinitionToExecute.getTableName(), dbImportDefinitionToExecute.getImportFilePathOrData());
		}
	}

	private void importFileOrData(final DbImportDefinition dbImportDefinition, final String tableNameForImport, final String filePathOrImportData) throws Exception, ExecutionException {
		dbImportDefinitionToExecute = dbImportDefinition;

		try {
			final DbImportWorker worker = dbImportDefinitionToExecute.getConfiguredWorker(this, false, tableNameForImport, filePathOrImportData);
			if (dbImportDefinitionToExecute.isVerbose()) {
				System.out.println(worker.getConfigurationLogString());
				System.out.println();
			}

			worker.setProgressDisplayDelayMilliseconds(2000);
			worker.run();

			// Get result to trigger possible Exception
			worker.get();

			// Only show errors. Other statistics are kept in log file if verbose was set
			if (worker.getNotImportedItems().size() > 0) {
				String errorText = "Not imported items (Number of Errors): " + worker.getNotImportedItems().size() + "\n";
				if (worker.getNotImportedItems().size() > 0) {
					final List<String> errorList = new ArrayList<>();
					for (int i = 0; i < Math.min(10, worker.getNotImportedItems().size()); i++) {
						errorList.add(Integer.toString(worker.getNotImportedItems().get(i)) + ": " + worker.getNotImportedItemsReasons().get(i));
					}
					if (worker.getNotImportedItems().size() > 10) {
						errorList.add("...");
					}
					errorText += "Not imported items indices: \n" + Utilities.join(errorList, " \n") + "\n";
				}
				System.err.println(errorText);
			}

			if (worker.getCreatedNewIndexName() != null) {
				System.out.println("Created new index name: " + worker.getCreatedNewIndexName());
			}

			System.out.println("Imported items: " + worker.getImportedItems());

			if (dbImportDefinitionToExecute.isVerbose()) {
				System.out.println(LangResources.get("importeddataamount") + ": " + Utilities.getHumanReadableNumber(worker.getImportedDataAmount(), "Byte", false, 5, false, Locale.getDefault()));
			}

			System.out.println();
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

	private void multiImportFiles(final DbImportDefinition dbImportDefinition, final List<File> filesToImport, final String tableName) throws Exception {
		try {
			final DbImportMultiWorker worker = new DbImportMultiWorker(dbImportDefinition, filesToImport, tableName);

			worker.setParent(this);

			worker.setProgressDisplayDelayMilliseconds(2000);
			worker.run();

			// Get result to trigger possible Exception
			worker.get();

			System.out.println();
			System.out.println("End of multiple file import.\nImported files: " + worker.getItemsDone());

			if (dbImportDefinitionToExecute.isVerbose()) {
				System.out.println(worker.getResult());
			}

			System.out.println();
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
			@SuppressWarnings("resource")
			Connection testConnection = null;
			try {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Creating db connection");
				if (connectionTestDefinition.getDbVendor() == DbVendor.Derby || (connectionTestDefinition.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(connectionTestDefinition.getHostnameAndPort())) || connectionTestDefinition.getDbVendor() == DbVendor.SQLite) {
					try {
						testConnection = DbUtilities.createConnection(connectionTestDefinition, false);
					} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
						testConnection = DbUtilities.createNewDatabase(connectionTestDefinition.getDbVendor(), connectionTestDefinition.getDbName());
					}
				} else {
					testConnection = DbUtilities.createConnection(connectionTestDefinition, false);
				}

				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Successfully created db connection");

				if (Utilities.isNotBlank(connectionTestDefinition.getCheckStatement())) {
					try (Statement statement = testConnection.createStatement()) {
						String statementString = connectionTestDefinition.getCheckStatement();
						if ("check".equalsIgnoreCase(statementString)) {
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
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": SQL-Error creating db connection: " + sqle.getMessage() + " (" + sqle.getErrorCode() + " / " + sqle.getSQLState() + ")");
				returnCode = 1;
			} catch (final Exception e) {
				System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Error creating db connection: " + e.getClass().getSimpleName() + ":" + e.getMessage());
				e.printStackTrace();
				returnCode = 1;
			} finally {
				if (testConnection != null) {
					try {
						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Closing db connection");
						testConnection.close();
					} catch (final SQLException e) {
						System.out.println(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + ": Error closing db connection: " + e.getMessage());
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
	 * @see de.soderer.utilities.WorkerParentSimple#showProgress(java.util.Date, long, long)
	 */
	@Override
	public void receiveProgressSignal(final LocalDateTime start, final long itemsToDo, final long itemsDone) {
		if (dbImportDefinitionToExecute.isVerbose() && !"*".equals(dbImportDefinitionToExecute.getTableName())) {
			printProgressBar(start, itemsToDo, itemsDone);
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showDone(java.util.Date, java.util.Date, long)
	 */
	@Override
	public void receiveDoneSignal(final LocalDateTime start, final LocalDateTime end, final long itemsDone) {
		if (dbImportDefinitionToExecute.isVerbose()) {
			int currentTerminalWidth;
			try {
				currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
			} catch (@SuppressWarnings("unused") final Exception e) {
				currentTerminalWidth = 80;
			}
			if ("*".equals(dbImportDefinitionToExecute.getTableName())) {
				System.out.println(Utilities.rightPad("Imported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsDone) + " files in " + DateUtilities.getShortHumanReadableTimespan(Duration.between(start, end), false, false), currentTerminalWidth));
			} else {
				System.out.println(Utilities.rightPad("Imported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsDone - 1) + " lines in " + DateUtilities.getShortHumanReadableTimespan(Duration.between(start, end), false, false), currentTerminalWidth));
			}
			System.out.println();
			System.out.println();
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
		// do nothing
	}

	private static int openConsoleMenu(final DbImportDefinition dbImportDefinition, final BlobImportDefinition blobImportDefinition, final ConnectionTestDefinition connectionTestDefinition) {
		try {
			final ConsoleMenu mainMenu = new ConsoleMenu(APPLICATION_NAME + " (v" + VERSION.toString() + ")");
			final ImportMenu importMenu = new ImportMenu(mainMenu);
			importMenu.setDbImportDefinition(dbImportDefinition);
			final BlobUpdateMenu blobUpdateMenu = new BlobUpdateMenu(mainMenu, importMenu.getDbImportDefinition());
			if (blobImportDefinition != null) {
				blobUpdateMenu.setBlobImportDefinition(blobImportDefinition);
			}
			final ConnectionTestMenu connectionTestMenu = new ConnectionTestMenu(mainMenu, importMenu.getDbImportDefinition());
			connectionTestMenu.setConnectionTestDefinition(connectionTestDefinition);
			final CreateTrustStoreMenu createTrustStoreMenu = new CreateTrustStoreMenu(mainMenu, importMenu.getDbImportDefinition());
			createTrustStoreMenu.setConnectionTestDefinition(connectionTestDefinition);
			@SuppressWarnings("unused")
			final ConsoleMenu updateMenu = new UpdateMenu(mainMenu);
			@SuppressWarnings("unused")
			final HelpMenu helpMenu = new HelpMenu(mainMenu);

			final int returnValue = mainMenu.show();

			if (returnValue == -1) {
				// Execute the defined import
				return -1;
			} else if (returnValue == -2) {
				// Connection test
				return -2;
			} else if (returnValue == -3) {
				// Blob import
				return -3;
			} else if (returnValue == -4) {
				// Update application
				return -4;
			} else if (returnValue == -5) {
				// create TrustStore
				return -5;
			} else {
				System.out.println();
				System.out.println("Bye");
				System.out.println();
				System.exit(returnValue);
				return 0;
			}
		} catch (final Exception e) {
			e.printStackTrace();
			System.exit(1);
			return 1;
		}
	}

	@Override
	public void receiveUnlimitedSubProgressSignal() {
		// Do nothing
	}

	@Override
	public void receiveItemStartSignal(final String itemName, final String description) {
		System.out.println("Importing " + itemName);
		if (dbImportDefinitionToExecute.isVerbose()) {
			System.out.println(description);
		}
	}

	@Override
	public void receiveItemProgressSignal(final LocalDateTime itemStart, final long subItemToDo, final long subItemDone) {
		if (dbImportDefinitionToExecute.isVerbose() && subItemToDo > 0) {
			printProgressBar(itemStart, subItemToDo, subItemDone);
		}
	}

	private void printProgressBar(final LocalDateTime itemStart, final long subItemToDo, final long subItemDone) {
		if (ConsoleUtilities.getConsoleType() == ConsoleType.ANSI) {
			int currentTerminalWidth;
			try {
				currentTerminalWidth = ConsoleUtilities.getTerminalSize().getWidth();
			} catch (@SuppressWarnings("unused") final Exception e) {
				currentTerminalWidth = 80;
			}

			ConsoleUtilities.saveCurrentCursorPosition();

			if (currentTerminalWidth < previousTerminalWidth) {
				System.out.print("\r" + Utilities.repeat(" ", currentTerminalWidth));
			}
			previousTerminalWidth = currentTerminalWidth;

			ConsoleUtilities.moveCursorToSavedPosition();

			System.out.print(ConsoleUtilities.getConsoleProgressString(currentTerminalWidth - 1, itemStart, subItemToDo, subItemDone));

			ConsoleUtilities.moveCursorToSavedPosition();
		} else if (ConsoleUtilities.getConsoleType() == ConsoleType.TEST) {
			System.out.print(ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, subItemToDo, subItemDone) + "\n");
		} else {
			System.out.print("\r" + ConsoleUtilities.getConsoleProgressString(80 - 1, itemStart, subItemToDo, subItemDone) + "\r");
		}
	}

	@Override
	public void receiveItemDoneSignal(final LocalDateTime itemStart, final LocalDateTime itemEnd, final long subItemsDone) {
		if (dbImportDefinitionToExecute.isVerbose()) {
			printProgressBar(itemStart, subItemsDone, subItemsDone);
			System.out.println("End (" + subItemsDone + " data items done in " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(itemStart, itemEnd), true) + ")");
			System.out.println();
		}
	}
}
