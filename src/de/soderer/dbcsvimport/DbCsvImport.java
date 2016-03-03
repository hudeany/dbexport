package de.soderer.dbcsvimport;


import java.awt.GraphicsEnvironment;
import java.io.Console;
import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;

import de.soderer.dbcsvimport.DbCsvImportDefinition.DataType;
import de.soderer.dbcsvimport.DbCsvImportDefinition.ImportMode;
import de.soderer.dbcsvimport.worker.AbstractDbImportWorker;
import de.soderer.utilities.ApplicationUpdateHelper;
import de.soderer.utilities.BasicUpdateableConsoleApplication;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.WorkerParentSimple;

/**
 * The Main-Class of DbCsvImport.
 */
public class DbCsvImport extends BasicUpdateableConsoleApplication implements WorkerParentSimple {
	/** The Constant APPLICATION_NAME. */
	public static final String APPLICATION_NAME = "DbCsvImport";
	
	/** The Constant VERSION_RESOURCE_FILE, which contains version number and versioninfo download url. */
	public static final String VERSION_RESOURCE_FILE = "/version.txt";
	
	/** The Constant CONFIGURATION_FILE. */
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + ".DbCsvImport.config");
	public static final String CONFIGURATION_DRIVERLOCATIONPROPERTYNAME = "driver_location";
	
	/** The Constant SECURE_PREFERENCES_FILE. */
	public static final File SECURE_PREFERENCES_FILE = new File(System.getProperty("user.home") + File.separator + ".DbCsvImport.secpref");

	/** The version is filled in at application start from the version.txt file. */
	public static String VERSION = null;
	
	/** The versioninfo download url is filled in at application start from the version.txt file. */
	public static String VERSIONINFO_DOWNLOAD_URL = null;

	/** The usage message. */
	private static String getUsageMessage() {
		return "DbCsvImport (by Andreas Soderer, mail: dbcsvimport@soderer.de)\n"
			+ "VERSION: " + VERSION + "\n\n"
			+ "Usage: java -jar DbCsvImport.jar [-gui] [optional parameters] dbtype hostname[:port] username dbname tablename [-data] importfilepathOrData [password]\n"
			+ "\n"
			+ "mandatory parameters\n"
			+ "\tdbtype: mysql | oracle | postgresql | firebird | sqlite | derby | hsql\n"
			+ "\thostname: with optional port (not needed for sqlite and derby)\n"
			+ "\tusername: username (not needed for sqlite and derby)\n"
			+ "\tdbname: dbname or filepath for sqlite db or derby db\n"
			+ "\ttablename: table to import to\n"
			+ "\timportfilepathOrData: file to import, maybe zipped (.zip) or data as text\n"
			+ "\tpassword: is asked interactivly, if not given as parameter (not needed for sqlite and derby)\n"
			+ "\n"
			+ "optional parameters\n"
			+ "\t-data: Declare importfilepathOrData explicitly as inline data (no filepath)\n"
			+ "\t-x importDataFormat: Data import format, default format is CSV\n"
			+ "\t\timportDataFormat: CSV | JSON | XML | SQL\n"
			+ "\t-m: column mappings (separated by semicolon or linebreak): when not configured a simple mapping by column names is used\n"
			+ "\t\tMapping entry format: dbcolumnname=\"data column name\" <formatinfo>\n"
			+ "\t\t<formatinfo> may be decimal delimiter (default .), date pattern (default dd.MM.yyyy HH:mm:ss) or file\n"
			+"\t\tExample: 'db1=\"def 1\" ,;db2=\"def 2\" .;db3=\"def 3\" dd.MM.yyyy HH:mm:ss;db4=\"def 4\" file'\n"
			+ "\t-mf: column mapping file, containing the mapping entries of -m\n"
			+ "\t-n 'NULL': set a string for null values (only for csv and xml, default is '')\n"
			+ "\t-l: log import information in .log files\n"
			+ "\t-v: progress and e.t.a. import in terminal\n"
			+ "\t-e: encoding for CSV and JSON data files and clob files (default UTF-8)\n"
			+ "\t-s: separator character, default ';', encapsulate by '\n"
			+ "\t-q: string quote character, default '\"', encapsulate by '\n"
			+ "\t-noheaders: first csv line is data and not headers\n"
			+ "\t-c: complete commit only (takes more time and makes rollback on any error)\n"
			+ "\t-a: allow underfilled lines\n"
			+ "\t-t: trim data values\n"
			+ "\t-i 'importmode': importmodes: CLEARINSERT | INSERT | UPDATE | UPSERT\n"
			+ "\t-u: don't update with null values from import data\n"
			+ "\t-k 'keycolumnslist': keycolumns list comma separated\n"
			+ "\t-insvalues 'valuelist': value list semicolon separated: Sometimes values not included in the data file are needed for inserts. E.g.: id=test_seq.NEXTVAL;flag='abc'\n"
			+ "\t-updvalues 'valuelist': value list semicolon separated: Sometimes values not included in the data file are needed for updates. E.g.: create=current_timestamp;flag='abc'\n"
			+ "\t-create: scan data and create suitable table, if not exists\n"
			+ "\t-logerrors: log error data items in file\n"
			+ "\n"
			+ "global/single parameters\n"
			+ "\t-help: show this help manual\n"
			+ "\t-gui: open a GUI\n"
			+ "\t-version: show current local version of this tool\n"
			+ "\t-update: check for online update and ask, whether an available update shell be installed\n";
	}

	/** The db csv import definition. */
	private DbCsvImportDefinition dbCsvImportDefinition;
	
	/** The worker. */
	private AbstractDbImportWorker worker;

	/**
	 * The main method.
	 *
	 * @param arguments the arguments
	 */
	public static void main(String[] arguments) {
		int returnCode = _main(arguments);
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
	protected static int _main(String[] arguments) {
		try {
			// Try to fill the version and versioninfo download url
			List<String> versionInfoLines = Utilities.readLines(DbCsvImport.class.getResourceAsStream(VERSION_RESOURCE_FILE), "UTF-8");
			VERSION = versionInfoLines.get(0);
			VERSIONINFO_DOWNLOAD_URL = versionInfoLines.get(1);
		} catch (Exception e) {
			// Without the version.txt file we may not go on
			System.err.println("Invalid version.txt");
			return 1;
		}

		DbCsvImportDefinition dbCsvImportDefinition = new DbCsvImportDefinition();

		try {
			if (arguments.length == 0) {
				// If started without any parameter we check for headless mode and show the usage help or the GUI
				if (GraphicsEnvironment.isHeadless()) {
					System.out.println(getUsageMessage());
					return 1;
				} else {
					arguments = new String[] { "-gui" };
				}
			}

			// Read the parameters
			for (int i = 0; i < arguments.length; i++) {
				if ("-help".equalsIgnoreCase(arguments[i]) || "--help".equalsIgnoreCase(arguments[i]) || "-h".equalsIgnoreCase(arguments[i]) || "--h".equalsIgnoreCase(arguments[i])
						|| "-?".equalsIgnoreCase(arguments[i]) || "--?".equalsIgnoreCase(arguments[i])) {
					System.out.println(getUsageMessage());
					return 1;
				} else if ("-version".equalsIgnoreCase(arguments[i])) {
					System.out.println(VERSION);
					return 1;
				} else if ("-update".equalsIgnoreCase(arguments[i])) {
					new DbCsvImport().updateApplication();
					return 1;
				} else if ("-gui".equalsIgnoreCase(arguments[i])) {
					if (GraphicsEnvironment.isHeadless()) {
						throw new DbCsvImportException("GUI can only be shown on a non-headless environment");
					}
					dbCsvImportDefinition.setOpenGui(true);
				} else if ("-x".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter for import format");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for import format");
					} else {
						dbCsvImportDefinition.setDataType(DataType.getFromString(arguments[i]));
					}
				} else if ("-n".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter null value string");
					} else {
						dbCsvImportDefinition.setNullValueString(arguments[i]);
					}
				} else if ("-m".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter for mapping");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for mapping");
					} else if (Utilities.isNotBlank(dbCsvImportDefinition.getMapping())) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping is already defined");
					} else {
						dbCsvImportDefinition.setMapping(arguments[i]);
					}
				} else if ("-mf".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter for mapping file");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for mapping file");
					} else if (!new File(arguments[i]).exists()) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping file does not exist");
					} else if (Utilities.isNotBlank(dbCsvImportDefinition.getMapping())) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Mapping is already defined");
					} else {
						dbCsvImportDefinition.setMapping(new String(Utilities.readFileToByteArray(new File(arguments[i])), "UTF-8"));
					}
				} else if ("-l".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setLog(true);
				} else if ("-v".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setVerbose(true);
				} else if ("-e".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter encoding");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter encoding");
					} else {
						dbCsvImportDefinition.setEncoding(arguments[i]);
					}
				} else if ("-s".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter separator character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter separator character");
					} else {
						dbCsvImportDefinition.setSeparator(arguments[i].charAt(0));
					}
				} else if ("-q".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter stringquote character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote character");
					} else {
						dbCsvImportDefinition.setStringQuote(arguments[i].charAt(0));
					}
				} else if ("-a".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setAllowUnderfilledLines(true);
				} else if ("-t".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setTrimData(true);
				} else if ("-i".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter importmode");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter importmode");
					} else {
						dbCsvImportDefinition.setImportmode(ImportMode.getFromString(arguments[i]));
					}
				} else if ("-u".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setUpdateNullData(false);
				} else if ("-k".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter keycolumnslist");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter keycolumnslist");
					} else {
						dbCsvImportDefinition.setKeycolumns(Utilities.splitAndTrimList(arguments[i]));
					}
				} else if ("-insvalues".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter valueslist");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter valueslist");
					} else {
						dbCsvImportDefinition.setAdditionalInsertValues(arguments[i]);
					}
				} else if ("-updvalues".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter valueslist");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter valueslist");
					} else {
						dbCsvImportDefinition.setAdditionalUpdateValues(arguments[i]);
					}
				} else if ("-create".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setCreateTable(true);
				} else if ("-logerrors".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setLogErrorneousData(true);
				} else if ("-noheaders".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setNoHeaders(true);
				} else if ("-c".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setCompleteCommit(true);
				} else if ("-data".equalsIgnoreCase(arguments[i])) {
					dbCsvImportDefinition.setInlineData(true);
				} else {
					if (dbCsvImportDefinition.getDbVendor() == null) {
						dbCsvImportDefinition.setDbVendor(DbVendor.getDbVendorByName(arguments[i]));
					} else if (dbCsvImportDefinition.getHostname() == null && dbCsvImportDefinition.getDbVendor() != DbVendor.SQLite && dbCsvImportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvImportDefinition.setHostname(arguments[i]);
					} else if (dbCsvImportDefinition.getUsername() == null && dbCsvImportDefinition.getDbVendor() != DbVendor.SQLite && dbCsvImportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvImportDefinition.setUsername(arguments[i]);
					} else if (dbCsvImportDefinition.getDbName() == null) {
						dbCsvImportDefinition.setDbName(arguments[i]);
					} else if (dbCsvImportDefinition.getTableName() == null) {
						dbCsvImportDefinition.setTableName(arguments[i]);
					} else if (dbCsvImportDefinition.getImportFilePathOrData() == null) {
						dbCsvImportDefinition.setImportFilePathOrData(arguments[i]);
					} else if (dbCsvImportDefinition.getPassword() == null && dbCsvImportDefinition.getDbVendor() != DbVendor.SQLite && dbCsvImportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvImportDefinition.setPassword(arguments[i]);
					} else {
						throw new ParameterException(arguments[i], "Invalid parameter");
					}
				}
			}

			// If started without GUI we may enter the missing password via the terminal
			if (!dbCsvImportDefinition.isOpenGui()) {
				if (Utilities.isNotBlank(dbCsvImportDefinition.getHostname()) && dbCsvImportDefinition.getPassword() == null
						&& dbCsvImportDefinition.getDbVendor() != DbVendor.SQLite
						&& dbCsvImportDefinition.getDbVendor() != DbVendor.Derby) {
					Console console = System.console();
					if (console == null) {
						throw new Exception("Couldn't get Console instance");
					}

					char[] passwordArray = console.readPassword(LangResources.get("enterDbPassword") + ": ");
					dbCsvImportDefinition.setPassword(new String(passwordArray));
				}

				// Validdate all given parameters
				dbCsvImportDefinition.checkParameters();
				if (!new DbCsvImportDriverSupplier(null, dbCsvImportDefinition.getDbVendor()).supplyDriver()) {
					throw new Exception("Cannot aquire db driver for db vendor: " + dbCsvImportDefinition.getDbVendor());
				}
			}
		} catch (ParameterException e) {
			System.err.println(e.getMessage());
			System.err.println();
			System.err.println(getUsageMessage());
			return 1;
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return 1;
		}

		if (dbCsvImportDefinition.isOpenGui()) {
			// open the preconfigured GUI
			try {
				DbCsvImportGui dbCsvImportGui = new DbCsvImportGui(dbCsvImportDefinition);
				SwingUtilities.invokeLater(new Runnable() {
					@Override
					public void run() {
						dbCsvImportGui.setVisible(true);
					}
				});
				return -1;
			} catch (Exception e) {
				e.printStackTrace();
				return 1;
			}
		} else {
			// Start the import worker for terminal output 
			try {
				new DbCsvImport().importData(dbCsvImportDefinition);
				return 0;
			} catch (DbCsvImportException e) {
				System.err.println(e.getMessage());
				return 1;
			} catch (Exception e) {
				e.printStackTrace();
				return 1;
			}
		}
	}

	/**
	 * Instantiates a new db csv import.
	 *
	 * @throws Exception the exception
	 */
	public DbCsvImport() throws Exception {
		super(APPLICATION_NAME, new Version(VERSION));
	}

	/**
	 * Import.
	 *
	 * @param dbCsvImportDefinition the db csv import definition
	 * @throws Exception the exception
	 */
	private void importData(DbCsvImportDefinition dbCsvImportDefinition) throws Exception {
		this.dbCsvImportDefinition = dbCsvImportDefinition;

		try {
			worker = dbCsvImportDefinition.getConfiguredWorker(this);

			if (dbCsvImportDefinition.isVerbose()) {
				System.out.println(worker.getConfigurationLogString());
				System.out.println();
			}

			worker.setShowProgressAfterMilliseconds(2000);
			worker.run();

			// Get result to trigger possible Exception
			worker.get();
			
			// Only show errors. Other statistics are kept in log file if verbose was set
			if (worker.getNotImportedItems().size() > 0) {
				String errorText = "Not imported items (Number of Errors): " + worker.getNotImportedItems().size() + "\n";
				if (worker.getNotImportedItems().size() > 0) {
					List<String> errorList = new ArrayList<String>();
					for (int i = 0; i < Math.min(10, worker.getNotImportedItems().size()); i++) {
						errorList.add(Integer.toString(worker.getNotImportedItems().get(i)));
					}
					if (worker.getNotImportedItems().size() > 10) {
						errorList.add("...");
					}
					errorText += "Not imported items indices: " + Utilities.join(errorList, ", ") + "\n";
				}
				System.err.println(errorText);
			}
			
			System.out.println("Imported items: " + worker.getImportedItems());
			
			if (dbCsvImportDefinition.isVerbose()) {
				System.out.println(LangResources.get("importeddataamount") + ": " + Utilities.getHumanReadableNumber(worker.getImportedDataAmount(), "Byte"));
			}
		} catch (ExecutionException e) {
			if (e.getCause() instanceof Exception) {
				throw (Exception) e.getCause();
			} else {
				throw e;
			}
		} catch (Exception e) {
			throw e;
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showUnlimitedProgress()
	 */
	@Override
	public void showUnlimitedProgress() {
		// Do nothing
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showProgress(java.util.Date, long, long)
	 */
	@Override
	public void showProgress(Date start, long itemsToDo, long itemsDone) {
		if (dbCsvImportDefinition.isVerbose()) {
			System.out.print("\r" + Utilities.getConsoleProgressString(80, start, itemsToDo, itemsDone));
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showDone(java.util.Date, java.util.Date, long)
	 */
	@Override
	public void showDone(Date start, Date end, long itemsDone) {
		if (dbCsvImportDefinition.isVerbose()) {
			System.out.print("\r" + Utilities.rightPad("Imported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsDone - 1) + " lines in " + DateUtilities.getShortHumanReadableTimespan(end.getTime() - start.getTime(), false), 80));
			System.out.println();
			System.out.println();
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#cancel()
	 */
	@Override
	public void cancel() {
		System.out.println("Canceled");
	}

	/**
	 * Update application.
	 *
	 * @throws Exception the exception
	 */
	private void updateApplication() throws Exception {
		new ApplicationUpdateHelper(APPLICATION_NAME, VERSION, VERSIONINFO_DOWNLOAD_URL, this, null).executeUpdate();
	}
}
