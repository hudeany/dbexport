package de.soderer.dbcsvexport;

import java.awt.GraphicsEnvironment;
import java.io.Console;
import java.io.File;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import javax.swing.SwingUtilities;

import de.soderer.dbcsvexport.worker.AbstractDbExportWorker;
import de.soderer.utilities.ApplicationUpdateHelper;
import de.soderer.utilities.BasicUpdateableConsoleApplication;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.WorkerParentDual;

/**
 * The Main-Class of DbCsvExport.
 */
public class DbCsvExport extends BasicUpdateableConsoleApplication implements WorkerParentDual {
	/** The Constant APPLICATION_NAME. */
	public static final String APPLICATION_NAME = "DbCsvExport";
	
	/** The Constant VERSION_RESOURCE_FILE, which contains version number and versioninfo download url. */
	public static final String VERSION_RESOURCE_FILE = "/version.txt";
	
	/** The Constant CONFIGURATION_FILE. */
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + ".DbCsvExport.config");
	public static final String CONFIGURATION_DRIVERLOCATIONPROPERTYNAME = "driver_location";
	
	/** The Constant SECURE_PREFERENCES_FILE. */
	public static final File SECURE_PREFERENCES_FILE = new File(System.getProperty("user.home") + File.separator + ".DbCsvExport.secpref");

	/** The version is filled in at application start from the version.txt file. */
	public static String VERSION = null;
	
	/** The versioninfo download url is filled in at application start from the version.txt file. */
	public static String VERSIONINFO_DOWNLOAD_URL = null;

	/** The usage message. */
	private static String getUsageMessage() {
		return "DbCsvExport (by Andreas Soderer, mail: dbcsvexport@soderer.de)\n"
			+ "VERSION: " + VERSION + "\n\n"
			+ "Usage: java -jar DbCsvExport.jar [-gui] [-l] [-z] [-e encoding] [-s ';'] [-q '\"'] [-i 'TAB'] [-a] [-f locale] [-blobfiles] [-clobfiles] [-beautify] [-x CSV|JSON|XML|SQL] [-n 'NULL'] dbtype hostname[:port] username dbname 'statement or list of tablepatterns' outputpath [password]\n"
			+ "Simple usage: java -jar DbCsvExport.jar dbtype hostname username dbname 'statement or list of tablepatterns' outputpath\n"
			+ "\n"
			+ "mandatory parameters\n"
			+ "\tdbtype: mysql | oracle | postgresql | firebird | sqlite | derby | hsql\n"
			+ "\thostname: with optional port (not needed for sqlite and derby)\n"
			+ "\tusername: username (not needed for sqlite and derby)\n"
			+ "\tdbname: dbname or filepath for sqlite db or derby db\n"
			+ "\tstatement or list of tablepatterns: statement, encapsulate by '\n"
			+ "\t  or a comma-separated list of tablenames with wildcards *? and !(not, before tablename)\n"
			+ "\toutputpath: file for single statement or directory for tablepatterns or 'console' for output to terminal\n"
			+ "\tpassword: is asked interactivly, if not given as parameter (not needed for sqlite and derby)\n"
			+ "\n"
			+ "optional parameters\n"
			+ "\t-gui: open a GUI\n"
			+ "\t-x exportformat: Data export format, default format is CSV\n"
			+ "\t\texportformat: CSV | JSON | XML | SQL\n"
			+ "\t\t(don't forget to beautify json for human readable data)\n"
			+ "\t-n 'NULL': set a string for null values (only for csv and xml, default is '')\n"
			+ "\t-l: log export information in .log files\n"
			+ "\t-v: progress and e.t.a. output in terminal\n"
			+ "\t-z: output as zipfile (not for console output)\n"
			+ "\t-e: encoding (default UTF-8)\n"
			+ "\t-s: separator character, default ';', encapsulate by '\n"
			+ "\t-q: string quote character, default '\"', encapsulate by '\n"
			+ "\t-i: indentation string for JSON and XML (TAB, BLANK, DOUBLEBLANK), default TAB or '\\t', encapsulate by '\n"
			+ "\t-a: always quote value\n"
			+ "\t-f: number and datetime format locale, default is systems locale, use 'de', 'en', etc. (not needed for sqlite)\n"
			+ "\t-blobfiles: create a file (.blob or .blob.zip) for each blob instead of base64 encoding\n"
			+ "\t-clobfiles: create a file (.clob or .clob.zip) for each clob instead of data in csv file\n"
			+ "\t-beautify: beautify csv output to make column values equal length (takes extra time)\n"
			+ "\t  or beautify json output to make it human readable with linebreak and indention\n"
			+ "\t-noheaders: don't export csv header line\n"
			+ "\t-structure: export the tables structure and column types\n"
			+ "\n"
			+ "global/single parameters\n"
			+ "\t-help: show this help manual\n"
			+ "\t-version: show current local version of this tool\n"
			+ "\t-update: check for online update and ask, whether an available update shell be installed\n";
	}

	/** The db csv export definition. */
	private DbCsvExportDefinition dbCsvExportDefinition;
	
	/** The worker. */
	private AbstractDbExportWorker worker;


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
			List<String> versionInfoLines = Utilities.readLines(DbCsvExport.class.getResourceAsStream(VERSION_RESOURCE_FILE), "UTF-8");
			VERSION = versionInfoLines.get(0);
			VERSIONINFO_DOWNLOAD_URL = versionInfoLines.get(1);
		} catch (Exception e) {
			// Without the version.txt file we may not go on
			System.err.println("Invalid version.txt");
			return 1;
		}

		DbCsvExportDefinition dbCsvExportDefinition = new DbCsvExportDefinition();

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
					new DbCsvExport().updateApplication();
					return 1;
				} else if ("-gui".equalsIgnoreCase(arguments[i])) {
					if (GraphicsEnvironment.isHeadless()) {
						throw new DbCsvExportException("GUI can only be shown on a non-headless environment");
					}
					dbCsvExportDefinition.setOpenGUI(true);
				} else if ("-x".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter for export format");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter for export format");
					} else {
						dbCsvExportDefinition.setExportType(arguments[i]);
					}
				} else if ("-n".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter null value string");
					} else {
						dbCsvExportDefinition.setNullValueString(arguments[i]);
					}
				} else if ("-l".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setLog(true);
				} else if ("-v".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setVerbose(true);
				} else if ("-z".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setZip(true);
				} else if ("-e".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter encoding");
					} else if (Utilities.isBlank(arguments[i])) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter encoding");
					} else {
						dbCsvExportDefinition.setEncoding(arguments[i]);
					}
				} else if ("-s".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter separator character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter separator character");
					} else {
						dbCsvExportDefinition.setSeparator(arguments[i].charAt(0));
					}
				} else if ("-q".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter stringquote character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote character");
					} else {
						dbCsvExportDefinition.setStringQuote(arguments[i].charAt(0));
					}
				} else if ("-i".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter indentation string");
					} else if (arguments[i].length() == 0) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter indentation string");
					}
					if ("TAB".equalsIgnoreCase(arguments[i])) {
						dbCsvExportDefinition.setIndentation("\t");
					} else if ("BLANK".equalsIgnoreCase(arguments[i])) {
						dbCsvExportDefinition.setIndentation(" ");
					} else if ("DOUBLEBLANK".equalsIgnoreCase(arguments[i])) {
						dbCsvExportDefinition.setIndentation("  ");
					} else {
						dbCsvExportDefinition.setIndentation(arguments[i]);
					}
				} else if ("-a".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setAlwaysQuote(true);
				} else if ("-f".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter format locale");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 2) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter format locale");
					} else {
						dbCsvExportDefinition.setDateAndDecimalLocale(new Locale(arguments[i]));
					}
				} else if ("-blobfiles".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setCreateBlobFiles(true);
				} else if ("-clobfiles".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setCreateClobFiles(true);
				} else if ("-beautify".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setBeautify(true);
				} else if ("-noheaders".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setNoHeaders(true);
				} else if ("-structure".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setExportStructure(true);
				} else {
					if (dbCsvExportDefinition.getDbVendor() == null) {
						dbCsvExportDefinition.setDbVendor(arguments[i]);
					} else if (dbCsvExportDefinition.getHostname() == null && dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite && dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvExportDefinition.setHostname(arguments[i]);
					} else if (dbCsvExportDefinition.getUsername() == null && dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite && dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvExportDefinition.setUsername(arguments[i]);
					} else if (dbCsvExportDefinition.getDbName() == null) {
						dbCsvExportDefinition.setDbName(arguments[i]);
					} else if (dbCsvExportDefinition.getSqlStatementOrTablelist() == null) {
						dbCsvExportDefinition.setSqlStatementOrTablelist(arguments[i]);
					} else if (dbCsvExportDefinition.getOutputpath() == null) {
						dbCsvExportDefinition.setOutputpath(arguments[i]);
					} else if (dbCsvExportDefinition.getPassword() == null && dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite && dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvExportDefinition.setPassword(arguments[i]);
					} else {
						throw new ParameterException(arguments[i], "Invalid parameter");
					}
				}
			}

			// If started without GUI we may enter the missing password via the terminal
			if (!dbCsvExportDefinition.isOpenGui()) {
				if (Utilities.isNotBlank(dbCsvExportDefinition.getHostname()) && dbCsvExportDefinition.getPassword() == null
						&& dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite
						&& dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
					Console console = System.console();
					if (console == null) {
						throw new Exception("Couldn't get Console instance");
					}

					char[] passwordArray = console.readPassword(LangResources.get("enterDbPassword") + ": ");
					dbCsvExportDefinition.setPassword(new String(passwordArray));
				}

				// Validdate all given parameters
				dbCsvExportDefinition.checkParameters();
				if (!new DbCsvExportDriverSupplier(null, dbCsvExportDefinition.getDbVendor()).supplyDriver()) {
					throw new Exception("Cannot aquire db driver for db vendor: " + dbCsvExportDefinition.getDbVendor());
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

		if (dbCsvExportDefinition.isOpenGui()) {
			// open the preconfigured GUI
			try {
				DbCsvExportGui dbCsvExportGui = new DbCsvExportGui(dbCsvExportDefinition);
				SwingUtilities.invokeLater(new Runnable() {
					@Override
					public void run() {
						dbCsvExportGui.setVisible(true);
					}
				});
				return -1;
			} catch (Exception e) {
				e.printStackTrace();
				return 1;
			}
		} else {
			// Start the export worker for terminal output 
			try {
				new DbCsvExport().export(dbCsvExportDefinition);
				return 0;
			} catch (DbCsvExportException e) {
				System.err.println(e.getMessage());
				return 1;
			} catch (Exception e) {
				e.printStackTrace();
				return 1;
			}
		}
	}

	/**
	 * Instantiates a new db csv export.
	 *
	 * @throws Exception the exception
	 */
	public DbCsvExport() throws Exception {
		super(APPLICATION_NAME, new Version(VERSION));
	}

	/**
	 * Export.
	 *
	 * @param dbCsvExportDefinition the db csv export definition
	 * @throws Exception the exception
	 */
	private void export(DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		this.dbCsvExportDefinition = dbCsvExportDefinition;

		try {
			worker = dbCsvExportDefinition.getConfiguredWorker(this);

			if (dbCsvExportDefinition.isVerbose()) {
				System.out.println(worker.getConfigurationLogString(new File(dbCsvExportDefinition.getOutputpath()).getName(), dbCsvExportDefinition.getSqlStatementOrTablelist()));
				System.out.println();
			}

			worker.setShowProgressAfterMilliseconds(2000);
			worker.run();

			// Get result to trigger possible Exception
			worker.get();
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
	 * @see de.soderer.utilities.WorkerParentDual#showUnlimitedSubProgress()
	 */
	@Override
	public void showUnlimitedSubProgress() {
		// Do nothing
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showProgress(java.util.Date, long, long)
	 */
	@Override
	public void showProgress(Date start, long itemsToDo, long itemsDone) {
		if (dbCsvExportDefinition.isVerbose()) {
			if (dbCsvExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
				System.out.print("\r" + Utilities.getConsoleProgressString(80, start, itemsToDo, itemsDone));
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
	public void showItemStart(String itemName) {
		if (itemName.equals("Scanning tables ...")) {
			System.out.println(itemName);
		} else {
			System.out.println("Table " + itemName);
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentDual#showItemProgress(java.util.Date, long, long)
	 */
	@Override
	public void showItemProgress(Date itemStart, long subItemToDo, long subItemDone) {
		if (dbCsvExportDefinition.isVerbose()) {
			System.out.print("\r" + Utilities.getConsoleProgressString(80, itemStart, subItemToDo, subItemDone));
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentDual#showItemDone(java.util.Date, java.util.Date, long)
	 */
	@Override
	public void showItemDone(Date itemStart, Date itemEnd, long subItemsDone) {
		if (dbCsvExportDefinition.isVerbose()) {
			System.out.print("\r" + Utilities.rightPad("Exported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(subItemsDone - 1) + " lines in " + DateUtilities.getShortHumanReadableTimespan(itemEnd.getTime() - itemStart.getTime(), false), 80));
			System.out.println();
		}
	}

	/* (non-Javadoc)
	 * @see de.soderer.utilities.WorkerParentSimple#showDone(java.util.Date, java.util.Date, long)
	 */
	@Override
	public void showDone(Date start, Date end, long itemsDone) {
		if (dbCsvExportDefinition.isVerbose()) {
			if (dbCsvExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
				System.out.print("\r" + Utilities.rightPad("Exported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsDone - 1) + " lines in " + DateUtilities.getShortHumanReadableTimespan(end.getTime() - start.getTime(), false), 80));
				System.out.println();
				System.out.println();
			} else {
				System.out.println();
				System.out.println("Done after " + DateUtilities.getShortHumanReadableTimespan(end.getTime() - start.getTime(), false));
				System.out.println();
			}
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
