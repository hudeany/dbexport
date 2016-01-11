package de.soderer.dbcsvexport;

import java.io.Console;
import java.io.File;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import de.soderer.utilities.ApplicationUpdateHelper;
import de.soderer.utilities.BasicUpdateableConsoleApplication;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.WorkerParentDual;

public class DbCsvExport extends BasicUpdateableConsoleApplication implements WorkerParentDual {
	public static final String VERSION = "3.2.1";
	public static final String APPLICATION_NAME = "DbCsvExport";
	public static final String VERSIONINFO_DOWNLOAD_URL = "http://downloads.sourceforge.net/project/dbcsvexport/Versions.xml?r=&ts=<time_seconds>&use_mirror=master";
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + ".DbCsvExport.config");

	private static String USAGE_MESSAGE = "DbCsvExport (by Andreas Soderer, mail: dbcsvexport@soderer.de)\n"
			+ "VERSION: " + VERSION + "\n\n"
			+ "Usage: java -jar DbCsvExport.jar [-gui] [-l] [-z] [-e encoding] [-s ';'] [-q '\"'] [-a] [-f locale] [-blobfiles] [-clobfiles] [-beautify] dbtype hostname[:port] username dbname 'statement or list of tablepatterns' outputpath [password]\n"
			+ "Simple usage: java -jar DbCsvExport.jar dbtype hostname username dbname 'statement or list of tablepatterns' outputpath\n"
			+ "\n"
			+ "mandatory parameters\n"
			+ "\tdbtype: mysql, oracle or postgresql\n"
			+ "\thostname: with optional port\n"
			+ "\tusername: username\n"
			+ "\tdbname: dbname\n"
			+ "\tstatement or list of tablepatterns: statement, encapsulate by '\n"
			+ "\t  or a comma-separated list of tablenames with wildcards *? and !(not, before tablename)\n"
			+ "\toutputpath: file for single statement or directory for tablepatterns or 'console' for output to terminal\n"
			+ "\tpassword: is asked interactive, if not given as parameter\n"
			+ "\n"
			+ "optional parameters\n"
			+ "\t-gui: open a GUI\n"
			+ "\t-l: log in file\n"
			+ "\t-v: progress output in terminal\n"
			+ "\t-z: output as zipfile\n"
			+ "\t-e: encoding (default UTF-8)\n"
			+ "\t-s: separator character, default ';', encapsulate by '\n"
			+ "\t-q: string quote character, default '\"', encapsulate by '\n"
			+ "\t-a: always quote value\n"
			+ "\t-f: number and datetime format locale, default is systems locale, use 'de', 'en', etc.\n"
			+ "\t-blobfiles: create a file (.blob or .blob.zip) for each blob instead of base64 encoding\n"
			+ "\t-clobfiles: create a file (.clob or .clob.zip) for each clob instead of data in csv file\n"
			+ "\t-beautify: beautify csv output to make column values equal length (takes extra time)\n"
			+ "\n"
			+ "global/single parameters\n"
			+ "\t-help: show this help manual\n"
			+ "\t-update: check for online update and ask, whether an available update shell be installed\n";
	
	private DbCsvExportWorker dbCsvExportWorker;

	public static void main(String[] arguments) {
		DbCsvExportDefinition dbCsvExportDefinition = new DbCsvExportDefinition();

		try {
			if (arguments.length == 0) {
				System.out.println(USAGE_MESSAGE);
				System.exit(1);
			}

			for (int i = 0; i < arguments.length; i++) {
				if ("-help".equalsIgnoreCase(arguments[i]) || "--help".equalsIgnoreCase(arguments[i]) || "-h".equalsIgnoreCase(arguments[i]) || "--h".equalsIgnoreCase(arguments[i])
						|| "-?".equalsIgnoreCase(arguments[i]) || "--?".equalsIgnoreCase(arguments[i])) {
					System.out.println(USAGE_MESSAGE);
					System.exit(1);
				} else if ("-update".equalsIgnoreCase(arguments[i])) {
					new DbCsvExport().updateApplication();
					System.exit(1);
				} else if ("-gui".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setOpenGUI(true);
				} else if ("-l".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setLog(true);
				} else if ("-v".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setVerbose(true);
				} else if ("-z".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setZip(true);
				} else if ("-e".equalsIgnoreCase(arguments[i])) {
					i++;
					dbCsvExportDefinition.setEncoding(arguments[i]);
				} else if ("-s".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new DbCsvExportException("Missing parameter separator character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new DbCsvExportException("Invalid parameter separator character: " + arguments[i]);
					}
					dbCsvExportDefinition.setSeparator(arguments[i].charAt(0));
				} else if ("-q".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new DbCsvExportException("Missing parameter stringquote character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new DbCsvExportException("Invalid parameter stringquote character: " + arguments[i]);
					}
					dbCsvExportDefinition.setStringQuote(arguments[i].charAt(0));
				} else if ("-a".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setAlwaysQuote(true);
				} else if ("-f".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new DbCsvExportException("Missing parameter format locale");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 2) {
						throw new DbCsvExportException("Invalid parameter format locale: " + arguments[i]);
					}
					Locale locale = new Locale(arguments[i]);
					dbCsvExportDefinition.setDateAndDecimalLocale(locale);
				} else if ("-blobfiles".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setCreateBlobFiles(true);
				} else if ("-clobfiles".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setCreateClobFiles(true);
				} else if ("-beautify".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setBeautify(true);
				} else {
					if (dbCsvExportDefinition.getDbVendor() == null) {
						dbCsvExportDefinition.setDbVendor(arguments[i]);
					} else if (dbCsvExportDefinition.getHostname() == null) {
						dbCsvExportDefinition.setHostname(arguments[i]);
					} else if (dbCsvExportDefinition.getUsername() == null) {
						dbCsvExportDefinition.setUsername(arguments[i]);
					} else if (dbCsvExportDefinition.getDbName() == null) {
						dbCsvExportDefinition.setDbName(arguments[i]);
					} else if (dbCsvExportDefinition.getSqlStatementOrTablelist() == null) {
						dbCsvExportDefinition.setSqlStatementOrTablelist(arguments[i]);
					} else if (dbCsvExportDefinition.getOutputpath() == null) {
						dbCsvExportDefinition.setOutputpath(arguments[i]);
					} else if (dbCsvExportDefinition.getPassword() == null) {
						dbCsvExportDefinition.setPassword(arguments[i]);
					} else {
						throw new DbCsvExportException("Invalid parameter: " + arguments[i]);
					}
				}
			}

			if (Utilities.isNotBlank(dbCsvExportDefinition.getHostname()) && dbCsvExportDefinition.getPassword() == null) {
				Console console = System.console();
				if (console == null) {
					throw new DbCsvExportException("Couldn't get Console instance");
				}

				char[] passwordArray = console.readPassword("Please enter db password: ");
				dbCsvExportDefinition.setPassword(new String(passwordArray));
			}

			if (!dbCsvExportDefinition.isOpenGui()) {
				dbCsvExportDefinition.checkParameters();
				dbCsvExportDefinition.checkAndLoadDbDrivers();
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.err.println();
			System.err.println(USAGE_MESSAGE);
			System.exit(1);
		}

		if (dbCsvExportDefinition.isOpenGui()) {
			try {
				new DbCsvExportGui(dbCsvExportDefinition);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else {
			try {
				new DbCsvExport().export(dbCsvExportDefinition);
				System.exit(0);
			} catch (DbCsvExportException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public DbCsvExport() throws Exception {
		super(APPLICATION_NAME, new Version(VERSION));
	}

	private void export(DbCsvExportDefinition dbCsvExportDefinition) throws Exception {
		if (dbCsvExportDefinition.isVerbose()) {
			System.out.println("Separator: " + dbCsvExportDefinition.getSeparator());
			System.out.println("Zip: " + dbCsvExportDefinition.isZip());
			System.out.println("Encoding: " + dbCsvExportDefinition.getEncoding());
			System.out.println("StringQuote: " + dbCsvExportDefinition.getStringQuote());
			System.out.println("AlwaysQuote: " + dbCsvExportDefinition.isAlwaysQuote());
			System.out.println("SqlStatementOrTablelist: " + dbCsvExportDefinition.getSqlStatementOrTablelist());
			System.out.println("OutputFormatLocale: " + dbCsvExportDefinition.getDateAndDecimalLocale().getLanguage());
			System.out.println("CreateBlobFiles: " + dbCsvExportDefinition.isCreateBlobFiles());
			System.out.println("CreateClobFiles: " + dbCsvExportDefinition.isCreateClobFiles());
			System.out.println("Beautify: " + dbCsvExportDefinition.isBeautify());
			System.out.println();
		}

		try {
			dbCsvExportWorker = new DbCsvExportWorker(this, dbCsvExportDefinition);
			dbCsvExportWorker.setShowProgressAfterMilliseconds(5000);
			dbCsvExportWorker.setShowProgressOverrideRefreshMilliseconds(0);
			dbCsvExportWorker.run();

			// Get result to trigger possible Exception
			dbCsvExportWorker.get();
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

	@Override
	public void showUnlimitedProgress() {
		// Do nothing
	}

	@Override
	public void showUnlimitedSubProgress() {
		// Do nothing
	}

	@Override
	public void showProgress(Date start, long itemsToDo, long itemsDone) {
		if (dbCsvExportWorker.getDbCsvExportDefinition().isVerbose()) {
			if (dbCsvExportWorker.getDbCsvExportDefinition().getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
				System.out.print("\r" + Utilities.getConsoleProgressString(80, start, itemsToDo, itemsDone));
			} else {
				System.out.println();
				System.out.println("Exporting table " + (itemsDone + 1) + " of " + itemsToDo);
			}
		}
	}

	@Override
	public void showItemStart(String itemName) {
		if (itemName.equals("Scanning tables ...")) {
			System.out.println(itemName);
		} else {
			System.out.println("Table " + itemName);
		}
	}

	@Override
	public void showItemProgress(Date itemStart, long subItemToDo, long subItemDone) {
		if (dbCsvExportWorker.getDbCsvExportDefinition().isVerbose()) {
			System.out.print("\r" + Utilities.getConsoleProgressString(80, itemStart, subItemToDo, subItemDone));
		}
	}

	@Override
	public void showItemDone(Date itemStart, Date itemEnd, long subItemsDone) {
		if (dbCsvExportWorker.getDbCsvExportDefinition().isVerbose()) {
			System.out.print("\r" + Utilities.rightPad("Exported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(subItemsDone - 1) + " lines in "
					+ DateUtilities.getShortHumanReadableTimespan(itemEnd.getTime() - itemStart.getTime(), false), 80));
			System.out.println();
		}
	}

	@Override
	public void showDone(Date start, Date end, long itemsDone) {
		if (dbCsvExportWorker.getDbCsvExportDefinition().isVerbose()) {
			if (dbCsvExportWorker.getDbCsvExportDefinition().getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
				System.out.print("\r" + Utilities.rightPad("Exported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsDone - 1) + " lines in "
						+ DateUtilities.getShortHumanReadableTimespan(end.getTime() - start.getTime(), false), 80));
				System.out.println();
				System.out.println();
			} else {
				System.out.println();
				System.out.println("Done after " + DateUtilities.getShortHumanReadableTimespan(end.getTime() - start.getTime(), false));
				System.out.println();
			}
		}
	}

	@Override
	public void cancel() {
		System.out.println("Canceled");
	}

	private void updateApplication() throws Exception {
		new ApplicationUpdateHelper(APPLICATION_NAME, VERSION, VERSIONINFO_DOWNLOAD_URL, this, null).executeUpdate();
	}
}
