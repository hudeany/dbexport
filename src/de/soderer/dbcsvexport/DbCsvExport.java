package de.soderer.dbcsvexport;

import java.awt.GraphicsEnvironment;
import java.io.Console;
import java.io.File;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import de.soderer.dbcsvexport.DbCsvExportDefinition.ExportType;
import de.soderer.utilities.ApplicationUpdateHelper;
import de.soderer.utilities.BasicUpdateableConsoleApplication;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.Version;
import de.soderer.utilities.WorkerDual;
import de.soderer.utilities.WorkerParentDual;
import de.soderer.utilities.DbUtilities.DbVendor;

public class DbCsvExport extends BasicUpdateableConsoleApplication implements WorkerParentDual {
	public static final String VERSION = "3.11.0";
	public static final String APPLICATION_NAME = "DbCsvExport";
	public static final String VERSIONINFO_DOWNLOAD_URL = "http://downloads.sourceforge.net/project/dbcsvexport/Versions.xml?r=&ts=<time_seconds>&use_mirror=master";
	public static final File CONFIGURATION_FILE = new File(System.getProperty("user.home") + File.separator + ".DbCsvExport.config");

	private static String USAGE_MESSAGE = "DbCsvExport (by Andreas Soderer, mail: dbcsvexport@soderer.de)\n"
			+ "VERSION: " + VERSION + "\n\n"
			+ "Usage: java -jar DbCsvExport.jar [-gui] [-l] [-z] [-e encoding] [-s ';'] [-q '\"'] [-i 'TAB'] [-a] [-f locale] [-blobfiles] [-clobfiles] [-beautify] [-x CSV|JSON|XML|SQL] dbtype hostname[:port] username dbname 'statement or list of tablepatterns' outputpath [password]\n"
			+ "Simple usage: java -jar DbCsvExport.jar dbtype hostname username dbname 'statement or list of tablepatterns' outputpath\n"
			+ "\n"
			+ "mandatory parameters\n"
			+ "\tdbtype: mysql | oracle | postgresql | sqlite | derby\n"
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
			+ "\n"
			+ "global/single parameters\n"
			+ "\t-help: show this help manual\n"
			+ "\t-version: show current local version of this tool\n"
			+ "\t-update: check for online update and ask, whether an available update shell be installed\n";
	
	private DbCsvExportDefinition dbCsvExportDefinition;
	private WorkerDual<Boolean> worker;

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
				} else if ("-version".equalsIgnoreCase(arguments[i])) {
					System.out.println(VERSION);
					System.exit(1);
				} else if ("-update".equalsIgnoreCase(arguments[i])) {
					new DbCsvExport().updateApplication();
					System.exit(1);
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
					}
					dbCsvExportDefinition.setExportType(arguments[i]);
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
						throw new ParameterException(arguments[i - 1], "Missing parameter separator character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter separator character");
					}
					dbCsvExportDefinition.setSeparator(arguments[i].charAt(0));
				} else if ("-q".equalsIgnoreCase(arguments[i])) {
					i++;
					if (i >= arguments.length) {
						throw new ParameterException(arguments[i - 1], "Missing parameter stringquote character");
					} else if (Utilities.isBlank(arguments[i]) || arguments[i].length() != 1) {
						throw new ParameterException(arguments[i - 1] + " " + arguments[i], "Invalid parameter stringquote character");
					}
					dbCsvExportDefinition.setStringQuote(arguments[i].charAt(0));
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
					}  else if ("DOUBLEBLANK".equalsIgnoreCase(arguments[i])) {
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
					}
					Locale locale = new Locale(arguments[i]);
					dbCsvExportDefinition.setDateAndDecimalLocale(locale);
				} else if ("-blobfiles".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setCreateBlobFiles(true);
				} else if ("-clobfiles".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setCreateClobFiles(true);
				} else if ("-beautify".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setBeautify(true);
				} else if ("-noheaders".equalsIgnoreCase(arguments[i])) {
					dbCsvExportDefinition.setNoHeaders(true);
				} else {
					if (dbCsvExportDefinition.getDbVendor() == null) {
						dbCsvExportDefinition.setDbVendor(arguments[i]);
					} else if (dbCsvExportDefinition.getHostname() == null && dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite && dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvExportDefinition.setHostname(arguments[i]);
					} else if (dbCsvExportDefinition.getUsername() == null && dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite&& dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvExportDefinition.setUsername(arguments[i]);
					} else if (dbCsvExportDefinition.getDbName() == null) {
						dbCsvExportDefinition.setDbName(arguments[i]);
					} else if (dbCsvExportDefinition.getSqlStatementOrTablelist() == null) {
						dbCsvExportDefinition.setSqlStatementOrTablelist(arguments[i]);
					} else if (dbCsvExportDefinition.getOutputpath() == null) {
						dbCsvExportDefinition.setOutputpath(arguments[i]);
					} else if (dbCsvExportDefinition.getPassword() == null && dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite&& dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
						dbCsvExportDefinition.setPassword(arguments[i]);
					} else {
						throw new ParameterException(arguments[i], "Invalid parameter");
					}
				}
			}

			if (!dbCsvExportDefinition.isOpenGui()) {
				if (Utilities.isNotBlank(dbCsvExportDefinition.getHostname()) && dbCsvExportDefinition.getPassword() == null && dbCsvExportDefinition.getDbVendor() != DbVendor.SQLite&& dbCsvExportDefinition.getDbVendor() != DbVendor.Derby) {
					Console console = System.console();
					if (console == null) {
						throw new DbCsvExportException("Couldn't get Console instance");
					}
	
					char[] passwordArray = console.readPassword("Please enter db password: ");
					dbCsvExportDefinition.setPassword(new String(passwordArray));
				}

				dbCsvExportDefinition.checkParameters();
				dbCsvExportDefinition.checkAndLoadDbDrivers();
			}
		} catch (ParameterException e) {
			System.err.println(e.getMessage());
			System.err.println();
			System.err.println(USAGE_MESSAGE);
			System.exit(1);
		} catch (Exception e) {
			System.err.println(e.getMessage());
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
				// System.exit(0); // Do not exit so junit tests can be executed
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
		this.dbCsvExportDefinition = dbCsvExportDefinition;

		try {
			if (dbCsvExportDefinition.getExportType() == ExportType.JSON) {
				worker = new DbJsonExportWorker(this, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbJsonExportWorker) worker).setLog(dbCsvExportDefinition.isLog());
				((DbJsonExportWorker) worker).setZip(dbCsvExportDefinition.isZip());
				((DbJsonExportWorker) worker).setEncoding(dbCsvExportDefinition.getEncoding());
				((DbJsonExportWorker) worker).setCreateBlobFiles(dbCsvExportDefinition.isCreateBlobFiles());
				((DbJsonExportWorker) worker).setCreateClobFiles(dbCsvExportDefinition.isCreateClobFiles());
				((DbJsonExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
				((DbJsonExportWorker) worker).setIndentation(dbCsvExportDefinition.getIndentation());
			} else if (dbCsvExportDefinition.getExportType() == ExportType.XML) {
				worker = new DbXmlExportWorker(this, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbXmlExportWorker) worker).setLog(dbCsvExportDefinition.isLog());
				((DbXmlExportWorker) worker).setZip(dbCsvExportDefinition.isZip());
				((DbXmlExportWorker) worker).setEncoding(dbCsvExportDefinition.getEncoding());
				((DbXmlExportWorker) worker).setCreateBlobFiles(dbCsvExportDefinition.isCreateBlobFiles());
				((DbXmlExportWorker) worker).setCreateClobFiles(dbCsvExportDefinition.isCreateClobFiles());
				((DbXmlExportWorker) worker).setDateAndDecimalLocale(dbCsvExportDefinition.getDateAndDecimalLocale());
				((DbXmlExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
				((DbXmlExportWorker) worker).setIndentation(dbCsvExportDefinition.getIndentation());
			} else if (dbCsvExportDefinition.getExportType() == ExportType.SQL) {
				worker = new DbSqlExportWorker(this, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbSqlExportWorker) worker).setLog(dbCsvExportDefinition.isLog());
				((DbSqlExportWorker) worker).setZip(dbCsvExportDefinition.isZip());
				((DbSqlExportWorker) worker).setEncoding(dbCsvExportDefinition.getEncoding());
				((DbSqlExportWorker) worker).setCreateBlobFiles(dbCsvExportDefinition.isCreateBlobFiles());
				((DbSqlExportWorker) worker).setCreateClobFiles(dbCsvExportDefinition.isCreateClobFiles());
				((DbSqlExportWorker) worker).setDateAndDecimalLocale(dbCsvExportDefinition.getDateAndDecimalLocale());
				((DbSqlExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
			} else {
				worker = new DbCsvExportWorker(this, dbCsvExportDefinition.getDbVendor(), dbCsvExportDefinition.getHostname(), dbCsvExportDefinition.getDbName(), dbCsvExportDefinition.getUsername(), dbCsvExportDefinition.getPassword(), dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());
				((DbCsvExportWorker) worker).setLog(dbCsvExportDefinition.isLog());
				((DbCsvExportWorker) worker).setZip(dbCsvExportDefinition.isZip());
				((DbCsvExportWorker) worker).setEncoding(dbCsvExportDefinition.getEncoding());
				((DbCsvExportWorker) worker).setCreateBlobFiles(dbCsvExportDefinition.isCreateBlobFiles());
				((DbCsvExportWorker) worker).setCreateClobFiles(dbCsvExportDefinition.isCreateClobFiles());
				((DbCsvExportWorker) worker).setDateAndDecimalLocale(dbCsvExportDefinition.getDateAndDecimalLocale());
				((DbCsvExportWorker) worker).setSeparator(dbCsvExportDefinition.getSeparator());
				((DbCsvExportWorker) worker).setStringQuote(dbCsvExportDefinition.getStringQuote());
				((DbCsvExportWorker) worker).setAlwaysQuote(dbCsvExportDefinition.isAlwaysQuote());
				((DbCsvExportWorker) worker).setBeautify(dbCsvExportDefinition.isBeautify());
				((DbCsvExportWorker) worker).setNoHeaders(dbCsvExportDefinition.isNoHeaders());
			}
			
			if (dbCsvExportDefinition.isVerbose()) {
				System.out.println(((AbstractDbExportWorker) worker).getConfigurationLogString(new File(dbCsvExportDefinition.getOutputpath()).getName(), dbCsvExportDefinition.getSqlStatementOrTablelist()));
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
		if (dbCsvExportDefinition.isVerbose()) {
			if (dbCsvExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
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
		if (dbCsvExportDefinition.isVerbose()) {
			System.out.print("\r" + Utilities.getConsoleProgressString(80, itemStart, subItemToDo, subItemDone));
		}
	}

	@Override
	public void showItemDone(Date itemStart, Date itemEnd, long subItemsDone) {
		if (dbCsvExportDefinition.isVerbose()) {
			System.out.print("\r" + Utilities.rightPad("Exported " + NumberFormat.getNumberInstance(Locale.getDefault()).format(subItemsDone - 1) + " lines in "
					+ DateUtilities.getShortHumanReadableTimespan(itemEnd.getTime() - itemStart.getTime(), false), 80));
			System.out.println();
		}
	}

	@Override
	public void showDone(Date start, Date end, long itemsDone) {
		if (dbCsvExportDefinition.isVerbose()) {
			if (dbCsvExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
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
