package de.soderer.dbcsvexport;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.SectionedProperties;
import de.soderer.utilities.Utilities;

public class DbCsvExportDefinition {
	// Default optional parameters
	private boolean openGui = false;
	private boolean log = false;
	private boolean verbose = false;
	private boolean zip = false;
	private String encoding = "UTF-8";
	private char separator = ';';
	private char stringQuote = '"';
	private boolean alwaysQuote = false;
	private boolean createBlobFiles = false;
	private boolean createClobFiles = false;
	private Locale dateAndDecimalLocale = Locale.getDefault();
	private DateFormat dateFormat = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.MEDIUM, SimpleDateFormat.MEDIUM, dateAndDecimalLocale);
	private NumberFormat decimalFormat = DecimalFormat.getNumberInstance(dateAndDecimalLocale);
	private boolean beautify;

	// Mandatory parameters
	private String dbType;
	private String hostname;
	private String dbName;
	private String username;
	private String sqlStatementOrTablelist;
	private String outputpath;

	// Password may be entered interactive
	private String password;

	public void setOpenGUI(boolean openGui) {
		this.openGui = openGui;
	}

	public void setLog(boolean log) {
		this.log = log;
	}

	public void setZip(boolean zip) {
		this.zip = zip;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}

	public void setStringQuote(char stringQuote) {
		this.stringQuote = stringQuote;
	}

	public void setAlwaysQuote(boolean alwaysQuote) {
		this.alwaysQuote = alwaysQuote;
	}

	public void setCreateBlobFiles(boolean createBlobFiles) {
		this.createBlobFiles = createBlobFiles;
	}

	public void setCreateClobFiles(boolean createClobFiles) {
		this.createClobFiles = createClobFiles;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType.trim().toLowerCase();
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setDateAndDecimalLocale(Locale dateAndDecimalLocale) {
		this.dateAndDecimalLocale = dateAndDecimalLocale;
		dateFormat = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM, dateAndDecimalLocale);
		decimalFormat = NumberFormat.getNumberInstance(dateAndDecimalLocale);
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setSqlStatementOrTablelist(String sqlStatementOrTablelist) {
		this.sqlStatementOrTablelist = sqlStatementOrTablelist;
	}

	public void setOutputpath(String outputpath) {
		this.outputpath = outputpath;
		if (this.outputpath != null) {
			this.outputpath = this.outputpath.replace("~", System.getProperty("user.home"));
			if (this.outputpath.endsWith(File.separator)) {
				this.outputpath = this.outputpath.substring(0, this.outputpath.length() - 1);
			}
		}
	}

	public String getDbType() {
		return dbType;
	}

	public String getHostname() {
		return hostname;
	}

	public String getDbName() {
		return dbName;
	}

	public String getUsername() {
		return username;
	}

	public String getSqlStatementOrTablelist() {
		return sqlStatementOrTablelist;
	}

	public String getOutputpath() {
		return outputpath;
	}

	public String getPassword() {
		return password;
	}

	public boolean isOpenGui() {
		return openGui;
	}

	public boolean isLog() {
		return log;
	}

	public boolean isZip() {
		return zip;
	}

	public String getEncoding() {
		return encoding;
	}

	public char getSeparator() {
		return separator;
	}

	public char getStringQuote() {
		return stringQuote;
	}

	public boolean isAlwaysQuote() {
		return alwaysQuote;
	}

	public boolean isCreateBlobFiles() {
		return createBlobFiles;
	}

	public boolean isCreateClobFiles() {
		return createClobFiles;
	}

	public Locale getDateAndDecimalLocale() {
		return dateAndDecimalLocale;
	}

	public DateFormat getDateFormat() {
		return dateFormat;
	}

	public NumberFormat getDecimalFormat() {
		return decimalFormat;
	}

	public void checkParameters() throws Exception {
		if (!"oracle".equalsIgnoreCase(dbType) && !"mysql".equalsIgnoreCase(dbType) && !"postgres".equalsIgnoreCase(dbType) && !"postgresql".equalsIgnoreCase(dbType)) {
			throw new DbCsvExportException("Invalid parameter dbType: " + dbType);
		}

		if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")) {
			if (new File(outputpath).exists() && !new File(outputpath).isDirectory()) {
				throw new DbCsvExportException("Outputpath file already exists: " + outputpath);
			}
		} else {
			if (!"console".equalsIgnoreCase(outputpath)) {
				if (!new File(outputpath).exists()) {
					throw new DbCsvExportException("Outputpath does not exist: " + outputpath);
				} else if (!new File(outputpath).isDirectory()) {
					throw new DbCsvExportException("Outputpath is not a directory: " + outputpath);
				}
			}
		}

		if (Utilities.isBlank(password)) {
			throw new DbCsvExportException("Missing or invalid empty password");
		}
	}
	
	public void checkAndLoadDbDrivers() throws Exception {
		if ("oracle".equalsIgnoreCase(dbType) || "mysql".equalsIgnoreCase(dbType) || "postgres".equalsIgnoreCase(dbType) || "postgresql".equalsIgnoreCase(dbType)) {
			// Check if driver is included in jar/classpath
			try {
				Class.forName(DbUtilities.getDriverClassName(dbType));
			} catch (ClassNotFoundException e) {
				// Driver is missing, so use the configured one
				SectionedProperties configuration = new SectionedProperties(true);
				if (DbCsvExport.CONFIGURATION_FILE.exists()) {
					try (InputStream inputStream = new FileInputStream(DbCsvExport.CONFIGURATION_FILE)) {
						configuration.load(inputStream);
					}
				}
				String driverFile = configuration.getValue(dbType.toLowerCase(), "driver_location");
				if (driverFile != null) {
					driverFile = driverFile.replace("~", System.getProperty("user.home"));
				}
				if (Utilities.isNotBlank(driverFile)) {
					try {
						if (!new File(driverFile).exists()) {
							System.out.println("File " + driverFile + " not found");
							throw new Exception("File " + driverFile + " not found");
						}
						Utilities.addFileToClasspath(driverFile);
						Class.forName(DbUtilities.getDriverClassName(dbType));
					} catch (Exception e1) {
						String newDriverFile = aquireNewDriver();
						configuration.setValue(dbType.toLowerCase(), "driver_location", newDriverFile);
						try (OutputStream outputStream = new FileOutputStream(DbCsvExport.CONFIGURATION_FILE)) {
							configuration.save(outputStream);
						}
					}
				} else {
					String newDriverFile = aquireNewDriver();
					configuration.setValue(dbType.toLowerCase(), "driver_location", newDriverFile);
					try (OutputStream outputStream = new FileOutputStream(DbCsvExport.CONFIGURATION_FILE)) {
						configuration.save(outputStream);
					}
				}
			}
		} else {
			throw new DbCsvExportException("Invalid parameter dbType: " + dbType);
		}
	}

	private String aquireNewDriver() throws Exception {
		if (isOpenGui()) {
			if (!DbCsvExport.CONFIGURATION_FILE.exists()) {
				try (OutputStream outputStream = new FileOutputStream(DbCsvExport.CONFIGURATION_FILE)) {
					SectionedProperties configuration = new SectionedProperties(true);
					configuration.setValue(dbType.toLowerCase(), "driver_location", "");
					configuration.save(outputStream);
				}
			}
			throw new Exception("Driver for " + dbType + " is missing. Please configure in " + DbCsvExport.CONFIGURATION_FILE);
		} else {
			System.out.println("Driver for " + dbType + " is missing");
			Console console = System.console();
			if (console == null) {
				throw new Exception("Cannot get Console instance for user driver input");
			}
			
			while (true) {
				String newFilePath = console.readLine("Please enter driverfilepath (empty for cancel): ");
				if (newFilePath != null) {
					newFilePath = newFilePath.replace("~", System.getProperty("user.home"));
				}
				if (Utilities.isBlank(newFilePath)) {
					throw new Exception("Driver input canceled by user");
				} else if (!new File(newFilePath).exists()) {
					System.out.println("File " + newFilePath + " not found");
					System.out.println();
				} else {
					try {
						Utilities.addFileToClasspath(newFilePath);
						Class.forName(DbUtilities.getDriverClassName(dbType));
						return newFilePath;
					} catch (Exception e) {
						System.out.println("File " + newFilePath + " does not contain a " + dbType + " driver");
						System.out.println();
					}
				}
			}
		}
	}

	public void setBeautify(boolean beautify) {
		this.beautify = beautify;
	}

	public boolean isBeautify() {
		return beautify;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
}
