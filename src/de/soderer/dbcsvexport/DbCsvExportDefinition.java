package de.soderer.dbcsvexport;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.SectionedProperties;
import de.soderer.utilities.Utilities;

public class DbCsvExportDefinition {
	// Default optional parameters
	private boolean openGui = false;
	private ExportType exportType = ExportType.CSV;
	private boolean log = false;
	private boolean verbose = false;
	private boolean zip = false;
	private String encoding = "UTF-8";
	private char separator = ';';
	private char stringQuote = '"';
	private String indentation = "\t";
	private boolean alwaysQuote = false;
	private boolean createBlobFiles = false;
	private boolean createClobFiles = false;
	private Locale dateAndDecimalLocale = null;
	private boolean beautify = false;
	private boolean noHeaders = false;

	// Mandatory parameters
	private DbUtilities.DbVendor dbVendor = null;
	private String hostname;
	private String dbName;
	private String username;
	private String sqlStatementOrTablelist;
	private String outputpath;
	
	public enum ExportType {
		CSV,
		JSON,
		XML,
		SQL;

		public static ExportType getFromString(String exportType) throws Exception {
			if ("CSV".equalsIgnoreCase(exportType)) {
				return ExportType.CSV;
			} else if ("JSON".equalsIgnoreCase(exportType)) {
				return ExportType.JSON;
			} else if ("XML".equalsIgnoreCase(exportType)) {
				return ExportType.XML;
			}  else if ("SQL".equalsIgnoreCase(exportType)) {
				return ExportType.SQL;
			} else {
				throw new Exception("Invalid export format: " + exportType);
			}
		}
	}

	// Password may be entered interactive
	private String password;

	public void setOpenGUI(boolean openGui) {
		this.openGui = openGui;
	}
	
	public void setExportType(ExportType exportType) {
		this.exportType = exportType;
		if (this.exportType == null) {
			this.exportType = ExportType.CSV;
		}
	}
	
	public void setExportType(String exportType) throws Exception {
		this.exportType = ExportType.getFromString(exportType);
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

	public void setIndentation(String indentation) {
		this.indentation = indentation;
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

	public void setDbVendor(String dbVendor) throws Exception {
		this.dbVendor = DbUtilities.DbVendor.getDbVendorByName(dbVendor);
	}

	public void setDbVendor(DbVendor dbVendor) {
		this.dbVendor = dbVendor;
	}

	public void setHostname(String hostname) throws Exception {
		this.hostname = hostname;
		
		if (Utilities.isNotBlank(hostname)) {
			String[] hostParts = this.hostname.split(":");
			if (hostParts.length == 2) {
				if (!Utilities.isNumber(hostParts[1])) {
					throw new Exception("Invalid port in hostname: " + hostname);
				}
			} else if (hostParts.length > 2) {
				throw new Exception("Invalid hostname: " + hostname);
			}
		} else if (dbVendor != DbVendor.SQLite) {
			throw new Exception("Invalid empty hostname");
		}
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setDateAndDecimalLocale(Locale dateAndDecimalLocale) {
		this.dateAndDecimalLocale = dateAndDecimalLocale;
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
			this.outputpath = this.outputpath.trim();
			this.outputpath = this.outputpath.replace("~", System.getProperty("user.home"));
			if (this.outputpath.endsWith(File.separator)) {
				this.outputpath = this.outputpath.substring(0, this.outputpath.length() - 1);
			}
		}
	}

	public DbVendor getDbVendor() {
		return dbVendor;
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

	public ExportType getExportType() {
		return exportType;
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

	public String getIndentation() {
		return indentation;
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
		if (dateAndDecimalLocale == null) {
			return Locale.getDefault();
		} else {
			return dateAndDecimalLocale;
		}
	}

	public void checkParameters() throws Exception {
		if (outputpath == null) {
			throw new DbCsvExportException("Outputpath is missing");
		} else if ("console".equalsIgnoreCase(outputpath)) {
			if (zip) {
				throw new DbCsvExportException("Zipping not allowed for console output");
			}
		} else if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")) {
			if (new File(outputpath).exists() && !new File(outputpath).isDirectory()) {
				throw new DbCsvExportException("Outputpath file already exists: " + outputpath);
			}
		} else {
			if (!new File(outputpath).exists()) {
				throw new DbCsvExportException("Outputpath directory does not exist: " + outputpath);
			} else if (!new File(outputpath).isDirectory()) {
				throw new DbCsvExportException("Outputpath is not a directory: " + outputpath);
			}
		}

		if (dbVendor == DbVendor.SQLite) {
			if (Utilities.isNotBlank(hostname)) {
				throw new DbCsvExportException("SQLite db connections so not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbCsvExportException("SQLite db connections so not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbCsvExportException("SQLite db connections so not support the password parameter");
			} else if (dateAndDecimalLocale != null) {
				throw new DbCsvExportException("SQLite db connections so not support the date and decimal locale parameter");
			}
		} else {
			if (Utilities.isBlank(password)) {
				throw new DbCsvExportException("Missing or invalid empty password");
			}
		}
		
		if (alwaysQuote && exportType != ExportType.CSV) {
			throw new DbCsvExportException("AlwaysQuote is not supported for export format " + exportType);
		}
		
		if (noHeaders && exportType != ExportType.CSV) {
			throw new DbCsvExportException("NoHeaders is not supported for export format " + exportType);
		}
		
		if (beautify && exportType != ExportType.CSV && exportType != ExportType.JSON && exportType != ExportType.XML) {
			throw new DbCsvExportException("Beautify is not supported for export format " + exportType);
		}
	}
	
	public void checkAndLoadDbDrivers() throws Exception {
		if (dbVendor != null) {
			// Check if driver is included in jar/classpath
			try {
				Class.forName(dbVendor.getDriverClassName());
			} catch (ClassNotFoundException e) {
				// Driver is missing, so use the configured one
				SectionedProperties configuration = new SectionedProperties(true);
				if (DbCsvExport.CONFIGURATION_FILE.exists()) {
					try (InputStream inputStream = new FileInputStream(DbCsvExport.CONFIGURATION_FILE)) {
						configuration.load(inputStream);
					}
				}
				String driverFile = configuration.getValue(dbVendor.toString().toLowerCase(), "driver_location");
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
						Class.forName(dbVendor.getDriverClassName());
					} catch (Exception e1) {
						String newDriverFile = aquireNewDriver();
						configuration.setValue(dbVendor.toString().toLowerCase(), "driver_location", newDriverFile);
						try (OutputStream outputStream = new FileOutputStream(DbCsvExport.CONFIGURATION_FILE)) {
							configuration.save(outputStream);
						}
					}
				} else {
					String newDriverFile = aquireNewDriver();
					configuration.setValue(dbVendor.toString().toLowerCase(), "driver_location", newDriverFile);
					try (OutputStream outputStream = new FileOutputStream(DbCsvExport.CONFIGURATION_FILE)) {
						configuration.save(outputStream);
					}
				}
			} catch (Throwable e) {
				throw new Exception("Cannot load db driver: " + e.getMessage());
			}
		} else {
			throw new Exception("Invalid empty db vendor");
		}
	}

	private String aquireNewDriver() throws Exception {
		if (isOpenGui()) {
			if (!DbCsvExport.CONFIGURATION_FILE.exists()) {
				try (OutputStream outputStream = new FileOutputStream(DbCsvExport.CONFIGURATION_FILE)) {
					SectionedProperties configuration = new SectionedProperties(true);
					configuration.setValue(dbVendor.toString().toLowerCase(), "driver_location", "");
					configuration.save(outputStream);
				}
			}
			throw new Exception("Driver for " + dbVendor.toString() + " is missing. Please configure in " + DbCsvExport.CONFIGURATION_FILE);
		} else {
			System.out.println("Driver for " + dbVendor.toString() + " is missing");
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
						Class.forName(dbVendor.getDriverClassName());
						return newFilePath;
					} catch (Exception e) {
						System.out.println("File " + newFilePath + " does not contain a " + dbVendor.toString() + " driver");
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

	public void setNoHeaders(boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	public boolean isNoHeaders() {
		return noHeaders;
	}
}
