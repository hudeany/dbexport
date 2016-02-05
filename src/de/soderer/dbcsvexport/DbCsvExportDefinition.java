package de.soderer.dbcsvexport;

import java.io.File;
import java.util.List;
import java.util.Locale;

import de.soderer.dbcsvexport.worker.AbstractDbExportWorker;
import de.soderer.dbcsvexport.worker.DbCsvExportWorker;
import de.soderer.dbcsvexport.worker.DbJsonExportWorker;
import de.soderer.dbcsvexport.worker.DbSqlExportWorker;
import de.soderer.dbcsvexport.worker.DbXmlExportWorker;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.SecureDataEntry;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerDual;
import de.soderer.utilities.WorkerParentDual;

/**
 * The Class DbCsvExportDefinition.
 */
public class DbCsvExportDefinition extends SecureDataEntry {

	/**
	 * The Enum ExportType.
	 */
	public enum ExportType {
		CSV,
		JSON,
		XML,
		SQL;

		/**
		 * Gets the string representation of export type.
		 *
		 * @param exportType
		 *            the export type
		 * @return the from string
		 * @throws Exception
		 *             the exception
		 */
		public static ExportType getFromString(String exportType) throws Exception {
			if ("CSV".equalsIgnoreCase(exportType)) {
				return ExportType.CSV;
			} else if ("JSON".equalsIgnoreCase(exportType)) {
				return ExportType.JSON;
			} else if ("XML".equalsIgnoreCase(exportType)) {
				return ExportType.XML;
			} else if ("SQL".equalsIgnoreCase(exportType)) {
				return ExportType.SQL;
			} else {
				throw new Exception("Invalid export format: " + exportType);
			}
		}
	}

	// Mandatory parameters
	
	/** The db vendor. */
	private DbUtilities.DbVendor dbVendor = null;

	/** The hostname. */
	private String hostname;

	/** The db name. */
	private String dbName;

	/** The username. */
	private String username;

	/** The sql statement or tablelist. */
	private String sqlStatementOrTablelist;

	/** The outputpath. */
	private String outputpath;

	/** The password, may be entered interactive */
	private String password;

	// Default optional parameters
	
	/** The open gui. */
	private boolean openGui = false;

	/** The export type. */
	private ExportType exportType = ExportType.CSV;

	/** The log. */
	private boolean log = false;

	/** The verbose. */
	private boolean verbose = false;

	/** The zip. */
	private boolean zip = false;

	/** The encoding. */
	private String encoding = "UTF-8";

	/** The separator. */
	private char separator = ';';

	/** The string quote. */
	private char stringQuote = '"';

	/** The indentation. */
	private String indentation = "\t";

	/** The always quote. */
	private boolean alwaysQuote = false;

	/** The create blob files. */
	private boolean createBlobFiles = false;

	/** The create clob files. */
	private boolean createClobFiles = false;

	/** The date and decimal locale. */
	private Locale dateAndDecimalLocale = null;

	/** The beautify. */
	private boolean beautify = false;

	/** The no headers. */
	private boolean noHeaders = false;

	/** The export structure. */
	private boolean exportStructure = false;

	/** The null value string. */
	private String nullValueString = "";

	/**
	 * Sets the open gui.
	 *
	 * @param openGui
	 *            the new open gui
	 */
	public void setOpenGUI(boolean openGui) {
		this.openGui = openGui;
	}

	/**
	 * Sets the export type.
	 *
	 * @param exportType
	 *            the new export type
	 */
	public void setExportType(ExportType exportType) {
		this.exportType = exportType;
		if (this.exportType == null) {
			this.exportType = ExportType.CSV;
		}
	}

	/**
	 * Sets the export type.
	 *
	 * @param exportType
	 *            the new export type
	 * @throws Exception
	 *             the exception
	 */
	public void setExportType(String exportType) throws Exception {
		this.exportType = ExportType.getFromString(exportType);
	}

	/**
	 * Sets the log.
	 *
	 * @param log
	 *            the new log
	 */
	public void setLog(boolean log) {
		this.log = log;
	}

	/**
	 * Sets the zip.
	 *
	 * @param zip
	 *            the new zip
	 */
	public void setZip(boolean zip) {
		this.zip = zip;
	}

	/**
	 * Sets the encoding.
	 *
	 * @param encoding
	 *            the new encoding
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * Sets the separator.
	 *
	 * @param separator
	 *            the new separator
	 */
	public void setSeparator(char separator) {
		this.separator = separator;
	}

	/**
	 * Sets the string quote.
	 *
	 * @param stringQuote
	 *            the new string quote
	 */
	public void setStringQuote(char stringQuote) {
		this.stringQuote = stringQuote;
	}

	/**
	 * Sets the indentation.
	 *
	 * @param indentation
	 *            the new indentation
	 */
	public void setIndentation(String indentation) {
		this.indentation = indentation;
	}

	/**
	 * Sets the always quote.
	 *
	 * @param alwaysQuote
	 *            the new always quote
	 */
	public void setAlwaysQuote(boolean alwaysQuote) {
		this.alwaysQuote = alwaysQuote;
	}

	/**
	 * Sets the creates the blob files.
	 *
	 * @param createBlobFiles
	 *            the new creates the blob files
	 */
	public void setCreateBlobFiles(boolean createBlobFiles) {
		this.createBlobFiles = createBlobFiles;
	}

	/**
	 * Sets the creates the clob files.
	 *
	 * @param createClobFiles
	 *            the new creates the clob files
	 */
	public void setCreateClobFiles(boolean createClobFiles) {
		this.createClobFiles = createClobFiles;
	}

	/**
	 * Sets the db vendor.
	 *
	 * @param dbVendor
	 *            the new db vendor
	 * @throws Exception
	 *             the exception
	 */
	public void setDbVendor(String dbVendor) throws Exception {
		this.dbVendor = DbUtilities.DbVendor.getDbVendorByName(dbVendor);
	}

	/**
	 * Sets the db vendor.
	 *
	 * @param dbVendor
	 *            the new db vendor
	 */
	public void setDbVendor(DbVendor dbVendor) {
		this.dbVendor = dbVendor;
	}

	/**
	 * Sets the hostname and optional port ("hostname:port")
	 *
	 * @param hostname
	 *            the new hostname
	 * @throws Exception
	 *             the exception
	 */
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
		}
	}

	/**
	 * Sets the db name.
	 *
	 * @param dbName
	 *            the new db name
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	/**
	 * Sets the username.
	 *
	 * @param username
	 *            the new username
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Sets the date and decimal locale.
	 *
	 * @param dateAndDecimalLocale
	 *            the new date and decimal locale
	 */
	public void setDateAndDecimalLocale(Locale dateAndDecimalLocale) {
		this.dateAndDecimalLocale = dateAndDecimalLocale;
	}

	/**
	 * Sets the password.
	 *
	 * @param password
	 *            the new password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Sets the sql statement or tablelist.
	 *
	 * @param sqlStatementOrTablelist
	 *            the new sql statement or tablelist
	 */
	public void setSqlStatementOrTablelist(String sqlStatementOrTablelist) {
		this.sqlStatementOrTablelist = sqlStatementOrTablelist;
	}

	/**
	 * Sets the outputpath.
	 *
	 * @param outputpath
	 *            the new outputpath
	 */
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

	/**
	 * Gets the db vendor.
	 *
	 * @return the db vendor
	 */
	public DbVendor getDbVendor() {
		return dbVendor;
	}

	/**
	 * Gets the hostname.
	 *
	 * @return the hostname
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * Gets the db name.
	 *
	 * @return the db name
	 */
	public String getDbName() {
		return dbName;
	}

	/**
	 * Gets the username.
	 *
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Gets the sql statement or tablelist.
	 *
	 * @return the sql statement or tablelist
	 */
	public String getSqlStatementOrTablelist() {
		return sqlStatementOrTablelist;
	}

	/**
	 * Gets the outputpath.
	 *
	 * @return the outputpath
	 */
	public String getOutputpath() {
		return outputpath;
	}

	/**
	 * Gets the password.
	 *
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Checks if is open gui.
	 *
	 * @return true, if is open gui
	 */
	public boolean isOpenGui() {
		return openGui;
	}

	/**
	 * Gets the export type.
	 *
	 * @return the export type
	 */
	public ExportType getExportType() {
		return exportType;
	}

	/**
	 * Checks if is log.
	 *
	 * @return true, if is log
	 */
	public boolean isLog() {
		return log;
	}

	/**
	 * Checks if is zip.
	 *
	 * @return true, if is zip
	 */
	public boolean isZip() {
		return zip;
	}

	/**
	 * Gets the encoding.
	 *
	 * @return the encoding
	 */
	public String getEncoding() {
		return encoding;
	}

	/**
	 * Gets the separator.
	 *
	 * @return the separator
	 */
	public char getSeparator() {
		return separator;
	}

	/**
	 * Gets the string quote.
	 *
	 * @return the string quote
	 */
	public char getStringQuote() {
		return stringQuote;
	}

	/**
	 * Gets the indentation.
	 *
	 * @return the indentation
	 */
	public String getIndentation() {
		return indentation;
	}

	/**
	 * Checks if is always quote.
	 *
	 * @return true, if is always quote
	 */
	public boolean isAlwaysQuote() {
		return alwaysQuote;
	}

	/**
	 * Checks if is creates the blob files.
	 *
	 * @return true, if is creates the blob files
	 */
	public boolean isCreateBlobFiles() {
		return createBlobFiles;
	}

	/**
	 * Checks if is creates the clob files.
	 *
	 * @return true, if is creates the clob files
	 */
	public boolean isCreateClobFiles() {
		return createClobFiles;
	}

	/**
	 * Gets the date and decimal locale.
	 *
	 * @return the date and decimal locale
	 */
	public Locale getDateAndDecimalLocale() {
		if (dateAndDecimalLocale == null) {
			return Locale.getDefault();
		} else {
			return dateAndDecimalLocale;
		}
	}

	/**
	 * Check parameters.
	 *
	 * @throws Exception
	 *             the exception
	 */
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
				throw new DbCsvExportException("SQLite db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbCsvExportException("SQLite db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbCsvExportException("SQLite db connections do not support the password parameter");
			} else if (dateAndDecimalLocale != null) {
				throw new DbCsvExportException("SQLite db connections do not support the date and decimal locale parameter");
			}
		} else if (dbVendor == DbVendor.Derby) {
			if (Utilities.isNotBlank(hostname)) {
				throw new DbCsvExportException("Derby db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbCsvExportException("Derby db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbCsvExportException("Derby db connections do not support the password parameter");
			}
		} else if (dbVendor == DbVendor.HSQL) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (dbName.startsWith("/")) {
				if (Utilities.isNotBlank(hostname)) {
					throw new DbCsvExportException("HSQL file db connections do not support the hostname parameter");
				} else if (Utilities.isNotBlank(username)) {
					throw new DbCsvExportException("HSQL file db connections do not support the username parameter");
				} else if (Utilities.isNotBlank(password)) {
					throw new DbCsvExportException("HSQL file db connections do not support the password parameter");
				}
			}
		} else {
			if (Utilities.isBlank(hostname)) {
				throw new DbCsvExportException("Missing or invalid hostname");
			}
			if (Utilities.isBlank(username)) {
				throw new DbCsvExportException("Missing or invalid username");
			}
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

	/**
	 * Sets the beautify.
	 *
	 * @param beautify
	 *            the new beautify
	 */
	public void setBeautify(boolean beautify) {
		this.beautify = beautify;
	}

	/**
	 * Checks if is beautify.
	 *
	 * @return true, if is beautify
	 */
	public boolean isBeautify() {
		return beautify;
	}

	/**
	 * Checks if is verbose.
	 *
	 * @return true, if is verbose
	 */
	public boolean isVerbose() {
		return verbose;
	}

	/**
	 * Sets the verbose.
	 *
	 * @param verbose
	 *            the new verbose
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * Sets the no headers.
	 *
	 * @param noHeaders
	 *            the new no headers
	 */
	public void setNoHeaders(boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	/**
	 * Checks if is no headers.
	 *
	 * @return true, if is no headers
	 */
	public boolean isNoHeaders() {
		return noHeaders;
	}

	/**
	 * Sets the null value string.
	 *
	 * @param nullValueString
	 *            the new null value string
	 */
	public void setNullValueString(String nullValueString) {
		this.nullValueString = nullValueString;
	}

	/**
	 * Gets the null value string.
	 *
	 * @return the null value string
	 */
	public String getNullValueString() {
		return nullValueString;
	}

	/**
	 * Sets the export structure.
	 *
	 * @param exportStructure
	 *            the new export structure
	 */
	public void setExportStructure(boolean exportStructure) {
		this.exportStructure = exportStructure;
	}

	/**
	 * Checks if is export structure.
	 *
	 * @return true, if is export structure
	 */
	public boolean isExportStructure() {
		return exportStructure;
	}

	/**
	 * Get the array containing all relevant configuration data to store it in a SecureKeyStore
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see de.soderer.utilities.SecureDataEntry#getStorageData()
	 */
	@Override
	public Object[] getStorageData() {
		return new Object[] {
			getEntryName(),
			dbVendor,
			hostname,
			dbName,
			username,
			password,
			sqlStatementOrTablelist,
			outputpath,
			exportType,
			log,
			verbose,
			zip,
			encoding,
			separator,
			stringQuote,
			indentation,
			alwaysQuote,
			createBlobFiles,
			createClobFiles,
			dateAndDecimalLocale,
			beautify,
			noHeaders,
			exportStructure,
			nullValueString };
	}
	
	/**
	 * Read the array given from a SecureKeyStore to get all relevant configuration data that was stored
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see de.soderer.utilities.SecureDataEntry#loadData(java.util.List)
	 */
	@Override
	public void loadData(List<String> valueStrings) throws Exception {
		int i = 0;
		setEntryName(valueStrings.get(i++));
		dbVendor = DbVendor.getDbVendorByName(valueStrings.get(i++));
		hostname = valueStrings.get(i++);
		dbName = valueStrings.get(i++);
		username = valueStrings.get(i++);
		password = valueStrings.get(i++);
		sqlStatementOrTablelist = valueStrings.get(i++);
		outputpath = valueStrings.get(i++);
		exportType = ExportType.getFromString(valueStrings.get(i++));
		log = Utilities.interpretAsBool(valueStrings.get(i++));
		verbose = Utilities.interpretAsBool(valueStrings.get(i++));
		zip = Utilities.interpretAsBool(valueStrings.get(i++));
		encoding = valueStrings.get(i++);
		separator = valueStrings.get(i++).toCharArray()[0];
		stringQuote = valueStrings.get(i++).toCharArray()[0];
		indentation = valueStrings.get(i++);
		alwaysQuote = Utilities.interpretAsBool(valueStrings.get(i++));
		createBlobFiles = Utilities.interpretAsBool(valueStrings.get(i++));
		createClobFiles = Utilities.interpretAsBool(valueStrings.get(i++));
		dateAndDecimalLocale = new Locale(valueStrings.get(i++));
		beautify = Utilities.interpretAsBool(valueStrings.get(i++));
		noHeaders = Utilities.interpretAsBool(valueStrings.get(i++));
		exportStructure = Utilities.interpretAsBool(valueStrings.get(i++));
		nullValueString = valueStrings.get(i++);
	}

	/**
	 * Create and configure a worker according to the current configuration
	 * 
	 * @param parent
	 * @return
	 * @throws Exception
	 */
	public WorkerDual<Boolean> getConfiguredWorker(WorkerParentDual parent) throws Exception {
		WorkerDual<Boolean> worker;
		if (getExportType() == ExportType.JSON) {
			worker = new DbJsonExportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				getSqlStatementOrTablelist(),
				getOutputpath());
			((DbJsonExportWorker) worker).setBeautify(isBeautify());
			((DbJsonExportWorker) worker).setIndentation(getIndentation());
		} else if (getExportType() == ExportType.XML) {
			worker = new DbXmlExportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				getSqlStatementOrTablelist(),
				getOutputpath());
			((DbXmlExportWorker) worker).setDateAndDecimalLocale(getDateAndDecimalLocale());
			((DbXmlExportWorker) worker).setBeautify(isBeautify());
			((DbXmlExportWorker) worker).setIndentation(getIndentation());
			((DbXmlExportWorker) worker).setNullValueText(getNullValueString());
		} else if (getExportType() == ExportType.SQL) {
			worker = new DbSqlExportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				getSqlStatementOrTablelist(),
				getOutputpath());
			((DbSqlExportWorker) worker).setDateAndDecimalLocale(getDateAndDecimalLocale());
			((DbSqlExportWorker) worker).setBeautify(isBeautify());
		} else {
			worker = new DbCsvExportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				getSqlStatementOrTablelist(),
				getOutputpath());
			((DbCsvExportWorker) worker).setDateAndDecimalLocale(getDateAndDecimalLocale());
			((DbCsvExportWorker) worker).setSeparator(getSeparator());
			((DbCsvExportWorker) worker).setStringQuote(getStringQuote());
			((DbCsvExportWorker) worker).setAlwaysQuote(isAlwaysQuote());
			((DbCsvExportWorker) worker).setBeautify(isBeautify());
			((DbCsvExportWorker) worker).setNoHeaders(isNoHeaders());
			((DbCsvExportWorker) worker).setNullValueText(getNullValueString());
		}
		((AbstractDbExportWorker) worker).setLog(isLog());
		((AbstractDbExportWorker) worker).setZip(isZip());
		((AbstractDbExportWorker) worker).setEncoding(getEncoding());
		((AbstractDbExportWorker) worker).setCreateBlobFiles(isCreateBlobFiles());
		((AbstractDbExportWorker) worker).setCreateClobFiles(isCreateClobFiles());
		((AbstractDbExportWorker) worker).setExportStructure(isExportStructure());
		
		return worker;
	}
}
