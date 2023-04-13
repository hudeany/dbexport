package de.soderer.dbexport;

import java.awt.GraphicsEnvironment;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.TimeZone;

import de.soderer.dbexport.worker.AbstractDbExportWorker;
import de.soderer.dbexport.worker.DbCsvExportWorker;
import de.soderer.dbexport.worker.DbJsonExportWorker;
import de.soderer.dbexport.worker.DbSqlExportWorker;
import de.soderer.dbexport.worker.DbXmlExportWorker;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.worker.WorkerParentDual;

/**
 * The Class DbExportDefinition.
 */
public class DbExportDefinition extends DbDefinition {
	public static final String CONNECTIONTEST_SIGN = "connectiontest";

	/**
	 * The Enum DataType.
	 */
	public enum DataType {
		CSV,
		JSON,
		XML,
		SQL;

		/**
		 * Gets the string representation of export type.
		 *
		 * @param dataType
		 *            the data type
		 * @return the from string
		 * @throws Exception
		 *             the exception
		 */
		public static DataType getFromString(final String dataTypeString) throws Exception {
			for (final DataType dataType : DataType.values()) {
				if (dataType.toString().equalsIgnoreCase(dataTypeString)) {
					return dataType;
				}
			}
			throw new Exception("Invalid export format: " + dataTypeString);
		}
	}

	// Mandatory parameters

	/** The sql statement or tablelist. */
	private String sqlStatementOrTablelist = "*";

	/** The outputpath. */
	private String outputpath;

	// Default optional parameters

	/** The export type. */
	private DataType dataType = DataType.CSV;

	/** Use statement file. */
	private boolean statementFile = false;

	/** Log activation. */
	private boolean log = false;

	/** The verbose. */
	private boolean verbose = false;

	/** The zip. */
	private boolean zip = false;

	/** The zippassword */
	private char[] zipPassword = null;

	private boolean useZipCrypto = false;

	private String databaseTimeZone = TimeZone.getDefault().getID();

	private String exportDataTimeZone = TimeZone.getDefault().getID();

	/** The encoding. */
	private Charset encoding = StandardCharsets.UTF_8;

	/** The separator. */
	private char separator = ';';

	/** The string quote. */
	private char stringQuote = '"';

	/** The string quote escape character. */
	private char stringQuoteEscapeCharacter = '"';

	/** The indentation. */
	private String indentation = "\t";

	/** The always quote. */
	private boolean alwaysQuote = false;

	/** The create blob files. */
	private boolean createBlobFiles = false;

	/** The create clob files. */
	private boolean createClobFiles = false;

	/** The date format locale. */
	private String dateFormatLocale = Locale.getDefault().getLanguage();

	/** The date format */
	private String dateFormat = null;

	/** The date time format */
	private String dateTimeFormat = null;

	/** The decimal separator */
	private Character decimalSeparator = null;

	/** The beautify. */
	private boolean beautify = false;

	/** The no headers. */
	private boolean noHeaders = false;

	/** The export structure. */
	private boolean exportStructure = false;

	/** The null value string. */
	private String nullValueString = "";

	/**
	 * Sets the data type.
	 *
	 * @param dataType
	 *            the new export type
	 */
	public void setDataType(final DataType dataType) {
		this.dataType = dataType;
		if (this.dataType == null) {
			this.dataType = DataType.CSV;
		}
	}

	/**
	 * Sets the data type.
	 *
	 * @param dataType
	 *            the new data type
	 * @throws Exception
	 *             the exception
	 */
	public void setDataType(final String dataType) throws Exception {
		this.dataType = DataType.getFromString(dataType);
	}

	/**
	 * Read statement or tablepattern from file
	 * @param useStatementFile
	 */
	public void setStatementFile(final boolean statementFile) {
		this.statementFile = statementFile;
	}

	/**
	 * Sets the log.
	 *
	 * @param log
	 *            the new log
	 */
	public void setLog(final boolean log) {
		this.log = log;
	}

	/**
	 * Sets the zip.
	 *
	 * @param zip
	 *            the new zip
	 */
	public void setZip(final boolean zip) {
		this.zip = zip;
	}

	/**
	 * Sets the zip password.
	 *
	 * @param zipPpassword
	 */
	public void setZipPassword(final char[] zipPpassword) {
		zipPassword = zipPpassword;
	}

	/**
	 * Sets the useZipCrypto.
	 *
	 * @param useZipCrypto
	 *            the new useZipCrypto
	 */
	public void setUseZipCrypto(final boolean useZipCrypto) {
		this.useZipCrypto = useZipCrypto;
	}

	public String getDatabaseTimeZone() {
		return databaseTimeZone;
	}

	public void setDatabaseTimeZone(final String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
		if (this.databaseTimeZone == null) {
			this.databaseTimeZone = TimeZone.getDefault().getID();
		}
	}

	public String getExportDataTimeZone() {
		return exportDataTimeZone;
	}

	public void setExportDataTimeZone(final String exportDataTimeZone) {
		this.exportDataTimeZone = exportDataTimeZone;
		if (this.exportDataTimeZone == null) {
			this.exportDataTimeZone = TimeZone.getDefault().getID();
		}
	}

	/**
	 * Sets the encoding.
	 *
	 * @param encoding
	 *            the new encoding
	 */
	public void setEncoding(final Charset encoding) {
		this.encoding = encoding;
	}

	/**
	 * Sets the separator.
	 *
	 * @param separator
	 *            the new separator
	 */
	public void setSeparator(final char separator) {
		this.separator = separator;
	}

	/**
	 * Sets the string quote.
	 *
	 * @param stringQuote
	 *            the new string quote
	 */
	public void setStringQuote(final char stringQuote) {
		this.stringQuote = stringQuote;
	}

	/**
	 * Sets the string quote escape character.
	 *
	 * @param stringQuoteEscapeCharacter
	 */
	public void setStringQuoteEscapeCharacter(final char stringQuoteEscapeCharacter) {
		this.stringQuoteEscapeCharacter = stringQuoteEscapeCharacter;
	}

	/**
	 * Sets the indentation.
	 *
	 * @param indentation
	 *            the new indentation
	 */
	public void setIndentation(final String indentation) {
		this.indentation = indentation;
	}

	/**
	 * Sets the always quote.
	 *
	 * @param alwaysQuote
	 *            the new always quote
	 */
	public void setAlwaysQuote(final boolean alwaysQuote) {
		this.alwaysQuote = alwaysQuote;
	}

	/**
	 * Sets the creates the blob files.
	 *
	 * @param createBlobFiles
	 *            the new creates the blob files
	 */
	public void setCreateBlobFiles(final boolean createBlobFiles) {
		this.createBlobFiles = createBlobFiles;
	}

	/**
	 * Sets the creates the clob files.
	 *
	 * @param createClobFiles
	 *            the new creates the clob files
	 */
	public void setCreateClobFiles(final boolean createClobFiles) {
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
	public void setDbVendor(final String dbVendor) throws Exception {
		this.dbVendor = DbUtilities.DbVendor.getDbVendorByName(dbVendor);
	}

	/**
	 * Sets the date format locale.
	 *
	 * @param dateFormatLocale
	 *            the new date format locale
	 */
	public void setDateFormatLocale(final Locale dateFormatLocale) {
		if (dateFormatLocale == null) {
			this.dateFormatLocale = Locale.getDefault().getLanguage();
		} else {
			this.dateFormatLocale = dateFormatLocale.toString();
		}
	}

	/**
	 * Sets the sql statement or tablelist.
	 *
	 * @param sqlStatementOrTablelist
	 *            the new sql statement or tablelist
	 */
	public void setSqlStatementOrTablelist(final String sqlStatementOrTablelist) {
		this.sqlStatementOrTablelist = sqlStatementOrTablelist;
	}

	/**
	 * Sets the outputpath.
	 *
	 * @param outputpath
	 *            the new outputpath
	 */
	public void setOutputpath(final String outputpath) {
		this.outputpath = outputpath;
		if (this.outputpath != null) {
			this.outputpath = this.outputpath.trim();
			this.outputpath = Utilities.replaceUsersHome(this.outputpath);
			if (this.outputpath.endsWith(File.separator)) {
				this.outputpath = this.outputpath.substring(0, this.outputpath.length() - 1);
			}
		}
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
	 * Gets the data type.
	 *
	 * @return the export type
	 */
	public DataType getDataType() {
		return dataType;
	}

	/**
	 * Checks if is log.
	 *
	 * @return true, if is log
	 */
	public boolean isStatementFile() {
		return statementFile;
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
	 * Get the optional zip password.
	 *
	 * @return zip password.
	 */
	public char[] getZipPassword() {
		return zipPassword;
	}

	/**
	 * Checks if is useZipCrypto.
	 *
	 * @return true, if is useZipCrypto
	 */
	public boolean isUseZipCrypto() {
		return useZipCrypto;
	}

	/**
	 * Gets the encoding.
	 *
	 * @return the encoding
	 */
	public Charset getEncoding() {
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
	 * Gets the string quote escape character.
	 *
	 * @return the string quote escape character
	 */
	public char getStringQuoteEscapeCharacter() {
		return stringQuoteEscapeCharacter;
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
	 * Gets the date format locale.
	 *
	 * @return the date format locale
	 */
	public Locale getDateFormatLocale() {
		if (dateFormatLocale == null) {
			return Locale.getDefault();
		} else {
			return Locale.forLanguageTag(dateFormatLocale);
		}
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(final String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getDateTimeFormat() {
		return dateTimeFormat;
	}

	public void setDateTimeFormat(final String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}

	public Character getDecimalSeparator() {
		return decimalSeparator;
	}

	public void setDecimalSeparator(final Character decimalSeparator) {
		this.decimalSeparator = decimalSeparator;
	}

	/**
	 * Check parameters.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Override
	public void checkParameters() throws DbExportException {
		super.checkParameters();

		if (outputpath == null) {
			throw new DbExportException("Outputpath is missing");
		} else if ("console".equalsIgnoreCase(outputpath)) {
			if (zip) {
				throw new DbExportException("Zipping not allowed for console output");
			}
		} else if ("gui".equalsIgnoreCase(outputpath)) {
			if (zip) {
				throw new DbExportException("Zipping not allowed for gui output");
			} else if (GraphicsEnvironment.isHeadless()) {
				throw new DbExportException("GUI output only works on non-headless systems");
			}
		} else if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\t")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\n")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\r")) {
			if (new File(outputpath).exists() && !new File(outputpath).isDirectory()) {
				throw new DbExportException("Outputpath file already exists: " + outputpath);
			}
		} else {
			if (exportStructure) {
				if (!new File(outputpath).exists()) {
					throw new DbExportException("Outputpath directory does not exist: " + outputpath);
				} else if (!new File(outputpath).isDirectory()) {
					throw new DbExportException("Outputpath is not a directory: " + outputpath);
				}
			}
		}

		if (!zip && zipPassword != null) {
			throw new DbExportException("ZipPassword is set without zipping output");
		}

		if (alwaysQuote && dataType != DataType.CSV) {
			throw new DbExportException("AlwaysQuote is not supported for export format " + dataType);
		}

		if (noHeaders && dataType != DataType.CSV) {
			throw new DbExportException("NoHeaders is not supported for export format " + dataType);
		}

		if (beautify && dataType != DataType.CSV && dataType != DataType.JSON && dataType != DataType.XML) {
			throw new DbExportException("Beautify is not supported for export format " + dataType);
		}
	}

	/**
	 * Sets the beautify.
	 *
	 * @param beautify
	 *            the new beautify
	 */
	public void setBeautify(final boolean beautify) {
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
	public void setVerbose(final boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * Sets the no headers.
	 *
	 * @param noHeaders
	 *            the new no headers
	 */
	public void setNoHeaders(final boolean noHeaders) {
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
	public void setNullValueString(final String nullValueString) {
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
	public void setExportStructure(final boolean exportStructure) {
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
	 * Create and configure a worker according to the current configuration
	 *
	 * @param parent
	 * @return
	 */
	public AbstractDbExportWorker getConfiguredWorker(final WorkerParentDual parent) {
		AbstractDbExportWorker worker;
		if (getDataType() == DataType.JSON) {
			worker = new DbJsonExportWorker(parent,
					getDbVendor(),
					getHostname(),
					getDbName(),
					getUsername(),
					getPassword(),
					getSecureConnection(),
					getTrustStoreFilePath(),
					getTrustStorePassword(),
					isStatementFile(),
					getSqlStatementOrTablelist(),
					getOutputpath());
			((DbJsonExportWorker) worker).setBeautify(isBeautify());
			((DbJsonExportWorker) worker).setIndentation(getIndentation());
		} else if (getDataType() == DataType.XML) {
			worker = new DbXmlExportWorker(parent,
					getDbVendor(),
					getHostname(),
					getDbName(),
					getUsername(),
					getPassword(),
					getSecureConnection(),
					getTrustStoreFilePath(),
					getTrustStorePassword(),
					isStatementFile(),
					getSqlStatementOrTablelist(),
					getOutputpath());
			((DbXmlExportWorker) worker).setDateFormatLocale(getDateFormatLocale());
			((DbXmlExportWorker) worker).setDateFormat(getDateFormat());
			((DbXmlExportWorker) worker).setDateTimeFormat(getDateTimeFormat());
			((DbXmlExportWorker) worker).setDecimalSeparator(getDecimalSeparator());
			((DbXmlExportWorker) worker).setBeautify(isBeautify());
			((DbXmlExportWorker) worker).setIndentation(getIndentation());
			((DbXmlExportWorker) worker).setNullValueText(getNullValueString());
		} else if (getDataType() == DataType.SQL) {
			worker = new DbSqlExportWorker(parent,
					getDbVendor(),
					getHostname(),
					getDbName(),
					getUsername(),
					getPassword(),
					getSecureConnection(),
					getTrustStoreFilePath(),
					getTrustStorePassword(),
					isStatementFile(),
					getSqlStatementOrTablelist(),
					getOutputpath());
			((DbSqlExportWorker) worker).setDateFormatLocale(getDateFormatLocale());
			((DbSqlExportWorker) worker).setDateFormat(getDateFormat());
			((DbSqlExportWorker) worker).setDateTimeFormat(getDateTimeFormat());
			((DbSqlExportWorker) worker).setDecimalSeparator(getDecimalSeparator());
			((DbSqlExportWorker) worker).setBeautify(isBeautify());
		} else {
			worker = new DbCsvExportWorker(parent,
					getDbVendor(),
					getHostname(),
					getDbName(),
					getUsername(),
					getPassword(),
					getSecureConnection(),
					getTrustStoreFilePath(),
					getTrustStorePassword(),
					isStatementFile(),
					getSqlStatementOrTablelist(),
					getOutputpath());
			((DbCsvExportWorker) worker).setDateFormatLocale(getDateFormatLocale());
			((DbCsvExportWorker) worker).setDateFormat(getDateFormat());
			((DbCsvExportWorker) worker).setDateTimeFormat(getDateTimeFormat());
			((DbCsvExportWorker) worker).setDecimalSeparator(getDecimalSeparator());
			((DbCsvExportWorker) worker).setSeparator(getSeparator());
			((DbCsvExportWorker) worker).setStringQuote(getStringQuote());
			((DbCsvExportWorker) worker).setStringQuoteEscapeCharacter(getStringQuoteEscapeCharacter());
			((DbCsvExportWorker) worker).setAlwaysQuote(isAlwaysQuote());
			((DbCsvExportWorker) worker).setBeautify(isBeautify());
			((DbCsvExportWorker) worker).setNoHeaders(isNoHeaders());
			((DbCsvExportWorker) worker).setNullValueText(getNullValueString());
		}
		worker.setLog(isLog());
		worker.setZip(isZip());
		worker.setZipPassword(getZipPassword());
		worker.setUseZipCrypto(isUseZipCrypto());
		worker.setEncoding(getEncoding());
		worker.setCreateBlobFiles(isCreateBlobFiles());
		worker.setCreateClobFiles(isCreateClobFiles());
		worker.setExportStructure(isExportStructure());
		worker.setDatabaseTimeZone(getDatabaseTimeZone());
		worker.setExportDataTimeZone(getExportDataTimeZone());

		return worker;
	}

	public String toParamsString() {
		String params = "";
		params += getDbVendor().name();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.HSQL && getDbVendor() != DbVendor.Derby) {
			params += " " + getHostname();
		}
		params += " " + getDbName();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.Derby) {
			if (getUsername() != null) {
				params += " " + getUsername();
			}
		}
		params += " -export '" + getSqlStatementOrTablelist().replace("'", "\\'") + "'";
		params += " -output '" + getOutputpath().replace("'", "\\'") + "'";
		if (getPassword() != null) {
			params += " '" + new String(getPassword()).replace("'", "\\'") + "'";
		}

		if (getDataType() != DataType.CSV) {
			params += " " + "-x" + " " + getDataType().name();
		}
		if (isStatementFile()) {
			params += " " + "-file";
		}
		if (isLog()) {
			params += " " + "-l";
		}
		if (isVerbose()) {
			params += " " + "-v";
		}
		if (isZip()) {
			params += " " + "-z";
		}
		if (getZipPassword() != null) {
			params += " " + "-zippassword" + " '" + new String(getZipPassword()).replace("'", "\\'") + "'";
		}
		if (isUseZipCrypto()) {
			params += " " + "-useZipCrypto";
		}
		if (TimeZone.getDefault().getDisplayName().equalsIgnoreCase(getDatabaseTimeZone())) {
			params += " " + "-dbtz" + " " + getDatabaseTimeZone();
		}
		if (TimeZone.getDefault().getDisplayName().equalsIgnoreCase(getExportDataTimeZone())) {
			params += " " + "-edtz" + " " + getExportDataTimeZone();
		}
		if (getEncoding() != StandardCharsets.UTF_8) {
			params += " " + "-e" + " " + getEncoding().name();
		}
		if (getSeparator() != ';') {
			params += " " + "-s" + " '" + Character.toString(getSeparator()).replace("'", "\\'") + "'";
		}
		if (getStringQuote() != '"') {
			params += " " + "-q" + " '" + Character.toString(getStringQuote()).replace("'", "\\'") + "'";
		}
		if (getStringQuoteEscapeCharacter() != '"') {
			params += " " + "-qe" + " '" + Character.toString(getStringQuoteEscapeCharacter()).replace("'", "\\'") + "'";
		}
		if (!"\t".equals(getIndentation())) {
			params += " " + "-i" + " '" + getIndentation().replace("'", "\\'") + "'";
		}
		if (isAlwaysQuote()) {
			params += " " + "-a";
		}
		if (isCreateBlobFiles()) {
			params += " " + "-blobfiles";
		}
		if (isCreateClobFiles()) {
			params += " " + "-clobfiles";
		}
		if (Locale.getDefault() != getDateFormatLocale()) {
			params += " " + "-f" + " " + getDateFormatLocale().getLanguage();
		}
		if (Utilities.isNotBlank(getDateFormat())) {
			params += " " + "-dateFormat" + " " + getDateFormat();
		}
		if (Utilities.isNotBlank(getDateTimeFormat())) {
			params += " " + "-dateTimeFormat" + " " + getDateTimeFormat();
		}
		if (isBeautify()) {
			params += " " + "-b";
		}
		if (isNoHeaders()) {
			params += " " + "-noheaders";
		}
		if (isExportStructure()) {
			params += " " + "-structure";
		}
		if (!"".equals(getNullValueString())) {
			params += " " + "-n" + " '" + getNullValueString() + "'";
		}
		return params;
	}
}
