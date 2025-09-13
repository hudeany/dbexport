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
import de.soderer.dbexport.worker.DbKdbxExportWorker;
import de.soderer.dbexport.worker.DbSqlExportWorker;
import de.soderer.dbexport.worker.DbVcfExportWorker;
import de.soderer.dbexport.worker.DbXmlExportWorker;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
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
		VCF,
		XML,
		SQL,
		KDBX;

		public static DataType getFromString(final String dataTypeString) {
			for (final DataType dataType : DataType.values()) {
				if (dataType.toString().equalsIgnoreCase(dataTypeString)) {
					return dataType;
				}
			}
			throw new RuntimeException("Invalid export format: " + dataTypeString);
		}
	}

	// Mandatory parameters

	/** The sql statement or tablelist. */
	private String sqlStatementOrTablelist = "*";

	/** The outputpath. */
	private String outputpath = null;

	// Default optional parameters

	/** The export type. */
	private DataType dataType = DataType.CSV;

	/** Use statement file. */
	private boolean statementFile = false;

	/** Log activation. */
	private boolean log = false;

	/** The verbose. */
	private boolean verbose = false;

	/** The compression. */
	private FileCompressionType compression = null;

	/** The zip password */
	private char[] zipPassword = null;

	/** The kdbx password */
	private char[] kdbxPassword = null;

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

	/** The export structure file. */
	private String exportStructureFilePath = null;

	/** The null value string. */
	private String nullValueString = "";

	private boolean replaceAlreadyExistingFiles = false;

	private boolean createOutputDirectoyIfNotExists = false;

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
	 * Sets the compression type.
	 *
	 * @param compression
	 *            the new compression type
	 */
	public void setCompression(final FileCompressionType compression) {
		this.compression = compression;
	}

	/**
	 * Sets the zip password.
	 *
	 * @param zip password
	 */
	public void setZipPassword(final char[] zipPassword) {
		this.zipPassword = zipPassword;
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

	/**
	 * Sets the kdbx password.
	 *
	 * @param kdbx password
	 */
	public void setKdbxPassword(final char[] kdbxPassword) {
		this.kdbxPassword = kdbxPassword;
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
	 * Sets the database vendor.
	 *
	 * @param dbVendor
	 *            the new database vendor
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
	 * Checks if is sql statement or file pattern.
	 *
	 * @return true, if is sql statement or file pattern
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
	 * Get CompressionType
	 *
	 * @return CompressionType
	 */
	public FileCompressionType getCompression() {
		return compression;
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
	 * Get the optional kdbx password.
	 *
	 * @return kdbx password.
	 */
	public char[] getKdbxPassword() {
		return kdbxPassword;
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
	public void checkParameters() throws Exception {
		super.checkParameters(DbExport.APPLICATION_NAME, DbExport.CONFIGURATION_FILE);

		if (outputpath == null && exportStructureFilePath == null) {
			throw new DbExportException("Outputpath is missing");
		} else if ("console".equalsIgnoreCase(outputpath)) {
			if (compression != null) {
				throw new DbExportException("Compression not allowed for console output");
			}
		} else if ("gui".equalsIgnoreCase(outputpath)) {
			if (compression != null) {
				throw new DbExportException("Compression not allowed for gui output");
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
		}

		if (compression != FileCompressionType.ZIP && zipPassword != null) {
			throw new DbExportException("ZipPassword is set without zip compression");
		}

		if (dataType == DataType.KDBX && kdbxPassword != null) {
			throw new DbExportException("KDBX data type is set without kdbx password");
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

	public void setExportStructureFilePath(final String exportStructureFilePath) {
		this.exportStructureFilePath = exportStructureFilePath;
	}

	public String getExportStructureFilePath() {
		if (exportStructureFilePath == null) {
			return null;
		} else if ("console".equalsIgnoreCase(exportStructureFilePath)) {
			return "console";
		} else if (outputpath != null) {
			File exportStructureFile = new File(exportStructureFilePath);
			if (exportStructureFile.getParentFile() == null) {
				exportStructureFile = new File(outputpath, exportStructureFilePath);
			}
			return exportStructureFile.getAbsolutePath();
		} else {
			return exportStructureFilePath;
		}
	}

	public boolean isReplaceAlreadyExistingFiles() {
		return replaceAlreadyExistingFiles;
	}

	public void setReplaceAlreadyExistingFiles(final boolean replaceAlreadyExistingFiles) {
		this.replaceAlreadyExistingFiles = replaceAlreadyExistingFiles;
	}

	public boolean isCreateOutputDirectoyIfNotExists() {
		return createOutputDirectoyIfNotExists;
	}

	public void setCreateOutputDirectoyIfNotExists(final boolean createOutputDirectoyIfNotExists) {
		this.createOutputDirectoyIfNotExists = createOutputDirectoyIfNotExists;
	}

	/**
	 * Create and configure a worker according to the current configuration
	 *
	 * @param parent
	 * @return
	 */
	public AbstractDbExportWorker getConfiguredWorker(final WorkerParentDual parent) {
		AbstractDbExportWorker worker;
		switch (getDataType()) {
			case CSV:
				worker = new DbCsvExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath());
				worker.setDateFormatLocale(getDateFormatLocale());
				worker.setDateFormat(getDateFormat());
				worker.setDateTimeFormat(getDateTimeFormat());
				worker.setDecimalSeparator(getDecimalSeparator());
				((DbCsvExportWorker) worker).setSeparator(getSeparator());
				((DbCsvExportWorker) worker).setStringQuote(getStringQuote());
				((DbCsvExportWorker) worker).setStringQuoteEscapeCharacter(getStringQuoteEscapeCharacter());
				((DbCsvExportWorker) worker).setAlwaysQuote(isAlwaysQuote());
				worker.setBeautify(isBeautify());
				((DbCsvExportWorker) worker).setNoHeaders(isNoHeaders());
				((DbCsvExportWorker) worker).setNullValueText(getNullValueString());
				break;
			case JSON:
				worker = new DbJsonExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath());
				worker.setBeautify(isBeautify());
				((DbJsonExportWorker) worker).setIndentation(getIndentation());
				break;
			case SQL:
				worker = new DbSqlExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath());
				worker.setDateFormatLocale(getDateFormatLocale());
				worker.setDateFormat(getDateFormat());
				worker.setDateTimeFormat(getDateTimeFormat());
				worker.setDecimalSeparator(getDecimalSeparator());
				worker.setBeautify(isBeautify());
				break;
			case VCF:
				worker = new DbVcfExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath());
				break;
			case XML:
				worker = new DbXmlExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath());
				worker.setDateFormatLocale(getDateFormatLocale());
				worker.setDateFormat(getDateFormat());
				worker.setDateTimeFormat(getDateTimeFormat());
				worker.setDecimalSeparator(getDecimalSeparator());
				worker.setBeautify(isBeautify());
				((DbXmlExportWorker) worker).setIndentation(getIndentation());
				((DbXmlExportWorker) worker).setNullValueText(getNullValueString());
				break;
			case KDBX:
				worker = new DbKdbxExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath(),
						getKdbxPassword());
				break;
			default:
				// default CSV
				worker = new DbCsvExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath());
				worker.setDateFormatLocale(getDateFormatLocale());
				worker.setDateFormat(getDateFormat());
				worker.setDateTimeFormat(getDateTimeFormat());
				worker.setDecimalSeparator(getDecimalSeparator());
				((DbCsvExportWorker) worker).setSeparator(getSeparator());
				((DbCsvExportWorker) worker).setStringQuote(getStringQuote());
				((DbCsvExportWorker) worker).setStringQuoteEscapeCharacter(getStringQuoteEscapeCharacter());
				((DbCsvExportWorker) worker).setAlwaysQuote(isAlwaysQuote());
				worker.setBeautify(isBeautify());
				((DbCsvExportWorker) worker).setNoHeaders(isNoHeaders());
				((DbCsvExportWorker) worker).setNullValueText(getNullValueString());
				break;
		}
		worker.setLog(isLog());
		worker.setCompression(getCompression());
		worker.setZipPassword(getZipPassword());
		worker.setUseZipCrypto(isUseZipCrypto());
		worker.setEncoding(getEncoding());
		worker.setCreateBlobFiles(isCreateBlobFiles());
		worker.setCreateClobFiles(isCreateClobFiles());
		worker.setExportStructureFilePath(getExportStructureFilePath());
		worker.setDatabaseTimeZone(getDatabaseTimeZone());
		worker.setExportDataTimeZone(getExportDataTimeZone());
		worker.setReplaceAlreadyExistingFiles(isReplaceAlreadyExistingFiles());
		worker.setCreateOutputDirectoyIfNotExists(isCreateOutputDirectoyIfNotExists());

		return worker;
	}

	public String toParamsString() {
		String params = "";
		params += getDbVendor().name();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.HSQL && getDbVendor() != DbVendor.Derby) {
			params += " " + getHostnameAndPort();
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
		if (getCompression() != null) {
			params += " " + "-compression " + getCompression().name();
		}
		if (getZipPassword() != null) {
			params += " " + "-zippassword" + " '" + new String(getZipPassword()).replace("'", "\\'") + "'";
		}
		if (getKdbxPassword() != null) {
			params += " " + "-kdbxpassword" + " '" + new String(getKdbxPassword()).replace("'", "\\'") + "'";
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
		if (getExportStructureFilePath() != null) {
			params += " " + "-structure \"" + getExportStructureFilePath() + "\"";
		}
		if (!"".equals(getNullValueString())) {
			params += " " + "-n" + " '" + getNullValueString() + "'";
		}
		if (isCreateOutputDirectoyIfNotExists()) {
			params += " " + "-createOutputDirectoyIfNotExists";
		}
		if (isReplaceAlreadyExistingFiles()) {
			params += " " + "-replaceAlreadyExistingFiles";
		}
		return params;
	}

	@Override
	public void importParameters(final DbDefinition otherDbDefinition) {
		super.importParameters(otherDbDefinition);

		if (otherDbDefinition == null) {
			sqlStatementOrTablelist = "*";
			outputpath = null;
			dataType = DataType.CSV;
			statementFile = false;
			log = false;
			verbose = false;
			compression = null;
			zipPassword = null;
			kdbxPassword = null;
			useZipCrypto = false;
			databaseTimeZone = TimeZone.getDefault().getID();
			exportDataTimeZone = TimeZone.getDefault().getID();
			encoding = StandardCharsets.UTF_8;
			separator = ';';
			stringQuote = '"';
			stringQuoteEscapeCharacter = '"';
			indentation = "\t";
			alwaysQuote = false;
			createBlobFiles = false;
			createClobFiles = false;
			dateFormatLocale = Locale.getDefault().getLanguage();
			dateFormat = null;
			dateTimeFormat = null;
			decimalSeparator = null;
			beautify = false;
			noHeaders = false;
			exportStructureFilePath = null;
			nullValueString = "";
			createOutputDirectoyIfNotExists = false;
			replaceAlreadyExistingFiles = false;
		} else if (otherDbDefinition instanceof DbExportDefinition) {
			final DbExportDefinition otherDbExportDefinition = (DbExportDefinition) otherDbDefinition;
			sqlStatementOrTablelist = otherDbExportDefinition.getSqlStatementOrTablelist();
			outputpath = otherDbExportDefinition.getOutputpath();
			dataType = otherDbExportDefinition.getDataType();
			statementFile = otherDbExportDefinition.isStatementFile();
			log = otherDbExportDefinition.isLog();
			verbose = otherDbExportDefinition.isVerbose();
			compression = otherDbExportDefinition.getCompression();
			zipPassword = otherDbExportDefinition.getZipPassword();
			kdbxPassword = otherDbExportDefinition.getKdbxPassword();
			useZipCrypto = otherDbExportDefinition.isUseZipCrypto();
			databaseTimeZone = otherDbExportDefinition.getDatabaseTimeZone();
			exportDataTimeZone = otherDbExportDefinition.getExportDataTimeZone();
			encoding = otherDbExportDefinition.getEncoding();
			separator = otherDbExportDefinition.getSeparator();
			stringQuote = otherDbExportDefinition.getStringQuote();
			stringQuoteEscapeCharacter = otherDbExportDefinition.getStringQuoteEscapeCharacter();
			indentation = otherDbExportDefinition.getIndentation();
			alwaysQuote = otherDbExportDefinition.isAlwaysQuote();
			createBlobFiles = otherDbExportDefinition.isCreateBlobFiles();
			createClobFiles = otherDbExportDefinition.isCreateClobFiles();
			if (otherDbExportDefinition.getDateFormatLocale() == null) {
				dateFormatLocale = null;
			} else {
				dateFormatLocale = otherDbExportDefinition.getDateFormatLocale().getLanguage();
			}
			dateFormat = otherDbExportDefinition.getDateFormat();
			dateTimeFormat = otherDbExportDefinition.getDateTimeFormat();
			decimalSeparator = otherDbExportDefinition.getDecimalSeparator();
			beautify = otherDbExportDefinition.isBeautify();
			noHeaders = otherDbExportDefinition.isNoHeaders();
			exportStructureFilePath = otherDbExportDefinition.getExportStructureFilePath();
			nullValueString = otherDbExportDefinition.getNullValueString();
			createOutputDirectoyIfNotExists = otherDbExportDefinition.isCreateOutputDirectoyIfNotExists();
			replaceAlreadyExistingFiles = otherDbExportDefinition.isReplaceAlreadyExistingFiles();
		}
	}
}
