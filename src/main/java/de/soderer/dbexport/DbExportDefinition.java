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
import de.soderer.dbexport.worker.DbYamlExportWorker;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbDefinitionException;
import de.soderer.utilities.worker.WorkerParentDual;

/**
 * All parameters of a data export, as given on the command line, in the console menu or in the GUI.
 *
 * <p>
 * {@link #getConfiguredWorker(WorkerParentDual)} creates the export worker for these parameters.
 * </p>
 */
public class DbExportDefinition extends DbConnectionDefinition {
	/**
	 * Command line keyword of the database connection test.
	 */
	public static final String CONNECTIONTEST_SIGN = "connectiontest";

	/**
	 * Supported export data formats.
	 */
	public enum DataType {
		/**
		 * Comma (or other character) separated values
		 */
		CSV,
		/**
		 * JSON array of objects
		 */
		JSON,
		/**
		 * YAML list of objects
		 */
		YAML,
		/**
		 * vCard file
		 */
		VCF,
		/**
		 * XML elements
		 */
		XML,
		/**
		 * SQL insert statements
		 */
		SQL,
		/**
		 * KeePass database file
		 */
		KDBX;

		/**
		 * Returns the data type for its name.
		 *
		 * @param dataTypeString the name of the data type (case insensitive)
		 * @return the data type
		 * @throws RuntimeException if there is no data type with this name
		 */
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

	/** Use escape sequences (e.g. \n, \t) when writing csv string values, instead of writing raw characters. */
	private boolean interpretEscapeSequences = true;

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
	 * Creates a definition with the default values, the parameters are set via the setter methods.
	 */
	public DbExportDefinition() {
		// Nothing to initialize
	}

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
	 * Sets whether the export data is read from a text file, whose path is given as statement or table list (default: false).
	 *
	 * @param statementFile true if the export data is read from a text file, whose path is given as statement or table list
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
	 * Sets the password of a zip output file.
	 *
	 * @param zipPassword the zip password, or null for an unencrypted zip file
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
	 * Sets the password of a KeePass output file (mandatory for the KDBX format).
	 *
	 * @param kdbxPassword the KeePass password
	 */
	public void setKdbxPassword(final char[] kdbxPassword) {
		this.kdbxPassword = kdbxPassword;
	}

	/**
	 * Returns the time zone of the database.
	 *
	 * @return the time zone ID, e.g. "Europe/Berlin"
	 */
	public String getDatabaseTimeZone() {
		return databaseTimeZone;
	}

	/**
	 * Sets the time zone of the database. Null means the system's default time zone.
	 *
	 * @param databaseTimeZone the time zone ID, e.g. "Europe/Berlin"
	 */
	public void setDatabaseTimeZone(final String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
		if (this.databaseTimeZone == null) {
			this.databaseTimeZone = TimeZone.getDefault().getID();
		}
	}

	/**
	 * Returns the time zone of the exported date values.
	 *
	 * @return the time zone ID, e.g. "Europe/Berlin"
	 */
	public String getExportDataTimeZone() {
		return exportDataTimeZone;
	}

	/**
	 * Sets the time zone of the exported date values. Null means the system's default time zone.
	 *
	 * @param exportDataTimeZone the time zone ID, e.g. "Europe/Berlin"
	 */
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
	 * Sets the character to escape the CSV string quote within quoted values. Default is '"'.
	 *
	 * @param stringQuoteEscapeCharacter the escape character
	 */
	public void setStringQuoteEscapeCharacter(final char stringQuoteEscapeCharacter) {
		this.stringQuoteEscapeCharacter = stringQuoteEscapeCharacter;
	}

	/**
	 * Sets whether line breaks and tabs in CSV string values are written as escape sequences like \n or \t (default: true).
	 *
	 * @param interpretEscapeSequences true if line breaks and tabs in CSV string values are written as escape sequences like \n or \t
	 */
	public void setInterpretEscapeSequences(final boolean interpretEscapeSequences) {
		this.interpretEscapeSequences = interpretEscapeSequences;
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
		this.dbVendor = DbVendor.getDbVendorByName(dbVendor);
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
			// Language tag ("de-DE"), because getDateFormatLocale() parses it with Locale.forLanguageTag(), which does
			// not understand Locale.toString() ("de_DE") and returned the root locale for it
			this.dateFormatLocale = dateFormatLocale.toLanguageTag();
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
	 * Checks if escape sequences (e.g. \n, \t) are used when writing csv string values.
	 *
	 * @return true, if escape sequences are used
	 */
	public boolean isInterpretEscapeSequences() {
		return interpretEscapeSequences;
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

	/**
	 * Returns the date format pattern, which overrides the format of the locale.
	 *
	 * @return the date format pattern, or null
	 */
	public String getDateFormat() {
		return dateFormat;
	}

	/**
	 * Sets the date format pattern (Java format characters), which overrides the format of the locale.
	 *
	 * @param dateFormat the date format pattern, or null
	 */
	public void setDateFormat(final String dateFormat) {
		this.dateFormat = dateFormat;
	}

	/**
	 * Returns the date time format pattern, which overrides the format of the locale.
	 *
	 * @return the date time format pattern, or null
	 */
	public String getDateTimeFormat() {
		return dateTimeFormat;
	}

	/**
	 * Sets the date time format pattern (Java format characters), which overrides the format of the locale.
	 *
	 * @param dateTimeFormat the date time format pattern, or null
	 */
	public void setDateTimeFormat(final String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}

	/**
	 * Returns the decimal separator, which overrides the one of the locale.
	 *
	 * @return the decimal separator, or null
	 */
	public Character getDecimalSeparator() {
		return decimalSeparator;
	}

	/**
	 * Sets the decimal separator, which overrides the one of the locale.
	 *
	 * @param decimalSeparator the decimal separator ('.' or ','), or null
	 */
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
	public void checkParameters() throws Exception {
		if (getDbVendor() != null) {
			try {
				if (!new DbDriverSupplier(null, getDbVendor()).supplyDriver(DbExport.APPLICATION_NAME, DbExport.CONFIGURATION_FILE)) {
					throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + getDbVendor());
				}
			} catch (final Exception e) {
				throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + getDbVendor(), e);
			}
		}

		super.checkParameters();

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
		} else if (outputpath != null && sqlStatementOrTablelist != null && (sqlStatementOrTablelist.toLowerCase().startsWith("select ")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\t")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\n")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\r"))) {
			if (new File(outputpath).exists() && !new File(outputpath).isDirectory() && ! replaceAlreadyExistingFiles) {
				throw new DbExportException("Outputpath file already exists: " + outputpath);
			}
		}

		if (compression != FileCompressionType.ZIP && zipPassword != null) {
			throw new DbExportException("ZipPassword is set without zip compression");
		}

		if (dataType == DataType.KDBX && kdbxPassword == null) {
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

	/**
	 * Sets the JSON file to export the structure of the selected tables to. If set, only the structure is exported,
	 * no data. A file name without directory is placed in the output directory (or next to the output file).
	 *
	 * @param exportStructureFilePath the structure file path, "console", or null for a data export
	 */
	public void setExportStructureFilePath(final String exportStructureFilePath) {
		this.exportStructureFilePath = exportStructureFilePath;
	}

	/**
	 * Returns the absolute path of the structure file (see {@link #setExportStructureFilePath(String)}).
	 *
	 * @return the structure file path, "console", or null for a data export
	 */
	public String getExportStructureFilePath() {
		if (exportStructureFilePath == null) {
			return null;
		} else if ("console".equalsIgnoreCase(exportStructureFilePath)) {
			return "console";
		} else if (outputpath != null) {
			File exportStructureFile = new File(exportStructureFilePath);
			if (exportStructureFile.getParentFile() == null) {
				// The output path is a file for a single statement export. Before, the structure file was always created
				// within the output path, which failed for an output file ("export.csv/dbstructure_....json").
				final File outputFile = new File(outputpath);
				final boolean outputIsDirectory = outputFile.isDirectory() || (!outputFile.exists() && !isSingleStatementExport());
				final File baseDirectory = outputIsDirectory ? outputFile : outputFile.getAbsoluteFile().getParentFile();
				exportStructureFile = new File(baseDirectory, exportStructureFilePath);
			}
			return exportStructureFile.getAbsolutePath();
		} else {
			return exportStructureFilePath;
		}
	}

	/**
	 * Returns whether already existing output files are replaced.
	 *
	 * @return true if already existing output files are replaced
	 */
	public boolean isReplaceAlreadyExistingFiles() {
		return replaceAlreadyExistingFiles;
	}

	/**
	 * Sets whether already existing output files are replaced (default: false).
	 *
	 * @param replaceAlreadyExistingFiles true if already existing output files are replaced
	 */
	public void setReplaceAlreadyExistingFiles(final boolean replaceAlreadyExistingFiles) {
		this.replaceAlreadyExistingFiles = replaceAlreadyExistingFiles;
	}

	/**
	 * Returns whether a missing output directory is created.
	 *
	 * @return true if a missing output directory is created
	 */
	public boolean isCreateOutputDirectoyIfNotExists() {
		return createOutputDirectoyIfNotExists;
	}

	/**
	 * Sets whether a missing output directory is created (default: false).
	 *
	 * @param createOutputDirectoyIfNotExists true if a missing output directory is created
	 */
	public void setCreateOutputDirectoyIfNotExists(final boolean createOutputDirectoyIfNotExists) {
		this.createOutputDirectoyIfNotExists = createOutputDirectoyIfNotExists;
	}

	/**
	 * Create and configure a worker according to the current configuration
	 *
	 * @param parent
	 * @return
	 */
	/**
	 * Checks whether the export data is a single SQL statement (and not a table list).
	 */
	private boolean isSingleStatementExport() {
		if (sqlStatementOrTablelist == null || statementFile) {
			return false;
		} else {
			final String lowerCaseStatement = sqlStatementOrTablelist.toLowerCase(Locale.ROOT);
			return lowerCaseStatement.startsWith("select ") || lowerCaseStatement.startsWith("select\t") || lowerCaseStatement.startsWith("select\n") || lowerCaseStatement.startsWith("select\r");
		}
	}

	/**
	 * Creates and configures the worker for the export format according to the current configuration.
	 *
	 * @param parent parent to signal the progress to, or null if it is set later
	 * @return the configured worker
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
				((DbCsvExportWorker) worker).setInterpretEscapeSequences(isInterpretEscapeSequences());
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
			case YAML:
				worker = new DbYamlExportWorker(parent,
						this,
						isStatementFile(),
						getSqlStatementOrTablelist(),
						getOutputpath());
				worker.setBeautify(isBeautify());
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
				((DbCsvExportWorker) worker).setInterpretEscapeSequences(isInterpretEscapeSequences());
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

	/**
	 * Returns the command line parameters for this export, e.g. to show them in the console menu.
	 *
	 * @return the command line parameters
	 */
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
		if (getOutputpath() != null) {
			// Not set for a structure only export
			params += " -output '" + getOutputpath().replace("'", "\\'") + "'";
		}
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
			// The command line parameter is "-compress" ("-compression" was rejected as invalid parameter)
			params += " " + "-compress " + getCompression().name();
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
		if (!TimeZone.getDefault().getID().equalsIgnoreCase(getDatabaseTimeZone())) {
			params += " " + "-dbtz" + " " + getDatabaseTimeZone();
		}
		if (!TimeZone.getDefault().getID().equalsIgnoreCase(getExportDataTimeZone())) {
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
		if (!isInterpretEscapeSequences()) {
			params += " " + "-noescapesequences";
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
		// Only the language is stored by "-f", so compare the languages (the default locale also has a country)
		if (!Locale.getDefault().getLanguage().equals(getDateFormatLocale().getLanguage())) {
			params += " " + "-f" + " " + getDateFormatLocale().getLanguage();
		}
		// Quoted, because date patterns usually contain blanks ("dd.MM.yyyy HH:mm:ss")
		if (Utilities.isNotBlank(getDateFormat())) {
			params += " " + "-dateFormat" + " '" + getDateFormat().replace("'", "\\'") + "'";
		}
		if (Utilities.isNotBlank(getDateTimeFormat())) {
			params += " " + "-dateTimeFormat" + " '" + getDateTimeFormat().replace("'", "\\'") + "'";
		}
		if (getDecimalSeparator() != null) {
			params += " " + "-decimalSeparator" + " '" + getDecimalSeparator() + "'";
		}
		if (isBeautify()) {
			// The command line parameter is "-beautify" ("-b" was rejected as invalid parameter)
			params += " " + "-beautify";
		}
		if (isNoHeaders()) {
			params += " " + "-noheaders";
		}
		if (getExportStructureFilePath() != null) {
			params += " " + "-structure '" + getExportStructureFilePath().replace("'", "\\'") + "'";
		}
		if (!"".equals(getNullValueString())) {
			params += " " + "-n" + " '" + getNullValueString().replace("'", "\\'") + "'";
		}
		if (isCreateOutputDirectoyIfNotExists()) {
			params += " " + "-createOutputDirectoyIfNotExists";
		}
		if (isReplaceAlreadyExistingFiles()) {
			params += " " + "-replaceAlreadyExistingFiles";
		}
		return params;
	}

	/**
	 * Takes over the parameters of another definition. The export parameters are only taken over from another
	 * {@link DbExportDefinition}, otherwise only the connection parameters.
	 *
	 * @param otherDbConnectionDefinition the definition to take over the parameters from, or null to reset all parameters
	 */
	@Override
	public void importParameters(final DbConnectionDefinition otherDbConnectionDefinition) {
		super.importParameters(otherDbConnectionDefinition);

		if (otherDbConnectionDefinition == null) {
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
			interpretEscapeSequences = true;
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
		} else if (otherDbConnectionDefinition instanceof DbExportDefinition) {
			final DbExportDefinition otherDbExportDefinition = (DbExportDefinition) otherDbConnectionDefinition;
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
			interpretEscapeSequences = otherDbExportDefinition.isInterpretEscapeSequences();
			indentation = otherDbExportDefinition.getIndentation();
			alwaysQuote = otherDbExportDefinition.isAlwaysQuote();
			createBlobFiles = otherDbExportDefinition.isCreateBlobFiles();
			createClobFiles = otherDbExportDefinition.isCreateClobFiles();
			if (otherDbExportDefinition.getDateFormatLocale() == null) {
				dateFormatLocale = null;
			} else {
				dateFormatLocale = otherDbExportDefinition.getDateFormatLocale().toLanguageTag();
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
