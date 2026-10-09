package de.soderer.dbexport.worker;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.dbexport.DbDriverSupplier;
import de.soderer.dbexport.DbExport;
import de.soderer.dbexport.DbExportException;
import de.soderer.dbexport.converter.CassandraDBValueConverter;
import de.soderer.dbexport.converter.DefaultDBValueConverter;
import de.soderer.dbexport.converter.FirebirdDBValueConverter;
import de.soderer.dbexport.converter.MariaDBValueConverter;
import de.soderer.dbexport.converter.MySQLDBValueConverter;
import de.soderer.dbexport.converter.OracleDBValueConverter;
import de.soderer.dbexport.converter.PostgreSQLDBValueConverter;
import de.soderer.dbexport.converter.SQLiteDBValueConverter;
import de.soderer.json.JsonArray;
import de.soderer.json.JsonObject;
import de.soderer.json.JsonWriter;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DatabaseConstraint;
import de.soderer.utilities.db.DatabaseForeignKey;
import de.soderer.utilities.db.DatabaseIndex;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbColumnType;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.db.data.DbSimpleDataType;
import de.soderer.utilities.db.data.DbVendor;
import de.soderer.utilities.db.exception.DbDefinitionException;
import de.soderer.utilities.db.utilities.CaseInsensitiveMap;
import de.soderer.utilities.db.utilities.CaseInsensitiveSet;
import de.soderer.utilities.worker.WorkerDual;
import de.soderer.utilities.worker.WorkerParentDual;
import de.soderer.utilities.zip.TarGzUtilities;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

/**
 * Base class of the export workers for the supported export formats.
 *
 * <p>
 * Exports the result of a single SQL select statement into one output file, or each table of a table list
 * (table name patterns with the wildcards "*" and "?", "!" excludes tables) into a separate file in the output
 * directory. The output may be compressed, written to the console ("console") or kept in memory ("gui").
 * Alternatively only the structure of the tables is exported as JSON file.
 * </p>
 *
 * <p>
 * Subclasses write the data in their format via the methods called for the output, each table line and each column.
 * </p>
 */
public abstract class AbstractDbExportWorker extends WorkerDual<Boolean> {
	// Mandatory parameters
	/**
	 * The connection parameters of the database.
	 */
	protected DbConnectionDefinition dbDefinition = null;
	private boolean isStatementFile = false;
	private String sqlStatementOrTablelist;
	private String outputpath;
	private ByteArrayOutputStream guiOutputStream = null;

	// Default optional parameters
	/**
	 * Whether the export information is logged in a log file next to the output file.
	 */
	protected boolean log = false;
	/**
	 * Compression of the output files, or null for uncompressed files.
	 */
	protected FileCompressionType compression = null;
	/**
	 * Password of zip output files, or null.
	 */
	protected char[] zipPassword = null;
	/**
	 * Whether zip files are encrypted with the weak ZipCrypto method instead of AES.
	 */
	protected boolean useZipCrypto = false;
	/**
	 * Character encoding of the output.
	 */
	protected Charset encoding = StandardCharsets.UTF_8;
	/**
	 * Whether blobs are exported as separate files instead of base64 encoded values.
	 */
	protected boolean createBlobFiles = false;
	/**
	 * Whether clobs are exported as separate files instead of text values.
	 */
	protected boolean createClobFiles = false;
	/**
	 * Locale of the date and number formats.
	 */
	protected Locale dateFormatLocale = Locale.getDefault();
	/**
	 * Date format pattern, which overrides the format of the locale, or null.
	 */
	protected String dateFormatPattern;
	/**
	 * Date time format pattern, which overrides the format of the locale, or null.
	 */
	protected String dateTimeFormatPattern;
	/**
	 * Number format of the locale (without grouping).
	 */
	protected NumberFormat decimalFormat ;
	/**
	 * Decimal separator, which overrides the one of the locale, or null.
	 */
	protected Character decimalSeparator;
	/**
	 * Whether the output is beautified (format dependent, e.g. aligned CSV columns or indented JSON).
	 */
	protected boolean beautify = false;
	/**
	 * JSON file to export the table structure to instead of the data, or null.
	 */
	protected String exportStructureFilePath = null;
	/**
	 * Whether already existing output files are replaced.
	 */
	protected boolean replaceAlreadyExistingFiles = false;
	/**
	 * Whether a missing output directory is created.
	 */
	protected boolean createOutputDirectoyIfNotExists = false;

	private int overallExportedLines = 0;
	private long overallExportedDataAmountRaw = 0;
	private long overallExportedDataAmountCompressed = 0;

	private String databaseTimeZone = TimeZone.getDefault().getID();
	private String exportDataTimeZone = TimeZone.getDefault().getID();

	private DefaultDBValueConverter dbValueConverter;

	{
		// Create the default number format
		decimalFormat = NumberFormat.getNumberInstance(dateFormatLocale);
		decimalFormat.setGroupingUsed(false);
	}

	/**
	 * Creates the worker.
	 *
	 * @param parent parent to signal the progress to
	 * @param dbDefinition the connection parameters of the database
	 * @param isStatementFile true if sqlStatementOrTablelist is the path of a file containing the statement or table list
	 * @param sqlStatementOrTablelist SQL select statement, or comma separated table name patterns
	 * @param outputpath output file (single statement) or directory (table list), or "console" or "gui"
	 */
	public AbstractDbExportWorker(final WorkerParentDual parent, final DbConnectionDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent);
		this.dbDefinition = dbDefinition;
		this.isStatementFile = isStatementFile;
		this.sqlStatementOrTablelist = sqlStatementOrTablelist;
		this.outputpath = outputpath;
	}

	/**
	 * Sets whether the export information is logged in a log file next to the output file (default: false).
	 *
	 * @param log true if the export information is logged in a log file next to the output file
	 */
	public void setLog(final boolean log) {
		this.log = log;
	}

	/**
	 * Sets the compression of the output files.
	 *
	 * @param compression the compression, or null for uncompressed files
	 */
	public void setCompression(final FileCompressionType compression) {
		this.compression = compression;
	}

	/**
	 * Sets the password of zip output files.
	 *
	 * @param zipPassword the zip password, or null for unencrypted zip files
	 */
	public void setZipPassword(final char[] zipPassword) {
		this.zipPassword = zipPassword;
	}

	/**
	 * Sets whether zip files are encrypted with the weak ZipCrypto method (supported by Windows) instead of AES (default: false).
	 *
	 * @param useZipCrypto true if zip files are encrypted with the weak ZipCrypto method (supported by Windows) instead of AES
	 */
	public void setUseZipCrypto(final boolean useZipCrypto) {
		this.useZipCrypto = useZipCrypto;
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
	 * Sets the character encoding of the output. Default is UTF-8.
	 *
	 * @param encoding the character encoding
	 */
	public void setEncoding(final Charset encoding) {
		this.encoding = encoding;
	}

	/**
	 * Sets whether blobs are exported as separate files instead of base64 encoded values (default: false).
	 *
	 * @param createBlobFiles true if blobs are exported as separate files instead of base64 encoded values
	 */
	public void setCreateBlobFiles(final boolean createBlobFiles) {
		this.createBlobFiles = createBlobFiles;
	}

	/**
	 * Sets whether clobs are exported as separate files instead of text values (default: false).
	 *
	 * @param createClobFiles true if clobs are exported as separate files instead of text values
	 */
	public void setCreateClobFiles(final boolean createClobFiles) {
		this.createClobFiles = createClobFiles;
	}

	/**
	 * Sets the locale of the date and number formats.
	 *
	 * @param dateFormatLocale the locale, or null for the system's default locale
	 */
	public void setDateFormatLocale(final Locale dateFormatLocale) {
		this.dateFormatLocale = dateFormatLocale == null ? Locale.getDefault() : dateFormatLocale;
		dateFormatterCache = null;
		dateTimeFormatterCache = null;
		decimalFormat = NumberFormat.getNumberInstance(this.dateFormatLocale);
		decimalFormat.setGroupingUsed(false);
	}

	/**
	 * Sets whether the output is beautified (format dependent) (default: false).
	 *
	 * @param beautify true if the output is beautified (format dependent)
	 */
	public void setBeautify(final boolean beautify) {
		this.beautify = beautify;
	}

	/**
	 * Sets the JSON file to export the table structure to. If set, no data is exported.
	 *
	 * @param exportStructureFilePath the structure file path, "console", "gui", or null for a data export
	 */
	public void setExportStructureFilePath(final String exportStructureFilePath) {
		this.exportStructureFilePath = exportStructureFilePath;
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
	 * Sets whether already existing output files are replaced (default: false).
	 *
	 * @param replaceAlreadyExistingFiles true if already existing output files are replaced
	 */
	public void setReplaceAlreadyExistingFiles(final boolean replaceAlreadyExistingFiles) {
		this.replaceAlreadyExistingFiles = replaceAlreadyExistingFiles;
	}

	/**
	 * Sets the date format pattern (Java format characters), which overrides the format of the locale.
	 *
	 * @param dateFormat the date format pattern (null is ignored)
	 */
	public void setDateFormat(final String dateFormat) {
		if (dateFormat != null) {
			dateFormatPattern = dateFormat;
			dateFormatterCache = null;
		}
	}

	/**
	 * Sets the date time format pattern (Java format characters), which overrides the format of the locale.
	 *
	 * @param dateTimeFormat the date time format pattern (null is ignored)
	 */
	public void setDateTimeFormat(final String dateTimeFormat) {
		if (dateTimeFormat != null) {
			dateTimeFormatPattern = dateTimeFormat;
			dateTimeFormatterCache = null;
		}
	}

	/**
	 * Sets the decimal separator, which overrides the one of the locale.
	 *
	 * @param decimalSeparator the decimal separator (null is ignored)
	 */
	public void setDecimalSeparator(final Character decimalSeparator) {
		if (decimalSeparator != null) {
			this.decimalSeparator = decimalSeparator;
		}
	}

	private DateTimeFormatter dateFormatterCache = null;
	/**
	 * Returns the formatter for date values (default pattern "yyyy-MM-dd").
	 *
	 * @return the date formatter
	 */
	protected DateTimeFormatter getDateFormatter() {
		if (dateFormatterCache == null) {
			DateTimeFormatter formatter;
			if (Utilities.isNotBlank(dateFormatPattern)) {
				formatter = DateTimeFormatter.ofPattern(dateFormatPattern);
			} else {
				formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			}
			if (dateFormatLocale != null) {
				formatter = formatter.localizedBy(dateFormatLocale);
			}
			dateFormatterCache = formatter;
		}
		return dateFormatterCache;
	}

	private DateTimeFormatter dateTimeFormatterCache = null;
	/**
	 * Returns the formatter for date time values (default pattern "yyyy-MM-dd'T'HH:mm:ss").
	 *
	 * @return the date time formatter
	 */
	protected DateTimeFormatter getDateTimeFormatter() {
		if (dateTimeFormatterCache == null) {
			DateTimeFormatter formatter;
			if (Utilities.isNotBlank(dateTimeFormatPattern)) {
				formatter = DateTimeFormatter.ofPattern(dateTimeFormatPattern);
			} else {
				formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
			}
			if (dateFormatLocale != null) {
				formatter = formatter.localizedBy(dateFormatLocale);
			}
			dateTimeFormatterCache = formatter;
		}
		return dateTimeFormatterCache;
	}

	/**
	 * Returns whether the export creates a single output, i.e. for a select statement or a single table name
	 * without wildcards.
	 *
	 * @return true for a single output
	 */
	public boolean isSingleExport() {
		if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\t")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\n")
				|| sqlStatementOrTablelist.toLowerCase().startsWith("select\r")) {
			return true;
		} else {
			boolean isFirstTableName = true;
			for (final String tablePattern : sqlStatementOrTablelist.split(",| |;|\\||\n")) {
				if (Utilities.isNotBlank(tablePattern)) {
					if (tablePattern.contains("*") || tablePattern.contains("?")) {
						return false;
					}
					if (!tablePattern.startsWith("!")) {
						if (!isFirstTableName) {
							return false;
						}
						isFirstTableName = false;
					}
				}
			}
			return true;
		}

	}

	/**
	 * Executes the export.
	 *
	 * @return true if the export was not canceled
	 * @throws Exception if the parameters are invalid, the database access fails or the output cannot be written
	 */
	@Override
	public Boolean work() throws Exception {
		overallExportedLines = 0;

		dbDefinition.checkParameters();

		if (dbDefinition.getDbVendor() != null) {
			try {
				if (!new DbDriverSupplier(null, dbDefinition.getDbVendor()).supplyDriver(DbExport.APPLICATION_NAME, DbExport.CONFIGURATION_FILE)) {
					throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + dbDefinition.getDbVendor());
				}
			} catch (final Exception e) {
				throw new DbDefinitionException("Cannot aquire database driver for database vendor: " + dbDefinition.getDbVendor(), e);
			}
		}

		switch (dbDefinition.getDbVendor()) {
			case Oracle:
				dbValueConverter = new OracleDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case SQLite:
				dbValueConverter = new SQLiteDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case MySQL:
				dbValueConverter = new MySQLDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case MariaDB:
				dbValueConverter = new MariaDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case PostgreSQL:
				dbValueConverter = new PostgreSQLDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case Firebird:
				dbValueConverter = new FirebirdDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case Cassandra:
				dbValueConverter = new CassandraDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case Derby:
				dbValueConverter = new DefaultDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case HSQL:
				dbValueConverter = new DefaultDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			case MsSQL:
				dbValueConverter = new DefaultDBValueConverter(compression, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
				break;
			default:
				throw new Exception("Unsupported database vendor: null");
		}

		try (Connection connection = DbUtilities.createConnection(dbDefinition, true)) {
			if (isStatementFile) {
				if (Utilities.isBlank(sqlStatementOrTablelist)) {
					throw new DbExportException("Statementfile is missing");
				} else {
					sqlStatementOrTablelist = Utilities.replaceUsersHome(sqlStatementOrTablelist);
					sqlStatementOrTablelist = DateUtilities.replaceDatePatternInString(sqlStatementOrTablelist, LocalDateTime.now());
					if (!new File(sqlStatementOrTablelist).exists()) {
						throw new DbExportException("Statementfile does not exist");
					} else {
						sqlStatementOrTablelist = new String(Utilities.readFileToByteArray(new File(sqlStatementOrTablelist)), StandardCharsets.UTF_8);
					}
				}
			}

			if (Utilities.isBlank(sqlStatementOrTablelist)) {
				throw new DbExportException("SqlStatement or tablelist is missing");
			}

			outputpath = DateUtilities.replaceDatePatternInString(outputpath, LocalDateTime.now());

			if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")
					|| sqlStatementOrTablelist.toLowerCase().startsWith("select\t")
					|| sqlStatementOrTablelist.toLowerCase().startsWith("select\n")
					|| sqlStatementOrTablelist.toLowerCase().startsWith("select\r")) {
				// Single export
				if (!"console".equalsIgnoreCase(outputpath) && !"gui".equalsIgnoreCase(outputpath)) {
					if (new File(outputpath).exists() && new File(outputpath).isDirectory()) {
						outputpath = outputpath + File.separator + "export_" + DateUtilities.formatDate("yyyy-MM-dd_HH-mm-ss", LocalDateTime.now());
					}
				}

				if (exportStructureFilePath != null) {
					exportDbStructure(connection, sqlStatementOrTablelist, exportStructureFilePath);
				} else {
					export(connection, sqlStatementOrTablelist, outputpath);
				}

				return !cancel;
			} else {
				signalItemStart("Scanning tables ...", null);
				signalUnlimitedProgress();
				final List<String> tablesToExport = DbUtilities.getAvailableTables(connection, sqlStatementOrTablelist);
				if ("*".equals(sqlStatementOrTablelist)) {
					Collections.sort(tablesToExport);
				}
				if (tablesToExport.size() == 0) {
					throw new DbExportException("No table found for export");
				} else if (tablesToExport.size() > 1 && exportStructureFilePath == null && !"console".equalsIgnoreCase(outputpath) && !"gui".equalsIgnoreCase(outputpath)) {
					// Multi export (console and gui output need no directory, before an export of several tables to
					// "console" failed with "Outputpath 'console' does not exist" or even created such a directory)
					final String basicOutputFilePath = outputpath;
					// Create directory if missing
					final File outputBaseDirecory = new File(basicOutputFilePath);
					if (!outputBaseDirecory.exists()) {
						if (createOutputDirectoyIfNotExists) {
							outputBaseDirecory.mkdirs();
						} else {
							throw new DbExportException("Outputpath '" + basicOutputFilePath + "' does not exist");
						}
					} else if (!outputBaseDirecory.isDirectory()) {
						throw new DbExportException("Outputpath '" + basicOutputFilePath + "' already exists but is not a directory, which is needed for an export of multiple data sets");
					}
					outputpath = basicOutputFilePath;
				}

				itemsToDo = tablesToExport.size();
				itemsDone = 0;
				if (exportStructureFilePath != null) {
					exportDbStructure(connection, tablesToExport, exportStructureFilePath);
				} else {
					for (int i = 0; i < tablesToExport.size() && !cancel; i++) {
						signalProgress(true);
						final String tableName = tablesToExport.get(i).toLowerCase();
						subItemsToDo = 0;
						subItemsDone = 0;
						signalItemStart(tableName, null);

						String nextOutputFilePath = outputpath;
						if ("console".equalsIgnoreCase(outputpath)) {
							System.out.println("Table: " + tableName);
						} else if ("gui".equalsIgnoreCase(outputpath)) {
							System.out.println("Table: " + tableName);
						} else {
							nextOutputFilePath = outputpath + File.separator + tableName.toLowerCase();
						}
						final List<String> columnNames = new ArrayList<>(DbUtilities.getColumnNames(connection, tableName));
						Collections.sort(columnNames);
						final List<String> keyColumnNames = new ArrayList<>(DbUtilities.getPrimaryKeyColumns(connection, tableName));
						Collections.sort(keyColumnNames);
						final List<String> readoutColumns = new ArrayList<>();
						readoutColumns.addAll(keyColumnNames);
						for (final String columnName : columnNames) {
							if (!readoutColumns.contains(columnName)) {
								readoutColumns.add(columnName);
							}
						}

						final List<String> escapedKeyColumns = new ArrayList<>();
						for (final String unescapedKeyColumnName : keyColumnNames) {
							escapedKeyColumns.add(DbUtilities.escapeVendorReservedNames(dbDefinition.getDbVendor(), unescapedKeyColumnName));
						}

						final List<String> escapedReadoutColumns = new ArrayList<>();
						for (final String unescapedColumnName : readoutColumns) {
							escapedReadoutColumns.add(DbUtilities.escapeVendorReservedNames(dbDefinition.getDbVendor(), unescapedColumnName));
						}

						String orderPart = "";
						if (!keyColumnNames.isEmpty()) {
							orderPart = " ORDER BY " + Utilities.join(escapedKeyColumns, ", ");
						}

						final String sqlStatement = "SELECT " + Utilities.join(escapedReadoutColumns, ", ") + " FROM " + tableName + orderPart;

						try {
							export(connection, sqlStatement, nextOutputFilePath);
						} catch (final DbExportException e) {
							throw e;
						} catch (final Exception e) {
							throw new Exception("Error occurred while exporting\n" + sqlStatement + "\n" + e.getMessage(), e);
						}

						signalItemDone();

						itemsDone++;
					}
				}
				return !cancel;
			}
		} catch (final Exception e) {
			throw e;
		} finally {
			if (dbDefinition.getDbVendor() == DbVendor.Derby) {
				DbUtilities.shutDownDerbyDb(dbDefinition.getDbName());
			}
		}
	}

	private void exportDbStructure(final Connection connection, final List<String> tablesToExport, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;

		try {
			if ("console".equalsIgnoreCase(outputFilePath)) {
				outputStream = new NonClosingOutputStream(System.out);
			} else if ("gui".equalsIgnoreCase(outputFilePath)) {
				guiOutputStream = new ByteArrayOutputStream();
				outputStream = guiOutputStream;
			} else {
				if (compression == FileCompressionType.ZIP) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".zip")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".zip";
					}
				} else if (compression == FileCompressionType.TARGZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".tar.gz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".tar.gz";
					}
				} else if (compression == FileCompressionType.TGZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".tgz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".tgz";
					}
				} else if (compression == FileCompressionType.GZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".gz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".gz";
					}
				} else if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
					outputFilePath = outputFilePath + ".json";
				}

				if (new File(outputFilePath).exists()) {
					throw new DbExportException("Outputfile already exists: " + outputFilePath);
				}

				if (compression == FileCompressionType.ZIP) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = new File(outputFilePath).getName();
					entryFileName = entryFileName.substring(0, entryFileName.length() - 4);
					if (!Utilities.endsWithIgnoreCase(entryFileName, ".json")) {
						entryFileName += ".json";
					}
					final ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(ZonedDateTime.now().toInstant().toEpochMilli());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else if (compression == FileCompressionType.TARGZ) {
					tempFile = File.createTempFile(new File(outputFilePath).getName(), "");
					outputStream = new FileOutputStream(tempFile);
				} else if (compression == FileCompressionType.TGZ) {
					tempFile = File.createTempFile(new File(outputFilePath).getName(), "");
					outputStream = new FileOutputStream(tempFile);
				} else if (compression == FileCompressionType.GZ) {
					outputStream = new GZIPOutputStream(new FileOutputStream(new File(outputFilePath)));
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
			}

			signalProgress();

			try (JsonWriter jsonWriter = new JsonWriter(outputStream)) {
				jsonWriter.openJsonObject();
				for (int i = 0; i < tablesToExport.size() && !cancel; i++) {
					final JsonObject tableJsonObject = createTableStructureJsonObject(connection, tablesToExport.get(i).toLowerCase());

					jsonWriter.openJsonObjectProperty(tablesToExport.get(i).toLowerCase());
					jsonWriter.add(tableJsonObject);

					itemsDone++;
					signalProgress();
				}
				jsonWriter.closeJsonObject();
			}

			if (compression == FileCompressionType.ZIP && zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(outputFilePath, zipPassword, useZipCrypto);
			} else if (compression == FileCompressionType.TARGZ) {
				String entryFileName = new File(outputFilePath).getName();
				if (entryFileName.toLowerCase().endsWith(".tar.gz")) {
					entryFileName = entryFileName.substring(0, entryFileName.length() - 7);
				}
				TarGzUtilities.compress(new File(outputFilePath), tempFile, entryFileName);
			} else if (compression == FileCompressionType.TGZ) {
				String entryFileName = new File(outputFilePath).getName();
				if (entryFileName.toLowerCase().endsWith(".tgz")) {
					entryFileName = entryFileName.substring(0, entryFileName.length() - 4);
				}
				TarGzUtilities.compress(new File(outputFilePath), tempFile, entryFileName);
			}
		} finally {
			Utilities.closeQuietly(outputStream);
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
				tempFile = null;
			}
		}
	}

	private static JsonObject createTableStructureJsonObject(final Connection connection, final String tablename) throws Exception {
		final CaseInsensitiveSet keyColumns = DbUtilities.getPrimaryKeyColumns(connection, tablename);
		final CaseInsensitiveMap<DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tablename);
		final List<DatabaseForeignKey> foreignKeys = DbUtilities.getForeignKeys(connection, tablename);
		final List<DatabaseIndex> indices = DbUtilities.getIndices(connection, tablename);
		final List<DatabaseConstraint> constraints = DbUtilities.getConstraints(connection, tablename);
		final CaseInsensitiveMap<Object> defaultValues = DbUtilities.getColumnDefaultValues(connection, tablename);

		final JsonObject tableJsonObject = new JsonObject();

		// Keycolumns list
		if (!keyColumns.isEmpty()) {
			final JsonArray keyColumnsJsonArray = new JsonArray();
			tableJsonObject.add("keycolumns", keyColumnsJsonArray);
			for (final String keyColumnName : Utilities.asSortedList(keyColumns)) {
				if (keyColumns.contains(keyColumnName)) {
					keyColumnsJsonArray.add(keyColumnName.toLowerCase());
				}
			}
		}

		final JsonArray columnsJsonArray = new JsonArray();
		tableJsonObject.add("columns", columnsJsonArray);

		// Keycolumns datatypes
		for (final String columnName : Utilities.asSortedList(dbColumns.keySet())) {
			if (keyColumns.contains(columnName)) {
				columnsJsonArray.add(createColumnJsonObject(columnName, dbColumns.get(columnName), defaultValues.get(columnName)));
			}
		}

		// Other columns datatypes
		for (final String columnName : Utilities.asSortedList(dbColumns.keySet())) {
			if (!keyColumns.contains(columnName)) {
				columnsJsonArray.add(createColumnJsonObject(columnName, dbColumns.get(columnName), defaultValues.get(columnName)));
			}
		}

		// Indices
		if (indices != null && !indices.isEmpty()) {
			final JsonArray indicesJsonArray = new JsonArray();
			tableJsonObject.add("indices", indicesJsonArray);
			for (final DatabaseIndex index : indices) {
				final JsonObject indexJsonObject = new JsonObject();
				indicesJsonArray.add(indexJsonObject);
				if (index.getIndexName() != null) {
					indexJsonObject.add("name", index.getIndexName());
				}
				final JsonArray indexedColumnsJsonArray = new JsonArray();
				indexJsonObject.add("indexedColumns", indexedColumnsJsonArray);
				for (final String columnName : index.getIndexedColumns()) {
					indexedColumnsJsonArray.add(columnName);
				}
			}
		}

		// Foreign keys
		if (foreignKeys != null && !foreignKeys.isEmpty()) {
			final JsonArray foreignKeysJsonArray = new JsonArray();
			tableJsonObject.add("foreignKeys", foreignKeysJsonArray);
			for (final DatabaseForeignKey foreignKey : foreignKeys) {
				final JsonObject foreignKeyJsonObject = new JsonObject();
				foreignKeysJsonArray.add(foreignKeyJsonObject);
				if (foreignKey.getForeignKeyName() != null) {
					foreignKeyJsonObject.add("name", foreignKey.getForeignKeyName());
				}
				foreignKeyJsonObject.add("columnName", foreignKey.getColumnName());
				foreignKeyJsonObject.add("referencedTable", foreignKey.getReferencedTableName());
				foreignKeyJsonObject.add("referencedColumn", foreignKey.getReferencedColumnName());
			}
		}

		// Constraints
		if (constraints != null && !constraints.isEmpty()) {
			final JsonArray constraintsJsonArray = new JsonArray();
			tableJsonObject.add("constraints", constraintsJsonArray);
			for (final DatabaseConstraint constraint : constraints) {
				final JsonObject constraintJsonObject = new JsonObject();
				constraintsJsonArray.add(constraintJsonObject);
				if (constraint.getConstraintName() != null) {
					constraintJsonObject.add("name", constraint.getConstraintName());
				}
				constraintJsonObject.add("type", constraint.getConstraintType().name());
				if (constraint.getColumnName() != null) {
					constraintJsonObject.add("columnName", constraint.getColumnName());
				}
			}
		}

		return tableJsonObject;
	}

	private static JsonObject createColumnJsonObject(final String columnName, final DbColumnType columnType, final Object defaultValue) throws Exception {
		final JsonObject columnJsonObject = new JsonObject();

		columnJsonObject.add("name", columnName.toLowerCase());
		columnJsonObject.add("datatype", columnType.getSimpleDataType().name());
		if (columnType.getSimpleDataType() == DbSimpleDataType.String) {
			columnJsonObject.add("datasize", columnType.getCharacterByteSize());
		}
		if (!columnType.isNullable()) {
			columnJsonObject.add("nullable", columnType.isNullable());
		}
		if (defaultValue != null) {
			columnJsonObject.add("defaultvalue", defaultValue);
		}
		columnJsonObject.add("databasevendorspecific_datatype", columnType.getTypeName());

		return columnJsonObject;
	}

	private void exportDbStructure(final Connection connection, final String sqlStatement, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;

		try {
			if ("console".equalsIgnoreCase(outputFilePath)) {
				outputStream = new NonClosingOutputStream(System.out);
			} else if ("gui".equalsIgnoreCase(outputFilePath)) {
				guiOutputStream = new ByteArrayOutputStream();
				outputStream = guiOutputStream;
			} else {
				if (compression == FileCompressionType.ZIP) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".zip")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".zip";
					}
				} else if (compression == FileCompressionType.TARGZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".tar.gz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".tar.gz";
					}
				} else if (compression == FileCompressionType.TGZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".tgz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".tgz";
					}
				} else if (compression == FileCompressionType.GZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".gz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
							outputFilePath = outputFilePath + ".json";
						}

						outputFilePath = outputFilePath + ".gz";
					}
				} else if (!Utilities.endsWithIgnoreCase(outputFilePath, ".json")) {
					outputFilePath = outputFilePath + ".json";
				}

				if (new File(outputFilePath).exists()) {
					throw new DbExportException("DB structure outputfile already exists: " + outputFilePath);
				}

				if (compression == FileCompressionType.ZIP) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = new File(outputFilePath).getName();
					entryFileName = entryFileName.substring(0, entryFileName.length() - 4);
					if (!Utilities.endsWithIgnoreCase(entryFileName, ".json")) {
						entryFileName += ".json";
					}
					final ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(ZonedDateTime.now().toInstant().toEpochMilli());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else if (compression == FileCompressionType.TARGZ) {
					tempFile = File.createTempFile(new File(outputFilePath).getName(), "");
					outputStream = new FileOutputStream(tempFile);
				} else if (compression == FileCompressionType.TGZ) {
					tempFile = File.createTempFile(new File(outputFilePath).getName(), "");
					outputStream = new FileOutputStream(tempFile);
				} else if (compression == FileCompressionType.GZ) {
					outputStream = new GZIPOutputStream(new FileOutputStream(new File(outputFilePath)));
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
			}

			try (JsonWriter jsonWriter = new JsonWriter(outputStream)) {
				jsonWriter.openJsonObject();

				final JsonArray columnsJsonArray = new JsonArray();

				try (Statement statement = connection.createStatement()) {
					statement.setFetchSize(100);
					try (ResultSet resultSet = statement.executeQuery(sqlStatement)) {
						final ResultSetMetaData metaData = resultSet.getMetaData();
						for (int i = 1; i <= metaData.getColumnCount(); i ++) {
							final JsonObject columnJsonObject = new JsonObject();
							columnsJsonArray.add(columnJsonObject);

							columnJsonObject.add("name", metaData.getColumnName(i));
							columnJsonObject.add("datatype", DbUtilities.getTypeNameById(metaData.getColumnType(i)));
							columnJsonObject.add("databasevendorspecific_datatype", metaData.getColumnTypeName(i));
						}
					}
				}

				jsonWriter.openJsonObjectProperty("statement");
				jsonWriter.addSimpleJsonObjectPropertyValue(sqlStatement);

				jsonWriter.openJsonObjectProperty("columns");
				jsonWriter.add(columnsJsonArray);

				jsonWriter.closeJsonObject();
			}

			if (compression == FileCompressionType.ZIP && zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(outputFilePath, zipPassword, useZipCrypto);
			} else if (compression == FileCompressionType.TARGZ) {
				String entryFileName = new File(outputFilePath).getName();
				if (entryFileName.toLowerCase().endsWith(".tar.gz")) {
					entryFileName = entryFileName.substring(0, entryFileName.length() - 7);
				}
				TarGzUtilities.compress(new File(outputFilePath), tempFile, entryFileName);
			} else if (compression == FileCompressionType.TGZ) {
				String entryFileName = new File(outputFilePath).getName();
				if (entryFileName.toLowerCase().endsWith(".tgz")) {
					entryFileName = entryFileName.substring(0, entryFileName.length() - 4);
				}
				TarGzUtilities.compress(new File(outputFilePath), tempFile, entryFileName);
			}
		} finally {
			Utilities.closeQuietly(outputStream);
			if (tempFile != null && tempFile.exists()) {
				tempFile.delete();
				tempFile = null;
			}
		}
	}

	private void export(final Connection connection, final String sqlStatement, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		File tempFile = null;
		OutputStream logOutputStream = null;
		boolean errorOccurred = false;
		boolean fileWasCreated = false;
		long exportedLines = 0;
		try {
			if ("console".equalsIgnoreCase(outputFilePath)) {
				// The writers close their stream, which closed System.out for all further output (e.g. the next table)
				outputStream = new NonClosingOutputStream(System.out);
			} else if ("gui".equalsIgnoreCase(outputFilePath)) {
				guiOutputStream = new ByteArrayOutputStream();
				outputStream = guiOutputStream;
			} else {
				if (compression == FileCompressionType.ZIP) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".zip")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, "." + getFileExtension())) {
							outputFilePath = outputFilePath + "." + getFileExtension();
						}

						outputFilePath = outputFilePath + ".zip";
					}
				} else if (compression == FileCompressionType.TARGZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".tar.gz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, "." + getFileExtension())) {
							outputFilePath = outputFilePath + "." + getFileExtension();
						}

						outputFilePath = outputFilePath + ".tar.gz";
					}
				} else if (compression == FileCompressionType.TGZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".tgz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath,"." + getFileExtension())) {
							outputFilePath = outputFilePath + "." + getFileExtension();
						}

						outputFilePath = outputFilePath + ".tgz";
					}
				} else if (compression == FileCompressionType.GZ) {
					if (!Utilities.endsWithIgnoreCase(outputFilePath, ".gz")) {
						if (!Utilities.endsWithIgnoreCase(outputFilePath, "." + getFileExtension())) {
							outputFilePath = outputFilePath + "." + getFileExtension();
						}

						outputFilePath = outputFilePath + ".gz";
					}
				} else if (!Utilities.endsWithIgnoreCase(outputFilePath, "." + getFileExtension())) {
					outputFilePath = outputFilePath + "." + getFileExtension();
				}

				if (new File(outputFilePath).exists()) {
					if (replaceAlreadyExistingFiles) {
						new File(outputFilePath).delete();
					} else {
						throw new DbExportException("Outputfile already exists: " + outputFilePath);
					}
				}

				// Via the absolute file, a file name without directory has no parent and failed with a NullPointerException
				final File outputDirectory = new File(outputFilePath).getAbsoluteFile().getParentFile();
				if (!outputDirectory.exists()) {
					if (createOutputDirectoyIfNotExists) {
						outputDirectory.mkdirs();
					} else {
						throw new DbExportException("Outputfile parent directory does not exist: " + outputDirectory.getAbsolutePath());
					}
				} else if (!outputDirectory.isDirectory()) {
					throw new DbExportException("Outputfile parent is not a directory: " + outputDirectory.getAbsolutePath());
				}

				if (log) {
					logOutputStream = new FileOutputStream(new File(outputFilePath + "." + DateUtilities.formatDate("yyyy-MM-dd_HH-mm-ss", LocalDateTime.now()) + ".log"));

					logToFile(logOutputStream, getConfigurationLogString(new File(outputFilePath).getName(), sqlStatement)
							+ (Utilities.isNotBlank(dateFormatPattern) ? "DateFormatPattern: " + dateFormatPattern + "\n" : "")
							+ (Utilities.isNotBlank(dateTimeFormatPattern) ? "DateTimeFormatPattern: " + dateTimeFormatPattern + "\n" : "")
							+ (databaseTimeZone != null && !databaseTimeZone.equals(exportDataTimeZone) ? "DatabaseZoneId: " + databaseTimeZone + "\nExportDataZoneId: " + exportDataTimeZone + "\n" : ""));
				}

				if (currentItemName == null) {
					logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));
				} else {
					logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), startTimeSub));
				}

				if (compression == FileCompressionType.ZIP) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = new File(outputFilePath).getName();
					entryFileName = entryFileName.substring(0, entryFileName.length() - 4);
					if (!Utilities.endsWithIgnoreCase(entryFileName, "." + getFileExtension())) {
						entryFileName += "." + getFileExtension();
					}
					final ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(ZonedDateTime.now().toInstant().toEpochMilli());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else if (compression == FileCompressionType.TARGZ) {
					tempFile = File.createTempFile(new File(outputFilePath).getName(), null);
					outputStream = new FileOutputStream(tempFile);
				} else if (compression == FileCompressionType.TGZ) {
					tempFile = File.createTempFile(new File(outputFilePath).getName(), null);
					outputStream = new FileOutputStream(tempFile);
				} else if (compression == FileCompressionType.GZ) {
					outputStream = new GZIPOutputStream(new FileOutputStream(outputFilePath));
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}

				fileWasCreated = true;
			}

			if (currentItemName == null) {
				signalUnlimitedProgress();
			} else {
				signalUnlimitedSubProgress();
			}

			try (Statement statement = DbUtilities.getStatementForLargeQuery(connection)) {
				String countSqlStatementString = "SELECT COUNT(*) FROM (" + sqlStatement + ") data";
				if (dbDefinition.getDbVendor() == DbVendor.Cassandra) {
					if (sqlStatement.toLowerCase().contains(" order by ")) {
						countSqlStatementString = "SELECT COUNT(*)" + sqlStatement.substring(sqlStatement.toLowerCase().indexOf(" from "), sqlStatement.toLowerCase().indexOf(" order by "));
					} else {
						countSqlStatementString = "SELECT COUNT(*)" + sqlStatement.substring(sqlStatement.toLowerCase().indexOf(" from "));
					}
				}
				try (ResultSet resultSet = statement.executeQuery(countSqlStatementString)) {
					resultSet.next();
					final int linesToExport = resultSet.getInt(1);
					logToFile(logOutputStream, "Lines to export: " + linesToExport);

					if (currentItemName == null) {
						itemsToDo = linesToExport;
						signalProgress();
					} else {
						subItemsToDo = linesToExport;
						signalItemProgress();
					}
				}

				openWriter(outputStream);

				try (ResultSet resultSet = statement.executeQuery(sqlStatement)) {
					final ResultSetMetaData metaData = resultSet.getMetaData();

					// Scan headers
					final List<String> columnNames = new ArrayList<>();
					final List<String> columnTypes = new ArrayList<>();
					for (int i = 1; i <= metaData.getColumnCount(); i++) {
						columnNames.add(metaData.getColumnName(i));
						columnTypes.add(metaData.getColumnTypeName(i));
					}

					if (currentItemName == null) {
						itemsDone = 0;
						signalProgress();
					} else {
						subItemsDone = 0;
						signalItemProgress();
					}

					if (currentItemName == null) {
						signalProgress();
					} else {
						signalItemProgress();
					}

					startOutput(connection, sqlStatement, columnNames);

					// Write values
					while (resultSet.next() && !cancel) {
						startTableLine();
						for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
							final String columnName = metaData.getColumnName(columnIndex);
							Object value = dbValueConverter.convert(metaData, resultSet, columnIndex, outputFilePath);
							if (value != null && value instanceof Date && metaData.getColumnType(columnIndex) == Types.DATE) {
								value = DateUtilities.changeDateTimeZone((Date) value, ZoneId.of(databaseTimeZone), ZoneId.of(exportDataTimeZone));
								writeDateColumn(columnName, DateUtilities.getLocalDateForDate((Date) value));
							} else if (value != null && value instanceof LocalDateTime && metaData.getColumnType(columnIndex) == Types.DATE) {
								value = DateUtilities.changeDateTimeZone((LocalDateTime) value, ZoneId.of(databaseTimeZone), ZoneId.of(exportDataTimeZone));
								writeDateColumn(columnName, ((LocalDateTime) value).toLocalDate());
							} else if (value != null && value instanceof LocalDate) {
								writeDateColumn(columnName, (LocalDate) value);
							} else if (value != null && value instanceof ZonedDateTime && metaData.getColumnType(columnIndex) == Types.DATE) {
								value = DateUtilities.changeDateTimeZone((ZonedDateTime) value, ZoneId.of(exportDataTimeZone));
								writeDateColumn(columnName, ((ZonedDateTime) value).toLocalDate());
							} else if (value != null && value instanceof Date) {
								value = DateUtilities.changeDateTimeZone((Date) value, ZoneId.of(databaseTimeZone), ZoneId.of(exportDataTimeZone));
								writeDateTimeColumn(columnName, DateUtilities.getLocalDateTimeForDate((Date) value));
							} else if (value != null && value instanceof LocalDateTime) {
								value = DateUtilities.changeDateTimeZone((LocalDateTime) value, ZoneId.of(databaseTimeZone), ZoneId.of(exportDataTimeZone));
								writeDateTimeColumn(columnName, (LocalDateTime) value);
							} else if (value != null && value instanceof ZonedDateTime) {
								value = DateUtilities.changeDateTimeZone((ZonedDateTime) value, ZoneId.of(exportDataTimeZone));
								writeDateTimeColumn(columnName, (ZonedDateTime) value);
							} else if (value != null && value instanceof File) {
								if (compression == FileCompressionType.ZIP) {
									overallExportedDataAmountRaw += ZipUtilities.getDataSizeUncompressed((File) value);
									overallExportedDataAmountCompressed += ((File) value).length();
								} else if (compression == FileCompressionType.TARGZ) {
									overallExportedDataAmountRaw += TarGzUtilities.getUncompressedSize((File) value);
									overallExportedDataAmountCompressed += ((File) value).length();
								} else if (compression == FileCompressionType.TGZ) {
									overallExportedDataAmountRaw += TarGzUtilities.getUncompressedSize((File) value);
									overallExportedDataAmountCompressed += ((File) value).length();
								} else if (compression == FileCompressionType.GZ) {
									try (InputStream gzStream = new GZIPInputStream(new FileInputStream((File) value))) {
										overallExportedDataAmountRaw += IoUtilities.getStreamSize(gzStream);
									}
									overallExportedDataAmountCompressed += ((File) value).length();
								} else {
									overallExportedDataAmountRaw += ((File) value).length();
								}
								value = ((File) value).getName();
								writeColumn(columnName, value);
							} else {
								writeColumn(columnName, value);
							}
						}
						endTableLine();

						if (currentItemName == null) {
							itemsDone++;
							signalProgress();
						} else {
							subItemsDone++;
							signalItemProgress();
						}
					}

					if (cancel) {
						// Statement must be cancelled, or the "ResultSet.close()" will wait for all remaining data to be read
						statement.cancel();
					}

					endOutput();
				}

				closeWriter();

				if (currentItemName == null) {
					exportedLines = itemsDone;
				} else {
					exportedLines = subItemsDone;
				}

				if (currentItemName == null) {
					setEndTime(LocalDateTime.now());
				} else {
					endTimeSub = LocalDateTime.now();
				}

				if (exportedLines > 0) {
					logToFile(logOutputStream, "Exported lines: " + exportedLines);

					long elapsedTimeInSeconds;
					if (currentItemName == null) {
						elapsedTimeInSeconds = Duration.between(getStartTime(), getEndTime()).getSeconds();
					} else {
						elapsedTimeInSeconds = Duration.between(startTimeSub, endTimeSub).getSeconds();
					}
					if (elapsedTimeInSeconds > 0) {
						final int linesPerSecond = (int) (exportedLines / elapsedTimeInSeconds);
						logToFile(logOutputStream, "Export speed: " + linesPerSecond + " lines/second");
					} else {
						logToFile(logOutputStream, "Export speed: immediately");
					}

					if (new File(outputFilePath).exists()) {
						logToFile(logOutputStream, "Exported data amount: " + Utilities.getHumanReadableNumber(new File(outputFilePath).length(), "Byte", false, 5, false, Locale.ENGLISH));
					}
				}

				if (currentItemName == null) {
					logToFile(logOutputStream, "End: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getEndTime()));
					logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(getStartTime(), getEndTime()), true));
				} else {
					logToFile(logOutputStream, "End: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), endTimeSub));
					logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(startTimeSub, endTimeSub), true));
				}

				overallExportedLines += exportedLines;
			}
		} catch (final SQLException sqle) {
			errorOccurred = true;
			throw new DbExportException("SQL error: " + sqle.getMessage(), sqle);
		} catch (final Exception e) {
			errorOccurred = true;
			try {
				logToFile(logOutputStream, "Error: " + e.getMessage());
			} catch (final Exception e1) {
				e1.printStackTrace();
			}
			throw e;
		} finally {
			closeWriter();

			Utilities.closeQuietly(outputStream);
			Utilities.closeQuietly(logOutputStream);

			if (errorOccurred && fileWasCreated && new File(outputFilePath).exists() && exportedLines == 0) {
				new File(outputFilePath).delete();
			} else if (cancel && fileWasCreated && new File(outputFilePath).exists()) {
				new File(outputFilePath).delete();
			}
			// The temporary file of a tar.gz/tgz export is only needed for the compression below
			if ((errorOccurred || cancel) && tempFile != null && tempFile.exists()) {
				tempFile.delete();
			}
		}

		if (cancel || "console".equalsIgnoreCase(outputFilePath) || "gui".equalsIgnoreCase(outputFilePath)) {
			// The output file was deleted on cancel (the compression below failed on the missing file)
			return;
		}

		if (compression == FileCompressionType.ZIP) {
			if (zipPassword != null) {
				Zip4jUtilities.createPasswordSecuredZipFile(outputFilePath, zipPassword, useZipCrypto);
				overallExportedDataAmountRaw += Zip4jUtilities.getUncompressedSize(new File(outputFilePath), zipPassword);
			} else {
				overallExportedDataAmountRaw += ZipUtilities.getDataSizeUncompressed(new File(outputFilePath));
			}
			overallExportedDataAmountCompressed += new File(outputFilePath).length();
		} else if (compression == FileCompressionType.TARGZ) {
			String entryFileName = new File(outputFilePath).getName();
			if (entryFileName.toLowerCase().endsWith(".tar.gz")) {
				entryFileName = entryFileName.substring(0, entryFileName.length() - 7);
			}
			TarGzUtilities.compress(new File(outputFilePath), tempFile, entryFileName);
			// The temporary file was never deleted before
			tempFile.delete();
			overallExportedDataAmountRaw += TarGzUtilities.getUncompressedSize(new File(outputFilePath));
			overallExportedDataAmountCompressed += new File(outputFilePath).length();
		} else if (compression == FileCompressionType.TGZ) {
			String entryFileName = new File(outputFilePath).getName();
			if (entryFileName.toLowerCase().endsWith(".tgz")) {
				entryFileName = entryFileName.substring(0, entryFileName.length() - 4);
			}
			TarGzUtilities.compress(new File(outputFilePath), tempFile, entryFileName);
			tempFile.delete();
			overallExportedDataAmountRaw += TarGzUtilities.getUncompressedSize(new File(outputFilePath));
			overallExportedDataAmountCompressed += new File(outputFilePath).length();
		} else {
			overallExportedDataAmountRaw += new File(outputFilePath).length();
		}
	}

	/**
	 * Output stream for System.out, which is only flushed but not closed, when the writers close their stream.
	 */
	private static class NonClosingOutputStream extends OutputStream {
		private final OutputStream delegate;

		NonClosingOutputStream(final OutputStream delegate) {
			this.delegate = delegate;
		}

		@Override
		public void write(final int b) throws IOException {
			delegate.write(b);
		}

		@Override
		public void write(final byte[] data, final int offset, final int length) throws IOException {
			delegate.write(data, offset, length);
		}

		@Override
		public void flush() throws IOException {
			delegate.flush();
		}

		@Override
		public void close() throws IOException {
			delegate.flush();
		}
	}

	private static void logToFile(final OutputStream logOutputStream, final String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message + "\n").getBytes(StandardCharsets.UTF_8));
		}
	}

	/**
	 * Returns the number of exported data lines of all tables.
	 *
	 * @return the number of exported data lines
	 */
	public int getOverallExportedLines() {
		return overallExportedLines;
	}

	/**
	 * Returns the amount of exported (uncompressed) data.
	 *
	 * @return the amount of exported data in bytes
	 */
	public long getOverallExportedDataAmountRaw() {
		return overallExportedDataAmountRaw;
	}

	/**
	 * Returns the amount of exported compressed data.
	 *
	 * @return the size of the compressed output files in bytes
	 */
	public long getOverallExportedDataAmountCompressed() {
		return overallExportedDataAmountCompressed;
	}

	/**
	 * Returns the output of an export to "gui".
	 *
	 * @return the output, or null if the output path is not "gui"
	 */
	public ByteArrayOutputStream getGuiOutputStream() {
		return guiOutputStream;
	}

	/**
	 * Returns the configuration of this export for the log.
	 *
	 * @param fileName the name of the output file
	 * @param sqlStatement the exported statement
	 * @return the configuration as text with one line per parameter
	 */
	public abstract String getConfigurationLogString(String fileName, String sqlStatement);

	/**
	 * Returns the file extension of the export format.
	 *
	 * @return the file extension without dot, e.g. "csv"
	 */
	protected abstract String getFileExtension();

	/**
	 * Opens the writer of the export format.
	 *
	 * @param outputStream the stream to write to (closed by {@link #closeWriter()})
	 * @throws Exception if the writer cannot be opened
	 */
	protected abstract void openWriter(OutputStream outputStream) throws Exception;

	/**
	 * Starts the output of an exported statement, e.g. writes the CSV header.
	 *
	 * @param connection the database connection
	 * @param sqlStatement the exported statement
	 * @param columnNames the column names of the result
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception;

	/**
	 * Starts a new data line.
	 *
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void startTableLine() throws Exception;

	/**
	 * Writes a value of the current data line.
	 *
	 * @param columnName the column name
	 * @param value the value (String, Number, Boolean, or file name of a lob file), or null
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void writeColumn(String columnName, Object value) throws Exception;

	/**
	 * Writes a date value of the current data line.
	 *
	 * @param columnName the column name
	 * @param value the date value
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void writeDateColumn(String columnName, LocalDate value) throws Exception;

	/**
	 * Writes a date time value of the current data line.
	 *
	 * @param columnName the column name
	 * @param value the date time value (in the export data time zone)
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void writeDateTimeColumn(String columnName, LocalDateTime value) throws Exception;

	/**
	 * Writes a date time value with time zone of the current data line.
	 *
	 * @param columnName the column name
	 * @param value the date time value (in the export data time zone)
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void writeDateTimeColumn(String columnName, ZonedDateTime value) throws Exception;

	/**
	 * Ends the current data line.
	 *
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void endTableLine() throws Exception;

	/**
	 * Ends the output of an exported statement.
	 *
	 * @throws Exception if the output cannot be written
	 */
	protected abstract void endOutput() throws Exception;

	/**
	 * Closes the writer and its output stream. May be called several times.
	 *
	 * @throws Exception if the writer cannot be closed
	 */
	protected abstract void closeWriter() throws Exception;
}
