package de.soderer.dbexport.worker;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.dbexport.DbExportException;
import de.soderer.dbexport.converter.CassandraDBValueConverter;
import de.soderer.dbexport.converter.DefaultDBValueConverter;
import de.soderer.dbexport.converter.FirebirdDBValueConverter;
import de.soderer.dbexport.converter.MariaDBValueConverter;
import de.soderer.dbexport.converter.MySQLDBValueConverter;
import de.soderer.dbexport.converter.OracleDBValueConverter;
import de.soderer.dbexport.converter.PostgreSQLDBValueConverter;
import de.soderer.dbexport.converter.SQLiteDBValueConverter;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.collection.CaseInsensitiveSet;
import de.soderer.utilities.worker.WorkerDual;
import de.soderer.utilities.worker.WorkerParentDual;
import de.soderer.utilities.zip.ZipUtilities;

public abstract class AbstractDbExportWorker extends WorkerDual<Boolean> {
	// Mandatory parameters
	protected DbUtilities.DbVendor dbVendor = null;
	private final String hostname;
	private final String dbName;
	private final String username;
	private final char[] password;
	private final boolean secureConnection;
	private final String trustStoreFilePath;
	private final char[] trustStorePassword;
	private boolean isStatementFile = false;
	private String sqlStatementOrTablelist;
	private String outputpath;
	private ByteArrayOutputStream guiOutputStream = null;

	// Default optional parameters
	protected boolean log = false;
	protected boolean zip = false;
	protected char[] zipPassword = null;
	protected boolean useZipCrypto = false;
	protected Charset encoding = StandardCharsets.UTF_8;
	protected boolean createBlobFiles = false;
	protected boolean createClobFiles = false;
	protected Locale dateAndDecimalLocale = Locale.getDefault();
	protected DateTimeFormatter dateFormatter = DateUtilities.getDateFormatter(dateAndDecimalLocale);
	protected DateTimeFormatter dateTimeFormatter = DateUtilities.getDateTimeFormatterWithSeconds(dateAndDecimalLocale);
	protected NumberFormat decimalFormat ;
	protected Character decimalSeparator;
	protected boolean beautify = false;
	protected boolean exportStructure = false;

	private int overallExportedLines = 0;
	private long overallExportedDataAmountRaw = 0;
	private long overallExportedDataAmountCompressed = 0;

	private String databaseTimeZone = TimeZone.getDefault().getID();
	private String exportDataTimeZone = TimeZone.getDefault().getID();

	private DefaultDBValueConverter dbValueConverter;

	{
		// Create the default number format
		decimalFormat = NumberFormat.getNumberInstance(dateAndDecimalLocale);
		decimalFormat.setGroupingUsed(false);
	}

	public AbstractDbExportWorker(final WorkerParentDual parent, final DbVendor dbVendor, final String hostname, final String dbName, final String username, final char[] password, final boolean secureConnection, final String trustStoreFilePath, final char[] trustStorePassword, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) throws Exception {
		super(parent);
		this.dbVendor = dbVendor;
		this.hostname = hostname;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
		this.secureConnection = secureConnection;
		this.trustStoreFilePath = trustStoreFilePath;
		this.trustStorePassword = trustStorePassword;
		this.isStatementFile = isStatementFile;
		this.sqlStatementOrTablelist = sqlStatementOrTablelist;
		this.outputpath = outputpath;
	}

	public void setLog(final boolean log) {
		this.log = log;
	}

	public void setZip(final boolean zip) {
		this.zip = zip;
	}

	public void setZipPassword(final char[] zipPassword) {
		this.zipPassword = zipPassword;
	}

	public void setUseZipCrypto(final boolean useZipCrypto) {
		this.useZipCrypto = useZipCrypto;
	}

	public void setDatabaseTimeZone(final String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
		if (this.databaseTimeZone == null) {
			this.databaseTimeZone = TimeZone.getDefault().getID();
		}
	}

	public void setExportDataTimeZone(final String exportDataTimeZone) {
		this.exportDataTimeZone = exportDataTimeZone;
		if (this.exportDataTimeZone == null) {
			this.exportDataTimeZone = TimeZone.getDefault().getID();
		}
	}

	public void setEncoding(final Charset encoding) {
		this.encoding = encoding;
	}

	public void setCreateBlobFiles(final boolean createBlobFiles) {
		this.createBlobFiles = createBlobFiles;
	}

	public void setCreateClobFiles(final boolean createClobFiles) {
		this.createClobFiles = createClobFiles;
	}

	public void setDateAndDecimalLocale(final Locale dateAndDecimalLocale) {
		this.dateAndDecimalLocale = dateAndDecimalLocale;

		dateTimeFormatter = DateUtilities.getDateTimeFormatterWithSeconds(dateAndDecimalLocale);
		decimalFormat = NumberFormat.getNumberInstance(dateAndDecimalLocale);
		decimalFormat.setGroupingUsed(false);
	}

	public void setBeautify(final boolean beautify) {
		this.beautify = beautify;
	}

	public void setExportStructure(final boolean exportStructure) {
		this.exportStructure = exportStructure;
	}

	public void setDateFormat(final String dateFormat) {
		if (dateFormat != null) {
			dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat);
			dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
		}
	}

	public void setDateTimeFormat(final String dateTimeFormat) {
		if (dateTimeFormat != null) {
			dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
			dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
		}
	}

	public void setDecimalSeparator(final Character decimalSeparator) {
		if (decimalSeparator != null) {
			this.decimalSeparator = decimalSeparator;
		}
	}

	@Override
	public Boolean work() throws Exception {
		overallExportedLines = 0;

		if (dbVendor == null) {
			throw new Exception("Unsupported db vendor: null");
		} else if (dbVendor == DbVendor.Oracle) {
			dbValueConverter = new OracleDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.SQLite) {
			dbValueConverter = new SQLiteDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.MySQL) {
			dbValueConverter = new MySQLDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.MariaDB) {
			dbValueConverter = new MariaDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.PostgreSQL) {
			dbValueConverter = new PostgreSQLDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.Firebird) {
			dbValueConverter = new FirebirdDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.Cassandra) {
			dbValueConverter = new CassandraDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		} else {
			dbValueConverter = new DefaultDBValueConverter(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, getFileExtension());
		}

		try (Connection connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password), secureConnection, Utilities.isNotBlank(trustStoreFilePath) ? new File(trustStoreFilePath) : null, trustStorePassword, true)) {
			if (isStatementFile) {
				if (Utilities.isBlank(sqlStatementOrTablelist)) {
					throw new DbExportException("Statementfile is missing");
				} else {
					sqlStatementOrTablelist = Utilities.replaceUsersHome(sqlStatementOrTablelist);
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

			if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")
					|| sqlStatementOrTablelist.toLowerCase().startsWith("select\t")
					|| sqlStatementOrTablelist.toLowerCase().startsWith("select\n")
					|| sqlStatementOrTablelist.toLowerCase().startsWith("select\r")) {
				if (!"console".equalsIgnoreCase(outputpath) && !"gui".equalsIgnoreCase(outputpath)) {
					if (!new File(outputpath).exists()) {
						final int lastSeparator = Math.max(outputpath.lastIndexOf("/"), outputpath.lastIndexOf("\\"));
						if (lastSeparator >= 0) {
							String filename = outputpath.substring(lastSeparator + 1);
							filename = DateUtilities.replaceDatePatternInString(filename, LocalDateTime.now());
							outputpath = outputpath.substring(0, lastSeparator + 1) + filename;
						}
					}

					if (new File(outputpath).exists() && new File(outputpath).isDirectory()) {
						outputpath = outputpath + File.separator + "export_" + DateUtilities.formatDate("yyyy-MM-dd_HH-mm-ss", LocalDateTime.now());
					}
				}

				if (exportStructure) {
					exportDbStructure(connection, sqlStatementOrTablelist, outputpath);
				} else {
					export(connection, sqlStatementOrTablelist, outputpath);
				}

				return !cancel;
			} else {
				showItemStart("Scanning tables ...");
				showUnlimitedProgress();
				final List<String> tablesToExport = DbUtilities.getAvailableTables(connection, sqlStatementOrTablelist);
				if (tablesToExport.size() == 0) {
					throw new DbExportException("No table found for export");
				}
				itemsToDo = tablesToExport.size();
				itemsDone = 0;
				if (exportStructure) {
					if (new File(outputpath).exists() && new File(outputpath).isDirectory()) {
						outputpath = outputpath + File.separator + "dbstructure_" + DateUtilities.formatDate("yyyy-MM-dd_HH-mm-ss", LocalDateTime.now());
					}
					exportDbStructure(connection, tablesToExport, outputpath);
				} else {
					for (int i = 0; i < tablesToExport.size() && !cancel; i++) {
						showProgress(true);
						final String tableName = tablesToExport.get(i).toLowerCase();
						subItemsToDo = 0;
						subItemsDone = 0;
						showItemStart(tableName);

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
							escapedKeyColumns.add(DbUtilities.escapeVendorReservedNames(dbVendor, unescapedKeyColumnName));
						}

						final List<String> escapedReadoutColumns = new ArrayList<>();
						for (final String unescapedColumnName : readoutColumns) {
							escapedReadoutColumns.add(DbUtilities.escapeVendorReservedNames(dbVendor, unescapedColumnName));
						}

						String orderPart = "";
						if (!keyColumnNames.isEmpty()) {
							orderPart = " ORDER BY " + Utilities.join(escapedKeyColumns, ", ");
						}

						final String sqlStatement = "SELECT " + Utilities.join(escapedReadoutColumns, ", ") + " FROM " + tableName + orderPart;

						try {
							export(connection, sqlStatement, nextOutputFilePath);
						} catch (final Exception e) {
							throw new Exception("Error occurred while exporting\n" + sqlStatement + "\n" + e.getMessage(), e);
						}

						showItemDone();

						itemsDone++;
					}
				}
				return !cancel;
			}
		} catch (final Exception e) {
			throw e;
		} finally {
			if (dbVendor == DbVendor.Derby) {
				DbUtilities.shutDownDerbyDb(dbName);
			}
		}
	}

	@SuppressWarnings("resource")
	private void exportDbStructure(final Connection connection, final List<String> tablesToExport, String outputFilePath) throws Exception {
		OutputStream outputStream = null;

		try {
			if ("console".equalsIgnoreCase(outputFilePath)) {
				outputStream = System.out;
			} else if ("gui".equalsIgnoreCase(outputFilePath)) {
				guiOutputStream = new ByteArrayOutputStream();
				outputStream = guiOutputStream;
			} else {
				if (zip) {
					if (!outputFilePath.toLowerCase().endsWith(".zip")) {
						if (!outputFilePath.toLowerCase().endsWith(".txt")) {
							outputFilePath = outputFilePath + ".txt";
						}

						outputFilePath = outputFilePath + ".zip";
					}
				} else if (!outputFilePath.toLowerCase().endsWith(".txt")) {
					outputFilePath = outputFilePath + ".txt";
				}

				if (new File(outputFilePath).exists()) {
					throw new DbExportException("Outputfile already exists: " + outputFilePath);
				}

				if (zip) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = outputFilePath.substring(0, outputFilePath.length() - 4);
					entryFileName = entryFileName.substring(entryFileName.lastIndexOf(File.separatorChar) + 1);
					if (!entryFileName.toLowerCase().endsWith(".txt")) {
						entryFileName += ".txt";
					}
					final ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(ZonedDateTime.now().toInstant().toEpochMilli());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
			}

			showProgress();

			for (int i = 0; i < tablesToExport.size() && !cancel; i++) {
				if (i > 0) {
					outputStream.write(("\n").getBytes(StandardCharsets.UTF_8));
				}

				// Tablename
				outputStream.write(("Table " + tablesToExport.get(i).toLowerCase() + ":\n").getBytes(StandardCharsets.UTF_8));

				final CaseInsensitiveSet keyColumns = DbUtilities.getPrimaryKeyColumns(connection, tablesToExport.get(i));
				final CaseInsensitiveMap<DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tablesToExport.get(i));
				final List<List<String>> foreignKeys = DbUtilities.getForeignKeys(connection, tablesToExport.get(i));

				// Columns (primary key columns first)
				for (final String keyColumn : Utilities.asSortedList(keyColumns)) {
					outputStream.write(("\t" + keyColumn + " " + dbColumns.get(keyColumn).getTypeName() + " (Simple: " + dbColumns.get(keyColumn).getSimpleDataType() + ")\n").getBytes(StandardCharsets.UTF_8));
				}
				final List<String> columnNames = new ArrayList<>(dbColumns.keySet());
				Collections.sort(columnNames);
				for (final String columnName : columnNames) {
					if (!keyColumns.contains(columnName)) {
						outputStream.write(("\t" + columnName.toLowerCase() + " " + dbColumns.get(columnName).getTypeName() + " (Simple: " + dbColumns.get(columnName).getSimpleDataType() + ")\n").getBytes(StandardCharsets.UTF_8));
					}
				}

				// Primary key
				if (!keyColumns.isEmpty()) {
					outputStream.write("Primary key:\n".getBytes(StandardCharsets.UTF_8));
					outputStream.write(("\t" + Utilities.join(Utilities.asSortedList(keyColumns), ", ").toLowerCase() + "\n").getBytes(StandardCharsets.UTF_8));
				}

				// Foreign keys
				if (!foreignKeys.isEmpty()) {
					outputStream.write("Foreign keys:\n".getBytes(StandardCharsets.UTF_8));
					for (final List<String> foreignKey : foreignKeys) {
						outputStream.write(("\t" + foreignKey.get(1) + " => " + foreignKey.get(2) + "." + foreignKey.get(3) + "\n").toLowerCase().getBytes(StandardCharsets.UTF_8));
					}
				}

				itemsDone++;
				showProgress();
			}
		} finally {
			Utilities.closeQuietly(outputStream);

			// TODO: need net.lingala.zip library
			//			if (zip && zipPassword != null) {
			//				final ZipParameters zipParameters = new ZipParameters();
			//				zipParameters.setCompressionMethod(CompressionMethod.DEFLATE);
			//				zipParameters.setEncryptFiles(true);
			//				if (!useZipCrypto) {
			//					zipParameters.setEncryptionMethod(EncryptionMethod.AES);
			//					zipParameters.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
			//				} else {
			//					zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
			//				}
			//				zipParameters.setFileNameInZip(new File(outputFilePath.substring(0, outputFilePath.length() - 4)).getName());
			//				try (ZipFile zipFile = new ZipFile(new File(outputFilePath + ".tmp"), zipPassword)) {
			//					try (ZipFile unencryptedZipFile = new ZipFile(new File(outputFilePath))) {
			//						try (InputStream inputStream = new InputStreamWithOtherItemsToClose(unencryptedZipFile.getInputStream(unencryptedZipFile.getFileHeaders().get(0)), unencryptedZipFile)) {
			//							zipFile.addStream(inputStream, zipParameters);
			//						}
			//					}
			//				}
			//				new File(outputFilePath).delete();
			//				new File(outputFilePath + ".tmp").renameTo(new File(outputFilePath));
			//			}
		}
	}

	@SuppressWarnings("resource")
	private void exportDbStructure(final Connection connection, final String sqlStatement, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		try {
			if ("console".equalsIgnoreCase(outputFilePath)) {
				outputStream = System.out;
			} else if ("gui".equalsIgnoreCase(outputFilePath)) {
				guiOutputStream = new ByteArrayOutputStream();
				outputStream = guiOutputStream;
			} else {
				if (zip) {
					if (!outputFilePath.toLowerCase().endsWith(".zip")) {
						if (!outputFilePath.toLowerCase().endsWith(".txt")) {
							outputFilePath = outputFilePath + ".txt";
						}

						outputFilePath = outputFilePath + ".zip";
					}
				} else if (!outputFilePath.toLowerCase().endsWith(".txt")) {
					outputFilePath = outputFilePath + ".txt";
				}

				if (new File(outputFilePath).exists()) {
					throw new DbExportException("Outputfile already exists: " + outputFilePath);
				}

				if (zip) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = outputFilePath.substring(0, outputFilePath.length() - 4);
					entryFileName = entryFileName.substring(entryFileName.lastIndexOf(File.separatorChar) + 1);
					if (!entryFileName.toLowerCase().endsWith(".txt")) {
						entryFileName += ".txt";
					}
					final ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(ZonedDateTime.now().toInstant().toEpochMilli());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
			}

			try (Statement statement = connection.createStatement();
					ResultSet resultSet = statement.executeQuery(sqlStatement)) {
				final ResultSetMetaData metaData = resultSet.getMetaData();

				outputStream.write((sqlStatement + "\n\n").getBytes(StandardCharsets.UTF_8));
				for (int i = 1; i <= metaData.getColumnCount(); i ++) {
					outputStream.write((metaData.getColumnName(i) + " " + metaData.getColumnTypeName(i) + " (" + DbUtilities.getTypeNameById(metaData.getColumnType(i)) + ")\n").getBytes(StandardCharsets.UTF_8));
				}
			}
		} finally {
			Utilities.closeQuietly(outputStream);

			// TODO: need net.lingala.zip library
			//			if (zip && zipPassword != null) {
			//				final ZipParameters zipParameters = new ZipParameters();
			//				zipParameters.setCompressionMethod(CompressionMethod.DEFLATE);
			//				zipParameters.setEncryptFiles(true);
			//				if (!useZipCrypto) {
			//					zipParameters.setEncryptionMethod(EncryptionMethod.AES);
			//					zipParameters.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
			//				} else {
			//					zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
			//				}
			//				zipParameters.setFileNameInZip(new File(outputFilePath.substring(0, outputFilePath.length() - 4)).getName());
			//				try (ZipFile zipFile = new ZipFile(new File(outputFilePath + ".tmp"), zipPassword)) {
			//					try (ZipFile unencryptedZipFile = new ZipFile(new File(outputFilePath))) {
			//						try (InputStream inputStream = new InputStreamWithOtherItemsToClose(unencryptedZipFile.getInputStream(unencryptedZipFile.getFileHeaders().get(0)), unencryptedZipFile)) {
			//							zipFile.addStream(inputStream, zipParameters);
			//						}
			//					}
			//				}
			//				new File(outputFilePath).delete();
			//				new File(outputFilePath + ".tmp").renameTo(new File(outputFilePath));
			//			}
		}
	}

	@SuppressWarnings("resource")
	private void export(final Connection connection, final String sqlStatement, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		OutputStream logOutputStream = null;
		boolean errorOccurred = false;
		boolean fileWasCreated = false;
		try {
			if ("console".equalsIgnoreCase(outputFilePath)) {
				outputStream = System.out;
			} else if ("gui".equalsIgnoreCase(outputFilePath)) {
				guiOutputStream = new ByteArrayOutputStream();
				outputStream = guiOutputStream;
			} else {
				if (zip) {
					if (!outputFilePath.toLowerCase().endsWith(".zip")) {
						if (!outputFilePath.toLowerCase().endsWith("." + getFileExtension())) {
							outputFilePath = outputFilePath + "." + getFileExtension();
						}

						outputFilePath = outputFilePath + ".zip";
					}
				} else if (!outputFilePath.toLowerCase().endsWith("." + getFileExtension())) {
					outputFilePath = outputFilePath + "." + getFileExtension();
				}

				if (new File(outputFilePath).exists()) {
					throw new DbExportException("Outputfile already exists: " + outputFilePath);
				}

				if (log) {
					logOutputStream = new FileOutputStream(new File(outputFilePath + "." + DateUtilities.formatDate("yyyy-MM-dd_HH-mm-ss", LocalDateTime.now()) + ".log"));

					logToFile(logOutputStream, getConfigurationLogString(new File(outputFilePath).getName(), sqlStatement));
				}

				if (currentItemName == null) {
					logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));
				} else {
					logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), startTimeSub));
				}

				if (zip) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = outputFilePath.substring(0, outputFilePath.length() - 4);
					entryFileName = entryFileName.substring(entryFileName.lastIndexOf(File.separatorChar) + 1);
					if (!entryFileName.toLowerCase().endsWith("." + getFileExtension())) {
						entryFileName += "." + getFileExtension();
					}
					final ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(ZonedDateTime.now().toInstant().toEpochMilli());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
				fileWasCreated = true;
			}

			if (currentItemName == null) {
				showUnlimitedProgress();
			} else {
				showUnlimitedSubProgress();
			}

			try (Statement statement = connection.createStatement()) {
				String countSqlStatementString = "SELECT COUNT(*) FROM (" + sqlStatement + ") data";
				if (dbVendor == DbVendor.Cassandra) {
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
						showProgress();
					} else {
						subItemsToDo = linesToExport;
						showItemProgress();
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
						showProgress();
					} else {
						subItemsDone = 0;
						showItemProgress();
					}

					if (currentItemName == null) {
						showProgress();
					} else {
						showItemProgress();
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
							} else if (value != null && value instanceof ZonedDateTime) {
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
								overallExportedDataAmountRaw += ((File) value).length();
								if (zip) {
									overallExportedDataAmountCompressed += ZipUtilities.getDataSizeUncompressed((File) value);
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
							showProgress();
						} else {
							subItemsDone++;
							showItemProgress();
						}
					}
					endOutput();
				}

				closeWriter();

				long exportedLines;
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
					logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(Duration.between(getStartTime(), getEndTime()), true));
				} else {
					logToFile(logOutputStream, "End: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), endTimeSub));
					logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(Duration.between(startTimeSub, endTimeSub), true));
				}

				overallExportedLines += exportedLines;
			}
		} catch (final SQLException sqle) {
			errorOccurred = true;
			throw new DbExportException("SQL error: " + sqle.getMessage());
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

			if (errorOccurred && fileWasCreated && new File(outputFilePath).exists() && overallExportedLines == 0) {
				new File(outputFilePath).delete();
			}
		}

		if (new File(outputFilePath).exists()) {
			// TODO: need net.lingala.zip library
			//			if (zip && zipPassword != null) {
			//				final ZipParameters zipParameters = new ZipParameters();
			//				zipParameters.setCompressionMethod(CompressionMethod.DEFLATE);
			//				zipParameters.setEncryptFiles(true);
			//				if (!useZipCrypto) {
			//					zipParameters.setEncryptionMethod(EncryptionMethod.AES);
			//					zipParameters.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
			//				} else {
			//					zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
			//				}
			//				zipParameters.setFileNameInZip(new File(outputFilePath.substring(0, outputFilePath.length() - 4)).getName());
			//				try (ZipFile zipFile = new ZipFile(new File(outputFilePath + ".tmp"), zipPassword)) {
			//					try (ZipFile unencryptedZipFile = new ZipFile(new File(outputFilePath))) {
			//						try (InputStream inputStream = new InputStreamWithOtherItemsToClose(unencryptedZipFile.getInputStream(unencryptedZipFile.getFileHeaders().get(0)), unencryptedZipFile)) {
			//							zipFile.addStream(inputStream, zipParameters);
			//						}
			//					}
			//				}
			//				new File(outputFilePath).delete();
			//				new File(outputFilePath + ".tmp").renameTo(new File(outputFilePath));
			//			}

			final File exportedFile = new File(outputFilePath);
			overallExportedDataAmountRaw += (exportedFile).length();
			// TODO: need net.lingala.zip library
			//			if (zip) {
			//				final ZipFile zipFile = new ZipFile(exportedFile, zipPassword);
			//				final List<FileHeader> fileHeaders = zipFile.getFileHeaders();
			//				long uncompressedSize = 0;
			//				for (final FileHeader fileHeader : fileHeaders) {
			//					final long originalSize = fileHeader.getUncompressedSize();
			//					if (originalSize >= 0) {
			//						uncompressedSize += originalSize;
			//					} else {
			//						// -1 indicates, that size is unknown
			//						uncompressedSize = originalSize;
			//						break;
			//					}
			//				}
			//				overallExportedDataAmountCompressed = uncompressedSize;
			//			}
		}
	}

	private static void logToFile(final OutputStream logOutputStream, final String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message + "\n").getBytes(StandardCharsets.UTF_8));
		}
	}

	public int getOverallExportedLines() {
		return overallExportedLines;
	}

	public long getOverallExportedDataAmountRaw() {
		return overallExportedDataAmountRaw;
	}

	public long getOverallExportedDataAmountCompressed() {
		return overallExportedDataAmountCompressed;
	}

	public ByteArrayOutputStream getGuiOutputStream() {
		return guiOutputStream;
	}

	public abstract String getConfigurationLogString(String fileName, String sqlStatement);

	protected abstract String getFileExtension();

	protected abstract void openWriter(OutputStream outputStream) throws Exception;

	protected abstract void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception;

	protected abstract void startTableLine() throws Exception;

	protected abstract void writeColumn(String columnName, Object value) throws Exception;

	protected abstract void writeDateColumn(String columnName, LocalDate value) throws Exception;

	protected abstract void writeDateTimeColumn(String columnName, LocalDateTime value) throws Exception;

	protected abstract void writeDateTimeColumn(String columnName, ZonedDateTime value) throws Exception;

	protected abstract void endTableLine() throws Exception;

	protected abstract void endOutput() throws Exception;

	protected abstract void closeWriter() throws Exception;
}
