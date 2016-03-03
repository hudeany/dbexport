package de.soderer.dbcsvexport.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.dbcsvexport.DbCsvExportException;
import de.soderer.dbcsvexport.converter.DefaultDBValueConverter;
import de.soderer.dbcsvexport.converter.FirebirdDBValueConverter;
import de.soderer.dbcsvexport.converter.MySQLDBValueConverter;
import de.soderer.dbcsvexport.converter.OracleDBValueConverter;
import de.soderer.dbcsvexport.converter.PostgreSQLDBValueConverter;
import de.soderer.dbcsvexport.converter.SQLiteDBValueConverter;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerDual;
import de.soderer.utilities.WorkerParentDual;
import de.soderer.utilities.ZipUtilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;

public abstract class AbstractDbExportWorker extends WorkerDual<Boolean> {
	// Mandatory parameters
	protected DbUtilities.DbVendor dbVendor = null;
	private String hostname;
	private String dbName;
	private String username;
	private String password;
	private String sqlStatementOrTablelist;
	private String outputpath;
	
	// Default optional parameters
	protected boolean log = false;
	protected boolean zip = false;
	protected String encoding = "UTF-8";
	protected boolean createBlobFiles = false;
	protected boolean createClobFiles = false;
	protected Locale dateAndDecimalLocale = Locale.getDefault();
	protected DateFormat dateFormat = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.MEDIUM, SimpleDateFormat.MEDIUM, dateAndDecimalLocale);
	protected NumberFormat decimalFormat = DecimalFormat.getNumberInstance(dateAndDecimalLocale);
	protected boolean beautify = false;
	protected boolean exportStructure = false;
	
	private int overallExportedLines = 0;
	private long overallExportedDataAmount = 0;
	
	private DefaultDBValueConverter dbValueConverter;

	public AbstractDbExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String sqlStatementOrTablelist, String outputpath) throws Exception {
		super(parent);
		this.dbVendor = dbVendor;
		this.hostname = hostname;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
		this.sqlStatementOrTablelist = sqlStatementOrTablelist;
		this.outputpath = outputpath;
		
		if (dbVendor == null) {
			throw new Exception("Unsupported db vendor: null");
		} else if (dbVendor == DbVendor.Oracle) {
			dbValueConverter = new OracleDBValueConverter(zip, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.SQLite) {
			dbValueConverter = new SQLiteDBValueConverter(zip, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.MySQL) {
			dbValueConverter = new MySQLDBValueConverter(zip, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.PostgreSQL) {
			dbValueConverter = new PostgreSQLDBValueConverter(zip, createBlobFiles, createClobFiles, getFileExtension());
		} else if (dbVendor == DbVendor.Firebird) {
			dbValueConverter = new FirebirdDBValueConverter(zip, createBlobFiles, createClobFiles, getFileExtension());
		} else {
			dbValueConverter = new DefaultDBValueConverter(zip, createBlobFiles, createClobFiles, getFileExtension());
		}
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

	public void setCreateBlobFiles(boolean createBlobFiles) {
		this.createBlobFiles = createBlobFiles;
	}

	public void setCreateClobFiles(boolean createClobFiles) {
		this.createClobFiles = createClobFiles;
	}

	public void setDateAndDecimalLocale(Locale dateAndDecimalLocale) {
		this.dateAndDecimalLocale = dateAndDecimalLocale;

		dateFormat = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.MEDIUM, SimpleDateFormat.MEDIUM, dateAndDecimalLocale);
		decimalFormat = DecimalFormat.getNumberInstance(dateAndDecimalLocale);
	}

	public void setBeautify(boolean beautify) {
		this.beautify = beautify;
	}

	public void setExportStructure(boolean exportStructure) {
		this.exportStructure = exportStructure;
	}

	@Override
	public Boolean work() throws Exception {
		Connection connection = null;
		try {
			overallExportedLines = 0;
			connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password.toCharArray()));

			if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")) {
				if (!"console".equalsIgnoreCase(outputpath)) {
					if (!new File(outputpath).exists()) {
						int lastSeparator = Math.max(outputpath.lastIndexOf("/"), outputpath.lastIndexOf("\\"));
						if (lastSeparator >= 0) {
							String filename = outputpath.substring(lastSeparator + 1);
							filename = DateUtilities.replaceDatePatternInString(filename, new Date());
							outputpath = outputpath.substring(0, lastSeparator + 1) + filename;
						}
					}

					if (new File(outputpath).exists() && new File(outputpath).isDirectory()) {
						outputpath = outputpath + File.separator + "export_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
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
				List<String> tablesToExport = DbUtilities.getAvailableTables(connection, sqlStatementOrTablelist);
				if (tablesToExport.size() == 0) {
					throw new DbCsvExportException("No table found for export");
				}
				itemsToDo = tablesToExport.size();
				itemsDone = 0;
				if (exportStructure) {
					if (new File(outputpath).exists() && new File(outputpath).isDirectory()) {
						outputpath = outputpath + File.separator + "dbstructure_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
					}
					exportDbStructure(connection, tablesToExport, outputpath);
				} else {
					for (int i = 0; i < tablesToExport.size() && !cancel; i++) {
						showProgress(true);
						String tableName = tablesToExport.get(i);
						subItemsToDo = 0;
						subItemsDone = 0;
						showItemStart(tableName);
	
						String nextOutputFilePath = outputpath;
						if (!"console".equalsIgnoreCase(outputpath)) {
							nextOutputFilePath = outputpath + File.separator + tableName.toLowerCase();
						} else {
							System.out.println("Table: " + tableName);
						}
						List<String> columnNames = DbUtilities.getColumnNames(connection, tableName);
						Collections.sort(columnNames);
						List<String> keyColumnNames = DbUtilities.getPrimaryKeyColumns(connection, tableName);
						Collections.sort(keyColumnNames);
						List<String> readoutColumns = new ArrayList<String>();
						readoutColumns.addAll(keyColumnNames);
						for (String columnName : columnNames) {
							if (!readoutColumns.contains(columnName)) {
								readoutColumns.add(columnName);
							}
						}
						String orderPart = "";
						if (!keyColumnNames.isEmpty()) {
							orderPart = " ORDER BY " + Utilities.join(keyColumnNames, ", ");
						}
						export(connection, "SELECT " + Utilities.join(readoutColumns, ", ") + " FROM " + tableName + orderPart, nextOutputFilePath);
	
						showItemDone();
	
						itemsDone++;
					}
				}
				return !cancel;
			}
		} catch (Exception e) {
			throw e;
		} finally {
			Utilities.closeQuietly(connection);
		}
	}

	private void exportDbStructure(Connection connection, List<String> tablesToExport, String outputFilePath) throws Exception {
		OutputStream outputStream = null;

		try {
			if (!"console".equalsIgnoreCase(outputFilePath)) {
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
					throw new DbCsvExportException("Outputfile already exists: " + outputFilePath);
				}

				if (zip) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = outputFilePath.substring(0, outputFilePath.length() - 4);
					entryFileName = entryFileName.substring(entryFileName.lastIndexOf(File.separatorChar) + 1);
					if (!entryFileName.toLowerCase().endsWith(".txt")) {
						entryFileName += ".txt";
					}
					ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(new Date().getTime());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
			} else {
				outputStream = System.out;
			}

			showProgress();

			for (int i = 0; i < tablesToExport.size() && !cancel; i++) {
				if (i > 0) {
					outputStream.write(("\n").getBytes("UTF-8"));
				}
				
				// Tablename
				outputStream.write(("Table " + tablesToExport.get(i).toLowerCase() + ":\n").getBytes("UTF-8"));

				List<String> keyColumnsCamelCase = DbUtilities.getPrimaryKeyColumns(connection, tablesToExport.get(i));
				Collections.sort(keyColumnsCamelCase);
				List<String> keyColumns = new ArrayList<String>();
				for (String keyColumnCamelCase : keyColumnsCamelCase) {
					keyColumns.add(keyColumnCamelCase.toLowerCase());
				}
				CaseInsensitiveMap<DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tablesToExport.get(i));
				List<List<String>> foreignKeys = DbUtilities.getForeignKeys(connection, tablesToExport.get(i));
				
				// Columns (primary key columns first)
				for (String keyColumn : keyColumns) {
					outputStream.write(("\t" + keyColumn + " " + dbColumns.get(keyColumn).getTypeName() + " (Simple: " + dbColumns.get(keyColumn).getSimpleDataType() + ")\n").getBytes("UTF-8"));
				}
				List<String> columnNames = new ArrayList<String>(dbColumns.keySet());
				Collections.sort(columnNames);
				for (String columnName : columnNames) {
					if (!keyColumns.contains(columnName)) {
						outputStream.write(("\t" + columnName.toLowerCase() + " " + dbColumns.get(columnName).getTypeName() + " (Simple: " + dbColumns.get(columnName).getSimpleDataType() + ")\n").getBytes("UTF-8"));
					}
				}
				
				// Primary key
				if (!keyColumns.isEmpty()) {
					outputStream.write("Primary key:\n".getBytes("UTF-8"));
					outputStream.write(("\t" + Utilities.join(keyColumns, ", ").toLowerCase() + "\n").getBytes("UTF-8"));
				}

				// Foreign keys
				if (!foreignKeys.isEmpty()) {
					outputStream.write("Foreign keys:\n".getBytes("UTF-8"));
					for (List<String> foreignKey : foreignKeys) {
						outputStream.write(("\t" + foreignKey.get(1) + " => " + foreignKey.get(2) + "." + foreignKey.get(3) + "\n").toLowerCase().getBytes("UTF-8"));
					}
				}

				itemsDone++;
				showProgress();
			}
		} finally {
			Utilities.closeQuietly(outputStream);
		}
	}

	private void exportDbStructure(Connection connection, String sqlStatement, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			if (!"console".equalsIgnoreCase(outputFilePath)) {
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
					throw new DbCsvExportException("Outputfile already exists: " + outputFilePath);
				}

				if (zip) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = outputFilePath.substring(0, outputFilePath.length() - 4);
					entryFileName = entryFileName.substring(entryFileName.lastIndexOf(File.separatorChar) + 1);
					if (!entryFileName.toLowerCase().endsWith(".txt")) {
						entryFileName += ".txt";
					}
					ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(new Date().getTime());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
			} else {
				outputStream = System.out;
			}

			statement = connection.createStatement();
			resultSet = statement.executeQuery(sqlStatement);
			ResultSetMetaData metaData = resultSet.getMetaData();
			
			outputStream.write((sqlStatement + "\n\n").getBytes("UTF-8"));
			for (int i = 1; i <= metaData.getColumnCount(); i ++) {
				outputStream.write((metaData.getColumnName(i) + " " + metaData.getColumnTypeName(i) + " (" + DbUtilities.getTypeNameById(metaData.getColumnType(i)) + ")\n").getBytes("UTF-8"));
			}
		} finally {
			Utilities.closeQuietly(resultSet);
			Utilities.closeQuietly(statement);
			Utilities.closeQuietly(outputStream);
		}
	}

	private void export(Connection connection, String sqlStatement, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		OutputStream logOutputStream = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			if (!"console".equalsIgnoreCase(outputFilePath)) {
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
					throw new DbCsvExportException("Outputfile already exists: " + outputFilePath);
				}

				if (log) {
					logOutputStream = new FileOutputStream(new File(outputFilePath + "." + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) + ".log"));
					
					logToFile(logOutputStream, getConfigurationLogString(new File(outputFilePath).getName(), sqlStatement));
				}

				if (currentItemName == null) {
					logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(getStartTime()));
				} else {
					logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(startTimeSub));
				}

				if (zip) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = outputFilePath.substring(0, outputFilePath.length() - 4);
					entryFileName = entryFileName.substring(entryFileName.lastIndexOf(File.separatorChar) + 1);
					if (!entryFileName.toLowerCase().endsWith("." + getFileExtension())) {
						entryFileName += "." + getFileExtension();
					}
					ZipEntry entry = new ZipEntry(entryFileName);
					entry.setTime(new Date().getTime());
					((ZipOutputStream) outputStream).putNextEntry(entry);
				} else {
					outputStream = new FileOutputStream(new File(outputFilePath));
				}
			} else {
				outputStream = System.out;
			}

			statement = connection.createStatement();

			if (currentItemName == null) {
				showUnlimitedProgress();
			} else {
				showUnlimitedSubProgress();
			}

			resultSet = statement.executeQuery("SELECT COUNT(*) FROM(" + sqlStatement + ") data");
			resultSet.next();
			int linesToExport = resultSet.getInt(1);
			logToFile(logOutputStream, "Lines to export: " + linesToExport);

			if (currentItemName == null) {
				itemsToDo = linesToExport;
				showProgress();
			} else {
				subItemsToDo = linesToExport;
				showItemProgress();
			}

			resultSet.close();
			resultSet = null;

			openWriter(outputStream);

			resultSet = statement.executeQuery(sqlStatement);
			ResultSetMetaData metaData = resultSet.getMetaData();
			
			// Scan headers
			List<String> columnNames = new ArrayList<String>();
			List<String> columnTypes = new ArrayList<String>();
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
					String columnName = metaData.getColumnName(columnIndex);
					Object value = dbValueConverter.convert(resultSet, columnIndex, outputFilePath);
					if (value != null && value instanceof File) {
						overallExportedDataAmount += ((File) value).length();
						value = ((File) value).getName();
					}
					writeColumn(columnName, value);
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
			
			long exportedLines;
			if (currentItemName == null) {
				exportedLines = itemsDone;
			} else {
				exportedLines = subItemsDone;
			}

			if (currentItemName == null) {
				setEndTime(new Date());
			} else {
				endTimeSub = new Date();
			}

			if (exportedLines > 0) {
				logToFile(logOutputStream, "Exported lines: " + exportedLines);

				int elapsedTimeInSeconds;
				if (currentItemName == null) {
					elapsedTimeInSeconds = (int) (getEndTime().getTime() - getStartTime().getTime()) / 1000;
				} else {
					elapsedTimeInSeconds = (int) (endTimeSub.getTime() - startTimeSub.getTime()) / 1000;
				}
				if (elapsedTimeInSeconds > 0) {
					int linesPerSecond = (int) (exportedLines / elapsedTimeInSeconds);
					logToFile(logOutputStream, "Export speed: " + linesPerSecond + " lines/second");
				} else {
					logToFile(logOutputStream, "Export speed: immediately");
				}
				
				if (new File(outputFilePath).exists()) {
					logToFile(logOutputStream, "Exported data amount: " + Utilities.getHumanReadableNumber(new File(outputFilePath).length(), "B"));
				}
			}

			if (currentItemName == null) {
				logToFile(logOutputStream, "End: " + DateFormat.getDateTimeInstance().format(getEndTime()));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(getEndTime().getTime() - getStartTime().getTime(), true));
			} else {
				logToFile(logOutputStream, "End: " + DateFormat.getDateTimeInstance().format(endTimeSub));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(endTimeSub.getTime() - startTimeSub.getTime(), true));
			}

			overallExportedLines += exportedLines;
		} catch (SQLException sqle) {
			throw new DbCsvExportException("SQL error: " + sqle.getMessage());
		} catch (Exception e) {
			try {
				logToFile(logOutputStream, "Error: " + e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			throw e;
		} finally {
			Utilities.closeQuietly(resultSet);
			Utilities.closeQuietly(statement);

			closeWriter();
			
			Utilities.closeQuietly(outputStream);
			Utilities.closeQuietly(logOutputStream);
		}

		if (new File(outputFilePath).exists()) {
			overallExportedDataAmount += new File(outputFilePath).length();
		}
	}

	private static void logToFile(OutputStream logOutputStream, String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message + "\n").getBytes("UTF-8"));
		}
	}

	public int getOverallExportedLines() {
		return overallExportedLines;
	}
	
	public long getOverallExportedDataAmount() {
		return overallExportedDataAmount;
	}
	
	public abstract String getConfigurationLogString(String fileName, String sqlStatement);
	
	protected abstract String getFileExtension();

	protected abstract void openWriter(OutputStream outputStream) throws Exception;

	protected abstract void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception;

	protected abstract void startTableLine() throws Exception;

	protected abstract void writeColumn(String columnName, Object value) throws Exception;

	protected abstract void endTableLine() throws Exception;
	
	protected abstract void endOutput() throws Exception;

	protected abstract void closeWriter() throws Exception;
}
