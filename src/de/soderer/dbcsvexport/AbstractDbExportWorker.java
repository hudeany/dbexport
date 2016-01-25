package de.soderer.dbcsvexport;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerDual;
import de.soderer.utilities.WorkerParentDual;
import de.soderer.utilities.ZipUtilities;

public abstract class AbstractDbExportWorker extends WorkerDual<Boolean> {
	// Mandatory parameters
	private DbUtilities.DbVendor dbVendor = null;
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
	
	private int overallExportedLines = 0;
	private long overallExportedDataAmount = 0;

	public AbstractDbExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String sqlStatementOrTablelist, String outputpath) {
		super(parent);
		this.dbVendor = dbVendor;
		this.hostname = hostname;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
		this.sqlStatementOrTablelist = sqlStatementOrTablelist;
		this.outputpath = outputpath;
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

	@Override
	public void run() {
		startTime = new Date();

		Connection connection = null;
		try {
			overallExportedLines = 0;
			connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password.toCharArray()));

			if (sqlStatementOrTablelist.toLowerCase().startsWith("select ")) {
				itemsToDo = 0;
				itemsDone = 0;

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

				export(connection, sqlStatementOrTablelist, outputpath);

				result = !cancel;
			} else {
				showItemStart("Scanning tables ...");
				showUnlimitedProgress();
				List<String> tablesToExport = DbUtilities.getAvailableTables(connection, sqlStatementOrTablelist);
				if (tablesToExport.size() == 0) {
					throw new DbCsvExportException("No table found for export");
				}
				itemsToDo = tablesToExport.size();
				itemsDone = 0;
				boolean success = true;
				for (int i = 0; i < tablesToExport.size() && success && !cancel; i++) {
					showProgress(true);
					String tableName = tablesToExport.get(i);
					subItemsToDo = 0;
					subItemsDone = 0;
					String keyColumn = DbUtilities.getPrimaryKeyColumn(connection, tableName);
					showItemStart(tableName);

					try {
						String nextOutputFilePath = outputpath;
						if (!"console".equalsIgnoreCase(outputpath)) {
							nextOutputFilePath = outputpath + File.separator + tableName.toLowerCase();
						}
						export(connection, "SELECT * FROM " + tableName + (Utilities.isNotEmpty(keyColumn) ? " ORDER BY " + keyColumn : ""), nextOutputFilePath);
					} catch (Exception e) {
						error = e;
						success = false;
					}

					showItemDone();

					itemsDone++;
				}
				result = success && !cancel;
			}
		} catch (Exception e) {
			error = e;
			result = false;
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		showDone();
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
					logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(startTime));
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
				outputStream = new ByteArrayOutputStream();
			}

			statement = connection.createStatement();

			if (currentItemName == null) {
				showUnlimitedProgress();
			} else {
				showUnlimitedSubProgress();
			}

			if (dbVendor == DbVendor.Oracle) {
				resultSet = statement.executeQuery("SELECT COUNT(*) FROM(" + sqlStatement + ")");
			} else if (dbVendor == DbVendor.MySQL) {
				resultSet = statement.executeQuery("SELECT COUNT(*) FROM(" + sqlStatement + ") AS data");
			} else if (dbVendor == DbVendor.PostgreSQL) {
				resultSet = statement.executeQuery("SELECT COUNT(*) FROM(" + sqlStatement + ") AS data");
			} else if (dbVendor == DbVendor.SQLite) {
				resultSet = statement.executeQuery("SELECT COUNT(*) FROM(" + sqlStatement + ") AS data");
			} else {
				throw new Exception("Unknown db vendor");
			}
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
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				columnNames.add(metaData.getColumnName(i));
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
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					String columnName = metaData.getColumnName(i);
					Object value;
					if (resultSet.getObject(i) == null) {
						value = null;
					} else if (metaData.getColumnType(i) == Types.BLOB) {
						if (createBlobFiles) {
							File blobOutputFile = new File(getLobFilePath(outputFilePath, "blob"));
							try (InputStream input = resultSet.getBlob(i).getBinaryStream()) {
								OutputStream output = null;
								try {
									if (zip) {
										output = ZipUtilities.openNewZipOutputStream(new FileOutputStream(blobOutputFile));
										String entryFileName = blobOutputFile.getName().substring(0, blobOutputFile.getName().lastIndexOf("."));
										ZipEntry entry = new ZipEntry(entryFileName);
										entry.setTime(new Date().getTime());
										((ZipOutputStream) output).putNextEntry(entry);
									} else {
										output = new FileOutputStream(blobOutputFile);
									}
									Utilities.copy(input, output);
								} finally {
									if (outputStream instanceof ZipOutputStream) {
										try {
											((ZipOutputStream) outputStream).closeEntry();
										} catch (Exception e) {
											e.printStackTrace();
										}
									}
									Utilities.closeQuietly(output);
								}
								overallExportedDataAmount += blobOutputFile.length();
								value = blobOutputFile.getName();
							} catch (Exception e) {
								logToFile(logOutputStream, "Cannot create blob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
								value = "Error creating blob file '" + blobOutputFile.getAbsolutePath() + "'";
							}
						} else {
							byte[] data = Utilities.toByteArray(resultSet.getBlob(i).getBinaryStream());
							value = Base64.getEncoder().encodeToString(data);
						}
					} else if (metaData.getColumnType(i) == Types.CLOB) {
						if (createClobFiles) {
							File clobOutputFile = new File(getLobFilePath(outputFilePath, "clob"));
							try (Reader input = resultSet.getClob(i).getCharacterStream()) {
								OutputStream clobOutputStream = null;
								try {
									if (zip) {
										clobOutputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(clobOutputFile));
										String entryFileName = clobOutputFile.getName().substring(0, clobOutputFile.getName().lastIndexOf("."));
										ZipEntry entry = new ZipEntry(entryFileName);
										entry.setTime(new Date().getTime());
										((ZipOutputStream) clobOutputStream).putNextEntry(entry);
									} else {
										clobOutputStream = new FileOutputStream(clobOutputFile);
									}
									Utilities.copy(input, clobOutputStream, "UTF-8");
								} finally {
									if (outputStream instanceof ZipOutputStream) {
										try {
											((ZipOutputStream) outputStream).closeEntry();
										} catch (Exception e) {
											e.printStackTrace();
										}
									}
									Utilities.closeQuietly(clobOutputStream);
								}
								overallExportedDataAmount += clobOutputFile.length();
								value = clobOutputFile.getName();
							} catch (Exception e) {
								logToFile(logOutputStream, "Cannot create blob file '" + clobOutputFile.getAbsolutePath() + "': " + e.getMessage());
								value = "Error creating blob file '" + clobOutputFile.getAbsolutePath() + "'";
							}
						} else {
							value = resultSet.getString(i);
						}
					} else if (dbVendor == DbVendor.Oracle && metaData.getColumnType(i) == DbUtilities.ORACLE_TIMESTAMPTZ_TYPECODE) {
						value = resultSet.getTimestamp(i);
					} else {
						value = resultSet.getObject(i);
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
				endTime = new Date();
			} else {
				endTimeSub = new Date();
			}

			if (exportedLines > 0) {
				logToFile(logOutputStream, "Exported lines: " + exportedLines);

				int elapsedTimeInSeconds;
				if (currentItemName == null) {
					elapsedTimeInSeconds = (int) (endTime.getTime() - startTime.getTime()) / 1000;
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
				logToFile(logOutputStream, "End: " + DateFormat.getDateTimeInstance().format(endTime));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(endTime.getTime() - startTime.getTime(), true));
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
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (statement != null) {
				try {
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			closeWriter();

			if (outputStream != null) {
				outputStream.close();
			}

			if (logOutputStream != null) {
				try {
					logOutputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		if (new File(outputFilePath).exists()) {
			overallExportedDataAmount += new File(outputFilePath).length();
		}
		
		if ("console".equalsIgnoreCase(outputFilePath) && outputStream != null && outputStream instanceof ByteArrayOutputStream) {
			System.out.println(new String(((ByteArrayOutputStream) outputStream).toByteArray(), "UTF-8"));
		}
	}

	private static void logToFile(OutputStream logOutputStream, String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message + "\n").getBytes("UTF-8"));
		}
	}
	
	protected String getLobFilePath(String outputFilePath, String lobType) throws Exception {
		String lobOutputFilePathPrefix = outputFilePath;
		if (outputFilePath.endsWith(".zip")) {
			outputFilePath.substring(0, outputFilePath.length() - 4);
		}
		if (outputFilePath.endsWith("." + getFileExtension())) {
			outputFilePath.substring(0, outputFilePath.length() - (getFileExtension().length() + 1));
		}
		return File.createTempFile(lobOutputFilePathPrefix + "_", "." + lobType + (zip ? ".zip" : "")).getAbsolutePath();
	}

	public int getOverallExportedLines() {
		return overallExportedLines;
	}
	
	public long getOverallExportedDataAmount() {
		return overallExportedDataAmount;
	}
	
	protected abstract String getConfigurationLogString(String fileName, String sqlStatement);
	
	protected abstract String getFileExtension();

	protected abstract void openWriter(OutputStream outputStream) throws Exception;

	protected abstract void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception;

	protected abstract void startTableLine() throws Exception;

	protected abstract void writeColumn(String columnName, Object value) throws Exception;

	protected abstract void endTableLine() throws Exception;
	
	protected abstract void endOutput() throws Exception;

	protected abstract void closeWriter() throws Exception;
}
