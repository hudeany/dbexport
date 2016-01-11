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
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.CsvWriter;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerDual;
import de.soderer.utilities.WorkerParentDual;
import de.soderer.utilities.ZipUtilities;

public class DbCsvExportWorker extends WorkerDual<Boolean> {
	private DbCsvExportDefinition dbCsvExportDefinition;
	private int overallExportedLines = 0;

	public DbCsvExportWorker(WorkerParentDual parent, DbCsvExportDefinition dbCsvExportDefinition) {
		super(parent);

		this.dbCsvExportDefinition = dbCsvExportDefinition;
	}

	@Override
	public void run() {
		startTime = new Date();

		Connection connection = null;
		try {
			dbCsvExportDefinition.checkParameters();
			dbCsvExportDefinition.checkAndLoadDbDrivers();

			overallExportedLines = 0;
			connection = DbCsvExportHelper.createConnection(dbCsvExportDefinition);

			if (dbCsvExportDefinition.getSqlStatementOrTablelist().toLowerCase().startsWith("select ")) {
				itemsToDo = 0;
				itemsDone = 0;

				if (!"console".equalsIgnoreCase(dbCsvExportDefinition.getOutputpath())) {
					if (!new File(dbCsvExportDefinition.getOutputpath()).exists()) {
						int lastSeparator = Math.max(dbCsvExportDefinition.getOutputpath().lastIndexOf("/"), dbCsvExportDefinition.getOutputpath().lastIndexOf("\\"));
						if (lastSeparator >= 0) {
							String filename = dbCsvExportDefinition.getOutputpath().substring(lastSeparator + 1);
							filename = DateUtilities.replaceDatePatternInString(filename, new Date());
							dbCsvExportDefinition.setOutputpath(dbCsvExportDefinition.getOutputpath().substring(0, lastSeparator + 1) + filename);
						}
					}

					if (new File(dbCsvExportDefinition.getOutputpath()).exists() && new File(dbCsvExportDefinition.getOutputpath()).isDirectory()) {
						dbCsvExportDefinition.setOutputpath(dbCsvExportDefinition.getOutputpath() + File.separator + "export_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
					}
				}

				export(connection, dbCsvExportDefinition.getSqlStatementOrTablelist(), dbCsvExportDefinition.getOutputpath());

				result = !cancel;
			} else {
				showItemStart("Scanning tables ...");
				showUnlimitedProgress();
				List<String> tablesToExport = DbCsvExportHelper.getTablesToExport(connection, dbCsvExportDefinition);
				itemsToDo = tablesToExport.size();
				itemsDone = 0;
				boolean success = true;
				for (int i = 0; i < tablesToExport.size() && success && !cancel; i++) {
					showProgress(true);
					String tableName = tablesToExport.get(i);
					subItemsToDo = 0;
					subItemsDone = 0;
					String keyColumn = DbCsvExportHelper.getPrimaryKeyColumn(connection, tableName, dbCsvExportDefinition);
					showItemStart(tableName);

					try {
						String nextOutputFilePath = dbCsvExportDefinition.getOutputpath();
						if (!"console".equalsIgnoreCase(dbCsvExportDefinition.getOutputpath())) {
							nextOutputFilePath = dbCsvExportDefinition.getOutputpath() + File.separator + tableName.toLowerCase();
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
			showDone();
		} catch (Exception e) {
			error = e;
			result = false;
			showDone();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void export(Connection connection, String sqlStatement, String outputFilePath) throws Exception {
		OutputStream outputStream = null;
		OutputStream logOutputStream = null;
		Statement statement = null;
		ResultSet resultSet = null;
		CsvWriter csvWriter = null;

		try {
			if (!"console".equalsIgnoreCase(outputFilePath)) {
				if (dbCsvExportDefinition.isZip()) {
					if (!outputFilePath.toLowerCase().endsWith(".zip")) {
						outputFilePath = outputFilePath + ".zip";
					}
				} else if (!outputFilePath.toLowerCase().endsWith(".csv")) {
					outputFilePath = outputFilePath + ".csv";
				}

				if (new File(outputFilePath).exists()) {
					throw new DbCsvExportException("Outputfile already exists: " + outputFilePath);
				}

				if (dbCsvExportDefinition.isLog()) {
					logOutputStream = new FileOutputStream(new File(outputFilePath + "." + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) + ".log"));

					logToFile(logOutputStream, "Separator: " + dbCsvExportDefinition.getSeparator());
					logToFile(logOutputStream, "Zip: " + dbCsvExportDefinition.isZip());
					logToFile(logOutputStream, "Encoding: " + dbCsvExportDefinition.getEncoding());
					logToFile(logOutputStream, "StringQuote: " + dbCsvExportDefinition.getStringQuote());
					logToFile(logOutputStream, "AlwaysQuote: " + dbCsvExportDefinition.isAlwaysQuote());
					logToFile(logOutputStream, "SqlStatement: " + sqlStatement);
					logToFile(logOutputStream, "OutputFormatLocale: " + dbCsvExportDefinition.getDateAndDecimalLocale().getLanguage());
					logToFile(logOutputStream, "OutputFormatLocale: " + dbCsvExportDefinition.getDateAndDecimalLocale().getLanguage());
					logToFile(logOutputStream, "CreateBlobFiles: " + dbCsvExportDefinition.isCreateBlobFiles());
					logToFile(logOutputStream, "CreateClobFiles: " + dbCsvExportDefinition.isCreateClobFiles());
					logToFile(logOutputStream, "Beautify: " + dbCsvExportDefinition.isBeautify());
				}

				if (currentItemName == null) {
					logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(startTime));
				} else {
					logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(startTimeSub));
				}

				if (dbCsvExportDefinition.isZip()) {
					outputStream = ZipUtilities.openNewZipOutputStream(new FileOutputStream(new File(outputFilePath)));
					String entryFileName = outputFilePath.substring(0, outputFilePath.length() - 4);
					entryFileName = entryFileName.substring(entryFileName.lastIndexOf(File.separatorChar) + 1);
					if (!entryFileName.toLowerCase().endsWith(".csv")) {
						entryFileName += ".csv";
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

			if ("oracle".equals(dbCsvExportDefinition.getDbType())) {
				resultSet = statement.executeQuery("SELECT COUNT(*) FROM(" + sqlStatement + ")");
			} else {
				resultSet = statement.executeQuery("SELECT COUNT(*) FROM(" + sqlStatement + ") AS data");
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

			int[] minimumColumnSizes = null;
			boolean[] columnPaddings = null;

			csvWriter = new CsvWriter(outputStream, dbCsvExportDefinition.getEncoding(), dbCsvExportDefinition.getSeparator(), dbCsvExportDefinition.getStringQuote());
			csvWriter.setAlwaysQuote(dbCsvExportDefinition.isAlwaysQuote());

			// Scan all data for column lengths
			if (dbCsvExportDefinition.isBeautify()) {
				resultSet = statement.executeQuery(sqlStatement);
				ResultSetMetaData metaData = resultSet.getMetaData();

				columnPaddings = new boolean[metaData.getColumnCount()];
				for (int i = 0; i < columnPaddings.length; i++) {
					columnPaddings[i] = true;
				}

				// Scan headers
				List<String> headers = new ArrayList<String>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					headers.add(metaData.getColumnName(i));
				}
				minimumColumnSizes = csvWriter.calculateOutputSizesOfValues(headers);

				if (currentItemName == null) {
					itemsDone++;
					showProgress();
				} else {
					subItemsDone++;
					showItemProgress();
				}

				// Scan values
				while (resultSet.next() && !cancel) {
					List<String> values = new ArrayList<String>();
					for (int i = 1; i <= metaData.getColumnCount(); i++) {
						String valueString;
						if (resultSet.getObject(i) == null) {
							valueString = "";
						} else if (metaData.getColumnType(i) == Types.BLOB) {
							if (dbCsvExportDefinition.isCreateBlobFiles()) {
								File blobOutputFile = File.createTempFile(outputFilePath.substring(0, outputFilePath.length() - 4) + "_", ".blob" + (dbCsvExportDefinition.isZip() ? ".zip" : ""),
										new File(outputFilePath).getParentFile());
								valueString = blobOutputFile.getName();
							} else {
								byte[] data = Utilities.toByteArray(resultSet.getBlob(i).getBinaryStream());
								valueString = Base64.getEncoder().encodeToString(data);
							}
						} else if (metaData.getColumnType(i) == Types.CLOB) {
							if (dbCsvExportDefinition.isCreateClobFiles()) {
								File clobOutputFile = File.createTempFile(outputFilePath.substring(0, outputFilePath.length() - 4) + "_", ".clob" + (dbCsvExportDefinition.isZip() ? ".zip" : ""),
										new File(outputFilePath).getParentFile());
								valueString = clobOutputFile.getName();
							} else {
								valueString = resultSet.getString(i);
							}
						} else if (metaData.getColumnType(i) == Types.DATE || metaData.getColumnType(i) == Types.TIMESTAMP) {
							valueString = dbCsvExportDefinition.getDateFormat().format(resultSet.getObject(i));
						} else if (metaData.getColumnType(i) == Types.DECIMAL || metaData.getColumnType(i) == Types.DOUBLE || metaData.getColumnType(i) == Types.FLOAT) {
							valueString = dbCsvExportDefinition.getDecimalFormat().format(resultSet.getObject(i));
							columnPaddings[i - 1] = false;
						} else if (metaData.getColumnType(i) == Types.BIGINT || metaData.getColumnType(i) == Types.BIT || metaData.getColumnType(i) == Types.INTEGER
								|| metaData.getColumnType(i) == Types.NUMERIC || metaData.getColumnType(i) == Types.SMALLINT || metaData.getColumnType(i) == Types.TINYINT) {
							valueString = resultSet.getString(i);
							columnPaddings[i - 1] = false;
						} else {
							valueString = resultSet.getString(i);
						}
						values.add(valueString);
					}
					int[] nextColumnSizes = csvWriter.calculateOutputSizesOfValues(values);
					for (int i = 0; i < nextColumnSizes.length; i++) {
						minimumColumnSizes[i] = Math.max(minimumColumnSizes[i], nextColumnSizes[i]);
					}

					if (currentItemName == null) {
						itemsDone++;
						showProgress();
					} else {
						subItemsDone++;
						showItemProgress();
					}
				}

				resultSet.close();
				resultSet = null;

				if (currentItemName == null) {
					itemsDone = 0;
					showProgress();
				} else {
					subItemsDone = 0;
					showItemProgress();
				}
			}

			resultSet = statement.executeQuery(sqlStatement);
			csvWriter.setColumnPaddings(columnPaddings);
			csvWriter.setMinimumColumnSizes(minimumColumnSizes);
			ResultSetMetaData metaData = resultSet.getMetaData();

			if (currentItemName == null) {
				itemsDone = 0;
				showProgress();
			} else {
				subItemsDone = 0;
				showItemProgress();
			}

			// Write headers
			List<String> headers = new ArrayList<String>();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				headers.add(metaData.getColumnName(i));
			}
			csvWriter.writeValues(headers);

			if (currentItemName == null) {
				itemsDone++;
				showProgress();
			} else {
				subItemsDone++;
				showItemProgress();
			}

			// Write values
			while (resultSet.next() && !cancel) {
				List<String> values = new ArrayList<String>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					String valueString;
					if (resultSet.getObject(i) == null) {
						valueString = "";
					} else if (metaData.getColumnType(i) == Types.BLOB) {
						if (dbCsvExportDefinition.isCreateBlobFiles()) {
							File blobOutputFile = File.createTempFile(outputFilePath.substring(0, outputFilePath.length() - 4) + "_", ".blob" + (dbCsvExportDefinition.isZip() ? ".zip" : ""),
									new File(outputFilePath).getParentFile());
							try (InputStream input = resultSet.getBlob(i).getBinaryStream()) {
								OutputStream output = null;
								try {
									if (dbCsvExportDefinition.isZip()) {
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
									Utilities.closeQuietly(output);
								}
								valueString = blobOutputFile.getName();
							} catch (Exception e) {
								logToFile(logOutputStream, "Cannot create blob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
								valueString = "Error creating blob file '" + blobOutputFile.getAbsolutePath() + "'";
							}
						} else {
							byte[] data = Utilities.toByteArray(resultSet.getBlob(i).getBinaryStream());
							valueString = Base64.getEncoder().encodeToString(data);
						}
					} else if (metaData.getColumnType(i) == Types.CLOB) {
						if (dbCsvExportDefinition.isCreateClobFiles()) {
							File clobOutputFile = File.createTempFile(outputFilePath.substring(0, outputFilePath.length() - 4) + "_", ".clob" + (dbCsvExportDefinition.isZip() ? ".zip" : ""),
									new File(outputFilePath).getParentFile());
							try (Reader input = resultSet.getClob(i).getCharacterStream()) {
								OutputStream output = null;
								try {
									if (dbCsvExportDefinition.isZip()) {
										output = ZipUtilities.openNewZipOutputStream(new FileOutputStream(clobOutputFile));
										String entryFileName = clobOutputFile.getName().substring(0, clobOutputFile.getName().lastIndexOf("."));
										ZipEntry entry = new ZipEntry(entryFileName);
										entry.setTime(new Date().getTime());
										((ZipOutputStream) output).putNextEntry(entry);
									} else {
										output = new FileOutputStream(clobOutputFile);
									}
									Utilities.copy(input, output, "UTF-8");
								} finally {
									Utilities.closeQuietly(output);
								}
								valueString = clobOutputFile.getName();
							} catch (Exception e) {
								logToFile(logOutputStream, "Cannot create blob file '" + clobOutputFile.getAbsolutePath() + "': " + e.getMessage());
								valueString = "Error creating blob file '" + clobOutputFile.getAbsolutePath() + "'";
							}
						} else {
							valueString = resultSet.getString(i);
						}
					} else if (metaData.getColumnType(i) == Types.DATE || metaData.getColumnType(i) == Types.TIMESTAMP) {
						valueString = dbCsvExportDefinition.getDateFormat().format(resultSet.getObject(i));
					} else if (metaData.getColumnType(i) == Types.DECIMAL || metaData.getColumnType(i) == Types.DOUBLE || metaData.getColumnType(i) == Types.FLOAT) {
						valueString = dbCsvExportDefinition.getDecimalFormat().format(resultSet.getObject(i));
					} else if (metaData.getColumnType(i) == Types.BIGINT || metaData.getColumnType(i) == Types.BIT || metaData.getColumnType(i) == Types.INTEGER
							|| metaData.getColumnType(i) == Types.NUMERIC || metaData.getColumnType(i) == Types.SMALLINT || metaData.getColumnType(i) == Types.TINYINT) {
						valueString = resultSet.getString(i);
					} else {
						valueString = resultSet.getString(i);
					}
					values.add(valueString);
				}
				csvWriter.writeValues(values);

				if (currentItemName == null) {
					itemsDone++;
					showProgress();
				} else {
					subItemsDone++;
					showItemProgress();
				}
			}

			if (currentItemName == null) {
				endTime = new Date();
			} else {
				endTimeSub = new Date();
			}

			if ("console".equalsIgnoreCase(outputFilePath)) {
				csvWriter.flush();
				System.out.println(new String(((ByteArrayOutputStream) outputStream).toByteArray(), "UTF-8"));
			}

			if ((csvWriter.getWrittenLines() - 1) > 0) {
				logToFile(logOutputStream, "Exported lines: " + (csvWriter.getWrittenLines() - 1));

				int elapsedTimeInSeconds;
				if (currentItemName == null) {
					elapsedTimeInSeconds = (int) (endTime.getTime() - startTime.getTime()) / 1000;
				} else {
					elapsedTimeInSeconds = (int) (endTimeSub.getTime() - startTimeSub.getTime()) / 1000;
				}
				if (elapsedTimeInSeconds > 0) {
					int linesPerSecond = (csvWriter.getWrittenLines() - 1) / elapsedTimeInSeconds;
					logToFile(logOutputStream, "Export speed: " + linesPerSecond + " lines/second");
				} else {
					logToFile(logOutputStream, "Export speed: immediately");
				}
			}

			if (currentItemName == null) {
				logToFile(logOutputStream, "End: " + DateFormat.getDateTimeInstance().format(endTime));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(endTime.getTime() - startTime.getTime(), true));
			} else {
				logToFile(logOutputStream, "End: " + DateFormat.getDateTimeInstance().format(endTimeSub));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(endTimeSub.getTime() - startTimeSub.getTime(), true));
			}

			overallExportedLines += csvWriter.getWrittenLines() - 1;
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

			if (csvWriter != null) {
				try {
					csvWriter.flush();
				} catch (Exception e) {
					e.printStackTrace();
				}

				if (dbCsvExportDefinition.isZip()) {
					if (outputStream != null) {
						try {
							((ZipOutputStream) outputStream).closeEntry();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}

				try {
					csvWriter.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (logOutputStream != null) {
				try {
					logOutputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void logToFile(OutputStream logOutputStream, String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message + "\n").getBytes("UTF-8"));
		}
	}

	public DbCsvExportDefinition getDbCsvExportDefinition() {
		return dbCsvExportDefinition;
	}

	public int getOverallExportedLines() {
		return overallExportedLines;
	}
}
