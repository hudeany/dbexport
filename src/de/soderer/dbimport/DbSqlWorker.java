package de.soderer.dbimport;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.SqlScriptReader;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.csv.CsvDataException;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.zip.Zip4jUtilities;

public class DbSqlWorker extends DbImportWorker {
	private SqlScriptReader sqlScriptReader = null;
	private Integer itemsAmount = null;

	private final boolean isInlineData;
	private final String importFilePathOrData;
	private final char[] zipPassword;

	private final Charset encoding = StandardCharsets.UTF_8;

	public DbSqlWorker(final WorkerParentSimple parent, final DbVendor dbVendor, final String hostname, final String dbName, final String username, final char[] password, final boolean secureConnection, final String trustStoreFilePath, final char[] trustStorePassword, final String tableName, final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, secureConnection, trustStoreFilePath, trustStorePassword, tableName, null, null);

		this.isInlineData = isInlineData;
		this.importFilePathOrData = importFilePathOrData;
		this.zipPassword = zipPassword;
	}

	@Override
	public String getConfigurationLogString() {
		String dataPart;
		if (isInlineData) {
			dataPart = "Data: " + importFilePathOrData + "\n";
		} else {
			dataPart = "File: " + importFilePathOrData + "\n"
					+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") + "\n";
		}
		return
				dataPart
				+ "Format: SQL" + "\n"
				+ "Encoding: " + encoding + "\n";
	}

	public int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			try (SqlScriptReader scanSqlScriptReader = new SqlScriptReader(getInputStream(), encoding)) {
				int statementsCount = 0;
				while (scanSqlScriptReader.readNextStatement() != null) {
					statementsCount++;
				}
				itemsAmount = statementsCount;
			} catch (final CsvDataException e) {
				throw new DbImportException(e.getMessage(), e);
			} catch (final Exception e) {
				throw e;
			}
		}
		return itemsAmount;
	}

	public void close() {
		Utilities.closeQuietly(sqlScriptReader);
		sqlScriptReader = null;
	}

	@SuppressWarnings("resource")
	@Override
	public Boolean work() throws Exception {
		OutputStream logOutputStream = null;

		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFilePathOrData);
			}
		}

		Connection connection = null;
		boolean previousAutoCommit = false;
		try {
			connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password), secureConnection, Utilities.isNotBlank(trustStoreFilePath) ? new File(trustStoreFilePath) : null, trustStorePassword, true);
			previousAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);

			validItems = 0;
			invalidItems = new ArrayList<>();

			try {
				if (logFile != null) {
					logOutputStream = new FileOutputStream(logFile);
					logToFile(logOutputStream, getConfigurationLogString());
				}

				logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));

				signalUnlimitedProgress();

				itemsToDo = getItemsAmountToImport();
				logToFile(logOutputStream, "Statements to execute: " + itemsToDo);
				signalProgress(true);

				try (Statement statement = connection.createStatement()) {
					openReader();

					// Execute statements
					String nextStatement;
					while ((nextStatement = sqlScriptReader.readNextStatement()) != null) {
						try {
							statement.execute(nextStatement);
							validItems++;
						} catch (final Exception e) {
							if (commitOnFullSuccessOnly) {
								connection.rollback();
								throw new Exception("Erroneous statement number " + (itemsDone + 1) + " at character index " + sqlScriptReader.getReadCharacters() + ": " + e.getMessage());
							}
							invalidItems.add((int) itemsDone);
							if (logErroneousData) {
								if (erroneousDataFile == null) {
									erroneousDataFile = new File(DateUtilities.formatDate(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName, getStartTime()) + ".errors");
								} else {
									FileUtilities.append(erroneousDataFile, "\n", StandardCharsets.UTF_8);
								}
								FileUtilities.append(erroneousDataFile, nextStatement, StandardCharsets.UTF_8);
							}
						}
						itemsDone++;
					}
					connection.commit();
				}

				setEndTime(LocalDateTime.now());

				importedDataAmount += isInlineData ? importFilePathOrData.length() : new File(importFilePathOrData).length();

				logToFile(logOutputStream, getResultStatistics());

				final long elapsedTimeInSeconds = Duration.between(getStartTime(), getEndTime()).toSeconds();
				if (elapsedTimeInSeconds > 0) {
					final long itemsPerSecond = validItems / elapsedTimeInSeconds;
					logToFile(logOutputStream, "Import speed: " + itemsPerSecond + " items/second");
				} else {
					logToFile(logOutputStream, "Import speed: immediately");
				}
				logToFile(logOutputStream, "End: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getEndTime()));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(getStartTime(), getEndTime()), true));
			} catch (final SQLException sqle) {
				throw new DbImportException("SQL error: " + sqle.getMessage());
			} catch (final Exception e) {
				try {
					logToFile(logOutputStream, "Error: " + e.getMessage());
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw e;
			} finally {
				close();
				Utilities.closeQuietly(logOutputStream);
			}

			return !cancel;
		} catch (final Exception e) {
			throw e;
		} finally {
			if (connection != null) {
				connection.rollback();
				connection.setAutoCommit(previousAutoCommit);
				connection.close();
			}
		}
	}

	private InputStream getInputStream() throws Exception {
		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).length() == 0) {
				throw new DbImportException("Import file is empty: " + importFilePathOrData);
			}

			InputStream inputStream = null;
			try {
				if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") || Utilities.isZipArchiveFile(new File(importFilePathOrData))) {
					if (zipPassword != null)  {
						inputStream = Zip4jUtilities.openPasswordSecuredZipFile(importFilePathOrData, zipPassword);
					} else {
						final List<String> filepathsFromZipArchiveFile = Utilities.getFilepathsFromZipArchiveFile(new File(importFilePathOrData));
						if (filepathsFromZipArchiveFile.size() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (filepathsFromZipArchiveFile.size() > 1) {
							throw new DbImportException("Zipped import file contains more than one file: " + importFilePathOrData);
						}

						inputStream = new ZipInputStream(new FileInputStream(new File(importFilePathOrData)));
						final ZipEntry zipEntry = ((ZipInputStream) inputStream).getNextEntry();
						if (zipEntry == null) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (zipEntry.getSize() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData + ": " + zipEntry.getName());
						}
					}
				} else {
					inputStream = new FileInputStream(new File(importFilePathOrData));
				}
				return inputStream;
			} catch (final Exception e) {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (@SuppressWarnings("unused") final IOException e1) {
						// do nothing
					}
				}
				throw e;
			}
		} else {
			return new ByteArrayInputStream(importFilePathOrData.getBytes(StandardCharsets.UTF_8));
		}
	}

	@SuppressWarnings("resource")
	private void openReader() throws Exception {
		final InputStream inputStream = null;
		try {
			sqlScriptReader = new SqlScriptReader(getInputStream(), encoding);
		} catch (final Exception e) {
			Utilities.closeQuietly(sqlScriptReader);
			Utilities.closeQuietly(inputStream);
			throw e;
		}
	}
}
