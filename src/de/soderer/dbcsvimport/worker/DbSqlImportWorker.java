package de.soderer.dbcsvimport.worker;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipInputStream;

import de.soderer.dbcsvimport.DbCsvImportException;
import de.soderer.dbcsvimport.DbCsvImportDefinition.DataType;
import de.soderer.utilities.CsvDataException;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.SqlScriptReader;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;

public class DbSqlImportWorker extends AbstractDbImportWorker {
	private SqlScriptReader sqlScriptReader = null;
	private Integer itemsAmount = null;
	
	public DbSqlImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, boolean isInlineData, String importFilePathOrData) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, null, isInlineData, importFilePathOrData, DataType.SQL);
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
			+ "Encoding: " + encoding + "\n"
			+ "CommitOnFullSuccessOnly: " + commitOnFullSuccessOnly + "\n";
	}
	
	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes() throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	protected int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			SqlScriptReader sqlScriptReader = null;
			InputStream inputStream = null;
			try {
				if (!isInlineData) {
					inputStream = new FileInputStream(new File(importFilePathOrData));
					if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
						inputStream = new ZipInputStream(inputStream);
						((ZipInputStream) inputStream).getNextEntry();
					}
				} else {
					inputStream = new ByteArrayInputStream(importFilePathOrData.getBytes("UTF-8"));
				}
				
				sqlScriptReader = new SqlScriptReader(inputStream, encoding);
				
				int statementsCount = 0;
				while (sqlScriptReader.readNextStatement() != null) {
					statementsCount++;
				}
				itemsAmount = statementsCount;
			} catch (CsvDataException e) {
				throw new DbCsvImportException(e.getMessage(), e);
			} catch (Exception e) {
				throw e;
			} finally {
				Utilities.closeQuietly(sqlScriptReader);
				Utilities.closeQuietly(inputStream);
			}
		}
		return itemsAmount;
	}

	private void openReader() throws Exception {
		InputStream inputStream = null;
		try {
			if (!isInlineData) {
				inputStream = new FileInputStream(new File(importFilePathOrData));
				if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
					inputStream = new ZipInputStream(inputStream);
					((ZipInputStream) inputStream).getNextEntry();
				}
			} else {
				inputStream = new ByteArrayInputStream(importFilePathOrData.getBytes("UTF-8"));
			}
			sqlScriptReader = new SqlScriptReader(inputStream, encoding);
		} catch (Exception e) {
			Utilities.closeQuietly(sqlScriptReader);
			Utilities.closeQuietly(inputStream);
		}
	}

	@Override
	protected Map<String, Object> getNextItemData() throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public void close() {
		Utilities.closeQuietly(sqlScriptReader);
		sqlScriptReader = null;
	}

	@Override
	protected File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception {
		throw new Exception("Not implemented");
	}
	
	@Override
	public Boolean work() throws Exception {
		OutputStream logOutputStream = null;

		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbCsvImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbCsvImportException("Import path is a directory: " + importFilePathOrData);
			}
		}
		
		Connection connection = null; 
		boolean previousAutoCommit = false;
		try {
			connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password.toCharArray()), true);
			previousAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			
			validItems = 0;
			invalidItems = new ArrayList<Integer>();
	
			try {
				if (log) {
					logOutputStream = new FileOutputStream(new File(importFilePathOrData + "." + DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName.format(getStartTime()) + ".import.log"));
					
					logToFile(logOutputStream, getConfigurationLogString());
				}
	
				logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(getStartTime()));

				showUnlimitedProgress();

				itemsToDo = getItemsAmountToImport();
				logToFile(logOutputStream, "Statements to execute: " + itemsToDo);
				showProgress(true);
				
				try (Statement statement = connection.createStatement()) {
					openReader();
					
					// Execute statements
					String nextStatement;
					while ((nextStatement = sqlScriptReader.readNextStatement()) != null) {
						try {
							statement.execute(nextStatement);
							validItems++;
						} catch (Exception e) {
							if (commitOnFullSuccessOnly) {
								connection.rollback();
								throw new Exception("Errorneous statement number " + (itemsDone + 1) + " at character index " + sqlScriptReader.getReadCharacters() + ": " + e.getMessage());
							}
							invalidItems.add((int) itemsDone);
						}
						itemsDone++;
					}
					connection.commit();
				}
				
				if (logErrorneousData & invalidItems.size() > 0) {
					errorneousDataFile = filterDataItems(invalidItems, DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName.format(getStartTime()) + ".errors");
				}
				
				setEndTime(new Date());
				
				importedDataAmount += isInlineData ? importFilePathOrData.length() : new File(importFilePathOrData).length();
				
				logToFile(logOutputStream, getResultStatistics());
				
				int elapsedTimeInSeconds = (int) (getEndTime().getTime() - getStartTime().getTime()) / 1000;
				if (elapsedTimeInSeconds > 0) {
					int itemsPerSecond = (int) (validItems / elapsedTimeInSeconds);
					logToFile(logOutputStream, "Import speed: " + itemsPerSecond + " items/second");
				} else {
					logToFile(logOutputStream, "Import speed: immediately");
				}
				logToFile(logOutputStream, "End: " + DateFormat.getDateTimeInstance().format(getEndTime()));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespan(getEndTime().getTime() - getStartTime().getTime(), true));
			} catch (SQLException sqle) {
				throw new DbCsvImportException("SQL error: " + sqle.getMessage());
			} catch (Exception e) {
				try {
					logToFile(logOutputStream, "Error: " + e.getMessage());
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				throw e;
			} finally {
				close();
				Utilities.closeQuietly(logOutputStream);
			}
	
			return !cancel;
		} catch (Exception e) {
			throw e;
		} finally {
			if (connection != null) {
				connection.rollback();
				connection.setAutoCommit(previousAutoCommit);
				connection.close();
			}
		}
	}
}
