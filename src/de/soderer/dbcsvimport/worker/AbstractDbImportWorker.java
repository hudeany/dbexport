package de.soderer.dbcsvimport.worker;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import de.soderer.dbcsvimport.DbCsvImportDefinition.ImportMode;
import de.soderer.dbcsvimport.DbCsvImportException;
import de.soderer.dbcsvimport.DbCsvImportMappingDialog;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;
import de.soderer.utilities.WorkerSimple;

public abstract class AbstractDbImportWorker extends WorkerSimple<Boolean> {
	// Mandatory parameters
	protected DbUtilities.DbVendor dbVendor = null;
	protected ImportMode importMode = ImportMode.UPSERT;
	protected List<String> keyColumns = null;
	protected String hostname;
	protected String dbName;
	protected String username;
	protected String password;
	protected String tableName;
	protected boolean createTableIfNotExists = false;
	protected boolean tableWasCreated = false;
	protected String importFilePath;
	protected boolean commitOnFullSuccessOnly = true;
	
	protected List<String> dbTableColumnsListToInsert;
	protected Map<String, Tuple<String, String>> mapping = null;
	
	// Default optional parameters
	protected boolean log = false;
	protected String encoding = "UTF-8";
	protected boolean updateWithNullValues = true;
	
	private int importedItems = 0;
	private List<Integer> notImportedItems = new ArrayList<Integer>();
	private long importedDataAmount = 0;
	private int deletedItems = 0;
	private int updatedItems = 0;
	private int ignoredDuplicates = 0;
	private int insertedItems = 0;
	private int itemsForUpdateInImportData = 0;

	protected String additionalInsertValues = null;
	protected String additionalUpdateValues = null;
	
	private boolean logErrorneousData = false;
	protected File errorneousDataFile = null;

	public AbstractDbImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String tableName, String importFilePath) throws Exception {
		super(parent);
		
		this.dbVendor = dbVendor;
		this.hostname = hostname;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
		this.tableName = tableName;
		this.importFilePath = importFilePath;
	}

	public void setLog(boolean log) {
		this.log = log;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public void setMapping(String mappingString) throws IOException, Exception {
		if (Utilities.isNotBlank(mappingString)) {
			mapping = DbCsvImportMappingDialog.parseMappingString(mappingString);
			dbTableColumnsListToInsert = new ArrayList<String>(mapping.keySet());
		}
	}

	public void setImportmode(ImportMode importMode) {
		this.importMode = importMode;
	}

	public void setKeycolumns(List<String> keyColumns) {
		this.keyColumns = keyColumns;
	}

	public void setCompleteCommit(boolean commitOnFullSuccessOnly) {
		this.commitOnFullSuccessOnly = commitOnFullSuccessOnly;
	}

	public void setAdditionalInsertValues(String additionalInsertValues) {
		this.additionalInsertValues = additionalInsertValues;
	}

	public void setAdditionalUpdateValues(String additionalUpdateValues) {
		this.additionalUpdateValues = additionalUpdateValues;
	}

	public void setUpdateNullData(boolean updateWithNullValues) {
		this.updateWithNullValues = updateWithNullValues;
	}

	public void setCreateTableIfNotExists(boolean createTableIfNotExists) {
		this.createTableIfNotExists = createTableIfNotExists;
	}

	public void setLogErrorneousData(boolean logErrorneousData) {
		this.logErrorneousData = logErrorneousData;
	}

	@Override
	public Boolean work() throws Exception {
		OutputStream logOutputStream = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		Statement statement = null;
		try {
			if (!new File(importFilePath).exists()) {
				throw new DbCsvImportException("Import file does not exist: " + importFilePath);
			} else if (new File(importFilePath).isDirectory()) {
				throw new DbCsvImportException("Import path is a directory: " + importFilePath);
			}
			
			connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password.toCharArray()));
			connection.setAutoCommit(false);
			
			importedItems = 0;
			notImportedItems = new ArrayList<Integer>();
	
			try {
				if (log) {
					logOutputStream = new FileOutputStream(new File(importFilePath + "." + DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName.format(getStartTime()) + ".import.log"));
					
					logToFile(logOutputStream, getConfigurationLogString());
				}
	
				logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(getStartTime()));

				showUnlimitedProgress();
				
				if (!DbUtilities.checkTableExist(connection, tableName)) {
					if (createTableIfNotExists) {
						Map<String, DbColumnType> importDataTypes = scanDataPropertyTypes();
						Map<String, DbColumnType> dbDataTypes = new HashMap<String, DbColumnType>();
						for (Entry<String, DbColumnType> importDataType : importDataTypes.entrySet()) {
							if (mapping != null) {
								for (Entry<String,Tuple<String,String>> mappingEntry : mapping.entrySet()) {
									if (mappingEntry.getValue().getFirst().equals(importDataType.getKey())) {
										dbDataTypes.put(mappingEntry.getKey(), importDataTypes.get(importDataType.getKey()));
										break;
									}
								}
							} else {
								if (!Pattern.matches("[_a-zA-Z0-9]{1,30}", importDataType.getKey())) {
									throw new DbCsvImportException("cannot create table without mapping for data propertyname: " + importDataType.getKey());
								}
								dbDataTypes.put(importDataType.getKey(), importDataTypes.get(importDataType.getKey()));
							}
						}
						if (dbVendor == DbVendor.PostgreSQL) {
							// Close a maybe open transaction to allow DDL-statement
							connection.rollback();
						}
						try {
							DbUtilities.createTable(connection, tableName, dbDataTypes, keyColumns);
							tableWasCreated = true;
						} catch (Exception e) {
							throw new DbCsvImportException("Cannot create new table '" + tableName + "': " + e.getMessage(), e);
						}
						if (dbVendor == DbVendor.PostgreSQL) {
							// Commit DDL-statement
							connection.commit();
						}
					} else {
						throw new DbCsvImportException("Table does not exist: " + tableName);
					}
				}

				Map<String, DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tableName);
				List<String> dataPropertyNames = getAvailableDataPropertyNames();
				if (mapping != null) {
					// Check mapping
					for (String dbColumnToInsert : dbTableColumnsListToInsert) {
						if (!dbColumns.containsKey(dbColumnToInsert)) {
							throw new DbCsvImportException("DB table does not contain mapped column: " + dbColumnToInsert);
						}
					}
					
					for (Entry<String, Tuple<String, String>> mappingEntry : mapping.entrySet()) {
						if (!dataPropertyNames.contains(mappingEntry.getValue().getFirst())) {
							throw new DbCsvImportException("Data does not contain mapped property: " + mappingEntry.getValue().getFirst());
						}
					}
				} else {
					// Create default mapping
					mapping = new HashMap<String, Tuple<String, String>>();
					dbTableColumnsListToInsert = new ArrayList<String>();
					for (String dbColumn : dbColumns.keySet()) {
						for (String dataPropertyName : dataPropertyNames) {
							if (dbColumn.equalsIgnoreCase(dataPropertyName)) {
								mapping.put(dbColumn, new Tuple<String, String>(dataPropertyName, ""));
								dbTableColumnsListToInsert.add(dbColumn);
								break;
							}
						}
					}
				}
				
				if (dbTableColumnsListToInsert.size() == 0) {
					throw new DbCsvImportException("Invalid empty mapping");
				}
				
				String[] keyColumnsArray = null;
				if (keyColumns != null) {
					keyColumns.toArray(new String[0]);
				}
				if (!DbUtilities.checkTableAndColumnsExist(connection, tableName, keyColumnsArray)) {
					throw new DbCsvImportException("Some keycolumn is not included in table");
				}
				
				if (importMode == ImportMode.CLEARINSERT) {
					preparedStatement = connection.prepareStatement("DELETE FROM " + tableName);
					deletedItems = preparedStatement.executeUpdate();
					preparedStatement.close();
					preparedStatement = null;
				}

				itemsToDo = getItemsAmountToImport();
				logToFile(logOutputStream, "Items to import: " + itemsToDo);
				showProgress(true);
				
				if ((importMode == ImportMode.CLEARINSERT || importMode == ImportMode.INSERT) && Utilities.isEmpty(keyColumns)) {
					// Just import in the destination table
					insertIntoTable(connection, tableName, dbColumns, null, additionalInsertValues);
				} else {
					statement = connection.createStatement();
					
					// Create temp table
					String dateSuffix = DateUtilities.YYYYMMDDHHMMSS.format(getStartTime());
					String tempTableName = "tmp_" + dateSuffix;
					int i = 0;
					while (DbUtilities.checkTableExist(connection, tempTableName) && i < 10) {
						Thread.sleep(1000);
						i++;
						dateSuffix = DateUtilities.YYYYMMDDHHMMSS.format(new Date());
						tempTableName = "tmp_" + dateSuffix;
					}
					String itemIndexColumn = "import_item_" + dateSuffix;
					String duplicateIndexColumn = "import_dupl_" + dateSuffix;
					
					if (dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
						statement.execute("CREATE TABLE " + tempTableName + " AS (SELECT " + Utilities.join(dbTableColumnsListToInsert, ", ") + " FROM " + tableName + ") WITH NO DATA");
					} else if (dbVendor == DbVendor.PostgreSQL) {
						// Close a maybe open transaction to allow DDL-statement
						connection.rollback();
						statement.execute("CREATE TABLE " + tempTableName + " AS SELECT " + Utilities.join(dbTableColumnsListToInsert, ", ") + " FROM " + tableName + " WHERE 1 = 0");
					} else if (dbVendor == DbVendor.Firebird) {
						// There is no "create table as select"-statmenet in firebird
						DbUtilities.createTable(connection, tempTableName, DbUtilities.getColumnDataTypes(connection, tableName), null);
					} else {
						statement.execute("CREATE TABLE " + tempTableName + " AS SELECT " + Utilities.join(dbTableColumnsListToInsert, ", ") + " FROM " + tableName + " WHERE 1 = 0");
					}
					statement.execute("ALTER TABLE " + tempTableName + " ADD " + itemIndexColumn + " INTEGER");
					statement.execute("ALTER TABLE " + tempTableName + " ADD " + duplicateIndexColumn + " INTEGER");
					statement.execute("CREATE INDEX " + tempTableName + "_idx1 ON " + tempTableName + " (" + Utilities.join(keyColumns, ", ") + ")");
					statement.execute("CREATE INDEX " + tempTableName + "_idx2 ON " + tempTableName + " (" + itemIndexColumn + ")");
					statement.execute("CREATE INDEX " + tempTableName + "_idx3 ON " + tempTableName + " (" + duplicateIndexColumn + ")");
					
					if (dbVendor == DbVendor.PostgreSQL || dbVendor == DbVendor.Firebird) {
						connection.commit();
					}
					
					// Insert in temp table
					insertIntoTable(connection, tempTableName, dbColumns, itemIndexColumn, null);
					
					showUnlimitedProgress();
					
					if (importMode == ImportMode.CLEARINSERT) {
						markTrailingDuplicates(connection, tempTableName, keyColumns, updateWithNullValues, itemIndexColumn, duplicateIndexColumn);
						ignoredDuplicates = removeDuplicates(connection, tempTableName, keyColumns, updateWithNullValues, itemIndexColumn, duplicateIndexColumn);
						insertedItems = insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, additionalInsertValues);
					} else if (importMode == ImportMode.INSERT) {
						ignoredDuplicates = deleteTableCrossDuplicates(connection, tableName, tempTableName, keyColumns);
						markTrailingDuplicates(connection, tempTableName, keyColumns, updateWithNullValues, itemIndexColumn, duplicateIndexColumn);
						ignoredDuplicates += removeDuplicates(connection, tempTableName, keyColumns, updateWithNullValues, itemIndexColumn, duplicateIndexColumn);
						insertedItems = insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, additionalInsertValues);
					} else if (importMode == ImportMode.UPDATE) {
						itemsForUpdateInImportData = getUpdateableItemsInTable(connection, tempTableName, tableName, keyColumns);
						updatedItems = getUpdateableItemsInTable(connection, tableName, tempTableName, keyColumns);
						// Update destination table
						updateExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, itemIndexColumn, updateWithNullValues, additionalUpdateValues);
					} else if (importMode == ImportMode.UPSERT) {
						itemsForUpdateInImportData = getUpdateableItemsInTable(connection, tempTableName, tableName, keyColumns);
						updatedItems = getUpdateableItemsInTable(connection, tableName, tempTableName, keyColumns);
						
						// Update destination table
						updateExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, itemIndexColumn, updateWithNullValues, additionalUpdateValues);

						markTrailingDuplicates(connection, tempTableName, keyColumns, updateWithNullValues, itemIndexColumn, duplicateIndexColumn);
						ignoredDuplicates = removeDuplicates(connection, tempTableName, keyColumns, updateWithNullValues, itemIndexColumn, duplicateIndexColumn);
						
						// Insert into destination table
						insertedItems = insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, additionalInsertValues);
					} else {
						throw new Exception("Invalid import mode");
					}
					
					// Drop temp table
					if (DbUtilities.checkTableExist(connection, tempTableName) && i < 10) {
						statement.execute("DROP TABLE " + tempTableName);
					}
				}
				
				if (logErrorneousData & notImportedItems.size() > 0) {
					errorneousDataFile = filterDataItems(notImportedItems, DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName.format(getStartTime()) + ".errors");
				}
				
				setEndTime(new Date());
				
				importedDataAmount += new File(importFilePath).length();
				
				logToFile(logOutputStream, getResultStatistics());
				
				int elapsedTimeInSeconds = (int) (getEndTime().getTime() - getStartTime().getTime()) / 1000;
				if (elapsedTimeInSeconds > 0) {
					int itemsPerSecond = (int) (importedItems / elapsedTimeInSeconds);
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
				closeReader();
				Utilities.closeQuietly(logOutputStream);
			}
	
			return !cancel;
		} catch (Exception e) {
			throw e;
		} finally {
			Utilities.closeQuietly(statement);
			Utilities.closeQuietly(preparedStatement);
			if (connection != null) {
				connection.rollback();
			}
			Utilities.closeQuietly(connection);
		}
	}

	private int getUpdateableItemsInTable(Connection connection, String intoTableName, String fromTableName, List<String> keyColumns) throws Exception {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			
			String selectDuplicatesNumber = "SELECT COUNT(*) FROM " + intoTableName + " a WHERE EXISTS (SELECT 1 FROM " + fromTableName + " b WHERE " + getWherePart(keyColumns, "a", "b") + ")";
			ResultSet resultSet = statement.executeQuery(selectDuplicatesNumber);
			resultSet.next();
			return resultSet.getInt(1);
		} catch (Exception e) {
			throw new Exception("Cannot getUpdateItemsInImportData: " + e.getMessage(), e);
		} finally {
			Utilities.closeQuietly(statement);
		}
	}

	public String getResultStatistics() {
		StringBuilder statistics = new StringBuilder();
		
		statistics.append("Found items: " + itemsDone + "\n");

		statistics.append("Imported items: " + importedItems + "\n");
		
		statistics.append("Not imported items (Number of Errors): " + notImportedItems.size() + "\n");
		if (notImportedItems.size() > 0) {
			List<String> errorList = new ArrayList<String>();
			for (int i = 0; i < Math.min(10, notImportedItems.size()); i++) {
				errorList.add(Integer.toString(notImportedItems.get(i) + 1));
			}
			if (notImportedItems.size() > 10) {
				errorList.add("...");
			}
			statistics.append("Not imported items indices: " + Utilities.join(errorList, ", ") + "\n");
			if (errorneousDataFile != null) {
				statistics.append("Errorneous data logged in file: " + errorneousDataFile + "\n");
			}
		}
		
		statistics.append("Imported data amount: " + Utilities.getHumanReadableNumber(importedDataAmount, "Byte") + "\n");

		if (importMode == ImportMode.CLEARINSERT) {
			statistics.append("Deleted items: " + deletedItems + "\n");
		}

		if (ignoredDuplicates > 0) {
			statistics.append("Ignored duplicate items: " + ignoredDuplicates + "\n");
		}

		if (deletedItems > 0) {
			statistics.append("Items for update found in import data: " + itemsForUpdateInImportData + "\n");
		}

		if (importMode == ImportMode.UPDATE || importMode == ImportMode.UPSERT) {
			statistics.append("Updated items in db: " + updatedItems + "\n");
		}

		if (importMode == ImportMode.CLEARINSERT || importMode == ImportMode.INSERT || importMode == ImportMode.UPSERT) {
			statistics.append("Inserted items: " + insertedItems + "\n");
		}
		
		return statistics.toString();
	}

	private void updateExistingItems(Connection connection, String fromTableName, String intoTableName, List<String> updateColumns, List<String> keyColumns, String itemIndexColumn, boolean updateWithNullValues, String additionalUpdateValues) throws Exception {
		Statement statement = null;
		try {
			String additionalUpdateValuesSql = "";
			if (Utilities.isNotBlank(additionalUpdateValues)) {
				for (String line : Utilities.splitAndTrimListQuoted(additionalUpdateValues, '\n', '\r', ';')) {
					String columnName = line.substring(0, line.indexOf("=")).trim();
					String columnvalue = line.substring(line.indexOf("=") + 1).trim();
					additionalUpdateValuesSql += columnName + " = " + columnvalue + ", ";
				}
			}
			
			statement = connection.createStatement();
			
			if (updateWithNullValues) {
				String updateSetPart = "";
				for (String updateColumn : updateColumns) {
					if (updateSetPart.length() > 0) {
						updateSetPart += ", ";
					}
					updateSetPart += updateColumn + " = (SELECT " + updateColumn + " FROM " + fromTableName + " WHERE " + itemIndexColumn + " ="
						+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + fromTableName + " c WHERE " + getWherePart(keyColumns, intoTableName, "c") + "))";
				}
				String updateAllAtOnce = "UPDATE " + intoTableName + " SET " + additionalUpdateValuesSql + updateSetPart
					+ " WHERE EXISTS (SELECT 1 FROM " + fromTableName + " b WHERE " + getWherePart(keyColumns, intoTableName, "b") + ")";
				statement.executeUpdate(updateAllAtOnce);
			} else {
				for (String updateColumn : updateColumns) {
					String updateSingleColumn = "UPDATE " + intoTableName
						+ " SET " + additionalUpdateValuesSql + updateColumn + " = (SELECT " + updateColumn + " FROM " + fromTableName + " WHERE " + itemIndexColumn + " ="
							+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + fromTableName + " c WHERE " + updateColumn + " IS NOT NULL AND " + getWherePart(keyColumns, intoTableName, "c") + "))"
						+ " WHERE EXISTS (SELECT 1 FROM " + fromTableName + " b WHERE " + updateColumn + " IS NOT NULL AND " + getWherePart(keyColumns, intoTableName, "b") + ")";
					statement.executeUpdate(updateSingleColumn);
				}
			}
			connection.commit();
		} catch (Exception e) {
			connection.rollback();
			throw new Exception("Cannot update: " + e.getMessage(), e);
		} finally {
			Utilities.closeQuietly(statement);
		}
	}

	private int insertNotExistingItems(Connection connection, String fromTableName, String intoTableName, List<String> insertColumns, List<String> keyColumns, String additionalInsertValues) throws Exception {
		Statement statement = null;
		try {
			String additionalInsertValuesSqlColumns = "";
			String additionalInsertValuesSqlValues = "";
			if (Utilities.isNotBlank(additionalInsertValues)) {
				for (String line : Utilities.splitAndTrimListQuoted(additionalInsertValues, '\n', '\r', ';')) {
					String columnName = line.substring(0, line.indexOf("=")).trim();
					String columnvalue = line.substring(line.indexOf("=") + 1).trim();
					additionalInsertValuesSqlColumns += columnName + ", ";
					additionalInsertValuesSqlValues += columnvalue + ", ";
				}
			}
			
			statement = connection.createStatement();
			String deleteDuplicates = "INSERT INTO " + intoTableName + " (" + additionalInsertValuesSqlColumns + Utilities.join(insertColumns, ", ") + ") SELECT " + additionalInsertValuesSqlValues + Utilities.join(insertColumns, ", ") + " FROM " + fromTableName + " a"
				+ " WHERE NOT EXISTS (SELECT 1 FROM " + intoTableName + " b WHERE " + getWherePart(keyColumns, "a", "b") + ")";
			int numberOfInserts = statement.executeUpdate(deleteDuplicates);
			connection.commit();
			return numberOfInserts;
		} catch (Exception e) {
			connection.rollback();
			throw new Exception("Cannot insert: " + e.getMessage(), e);
		} finally {
			Utilities.closeQuietly(statement);
		}
	}

	private int deleteTableCrossDuplicates(Connection connection, String keepInTableName, String deleteInTableName, List<String> keyColumns) throws Exception {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			String deleteDuplicates = "DELETE FROM " + deleteInTableName + " WHERE " + Utilities.join(keyColumns, ", ") + " IN (SELECT " + Utilities.join(keyColumns, ", ") + " FROM " + keepInTableName + ")";
			int numberOfDeletedDuplicates = statement.executeUpdate(deleteDuplicates);
			connection.commit();
			return numberOfDeletedDuplicates;
		} catch (Exception e) {
			connection.rollback();
			throw new Exception("Cannot deleteTableCrossDuplicates: " + e.getMessage(), e);
		} finally {
			Utilities.closeQuietly(statement);
		}
	}

	private void insertIntoTable(Connection connection, String tableName, Map<String, DbColumnType> dbColumns, String itemIndexColumn, String additionalInsertValues) throws SQLException, Exception {
		PreparedStatement preparedStatement = null;
		List<Closeable> itemsToCloseAfterwards = new ArrayList<Closeable>();
		try {
			String additionalInsertValuesSqlColumns = "";
			String additionalInsertValuesSqlValues = "";
			if (Utilities.isNotBlank(additionalInsertValues)) {
				for (String line : Utilities.splitAndTrimListQuoted(additionalInsertValues, '\n', '\r', ';')) {
					String columnName = line.substring(0, line.indexOf("=")).trim();
					String columnvalue = line.substring(line.indexOf("=") + 1).trim();
					additionalInsertValuesSqlColumns += columnName + ", ";
					additionalInsertValuesSqlValues += columnvalue + ", ";
				}
			}
			
			String statementString;
			if (Utilities.isBlank(itemIndexColumn)) {
				statementString = "INSERT INTO " + tableName + " (" + additionalInsertValuesSqlColumns + Utilities.join(dbTableColumnsListToInsert, ", ") + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ")";
			} else {
				statementString = "INSERT INTO " + tableName + " (" + additionalInsertValuesSqlColumns + Utilities.join(dbTableColumnsListToInsert, ", ") + ", " + itemIndexColumn + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ", ?)";
			}
			
			preparedStatement = connection.prepareStatement(statementString);
			
			openReader();
			
			int batchBlockSize = 1000;
			boolean hasUnexecutedData = false;
			
			Map<String, Object> itemData;
			while((itemData = getNextItemData()) != null) {
				try {
					int i = 1;
					for (String dbColumnToInsert : dbTableColumnsListToInsert) {
						SimpleDataType simpleDataType = dbColumns.get(dbColumnToInsert).getSimpleDataType();
						Object dataValue = itemData.get(mapping.get(dbColumnToInsert).getFirst());
						String formatInfo = mapping.get(dbColumnToInsert).getSecond();
						
						itemsToCloseAfterwards.add(setParameter(preparedStatement, i++, simpleDataType, dataValue, formatInfo));
					}
					
					if (Utilities.isNotBlank(itemIndexColumn)) {
						// Add additional integer value to identify data item index
						setParameter(preparedStatement, i++, SimpleDataType.Integer, itemsDone + 1, null);
					}
					
					preparedStatement.addBatch();
					
					importedItems++;
					showProgress();
				} catch (Exception e) {
					notImportedItems.add((int) itemsDone + 1);
					if (commitOnFullSuccessOnly) {
						connection.rollback();
						throw new DbCsvImportException(e.getClass().getSimpleName() + " error in item index " + (itemsDone + 1) + ": " + e.getMessage(), e);
					} else {
						if (dbVendor == DbVendor.SQLite) {
							// SQLite seems to not react on preparedStatement.clearParameters() calls
							for (int i = 1; i <= dbTableColumnsListToInsert.size(); i++) {
								preparedStatement.setObject(i, null);
							}
						} else {
							preparedStatement.clearParameters();
						}
					}
				}
				itemsDone++;
				
				if (importedItems > 0) {
					if (importedItems % batchBlockSize == 0) {
						int[] results = preparedStatement.executeBatch();
						for (Closeable itemToClose : itemsToCloseAfterwards) {
							Utilities.closeQuietly(itemToClose);
						}
						itemsToCloseAfterwards.clear();
						for (int i = 0; i < results.length; i++) {
							if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
								notImportedItems.add((int) (itemsDone - batchBlockSize) + i);
							}
						}
						if (!commitOnFullSuccessOnly) {
							connection.commit();
							if (dbVendor == DbVendor.Firebird) {
								preparedStatement.close();
								preparedStatement = connection.prepareStatement(statementString);
							}
						}
						hasUnexecutedData = false;
						showProgress();
					} else {
						hasUnexecutedData = true;
					}
				}
			}
			
			if (hasUnexecutedData) {
				int[] results = preparedStatement.executeBatch();
				for (Closeable itemToClose : itemsToCloseAfterwards) {
					Utilities.closeQuietly(itemToClose);
				}
				itemsToCloseAfterwards.clear();
				for (int i = 0; i < results.length; i++) {
					if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
						notImportedItems.add((int) (itemsDone - (itemsDone % batchBlockSize)) + i);
					}
				}
				if (!commitOnFullSuccessOnly) {
					connection.commit();
				}
			}
			
			if (commitOnFullSuccessOnly) {
				if (notImportedItems.size() == 0) {
					connection.commit();
				} else {
					connection.rollback();
				}
			}
		} catch (Exception e) {
			connection.rollback();
			throw e;
		} finally {
			for (Closeable itemToClose : itemsToCloseAfterwards) {
				Utilities.closeQuietly(itemToClose);
			}
			itemsToCloseAfterwards.clear();
			Utilities.closeQuietly(preparedStatement);
		}
	}

	private void markTrailingDuplicates(Connection connection, String tempTableName, List<String> keyColumns, boolean updateWithNullValues, String itemIndexColumn, String duplicateIndexColumn) throws Exception {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			
			String setDuplicateReferences = "UPDATE " + tempTableName + " SET " + duplicateIndexColumn + " = (SELECT subselect." + itemIndexColumn + " FROM"
				+ " (SELECT " + Utilities.join(keyColumns, ", ") + ", MIN(" + itemIndexColumn + ") AS " + itemIndexColumn + " FROM " + tempTableName + " GROUP BY " + Utilities.join(keyColumns, ", ") + ") subselect"
				+ " WHERE " + getWherePart(keyColumns, "subselect", tempTableName) + ")";
			statement.executeUpdate(setDuplicateReferences);
			connection.commit();
		} catch (Exception e) {
			connection.rollback();
			throw new Exception("Cannot markTrailingDuplicates: " + e.getMessage(), e);
		} finally {
			Utilities.closeQuietly(statement);
		}
	}

	private int removeDuplicates(Connection connection, String tempTableName, List<String> keyColumns, boolean updateWithNullValues, String lineIndexColumn, String duplicateIndexColumn) throws Exception {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			int numberOfDeletedDuplicates = statement.executeUpdate("DELETE FROM " + tempTableName + " WHERE " + duplicateIndexColumn + " != " + lineIndexColumn);
			connection.commit();
			return numberOfDeletedDuplicates;
		} catch (Exception e) {
			connection.rollback();
			throw new Exception("Cannot removeTrailingDuplicates: " + e.getMessage(), e);
		} finally {
			Utilities.closeQuietly(statement);
		}
	}
	
	private static String getWherePart(List<String> columnNames, String table1, String table2) {
		StringBuilder returnValue = new StringBuilder();
		for (String columnName : columnNames) {
			if (returnValue.length() > 0) {
				returnValue.append(", ");
			}
			if (Utilities.isNotBlank(table1)) {
				returnValue.append(table1);
				returnValue.append(".");
			}
			returnValue.append(columnName);
			
			returnValue.append(" = ");

			if (Utilities.isNotBlank(table2)) {
				returnValue.append(table2);
				returnValue.append(".");
			}
			returnValue.append(columnName);
		}
		return returnValue.toString();
	}
	
	private Closeable setParameter(PreparedStatement preparedStatement, int columnIndex, SimpleDataType simpleDataType, Object dataValue, String formatInfo) throws Exception {
		Closeable itemToCloseAfterwards = null;
		if (dataValue instanceof String && Utilities.isNotBlank(formatInfo)) {
			String valueString = (String) dataValue;
			
			if (".".equals(formatInfo)) {
				valueString = valueString.replace(",", "");
				if (valueString.contains(".")) {
					preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
				} else {
					preparedStatement.setInt(columnIndex, Integer.parseInt(valueString));
				}
			} else if (",".equals(formatInfo)) {
				valueString = valueString.replace(".", "").replace(",", ".");
				if (valueString.contains(".")) {
					preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
				} else {
					preparedStatement.setInt(columnIndex, Integer.parseInt(valueString));
				}
			} else if ("file".equalsIgnoreCase(formatInfo)) {
				if (!new File(valueString).exists()) {
					throw new Exception("File does not exist: " + valueString);
				} else if (simpleDataType == SimpleDataType.Blob) {
					itemToCloseAfterwards = new FileInputStream(valueString);
					preparedStatement.setBinaryStream(columnIndex, (FileInputStream) itemToCloseAfterwards);
					importedDataAmount += new File(valueString).length();
				} else {
					if (dbVendor == DbVendor.SQLite || dbVendor == DbVendor.PostgreSQL) {
						// PostgreSQL and SQLite do not read the stream
						byte[] data = Utilities.readFileToByteArray(new File(valueString));
						preparedStatement.setString(columnIndex, new String(data, encoding));
					} else {
						itemToCloseAfterwards = new InputStreamReader(new FileInputStream(valueString), encoding);
						preparedStatement.setCharacterStream(columnIndex, (InputStreamReader) itemToCloseAfterwards);
					}
					importedDataAmount += new File(valueString).length();
				}
			} else {
				preparedStatement.setTimestamp(columnIndex, new java.sql.Timestamp(new SimpleDateFormat(formatInfo).parse(valueString).getTime()));
			}
		} else {
			if (dataValue instanceof String) {
				if (simpleDataType == SimpleDataType.Blob) {
					preparedStatement.setBytes(columnIndex, Utilities.decodeBase64((String) dataValue));
				} else if (simpleDataType == SimpleDataType.Double) {
					String valueString = ((String) dataValue).trim();
					if (valueString.contains(".")) {
						preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
					} else {
						preparedStatement.setInt(columnIndex, Integer.parseInt(valueString));
					}
				} else if (simpleDataType == SimpleDataType.Integer) {
					String valueString = ((String) dataValue).trim();
					if (valueString.contains(".")) {
						preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
					} else {
						preparedStatement.setInt(columnIndex, Integer.parseInt(valueString));
					}
				} else if (simpleDataType == SimpleDataType.String || simpleDataType == SimpleDataType.Clob) {
					preparedStatement.setString(columnIndex, (String) dataValue);
				} else if (simpleDataType == SimpleDataType.Date) {
					throw new Exception("Date field to insert without mapping date format");
				} else {
					throw new Exception("Unknown data type field to insert without mapping format");
				}
			} else {
				preparedStatement.setObject(columnIndex, dataValue);
			}
		}
		return itemToCloseAfterwards;
	}

	private static void logToFile(OutputStream logOutputStream, String message) throws Exception {
		if (logOutputStream != null) {
			logOutputStream.write((message.trim() + "\n").getBytes("UTF-8"));
		}
	}

	public int getDeletedItems() {
		return deletedItems;
	}

	public int getUpdatedItems() {
		return updatedItems;
	}

	public int getImportedItems() {
		return importedItems;
	}

	public List<Integer> getNotImportedItems() {
		return notImportedItems;
	}
	
	public long getImportedDataAmount() {
		return importedDataAmount;
	}
	
	public int getIgnoredDuplicates() {
		return ignoredDuplicates;
	}

	public int getInsertedItems() {
		return insertedItems;
	}
	
	public int getItemsForUpdateInImportData() {
		return itemsForUpdateInImportData;
	}
	
	public static String convertMappingToString(Map<String, Tuple<String, String>> mapping) {
		StringBuilder returnValue = new StringBuilder();
		
		if (mapping != null) {
			for (Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
				returnValue.append(entry.getKey() + "=\"" + entry.getValue().getFirst() + "\"");
				if (Utilities.isNotBlank(entry.getValue().getSecond())) {
					returnValue.append(" " + entry.getValue().getSecond());
				}
				returnValue.append("\n");
			}
		}
		
		return returnValue.toString().trim();
	}

	public abstract String getConfigurationLogString();
	
	public abstract List<String> getAvailableDataPropertyNames() throws Exception;

	protected abstract int getItemsAmountToImport() throws Exception;

	protected abstract void openReader() throws Exception;
	
	protected abstract Map<String, Object> getNextItemData() throws Exception;

	protected abstract void closeReader() throws Exception;

	protected abstract Map<String, DbColumnType> scanDataPropertyTypes() throws Exception;
	
	protected abstract File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception;
}
