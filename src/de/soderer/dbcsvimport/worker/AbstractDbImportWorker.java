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
import java.util.Set;
import java.util.regex.Pattern;

import de.soderer.dbcsvimport.DbCsvImportDefinition.DataType;
import de.soderer.dbcsvimport.DbCsvImportDefinition.DuplicateMode;
import de.soderer.dbcsvimport.DbCsvImportDefinition.ImportMode;
import de.soderer.dbcsvimport.DbCsvImportException;
import de.soderer.dbcsvimport.DbCsvImportMappingDialog;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbColumnType;
import de.soderer.utilities.DbColumnType.SimpleDataType;
import de.soderer.utilities.DbNotExistsException;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.NetworkUtilities;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;
import de.soderer.utilities.WorkerSimple;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.collection.CaseInsensitiveSet;

public abstract class AbstractDbImportWorker extends WorkerSimple<Boolean> implements Closeable {
	// Mandatory parameters
	protected DbUtilities.DbVendor dbVendor = null;
	protected ImportMode importMode = ImportMode.INSERT;
	protected DuplicateMode duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;
	protected List<String> keyColumns = null;
	protected List<String> keyColumnsWithFunctions = null;
	protected String hostname;
	protected String dbName;
	protected String username;
	protected String password;
	protected String tableName;
	protected boolean createTableIfNotExists = false;
	protected boolean tableWasCreated = false;
	protected boolean isInlineData;
	protected String importFilePathOrData;
	protected boolean commitOnFullSuccessOnly = true;
	protected boolean createNewIndexIfNeeded = true;
	private String newIndexName = null;
	private DataType dataType;
	
	protected List<String> dbTableColumnsListToInsert = null;
	protected Map<String, Tuple<String, String>> mapping = null;
	
	// Default optional parameters
	protected boolean log = false;
	protected String encoding = "UTF-8";
	protected boolean updateWithNullValues = true;
	
	protected int validItems = 0;
	protected int duplicatesItems = 0;
	protected List<Integer> invalidItems = new ArrayList<Integer>();
	protected long importedDataAmount = 0;
	protected int deletedItems = 0;
	protected int insertedItems = 0;
	protected int updatedItems = 0;
	protected int countItems = 0;
	protected int deletedDuplicatesInDB = 0;

	protected boolean analyseDataOnly = false;
	protected List<String> availableDataPropertyNames = null;

	protected String additionalInsertValues = null;
	protected String additionalUpdateValues = null;
	
	protected boolean logErrorneousData = false;
	protected File errorneousDataFile = null;

	public AbstractDbImportWorker(WorkerParentSimple parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String tableName, boolean isInlineData, String importFilePathOrData, DataType dataType) throws Exception {
		super(parent);
		
		this.dbVendor = dbVendor;
		this.hostname = hostname;
		this.dbName = dbName;
		this.username = username;
		this.password = password;
		this.tableName = tableName;
		this.isInlineData = isInlineData;
		this.importFilePathOrData = importFilePathOrData;
		this.dataType = dataType;
	}

	public void setAnalyseDataOnly(boolean analyseDataOnly) {
		this.analyseDataOnly = analyseDataOnly;
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
			dbTableColumnsListToInsert = new ArrayList<String>();
			for (String dbColumn : mapping.keySet()) {
				dbTableColumnsListToInsert.add(DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumn));
			}
		} else {
			mapping = null;
		}
	}

	public Map<String, Tuple<String, String>> getMapping() throws IOException, Exception {
		if (mapping == null && dataType != DataType.SQL) {
			mapping = new HashMap<String, Tuple<String, String>>();
			for (String propertyName : getAvailableDataPropertyNames()) {
				mapping.put(propertyName.toLowerCase(), new Tuple<String, String>(propertyName, ""));
			}
			dbTableColumnsListToInsert = new ArrayList<String>();
			for (String dbColumn : mapping.keySet()) {
				dbTableColumnsListToInsert.add(DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumn));
			}
		}
		return mapping;
	}

	private void checkMapping(Map<String, DbColumnType> dbColumns) throws Exception, DbCsvImportException {
		List<String> dataPropertyNames = getAvailableDataPropertyNames();
		if (mapping != null) {
			for (String dbColumnToInsert : dbTableColumnsListToInsert) {
				dbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumnToInsert);
				if (!dbColumns.containsKey(dbColumnToInsert)) {
					throw new DbCsvImportException("DB table does not contain mapped column: " + dbColumnToInsert);
				}
			}
			
			Set<String> mappedDbColumns = new CaseInsensitiveSet();
			for (Entry<String, Tuple<String, String>> mappingEntry : mapping.entrySet()) {
				if (Utilities.isNotBlank(mappingEntry.getKey()) && !mappedDbColumns.add(mappingEntry.getKey())) {
					throw new DbCsvImportException("Mapping contains db column multiple times: " + mappingEntry.getKey());
				} else if (!dataPropertyNames.contains(mappingEntry.getValue().getFirst())) {
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
		
		if (keyColumns != null && keyColumns.size() > 0) {
			for (String keyColumn : keyColumns) {
				boolean isIncluded = false;
				for (Entry<String, Tuple<String, String>> entry : mapping.entrySet()) {
					if (DbUtilities.unescapeVendorReservedNames(dbVendor, keyColumn).equals(DbUtilities.unescapeVendorReservedNames(dbVendor, entry.getKey()))) {
						isIncluded = true;
						break;
					}
				}
				if (!isIncluded) {
					throw new DbCsvImportException("Mapping doesn't include the defined keycolumn: " + keyColumn);
				}
			}
		}
	}

	public void setImportMode(ImportMode importMode) {
		this.importMode = importMode;
	}

	public void setDuplicateMode(DuplicateMode duplicateMode) {
		this.duplicateMode = duplicateMode;
	}

	public void setKeycolumns(List<String> keyColumnList) {
		if (Utilities.isNotEmpty(keyColumnList)) {
			Map<String, String> columnFunctions = new CaseInsensitiveMap<String>();
			
			// Remove the optional functions from keycolumns
			for (String keyColumn : keyColumnList) {
				keyColumn = keyColumn.trim();
				if (Utilities.isNotEmpty(keyColumn)) {
					String function = null;
					if (keyColumn.contains("(") && keyColumn.endsWith(")")) {
						function = keyColumn.substring(0, keyColumn.indexOf("(")).trim().toUpperCase();
						keyColumn = keyColumn.substring(keyColumn.indexOf("(") + 1, keyColumn.length() - 1).trim();
					}
					
					columnFunctions.put(keyColumn, function);
				}
			}
			
			if (columnFunctions.size() > 0) {
				columnFunctions = Utilities.sortMap(columnFunctions);
				keyColumns = new ArrayList<String>();
				keyColumnsWithFunctions = new ArrayList<String>();
				for (String keyColumn : columnFunctions.keySet()) {
					keyColumns.add(keyColumn);
					if (columnFunctions.get(keyColumn) != null) {
						keyColumnsWithFunctions.add(columnFunctions.get(keyColumn).toUpperCase() + "(" + keyColumn + ")");
					} else {
						keyColumnsWithFunctions.add(keyColumn);
					}
				}
			}
		}
	}

	public void setCompleteCommit(boolean commitOnFullSuccessOnly) {
		this.commitOnFullSuccessOnly = commitOnFullSuccessOnly;
	}

	public void setCreateNewIndexIfNeeded(boolean createNewIndexIfNeeded) {
		this.createNewIndexIfNeeded = createNewIndexIfNeeded;
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
		showUnlimitedProgress();
		
		if (analyseDataOnly) {
			try {
				if (!isInlineData) {
					if (!new File(importFilePathOrData).exists()) {
						throw new DbCsvImportException("Import file does not exist: " + importFilePathOrData);
					} else if (new File(importFilePathOrData).isDirectory()) {
						throw new DbCsvImportException("Import path is a directory: " + importFilePathOrData);
					}
				}
				
				parent.changeTitle(LangResources.get("analyseData"));
				
				availableDataPropertyNames = getAvailableDataPropertyNames();
			} finally {
				close();
			}
		} else {
			OutputStream logOutputStream = null;
			Connection connection = null;
			boolean previousAutoCommit = false;
			String tempTableName = null;
			try {
				if (!isInlineData) {
					if (!new File(importFilePathOrData).exists()) {
						throw new DbCsvImportException("Import file does not exist: " + importFilePathOrData);
					} else if (new File(importFilePathOrData).isDirectory()) {
						throw new DbCsvImportException("Import path is a directory: " + importFilePathOrData);
					}
				}
	
				if (dbVendor == DbVendor.Derby || (dbVendor == DbVendor.HSQL && Utilities.isBlank(hostname)) || dbVendor == DbVendor.SQLite) {
					try {
						connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password.toCharArray()), true);
					} catch (DbNotExistsException e) {
						connection = DbUtilities.createNewDatabase(dbVendor, dbName);
					}
				} else {
					connection = DbUtilities.createConnection(dbVendor, hostname, dbName, username, (password == null ? null : password.toCharArray()));
				}
				
				previousAutoCommit = connection.getAutoCommit();
				connection.setAutoCommit(false);
				
				validItems = 0;
				invalidItems = new ArrayList<Integer>();
				
				if (log && !isInlineData) {
					logOutputStream = new FileOutputStream(new File(importFilePathOrData + "." + new SimpleDateFormat(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName).format(getStartTime()) + ".import.log"));
					
					logToFile(logOutputStream, getConfigurationLogString());
				}
	
				logToFile(logOutputStream, "Start: " + DateFormat.getDateTimeInstance().format(getStartTime()));
				
				createTableIfNeeded(connection, tableName, keyColumns);

				Map<String, DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tableName);
				checkMapping(dbColumns);
				
				if (dbTableColumnsListToInsert.size() == 0) {
					throw new DbCsvImportException("Invalid empty mapping");
				}

				if (!DbUtilities.checkTableAndColumnsExist(connection, tableName, keyColumns == null ? null : keyColumns.toArray(new String[0]))) {
					throw new DbCsvImportException("Some keycolumn is not included in table");
				}
				
				if (importMode == ImportMode.CLEARINSERT) {
					deletedItems = DbUtilities.clearTable(connection, tableName);
				}

				parent.changeTitle(LangResources.get("readData"));
				itemsToDo = getItemsAmountToImport();
				if (!log) {
					logToFile(logOutputStream, "Items to import: " + itemsToDo);
				}
				showProgress(true);
				
				if ((importMode == ImportMode.CLEARINSERT || importMode == ImportMode.INSERT) && Utilities.isEmpty(keyColumns)) {
					// Just import in the destination table
					insertIntoTable(connection, tableName, dbColumns, null, additionalInsertValues, getMapping());
					insertedItems = validItems;
				} else {
					// Make table entries unique
					if (duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
						deletedDuplicatesInDB = DbUtilities.dropDuplicates(connection, tableName, keyColumnsWithFunctions);
					} else if (duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN) {
						deletedDuplicatesInDB = DbUtilities.joinDuplicates(connection, tableName, keyColumnsWithFunctions, updateWithNullValues);
					}
					
					// Create temp table
					String dateSuffix = new SimpleDateFormat(DateUtilities.YYYYMMDDHHMMSS).format(getStartTime());
					tempTableName = "tmp_" + dateSuffix;
					int i = 0;
					while (DbUtilities.checkTableExist(connection, tempTableName) && i < 10) {
						Thread.sleep(1000);
						i++;
						dateSuffix = new SimpleDateFormat(DateUtilities.YYYYMMDDHHMMSS).format(new Date());
						tempTableName = "tmp_" + dateSuffix;
					}
					if (i >= 10) {
						tempTableName = null;
						throw new Exception("Cannot create temp table");
					}
					DbUtilities.copyTableStructure(connection, tableName, dbTableColumnsListToInsert, keyColumns, tempTableName);
					if (Utilities.isNotEmpty(keyColumns)) {
						Boolean hasIndexedKeyColumns = DbUtilities.checkForIndex(connection, tableName, keyColumns);
						if ((hasIndexedKeyColumns == null || !hasIndexedKeyColumns) && createNewIndexIfNeeded) {
							try {
								newIndexName = DbUtilities.createIndex(connection, tableName, keyColumns);
							} catch (Exception e) {
								System.err.println("Cannot create index for table '" + tableName + "' on columns '" + Utilities.join(keyColumns, ", ") + "': " + e.getMessage());
							}
						}
					}
					String tempItemIndexColumn = DbUtilities.addIndexedIntegerColumn(connection, tempTableName, "import_item");
					connection.commit();
					
					// Insert in temp table
					insertIntoTable(connection, tempTableName, dbColumns, tempItemIndexColumn, null, getMapping());
					
					itemsToDo = 4;
					itemsDone = 0;
					showProgress(true);
					parent.changeTitle(LangResources.get("dropDuplicates"));

					// Handle duplicates in import data
					if (duplicateMode == DuplicateMode.NO_CHECK) {
						// Do not check for duplicates
					} else if (duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_ALL_DROP || duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
						duplicatesItems = DbUtilities.dropDuplicates(connection, tempTableName, keyColumns);
					} else if (duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN || duplicateMode == DuplicateMode.UPDATE_ALL_JOIN || duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN) {
						duplicatesItems = DbUtilities.joinDuplicates(connection, tempTableName, keyColumns, updateWithNullValues);
					} else {
						throw new Exception("Invalid duplicate mode");
					}

					itemsDone = 1;
					showProgress(true);

					if (importMode == ImportMode.CLEARINSERT) {
						parent.changeTitle(LangResources.get("insertData"));
						
						insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

						itemsDone = 2;
						showProgress(true);
					} else if (importMode == ImportMode.INSERT) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							parent.changeTitle(LangResources.get("insertData"));
							
							// Insert all entries
							insertedItems = DbUtilities.insertAllItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, additionalInsertValues);

							itemsDone = 2;
							showProgress(true);
						} else {
							parent.changeTitle(LangResources.get("dropDuplicates"));
							
							// Insert only not existing entries
							duplicatesItems += DbUtilities.dropDuplicatesCrossTable(connection, tableName, tempTableName, keyColumnsWithFunctions);

							itemsDone = 2;
							showProgress(true);
							parent.changeTitle(LangResources.get("insertData"));
							
							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

							itemsDone = 3;
							showProgress(true);
						}
					} else if (importMode == ImportMode.UPDATE) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							// Do nothing
						} else if (DbUtilities.detectDuplicates(connection, tableName, keyColumnsWithFunctions) > 0 && (duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN)) {
							parent.changeTitle(LangResources.get("updateData"));
							
							// Update only the first occurrence
							updatedItems = DbUtilities.updateFirstExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);
							
							itemsDone = 2;
							showProgress(true);
						} else {
							parent.changeTitle(LangResources.get("updateData"));
							
							// Update destination table
							updatedItems = DbUtilities.updateAllExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);

							itemsDone = 2;
							showProgress(true);
						}
					} else if (importMode == ImportMode.UPSERT) {
						if (duplicateMode == DuplicateMode.NO_CHECK || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_DROP || duplicateMode == DuplicateMode.CKECK_SOURCE_ONLY_JOIN) {
							parent.changeTitle(LangResources.get("insertData"));
							
							// Insert all entries
							insertedItems = DbUtilities.insertAllItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, additionalInsertValues);

							itemsDone = 2;
							showProgress(true);
						} else if (DbUtilities.detectDuplicates(connection, tableName, keyColumnsWithFunctions) > 0 && (duplicateMode == DuplicateMode.UPDATE_FIRST_DROP || duplicateMode == DuplicateMode.UPDATE_FIRST_JOIN)) {
							parent.changeTitle(LangResources.get("updateData"));
							
							// Update only the first occurrence
							updatedItems = DbUtilities.updateFirstExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);

							itemsDone = 2;
							showProgress(true);
							parent.changeTitle(LangResources.get("insertData"));
							
							// Insert into destination table
							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

							itemsDone = 3;
							showProgress(true);
						} else {
							parent.changeTitle(LangResources.get("updateData"));
							
							// Update destination table
							updatedItems = DbUtilities.updateAllExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumns, tempItemIndexColumn, updateWithNullValues, additionalUpdateValues);

							itemsDone = 2;
							showProgress(true);
							parent.changeTitle(LangResources.get("insertData"));

							// Insert into destination table
							insertedItems = DbUtilities.insertNotExistingItems(connection, tempTableName, tableName, dbTableColumnsListToInsert, keyColumnsWithFunctions, additionalInsertValues);

							itemsDone = 3;
							showProgress(true);
						}
					} else {
						throw new Exception("Invalid import mode");
					}
				}
				connection.commit();
				
				parent.changeTitle(LangResources.get("collectResult"));
				
				countItems = DbUtilities.getTableEntriesCount(connection, tableName);
				
				itemsDone = 4;
				showProgress(true);
				
				if (logErrorneousData & invalidItems.size() > 0) {
					errorneousDataFile = filterDataItems(invalidItems, new SimpleDateFormat(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName).format(getStartTime()) + ".errors");
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
				
				// Drop temp table
				DbUtilities.dropTableIfExists(connection, tempTableName);
				
				if (connection != null) {
					connection.rollback();
					connection.setAutoCommit(previousAutoCommit);
					connection.close();
					connection = null;
					if (dbVendor == DbVendor.Derby) {
						DbUtilities.shutDownDerbyDb(dbName);
					}
				}
			}
		}
		
		return !cancel;
	}

	private void createTableIfNeeded(Connection connection, String tableName, List<String> keyColumns) throws Exception, DbCsvImportException, SQLException {
		if (!DbUtilities.checkTableExist(connection, tableName)) {
			if (createTableIfNotExists) {
				Map<String, DbColumnType> importDataTypes = scanDataPropertyTypes();
				Map<String, DbColumnType> dbDataTypes = new HashMap<String, DbColumnType>();
				for (Entry<String, DbColumnType> importDataType : importDataTypes.entrySet()) {
					if (getMapping() != null) {
						for (Entry<String,Tuple<String,String>> mappingEntry : getMapping().entrySet()) {
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
	}

	public String getResultStatistics() {
		StringBuilder statistics = new StringBuilder();
		
		statistics.append("Found items: " + dataItemsDone + "\n");

		statistics.append("Valid items: " + validItems + "\n");
		
		statistics.append("Invalid items: " + invalidItems.size() + "\n");
		if (invalidItems.size() > 0) {
			List<String> errorList = new ArrayList<String>();
			for (int i = 0; i < Math.min(10, invalidItems.size()); i++) {
				errorList.add(Integer.toString(invalidItems.get(i)));
			}
			if (invalidItems.size() > 10) {
				errorList.add("...");
			}
			statistics.append("Indices of invalid items: " + Utilities.join(errorList, ", ") + "\n");
			if (errorneousDataFile != null) {
				statistics.append("Errorneous data logged in file: " + errorneousDataFile + "\n");
			}
		}
		
		if (duplicatesItems > 0) {
			statistics.append("Duplicate items: " + duplicatesItems + "\n");
		}
		
		statistics.append("Imported data amount: " + Utilities.getHumanReadableNumber(importedDataAmount, "Byte", false) + "\n");

		if (importMode == ImportMode.CLEARINSERT) {
			statistics.append("Deleted items from db: " + deletedItems + "\n");
		}
		
		if (duplicateMode == DuplicateMode.MAKE_UNIQUE_JOIN || duplicateMode == DuplicateMode.MAKE_UNIQUE_DROP) {
			statistics.append("Deleted duplicate items in db: " + deletedDuplicatesInDB + "\n");
		}

		if (importMode == ImportMode.CLEARINSERT || importMode == ImportMode.INSERT || importMode == ImportMode.UPSERT) {
			statistics.append("Inserted items: " + insertedItems + "\n");
		}

		if (importMode == ImportMode.UPDATE || importMode == ImportMode.UPSERT) {
			statistics.append("Updated items: " + updatedItems + "\n");
		}

		if (newIndexName != null) {
			statistics.append("Newly created index: " + newIndexName + "\n");
		}

		statistics.append("Count items after import: " + countItems + "\n");
		
		return statistics.toString();
	}

	private void insertIntoTable(Connection connection, String tableName, Map<String, DbColumnType> dbColumns, String itemIndexColumn, String additionalInsertValues, Map<String, Tuple<String, String>> mapping) throws SQLException, Exception {
		List<Closeable> itemsToCloseAfterwards = new ArrayList<Closeable>();
		
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
			statementString = "INSERT INTO " + tableName + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbVendor, dbTableColumnsListToInsert) + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ")";
		} else {
			statementString = "INSERT INTO " + tableName + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbVendor, dbTableColumnsListToInsert) + ", " + itemIndexColumn + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbTableColumnsListToInsert.size(), ", ") + ", ?)";
		}
		
		PreparedStatement preparedStatement = null;
		try {			
			preparedStatement = connection.prepareStatement(statementString);
			
			int batchBlockSize = 1000;
			boolean hasUnexecutedData = false;
			
			Map<String, Object> itemData;
			while((itemData = getNextItemData()) != null) {
				try {
					int i = 1;
					for (String dbColumnToInsert : dbTableColumnsListToInsert) {
						SimpleDataType simpleDataType = dbColumns.get(dbColumnToInsert).getSimpleDataType();
						String unescapedDbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbVendor, dbColumnToInsert);
						Object dataValue = itemData.get(mapping.get(unescapedDbColumnToInsert).getFirst());
						String formatInfo = mapping.get(unescapedDbColumnToInsert).getSecond();
						
						itemsToCloseAfterwards.add(setParameter(preparedStatement, i++, simpleDataType, dataValue, formatInfo));
					}
					
					if (Utilities.isNotBlank(itemIndexColumn)) {
						// Add additional integer value to identify data item index
						setParameter(preparedStatement, i++, SimpleDataType.Integer, itemsDone + 1, null);
					}
					
					preparedStatement.addBatch();
					
					validItems++;
					showProgress();
				} catch (Exception e) {
					invalidItems.add((int) itemsDone + 1);
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
				
				if (validItems > 0) {
					if (validItems % batchBlockSize == 0) {
						int[] results = preparedStatement.executeBatch();
						for (Closeable itemToClose : itemsToCloseAfterwards) {
							Utilities.closeQuietly(itemToClose);
						}
						itemsToCloseAfterwards.clear();
						for (int i = 0; i < results.length; i++) {
							if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
								invalidItems.add((int) (itemsDone - batchBlockSize) + i);
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
						invalidItems.add((int) (itemsDone - (itemsDone % batchBlockSize)) + i);
					}
				}
				if (!commitOnFullSuccessOnly) {
					connection.commit();
				}
			}
			
			if (commitOnFullSuccessOnly) {
				if (invalidItems.size() == 0) {
					connection.commit();
				} else {
					connection.rollback();
				}
			}

			dataItemsDone = itemsDone;
		} catch (Exception e) {
			connection.rollback();
			throw e;
		} finally {
			for (Closeable itemToClose : itemsToCloseAfterwards) {
				Utilities.closeQuietly(itemToClose);
			}
			itemsToCloseAfterwards.clear();
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
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
					if (dbVendor == DbVendor.SQLite) {
						// SQLite ignores "setBinaryStream"
						byte[] data = Utilities.readFileToByteArray(new File(valueString));
						preparedStatement.setBytes(columnIndex, data);
					} else {
						itemToCloseAfterwards = new FileInputStream(valueString);
						preparedStatement.setBinaryStream(columnIndex, (FileInputStream) itemToCloseAfterwards);
					}
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
			} else if ("lc".equalsIgnoreCase(formatInfo)) {
				valueString = valueString.toLowerCase();
				preparedStatement.setString(columnIndex, valueString);
			} else if ("uc".equalsIgnoreCase(formatInfo)) {
				valueString = valueString.toUpperCase();
				preparedStatement.setString(columnIndex, valueString);
			} else if ("email".equalsIgnoreCase(formatInfo)) {
				valueString = valueString.toLowerCase().trim();
				if (!NetworkUtilities.isValidEmail(valueString)) {
					throw new Exception("Invalid email address: " + valueString);
				}
				preparedStatement.setString(columnIndex, valueString);
			} else {
				preparedStatement.setTimestamp(columnIndex, new java.sql.Timestamp(new SimpleDateFormat(formatInfo).parse(valueString).getTime()));
			}
		} else {
			if (dataValue instanceof String) {
				if (simpleDataType == SimpleDataType.Blob) {
					preparedStatement.setBytes(columnIndex, Utilities.decodeBase64((String) dataValue));
				} else if (simpleDataType == SimpleDataType.Double) {
					String valueString = ((String) dataValue).trim();
					preparedStatement.setDouble(columnIndex, Double.parseDouble(valueString));
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
			} else if (simpleDataType == SimpleDataType.Double && dataValue instanceof Float) {
				// Keep the right precision when inserting a float value to a double column
				preparedStatement.setDouble(columnIndex, Double.parseDouble(dataValue.toString()));
			} else {
				preparedStatement.setObject(columnIndex, dataValue);
			}
		}
		return itemToCloseAfterwards;
	}

	protected static void logToFile(OutputStream logOutputStream, String message) throws Exception {
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
		return validItems;
	}

	public String getCreatedNewIndexName() {
		return newIndexName;
	}

	public List<Integer> getNotImportedItems() {
		return invalidItems;
	}
	
	public long getImportedDataAmount() {
		return importedDataAmount;
	}
	
	public int getIgnoredDuplicates() {
		return duplicatesItems;
	}

	public int getInsertedItems() {
		return insertedItems;
	}

	public List<String> getDataPropertyNames() {
		return availableDataPropertyNames;
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
	
	protected abstract List<String> getAvailableDataPropertyNames() throws Exception;

	protected abstract int getItemsAmountToImport() throws Exception;
	
	protected abstract Map<String, Object> getNextItemData() throws Exception;

	protected abstract Map<String, DbColumnType> scanDataPropertyTypes() throws Exception;
	
	protected abstract File filterDataItems(List<Integer> indexList, String fileSuffix) throws Exception;

	public abstract void close();
}
