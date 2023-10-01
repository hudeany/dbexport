package de.soderer.dbimport;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import de.soderer.dbimport.DbImportDefinition.DuplicateMode;
import de.soderer.dbimport.DbImportDefinition.ImportMode;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.Tuple;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveSet;
import de.soderer.utilities.db.DbColumnType;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.db.SimpleDataType;
import de.soderer.utilities.worker.WorkerParentSimple;

public class DbNoSqlImportWorker extends DbImportWorker {
	public DbNoSqlImportWorker(final WorkerParentSimple parent, final DbDefinition dbDefinition, final String tableName) {
		super(parent, dbDefinition, tableName, null, null);

		commitOnFullSuccessOnly = false;
	}

	@Override
	public void setDuplicateMode(final DuplicateMode duplicateMode) throws Exception {
		if (duplicateMode != DuplicateMode.UPDATE_ALL_JOIN) {
			throw new DbImportException("Only duplicate mode 'UPDATE_ALL_JOIN' is allowed for this NoSql import");
		}
	}

	@Override
	public void setUpdateNullData(final boolean updateWithNullValues) throws Exception {
		if (!updateWithNullValues) {
			throw new DbImportException("Only UpdateNullData mode 'Update with null values' is allowed for this NoSql import");
		}
	}

	@Override
	public void setCompleteCommit(final boolean commitOnFullSuccessOnly) throws Exception {
		if (commitOnFullSuccessOnly) {
			throw new DbImportException("Only commitOnFullSuccessOnly mode 'always commit' is allowed for this NoSql import");
		}
	}

	@SuppressWarnings("resource")
	@Override
	public Boolean work() throws Exception {
		signalUnlimitedProgress();

		if (analyseDataOnly) {
			parent.changeTitle(LangResources.get("analyseData"));
			availableDataPropertyNames = dataProvider.getAvailableDataPropertyNames();
		} else {
			if (duplicateMode != DuplicateMode.UPDATE_ALL_JOIN) {
				throw new Exception("Invalid duplicate mode for this NoSql import");
			}

			if (!updateWithNullValues) {
				throw new Exception("Invalid UpdateWithNullValues mode for this NoSql import");
			}

			OutputStream logOutputStream = null;
			Connection connection = null;
			boolean previousAutoCommit = false;
			try {
				connection = DbUtilities.createConnection(dbDefinition, false);

				previousAutoCommit = connection.getAutoCommit();
				connection.setAutoCommit(false);

				validItems = 0;
				invalidItems = new ArrayList<>();

				if (logFile != null) {
					logOutputStream = new FileOutputStream(logFile);
					logToFile(logOutputStream, getConfigurationLogString());
				}

				logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));

				if (!DbUtilities.checkTableExist(connection, tableName) && Utilities.isEmpty(keyColumns)) {
					throw new Exception("Creation of new table needs key column definition in NoSql import");
				}

				createTableIfNeeded(connection, tableName, keyColumns);

				final CaseInsensitiveSet tableKeys = DbUtilities.getPrimaryKeyColumns(connection, tableName);
				if (Utilities.isEmpty(keyColumns)) {
					keyColumns = new ArrayList<>(tableKeys);
				}
				for (final String column : tableKeys) {
					if (!keyColumns.contains(column)) {
						throw new Exception("Key columns of import (" + Utilities.join(keyColumns, ", ") + ") and table (" + Utilities.join(tableKeys, ", ") + ") do not match");
					}
				}
				for (final String column : keyColumns) {
					if (!tableKeys.contains(column)) {
						throw new Exception("Key columns of import (" + Utilities.join(keyColumns, ", ") + ") and table (" + Utilities.join(tableKeys, ", ") + ") do not match");
					}
				}

				final Map<String, DbColumnType> dbColumns = DbUtilities.getColumnDataTypes(connection, tableName);
				checkMapping(dbColumns);

				if (dbTableColumnsListToInsert.size() == 0) {
					throw new DbImportException("Invalid empty mapping");
				}

				if (!DbUtilities.checkTableAndColumnsExist(connection, tableName, keyColumns == null ? null : keyColumns.toArray(new String[0]))) {
					throw new DbImportException("Some keycolumn is not included in table");
				}

				if (importMode == ImportMode.CLEARINSERT) {
					deletedItems = DbUtilities.clearTable(connection, tableName);
				}

				parent.changeTitle(LangResources.get("readData"));
				itemsToDo = dataProvider.getItemsAmountToImport();
				itemsDone = 0;
				logToFile(logOutputStream, "Items to import: " + itemsToDo);
				signalProgress(true);

				if (importMode == ImportMode.CLEARINSERT) {
					parent.changeTitle(LangResources.get("insertData"));
					signalProgress(true);

					// Just import in the destination table
					insertItemsWithoutKeyColumns(connection, dbColumns, dbTableColumnsListToInsert, null, getMapping());

					updatedItems = 0;
					insertedItems = validItems - invalidItems.size();

					signalProgress(true);
				} else if (importMode == ImportMode.INSERT) {
					parent.changeTitle(LangResources.get("insertData"));
					signalProgress(true);

					// Insert into destination table
					updateAndInsertItems(connection, dbColumns, dbTableColumnsListToInsert, getMapping());

					signalProgress(true);
				} else if (importMode == ImportMode.UPDATE) {
					parent.changeTitle(LangResources.get("updateData"));
					signalProgress(true);

					// Update destination table
					updateAndInsertItems(connection, dbColumns, dbTableColumnsListToInsert, getMapping());

					signalProgress(true);
				} else if (importMode == ImportMode.UPSERT) {
					parent.changeTitle(LangResources.get("insertData"));
					signalProgress(true);

					// Insert and update into destination table
					updateAndInsertItems(connection, dbColumns, dbTableColumnsListToInsert, getMapping());

					signalProgress(true);
				} else {
					throw new Exception("Invalid import mode");
				}
				connection.commit();

				parent.changeTitle(LangResources.get("collectResult"));

				countItems = DbUtilities.getTableEntriesCount(connection, tableName);

				signalProgress(true);

				if (logErroneousData & invalidItems.size() > 0) {
					erroneousDataFile = dataProvider.filterDataItems(invalidItems, DateUtilities.formatDate(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName, getStartTime()) + ".errors");
				}

				setEndTime(LocalDateTime.now());

				importedDataAmount += dataProvider.getImportDataAmount();

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
				dataProvider.close();
				Utilities.closeQuietly(logOutputStream);

				if (connection != null) {
					connection.rollback();
					connection.setAutoCommit(previousAutoCommit);
					connection.close();
					connection = null;
				}
			}
		}

		return !cancel;
	}

	protected void insertItemsWithoutKeyColumns(final Connection connection, final Map<String, DbColumnType> dbColumns, final List<String> dbColumnsListToInsert, final String additionalInsertValuesToUse, final Map<String, Tuple<String, String>> mappingToUse) throws Exception {
		final List<Closeable> itemsToCloseAfterwards = new ArrayList<>();

		String additionalInsertValuesSqlColumns = "";
		String additionalInsertValuesSqlValues = "";
		if (Utilities.isNotBlank(additionalInsertValuesToUse)) {
			for (final String line : Utilities.splitAndTrimListQuoted(additionalInsertValuesToUse, '\n', '\r', ';')) {
				final String columnName = line.substring(0, line.indexOf("=")).trim();
				final String columnvalue = line.substring(line.indexOf("=") + 1).trim();
				additionalInsertValuesSqlColumns += columnName + ", ";
				additionalInsertValuesSqlValues += columnvalue + ", ";
			}
		}

		final String statementString = "INSERT INTO " + tableName + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), dbColumnsListToInsert) + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbColumnsListToInsert.size(), ", ") + ")";

		try (PreparedStatement preparedInsertStatement = connection.prepareStatement(statementString)) {

			final int batchBlockSize = 1000;
			boolean hasUnexecutedData = false;

			Map<String, Object> itemData;
			while ((itemData = dataProvider.getNextItemData()) != null) {
				try {
					int i = 1;
					for (final String dbColumnToInsert : dbColumnsListToInsert) {
						final SimpleDataType simpleDataType = dbColumns.get(dbColumnToInsert).getSimpleDataType();
						final String unescapedDbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumnToInsert);
						final Object dataValue = itemData.get(mappingToUse.get(unescapedDbColumnToInsert).getFirst());
						final String formatInfo = mappingToUse.get(unescapedDbColumnToInsert).getSecond();

						final
						Closeable itemToClose = setParameter(preparedInsertStatement, i++, simpleDataType, dataValue, formatInfo);
						if (itemToClose != null) {
							itemsToCloseAfterwards.add(itemToClose);
						}
					}

					preparedInsertStatement.addBatch();

					validItems++;
					signalProgress();
				} catch (@SuppressWarnings("unused") final Exception e) {
					invalidItems.add((int) itemsDone + 1);
					preparedInsertStatement.clearParameters();
				}
				itemsDone++;

				if (validItems > 0) {
					if (validItems % batchBlockSize == 0) {
						final int[] results = preparedInsertStatement.executeBatch();
						for (final Closeable itemToClose : itemsToCloseAfterwards) {
							Utilities.closeQuietly(itemToClose);
						}
						itemsToCloseAfterwards.clear();
						for (int i = 0; i < results.length; i++) {
							if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
								invalidItems.add((int) (itemsDone - batchBlockSize) + i);
							}
						}
						connection.commit();
						hasUnexecutedData = false;
						signalProgress();
					} else {
						hasUnexecutedData = true;
					}
				}
			}

			if (hasUnexecutedData) {
				final int[] results = preparedInsertStatement.executeBatch();
				for (final Closeable itemToClose : itemsToCloseAfterwards) {
					Utilities.closeQuietly(itemToClose);
				}
				itemsToCloseAfterwards.clear();
				for (int i = 0; i < results.length; i++) {
					if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
						invalidItems.add((int) (itemsDone - (itemsDone % batchBlockSize)) + i);
					}
				}
				connection.commit();
			}

			dataItemsDone = itemsDone;
			updatedItems = -1;
			insertedItems = -1;
		} catch (final Exception e) {
			connection.rollback();
			throw e;
		} finally {
			for (final Closeable itemToClose : itemsToCloseAfterwards) {
				Utilities.closeQuietly(itemToClose);
			}
			itemsToCloseAfterwards.clear();
		}
	}

	private void updateAndInsertItems(final Connection connection, final Map<String, DbColumnType> dbColumns, final List<String> dbColumnsListToUpdateAndInsert, final Map<String, Tuple<String, String>> mappingToUse) throws Exception {
		final List<Closeable> updateItemsToCloseAfterwards = new ArrayList<>();
		final List<Closeable> insertItemsToCloseAfterwards = new ArrayList<>();
		final List<Closeable> detectItemsToCloseAfterwards = new ArrayList<>();

		String keyColumnsWherePart = "";
		for (final String keyColumn : keyColumns) {
			if (keyColumnsWherePart.length() > 0) {
				keyColumnsWherePart += " AND ";
			}
			keyColumnsWherePart += keyColumn + " = ?";
		}
		final String detectStatementString = "SELECT COUNT(*) FROM " + tableName + " WHERE " + keyColumnsWherePart;

		String additionalUpdateValuesSqlPart = "";
		if (Utilities.isNotBlank(additionalUpdateValues)) {
			for (final String line : Utilities.splitAndTrimListQuoted(additionalUpdateValues, '\n', '\r', ';')) {
				final String columnName = line.substring(0, line.indexOf("=")).trim();
				final String columnvalue = line.substring(line.indexOf("=") + 1).trim();
				additionalUpdateValuesSqlPart += columnName + " = " + columnvalue + ", ";
			}
		}

		final StringBuilder updateValuesSqlPart = new StringBuilder();
		for (final String columnName : dbColumnsListToUpdateAndInsert) {
			if (!keyColumns.contains(columnName)) {
				if (updateValuesSqlPart.length() > 0) {
					updateValuesSqlPart.append(", ");
				}
				updateValuesSqlPart.append(DbUtilities.escapeVendorReservedNames(dbDefinition.getDbVendor(), columnName)).append(" = ?");
			}
		}

		final String updateStatementString = "UPDATE " + tableName + " SET " + additionalUpdateValuesSqlPart + updateValuesSqlPart + " WHERE " + keyColumnsWherePart;

		String additionalInsertValuesSqlColumns = "";
		String additionalInsertValuesSqlValues = "";
		if (Utilities.isNotBlank(additionalInsertValues)) {
			for (final String line : Utilities.splitAndTrimListQuoted(additionalInsertValues, '\n', '\r', ';')) {
				final String columnName = line.substring(0, line.indexOf("=")).trim();
				final String columnvalue = line.substring(line.indexOf("=") + 1).trim();
				additionalInsertValuesSqlColumns += columnName + ", ";
				additionalInsertValuesSqlValues += columnvalue + ", ";
			}
		}

		final String insertStatementString = "INSERT INTO " + tableName + " (" + additionalInsertValuesSqlColumns + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), dbColumnsListToUpdateAndInsert) + ") VALUES (" + additionalInsertValuesSqlValues + Utilities.repeat("?", dbColumnsListToUpdateAndInsert.size(), ", ") + ")";

		try (PreparedStatement preparedDetectStatement = connection.prepareStatement(detectStatementString);
				PreparedStatement preparedUpdateStatement = connection.prepareStatement(updateStatementString);
				PreparedStatement preparedInsertStatement = connection.prepareStatement(insertStatementString)) {
			final int batchBlockSize = 1000;
			final List<String> waitingInsertKeys = new ArrayList<>();
			boolean hasUnexecutedUpdateData = false;
			int itemsToUpdate = 0;
			int invalidItemsToUpdate = 0;
			boolean hasUnexecutedInsertData = false;
			int itemsToInsert = 0;
			int invalidItemsToInsert = 0;

			Map<String, Object> itemData;
			while ((itemData = dataProvider.getNextItemData()) != null) {
				String insertKey = "";
				preparedDetectStatement.clearParameters();
				int keyIndex = 1;
				for (final String keyColumn : keyColumns) {
					final SimpleDataType simpleDataType = dbColumns.get(keyColumn).getSimpleDataType();
					final String unescapedDbKeyColumn = DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), keyColumn);
					Object keyDataValue = itemData.get(mappingToUse.get(unescapedDbKeyColumn).getFirst());
					final String formatInfo = mappingToUse.get(unescapedDbKeyColumn).getSecond();
					Object insertKeyValue = keyDataValue;
					if (simpleDataType == SimpleDataType.String) {
						if ("lc".equalsIgnoreCase(formatInfo)) {
							insertKeyValue = ((String) insertKeyValue).toLowerCase();
						} else if ("uc".equalsIgnoreCase(formatInfo)) {
							insertKeyValue = ((String) insertKeyValue).toUpperCase();
						}
					}
					insertKey = insertKey + insertKeyValue + ";";
					if (dbDefinition.getDbVendor() == DbVendor.Cassandra && simpleDataType == SimpleDataType.String) {
						// Bug mitigation for Cassandra JDBC driver: Driver does not set apostrophes around strings as key column value in prepared statements
						keyDataValue = "'" + keyDataValue + "'";
					}
					final Closeable keyItemToClose = setParameter(preparedDetectStatement, keyIndex++, simpleDataType, keyDataValue, formatInfo);
					if (keyItemToClose != null) {
						detectItemsToCloseAfterwards.add(keyItemToClose);
					}
				}
				boolean itemExists = false;
				try (ResultSet detectResultSet = preparedDetectStatement.executeQuery()) {
					detectResultSet.next();
					final int count = detectResultSet.getInt(1);
					if (count > 0) {
						itemExists = true;
					}
				}
				for (final Closeable itemToClose : detectItemsToCloseAfterwards) {
					Utilities.closeQuietly(itemToClose);
				}
				detectItemsToCloseAfterwards.clear();

				if (!itemExists) {
					// Check currently waiting items for insert
					if (waitingInsertKeys.contains(insertKey)) {
						itemExists = true;

						// Execute all waiting inserts before update
						final int[] results = preparedInsertStatement.executeBatch();
						for (final Closeable itemToClose : insertItemsToCloseAfterwards) {
							Utilities.closeQuietly(itemToClose);
						}
						insertItemsToCloseAfterwards.clear();
						for (int i = 0; i < results.length; i++) {
							if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
								invalidItems.add((int) (itemsDone - results.length) + i);
								invalidItemsToInsert++;
							}
						}
						connection.commit();
						hasUnexecutedInsertData = false;
						signalProgress();

						waitingInsertKeys.clear();
					}
				}

				if (itemExists && (importMode == ImportMode.UPDATE || importMode == ImportMode.UPSERT)) {
					try {
						int i = 1;
						for (final String dbColumnToUpdate : dbColumnsListToUpdateAndInsert) {
							if (!keyColumns.contains(dbColumnToUpdate)) {
								final SimpleDataType simpleDataType = dbColumns.get(dbColumnToUpdate).getSimpleDataType();
								final String unescapedDbColumnToUpdate = DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumnToUpdate);
								final Object dataValue = itemData.get(mappingToUse.get(unescapedDbColumnToUpdate).getFirst());
								final String formatInfo = mappingToUse.get(unescapedDbColumnToUpdate).getSecond();

								final Closeable itemToClose = setParameter(preparedUpdateStatement, i++, simpleDataType, dataValue, formatInfo);
								if (itemToClose != null) {
									updateItemsToCloseAfterwards.add(itemToClose);
								}
							}
						}

						for (final String keyColumn : keyColumns) {
							final SimpleDataType simpleDataType = dbColumns.get(keyColumn).getSimpleDataType();
							final String unescapedDbKeyColumn = DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), keyColumn);
							final Object keyDataValue = itemData.get(mappingToUse.get(unescapedDbKeyColumn).getFirst());
							final String formatInfo = mappingToUse.get(unescapedDbKeyColumn).getSecond();

							final Closeable itemToClose = setParameter(preparedUpdateStatement, i++, simpleDataType, keyDataValue, formatInfo);
							if (itemToClose != null) {
								detectItemsToCloseAfterwards.add(itemToClose);
							}
						}

						preparedUpdateStatement.addBatch();

						validItems++;
						itemsToUpdate++;
						signalProgress();
					} catch (@SuppressWarnings("unused") final Exception e) {
						invalidItems.add((int) itemsDone + 1);
						preparedUpdateStatement.clearParameters();
					}
				} else if (!itemExists && (importMode == ImportMode.INSERT || importMode == ImportMode.UPSERT)) {
					try {
						int i = 1;
						for (final String dbColumnToInsert : dbColumnsListToUpdateAndInsert) {
							final SimpleDataType simpleDataType = dbColumns.get(dbColumnToInsert).getSimpleDataType();
							final String unescapedDbColumnToInsert = DbUtilities.unescapeVendorReservedNames(dbDefinition.getDbVendor(), dbColumnToInsert);
							final Object dataValue = itemData.get(mappingToUse.get(unescapedDbColumnToInsert).getFirst());
							final String formatInfo = mappingToUse.get(unescapedDbColumnToInsert).getSecond();

							final Closeable itemToClose = setParameter(preparedInsertStatement, i++, simpleDataType, dataValue, formatInfo);
							if (itemToClose != null) {
								insertItemsToCloseAfterwards.add(itemToClose);
							}
						}

						waitingInsertKeys.add(insertKey);
						preparedInsertStatement.addBatch();

						validItems++;
						itemsToInsert++;
						signalProgress();
					} catch (@SuppressWarnings("unused") final Exception e) {
						invalidItems.add((int) itemsDone + 1);
						preparedInsertStatement.clearParameters();
					}
				} else {
					validItems++;
				}
				itemsDone++;

				if (validItems > 0 && (itemsToUpdate > 0 || itemsToInsert > 0)) {
					if (itemsToUpdate % batchBlockSize == 0) {
						final int[] results = preparedUpdateStatement.executeBatch();
						for (final Closeable itemToClose : updateItemsToCloseAfterwards) {
							Utilities.closeQuietly(itemToClose);
						}
						updateItemsToCloseAfterwards.clear();
						for (int i = 0; i < results.length; i++) {
							if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
								invalidItems.add((int) (itemsDone - results.length) + i);
								invalidItemsToUpdate++;
							}
						}
						connection.commit();
						hasUnexecutedUpdateData = false;
						signalProgress();
					} else {
						hasUnexecutedUpdateData = true;
					}

					if (itemsToInsert % batchBlockSize == 0) {
						final int[] results = preparedInsertStatement.executeBatch();
						for (final Closeable itemToClose : insertItemsToCloseAfterwards) {
							Utilities.closeQuietly(itemToClose);
						}
						insertItemsToCloseAfterwards.clear();
						for (int i = 0; i < results.length; i++) {
							if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
								invalidItems.add((int) (itemsDone - results.length) + i);
								invalidItemsToInsert++;
							}
						}
						connection.commit();
						hasUnexecutedInsertData = false;
						signalProgress();

						waitingInsertKeys.clear();
					} else {
						hasUnexecutedInsertData = true;
					}
				}
			}

			if (hasUnexecutedUpdateData) {
				final int[] results = preparedUpdateStatement.executeBatch();
				for (final Closeable itemToClose : updateItemsToCloseAfterwards) {
					Utilities.closeQuietly(itemToClose);
				}
				updateItemsToCloseAfterwards.clear();
				for (int i = 0; i < results.length; i++) {
					if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
						invalidItems.add((int) (itemsDone - (itemsDone % batchBlockSize)) + i);
					}
				}
				connection.commit();
			}

			if (hasUnexecutedInsertData) {
				final int[] results = preparedInsertStatement.executeBatch();
				for (final Closeable itemToClose : insertItemsToCloseAfterwards) {
					Utilities.closeQuietly(itemToClose);
				}
				insertItemsToCloseAfterwards.clear();
				for (int i = 0; i < results.length; i++) {
					if (results[i] != 1 && results[i] != Statement.SUCCESS_NO_INFO) {
						invalidItems.add((int) (itemsDone - (itemsDone % batchBlockSize)) + i);
					}
				}
				connection.commit();

				waitingInsertKeys.clear();
			}

			dataItemsDone = itemsDone;
			updatedItems = itemsToUpdate - invalidItemsToUpdate;
			insertedItems = itemsToInsert - invalidItemsToInsert;
		} catch (final Exception e) {
			connection.rollback();
			throw e;
		} finally {
			for (final Closeable itemToClose : updateItemsToCloseAfterwards) {
				Utilities.closeQuietly(itemToClose);
			}
			updateItemsToCloseAfterwards.clear();

			for (final Closeable itemToClose : insertItemsToCloseAfterwards) {
				Utilities.closeQuietly(itemToClose);
			}
			insertItemsToCloseAfterwards.clear();

			for (final Closeable itemToClose : detectItemsToCloseAfterwards) {
				Utilities.closeQuietly(itemToClose);
			}
			detectItemsToCloseAfterwards.clear();
		}
	}
}
