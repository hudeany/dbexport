package de.soderer.dbimport;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import de.soderer.utilities.DbDefinition;
import de.soderer.utilities.DbNotExistsException;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.LangResources;
import de.soderer.utilities.SimpleDataType;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveSet;
import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader;
import de.soderer.utilities.worker.WorkerParentSimple;
import de.soderer.utilities.worker.WorkerSimple;

public class DbStructureWorker extends WorkerSimple<Boolean> {
	// Mandatory parameters
	protected DbDefinition dbDefinition = null;

	protected long createdTables = 0;
	protected long createdColumns = 0;

	protected InputStream jsonStructureDataInputStream;

	public DbStructureWorker(final WorkerParentSimple parent, final DbDefinition dbDefinition, final InputStream jsonStructureDataInputStream) throws Exception {
		super(parent);

		this.dbDefinition = dbDefinition;
		this.jsonStructureDataInputStream = jsonStructureDataInputStream;
	}

	@Override
	public Boolean work() throws Exception {
		signalUnlimitedProgress();

		Connection connection = null;
		boolean previousAutoCommit = false;
		try (JsonReader jsonReader = new JsonReader(jsonStructureDataInputStream)) {
			if (dbDefinition.getDbVendor() == DbVendor.Derby || (dbDefinition.getDbVendor() == DbVendor.HSQL && Utilities.isBlank(dbDefinition.getHostnameAndPort())) || dbDefinition.getDbVendor() == DbVendor.SQLite) {
				try {
					connection = DbUtilities.createConnection(dbDefinition, true);
				} catch (@SuppressWarnings("unused") final DbNotExistsException e) {
					connection = DbUtilities.createNewDatabase(dbDefinition.getDbVendor(), dbDefinition.getDbName());
				}
			} else {
				connection = DbUtilities.createConnection(dbDefinition, false);
			}

			previousAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);

			createdTables = 0;
			createdColumns = 0;

			parent.changeTitle(LangResources.get("creatingMissingTablesAndColumns"));

			final JsonNode dbStructureJsonNode = jsonReader.read();

			if (!dbStructureJsonNode.isJsonObject()) {
				throw new Exception("Invalid db structure file. Must contain JsonObject with table properties");
			}

			final JsonObject dbStructureJsonObject = (JsonObject) dbStructureJsonNode.getValue();

			itemsToDo = dbStructureJsonObject.size();
			itemsDone = 0;

			for (final Entry<String, Object> tableEntry : dbStructureJsonObject.entrySet()) {
				final String tableName = tableEntry.getKey();
				parent.changeTitle(LangResources.get("workingOnTable", tableName));
				final JsonObject tableJsonObject = (JsonObject) tableEntry.getValue();
				if (!DbUtilities.checkTableExist(connection, tableName)) {
					createTable(connection, tableName, tableJsonObject);
					createdTables++;
				} else {
					final CaseInsensitiveSet columnNames = DbUtilities.getColumnNames(connection, tableName);
					final JsonArray tableColumnsJsonArray = (JsonArray) tableJsonObject.get("columns");
					for (final Object columnObject : tableColumnsJsonArray) {
						final JsonObject columnJsonObject = (JsonObject) columnObject;
						final String columnName = (String) columnJsonObject.get("name");
						if (!columnNames.contains(columnName)) {
							createTableColumn(connection, tableName, columnJsonObject);
							createdColumns++;
						}
					}
				}

				itemsDone++;
				signalProgress(false);
			}

			signalProgress(true);
			setEndTime(LocalDateTime.now());
		} finally {
			if (connection != null) {
				connection.rollback();
				connection.setAutoCommit(previousAutoCommit);
				connection.close();
				connection = null;
				if (dbDefinition.getDbVendor() == DbVendor.Derby) {
					DbUtilities.shutDownDerbyDb(dbDefinition.getDbName());
				}
			}
		}

		return !cancel;
	}

	private void createTable(final Connection connection, final String tableName, final JsonObject tableJsonObject) throws Exception {
		if  (tableJsonObject == null) {
			throw new Exception("Cannot create table without table definition");
		}

		final JsonArray columnsJsonArray = (JsonArray) tableJsonObject.get("columns");
		if  (columnsJsonArray == null) {
			throw new Exception("Cannot create table without columns definition");
		}

		try (Statement statement = connection.createStatement()) {
			String columnsPart = "";
			for (final Object columnObject : columnsJsonArray) {
				final JsonObject columnJsonObject = (JsonObject) columnObject;

				if (columnsPart.length() > 0) {
					columnsPart += ", ";
				}

				columnsPart = columnsPart + getColumnNameAndType(columnJsonObject);
			}

			final List<String> keyColumns = ((JsonArray) tableJsonObject.get("keycolumns")).stream().map(String.class::cast).collect(Collectors.toList());

			String primaryKeyPart = "";
			if (Utilities.isNotEmpty(keyColumns)) {
				primaryKeyPart = ", PRIMARY KEY (" + DbUtilities.joinColumnVendorEscaped(dbDefinition.getDbVendor(), keyColumns) + ")";
			}
			statement.execute("CREATE TABLE " + tableName + " (" + columnsPart + primaryKeyPart + ")");
			if (dbDefinition.getDbVendor() == DbVendor.Derby) {
				connection.commit();
			}
		}
	}

	private void createTableColumn(final Connection connection, final String tableName, final JsonObject columnJsonObject) throws Exception {
		if  (columnJsonObject == null) {
			throw new Exception("Cannot create table column without column definition");
		}

		try (Statement statement = connection.createStatement()) {
			final String columnsPart = getColumnNameAndType(columnJsonObject);

			statement.execute("ALTER TABLE " + tableName + " ADD " + columnsPart);
			if (dbDefinition.getDbVendor() == DbVendor.Derby) {
				connection.commit();
			}
		}
	}

	private String getColumnNameAndType(final JsonObject columnJsonObject) throws Exception {
		final String name = DbUtilities.escapeVendorReservedNames(dbDefinition.getDbVendor(), (String) columnJsonObject.get("name"));
		final SimpleDataType simpleDataType = SimpleDataType.getSimpleDataTypeByName((String) columnJsonObject.get("datatype"));
		int characterByteSize = -1;
		if (columnJsonObject.containsPropertyKey("datasize")) {
			characterByteSize = (Integer) columnJsonObject.get("datasize");
		}

		String defaultvalue = null;
		if (columnJsonObject.containsPropertyKey("defaultvalue")) {
			defaultvalue = (String) columnJsonObject.get("defaultvalue");
		}
		String defaultvaluePart = "";
		if (defaultvalue != null) {
			if (simpleDataType == SimpleDataType.String) {
				defaultvaluePart = defaultvalue;
				if (!defaultvaluePart.startsWith("'") || !defaultvaluePart.endsWith("'")) {
					defaultvaluePart = "'" + defaultvaluePart + "'";
				}
			} else {
				defaultvaluePart = defaultvalue;
			}

			defaultvaluePart = " DEFAULT " + defaultvaluePart;
		}

		// "databasevendorspecific_datatype"

		return name + " " + DbUtilities.getDataType(dbDefinition.getDbVendor(), simpleDataType) + (characterByteSize > -1 ? "(" + characterByteSize + ")" : "") + defaultvaluePart;
	}

	public long getCreatedTables() {
		return createdTables;
	}

	public long getCreatedColumns() {
		return createdColumns;
	}
}
