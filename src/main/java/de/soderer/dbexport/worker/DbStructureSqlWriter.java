package de.soderer.dbexport.worker;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import de.soderer.dbexport.DbExportException;
import de.soderer.utilities.db.DbUtilities;
import de.soderer.utilities.db.data.DbVendor;

/**
 * Creates the structure of database tables as SQL DDL statements (like "pg_dump --schema-only"), without any data.
 *
 * <p>
 * The structure is read via the JDBC {@link DatabaseMetaData}, so it works for all JDBC databases. The statements use
 * the vendor specific data types and syntax of the source database:
 * </p>
 * <ul>
 * <li>CREATE TABLE with columns (data type, default value, NOT NULL, auto increment) and primary key</li>
 * <li>CREATE [UNIQUE] INDEX for all further indices</li>
 * <li>ALTER TABLE ... ADD FOREIGN KEY at the end, so the order of the tables does not matter (SQLite: within CREATE
 * TABLE, because SQLite cannot add foreign keys later)</li>
 * </ul>
 *
 * <p>
 * Not included: views, sequences (besides serial/identity columns), triggers, procedures, grants and comments.
 * Check constraints are only included for PostgreSQL, because JDBC offers no metadata for them.
 * </p>
 *
 * <p>
 * Usage: {@link #createHeader(String)} once, {@link #createTableStatement(String)} for each table, then
 * {@link #createDeferredStatements()} once for the collected indices and foreign keys.
 * </p>
 */
public class DbStructureSqlWriter {
	/** Indentation of the column definitions (like pg_dump) */
	private static final String INDENTATION = "    ";

	/** Column sizes from this value on mean "unlimited" (e.g. PostgreSQL varchar without length reports Integer.MAX_VALUE) */
	private static final int UNLIMITED_COLUMN_SIZE = 1_000_000_000;

	/** Maximum precision of a numeric type (PostgreSQL reports 131089 for "numeric" without precision) */
	private static final int MAX_NUMERIC_PRECISION = 1000;

	/** Identifiers which need no quoting regarding their characters */
	private static final Pattern SIMPLE_IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_$#]*");

	/** Derby system generated names, e.g. "SQL0000000001-9c4b8f1a-..." or "SQL120101120000000" */
	private static final Pattern DERBY_SYSTEM_NAME_PATTERN = Pattern.compile("SQL\\d.*");

	/** MsSQL system generated names, e.g. "PK__mytable__3213E83F1A2B3C4D" */
	private static final Pattern MSSQL_SYSTEM_NAME_PATTERN = Pattern.compile("(PK|UQ|FK|DF|CK)__.*__[0-9A-F]+");

	private final DbVendor dbVendor;
	private final Connection connection;
	private final DatabaseMetaData metaData;
	private final String catalog;
	private final String schema;

	/** Real table names (as stored in the database) by their lower case name */
	private final Map<String, String> tableNamesByLowerCaseName = new HashMap<>();

	/** CREATE INDEX statements, written after all tables */
	private final List<String> indexStatements = new ArrayList<>();

	/** ALTER TABLE ... ADD FOREIGN KEY statements, written after all indices */
	private final List<String> foreignKeyStatements = new ArrayList<>();

	/**
	 * Creates the writer for the current catalog and schema of the connection.
	 *
	 * @param dbVendor vendor of the database, decides about the syntax of the statements
	 * @param connection database connection to read the structure from
	 * @throws Exception if the vendor is not supported (Cassandra has no SQL DDL) or the metadata cannot be read
	 */
	public DbStructureSqlWriter(final DbVendor dbVendor, final Connection connection) throws Exception {
		if (dbVendor == DbVendor.Cassandra) {
			throw new DbExportException("SQL structure export is not supported for database vendor " + dbVendor);
		}

		this.dbVendor = dbVendor;
		this.connection = connection;
		metaData = connection.getMetaData();
		catalog = getCurrentCatalog(connection);
		schema = getCurrentSchema(connection);

		try (ResultSet tablesResultSet = metaData.getTables(catalog, schema, "%", new String[] { "TABLE" })) {
			while (tablesResultSet.next()) {
				final String tableName = tablesResultSet.getString("TABLE_NAME");
				if (tableName != null) {
					tableNamesByLowerCaseName.putIfAbsent(tableName.toLowerCase(Locale.ROOT), tableName);
				}
			}
		}
	}

	/**
	 * Creates the comment header of the dump.
	 *
	 * @param creatorText text about the creator, e.g. "DbExport 1.2.3 at 2026-11-27 11:00:00", may be null
	 * @return the header comment lines
	 */
	public String createHeader(final String creatorText) {
		final StringBuilder header = new StringBuilder();
		header.append("--\n");
		header.append("-- Database structure dump (no data)\n");
		header.append("--\n");
		header.append("-- Database vendor: ").append(dbVendor).append("\n");
		try {
			header.append("-- Database product: ").append(metaData.getDatabaseProductName()).append(" ").append(metaData.getDatabaseProductVersion()).append("\n");
		} catch (@SuppressWarnings("unused") final SQLException e) {
			// The product information is only informative
		}
		if (catalog != null) {
			header.append("-- Catalog: ").append(catalog).append("\n");
		}
		if (schema != null) {
			header.append("-- Schema: ").append(schema).append("\n");
		}
		if (creatorText != null) {
			header.append("-- Created by: ").append(creatorText).append("\n");
		}
		header.append("--\n\n");
		return header.toString();
	}

	/**
	 * Creates the CREATE TABLE statement of a table. Its indices and foreign keys are collected for
	 * {@link #createDeferredStatements()}.
	 *
	 * @param tableName name of the table (case insensitive)
	 * @return the CREATE TABLE statement with a leading comment
	 * @throws Exception if the table does not exist or its structure cannot be read
	 */
	public String createTableStatement(final String tableName) throws Exception {
		final String realTableName = tableNamesByLowerCaseName.getOrDefault(tableName.toLowerCase(Locale.ROOT), tableName);
		final String quotedTableName = quoteIdentifier(realTableName);

		final PrimaryKey primaryKey = readPrimaryKey(realTableName);
		final List<String> columnDefinitions = readColumnDefinitions(realTableName, primaryKey);
		if (columnDefinitions.isEmpty()) {
			throw new DbExportException("Table not found or without columns: " + tableName);
		}

		final List<String> tableElements = new ArrayList<>(columnDefinitions);
		if (primaryKey != null && !primaryKey.isDefinedInline) {
			tableElements.add((isSystemGeneratedName(primaryKey.name) ? "" : "CONSTRAINT " + quoteIdentifier(primaryKey.name) + " ")
					+ "PRIMARY KEY (" + joinQuoted(primaryKey.columnNames) + ")");
		}

		tableElements.addAll(readCheckConstraintDefinitions(realTableName));

		final List<String> foreignKeyDefinitions = readForeignKeyDefinitions(realTableName);
		if (dbVendor == DbVendor.SQLite) {
			// SQLite cannot add foreign keys by ALTER TABLE
			tableElements.addAll(foreignKeyDefinitions);
		} else {
			for (final String foreignKeyDefinition : foreignKeyDefinitions) {
				foreignKeyStatements.add("ALTER TABLE " + quotedTableName + " ADD " + foreignKeyDefinition + ";");
			}
		}

		indexStatements.addAll(readIndexStatements(realTableName, quotedTableName, primaryKey));

		final StringBuilder statement = new StringBuilder();
		statement.append("--\n");
		statement.append("-- Table: ").append(realTableName).append("\n");
		statement.append("--\n");
		statement.append("CREATE TABLE ").append(quotedTableName).append(" (\n");
		for (int i = 0; i < tableElements.size(); i++) {
			statement.append(INDENTATION).append(tableElements.get(i));
			if (i < tableElements.size() - 1) {
				statement.append(",");
			}
			statement.append("\n");
		}
		statement.append(");\n\n");
		return statement.toString();
	}

	/**
	 * Creates the statements collected by {@link #createTableStatement(String)}: the indices and then the foreign keys.
	 *
	 * @return the statements with leading comments, or an empty string if there are none
	 */
	public String createDeferredStatements() {
		final StringBuilder statements = new StringBuilder();
		if (!indexStatements.isEmpty()) {
			statements.append("--\n");
			statements.append("-- Indices\n");
			statements.append("--\n");
			for (final String indexStatement : indexStatements) {
				statements.append(indexStatement).append("\n");
			}
			statements.append("\n");
		}
		if (!foreignKeyStatements.isEmpty()) {
			statements.append("--\n");
			statements.append("-- Foreign keys\n");
			statements.append("--\n");
			for (final String foreignKeyStatement : foreignKeyStatements) {
				statements.append(foreignKeyStatement).append("\n");
			}
			statements.append("\n");
		}
		return statements.toString();
	}

	/**
	 * Creates a CREATE TABLE statement for the result columns of a select statement. Only column names, data types
	 * and NOT NULL are known for a result, no keys or indices.
	 *
	 * @param tableName name of the table to create
	 * @param resultSetMetaData the metadata of the result of the select statement
	 * @return the CREATE TABLE statement
	 * @throws SQLException if the metadata cannot be read
	 */
	public String createTableStatementForResult(final String tableName, final ResultSetMetaData resultSetMetaData) throws SQLException {
		final StringBuilder statement = new StringBuilder();
		// The table name of a FROM clause may be qualified by its schema
		final List<String> quotedTableNameParts = new ArrayList<>();
		for (final String tableNamePart : tableName.split("\\.")) {
			quotedTableNameParts.add(quoteIdentifier(tableNamePart));
		}
		statement.append("CREATE TABLE ").append(String.join(".", quotedTableNameParts)).append(" (\n");
		for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
			statement.append(INDENTATION).append(quoteIdentifier(resultSetMetaData.getColumnLabel(i))).append(" ");
			statement.append(formatDataType(resultSetMetaData.getColumnTypeName(i), resultSetMetaData.getColumnType(i), resultSetMetaData.getPrecision(i), resultSetMetaData.getScale(i)));
			if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
				statement.append(" NOT NULL");
			}
			if (i < resultSetMetaData.getColumnCount()) {
				statement.append(",");
			}
			statement.append("\n");
		}
		statement.append(");\n\n");
		return statement.toString();
	}

	private List<String> readColumnDefinitions(final String tableName, final PrimaryKey primaryKey) throws SQLException {
		// Sorted by the ordinal position, because the drivers do not guarantee the order
		final TreeMap<Integer, String> columnDefinitions = new TreeMap<>();
		int fallbackPosition = 100_000;
		try (ResultSet columnsResultSet = metaData.getColumns(catalog, schema, escapeMetaDataPattern(tableName), "%")) {
			while (columnsResultSet.next()) {
				// The table name is a pattern, where "_" matches any character
				if (!tableName.equals(columnsResultSet.getString("TABLE_NAME"))) {
					continue;
				}

				final String columnName = columnsResultSet.getString("COLUMN_NAME");
				final String typeName = columnsResultSet.getString("TYPE_NAME");
				final int dataType = columnsResultSet.getInt("DATA_TYPE");
				final int columnSize = columnsResultSet.getInt("COLUMN_SIZE");
				final int decimalDigits = columnsResultSet.getInt("DECIMAL_DIGITS");
				final boolean notNull = columnsResultSet.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
				String defaultValue = columnsResultSet.getString("COLUMN_DEF");
				defaultValue = defaultValue == null ? null : defaultValue.trim();
				final boolean autoIncrement = isAutoIncrement(columnsResultSet, typeName, defaultValue);
				int ordinalPosition = columnsResultSet.getInt("ORDINAL_POSITION");
				if (ordinalPosition <= 0) {
					ordinalPosition = fallbackPosition++;
				}

				columnDefinitions.put(ordinalPosition, createColumnDefinition(columnName, typeName, dataType, columnSize, decimalDigits, notNull, defaultValue, autoIncrement, primaryKey));
			}
		}
		return new ArrayList<>(columnDefinitions.values());
	}

	private String createColumnDefinition(final String columnName, final String typeName, final int dataType, final int columnSize, final int decimalDigits, final boolean notNull, final String defaultValue, final boolean autoIncrement, final PrimaryKey primaryKey) {
		String typeDefinition = formatDataType(typeName, dataType, columnSize, decimalDigits);
		String autoIncrementClause = "";
		boolean writeDefaultValue = defaultValue != null && !"NULL".equalsIgnoreCase(defaultValue);

		if (autoIncrement) {
			writeDefaultValue = false;
			switch (dbVendor) {
				case PostgreSQL:
					if (defaultValue != null && defaultValue.toLowerCase(Locale.ROOT).startsWith("nextval(")) {
						// serial column: the sequence is created implicitly by the serial type
						typeDefinition = getPostgresSerialType(typeDefinition);
					} else {
						autoIncrementClause = " GENERATED BY DEFAULT AS IDENTITY";
					}
					break;
				case MySQL:
				case MariaDB:
					autoIncrementClause = " AUTO_INCREMENT";
					break;
				case MsSQL:
					autoIncrementClause = " IDENTITY(1,1)";
					break;
				case SQLite:
					if (primaryKey != null && primaryKey.columnNames.size() == 1 && primaryKey.columnNames.get(0).equals(columnName)) {
						// SQLite only knows AUTOINCREMENT for an INTEGER PRIMARY KEY defined at the column
						typeDefinition = "INTEGER";
						autoIncrementClause = " PRIMARY KEY AUTOINCREMENT";
						primaryKey.isDefinedInline = true;
					}
					break;
				default:
					// Oracle 12c+, Derby, HSQL, Firebird 3+
					autoIncrementClause = " GENERATED BY DEFAULT AS IDENTITY";
					break;
			}
		}

		final StringBuilder columnDefinition = new StringBuilder();
		columnDefinition.append(quoteIdentifier(columnName)).append(" ").append(typeDefinition).append(autoIncrementClause);
		if (writeDefaultValue) {
			columnDefinition.append(" DEFAULT ").append(formatDefaultValue(defaultValue, dataType));
		}
		if (notNull && !autoIncrementClause.contains("PRIMARY KEY")) {
			columnDefinition.append(" NOT NULL");
		}
		return columnDefinition.toString();
	}

	private boolean isAutoIncrement(final ResultSet columnsResultSet, final String typeName, final String defaultValue) {
		if (typeName != null && typeName.toLowerCase(Locale.ROOT).endsWith(" identity")) {
			// MsSQL reports the type as "int identity"
			return true;
		}
		try {
			if ("YES".equalsIgnoreCase(columnsResultSet.getString("IS_AUTOINCREMENT"))) {
				return true;
			}
		} catch (@SuppressWarnings("unused") final SQLException e) {
			// IS_AUTOINCREMENT is only available since JDBC 4.0, then the default value is checked below
		}
		return dbVendor == DbVendor.PostgreSQL && defaultValue != null && defaultValue.toLowerCase(Locale.ROOT).startsWith("nextval(");
	}

	/**
	 * Formats a data type with its length or precision and scale, e.g. "varchar(50)" or "numeric(10,2)".
	 */
	String formatDataType(final String typeName, final int dataType, final int columnSize, final int decimalDigits) {
		String type = typeName == null ? "VARCHAR" : typeName.trim();
		if (type.toLowerCase(Locale.ROOT).endsWith(" identity")) {
			// MsSQL "int identity": the identity is added as clause
			type = type.substring(0, type.length() - " identity".length()).trim();
		}

		if (dbVendor == DbVendor.PostgreSQL) {
			if (type.startsWith("_")) {
				// PostgreSQL array types are reported as "_int4"
				return type.substring(1) + "[]";
			} else if ("bpchar".equalsIgnoreCase(type)) {
				type = "char";
			}
		}

		if (type.contains("(")) {
			// Already contains its parameters, e.g. Oracle "TIMESTAMP(6)"
			return type;
		}

		switch (dataType) {
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.BINARY:
			case Types.VARBINARY:
				if (columnSize >= UNLIMITED_COLUMN_SIZE) {
					// MsSQL "varchar" without length means length 1, so "max" is needed, others need no length
					return dbVendor == DbVendor.MsSQL ? type + "(max)" : type;
				} else if (columnSize > 0 && !isTypeWithoutLength(type)) {
					return type + "(" + columnSize + ")";
				} else {
					return type;
				}
			case Types.DECIMAL:
			case Types.NUMERIC:
				if (columnSize <= 0 || columnSize > MAX_NUMERIC_PRECISION || isTypeWithoutLength(type)) {
					// Without precision, e.g. Oracle "NUMBER" or PostgreSQL "numeric"
					return type;
				} else if (decimalDigits > 0) {
					return type + "(" + columnSize + "," + decimalDigits + ")";
				} else if (decimalDigits == 0) {
					return type + "(" + columnSize + ")";
				} else {
					// Oracle reports -127 for a NUMBER without scale
					return type;
				}
			default:
				return type;
		}
	}

	/**
	 * Types which are reported with a size, but must not get a length, e.g. PostgreSQL "text" (reported as VARCHAR)
	 * or MsSQL "money" (reported as DECIMAL).
	 */
	private static boolean isTypeWithoutLength(final String type) {
		final String lowerType = type.toLowerCase(Locale.ROOT);
		return "text".equals(lowerType)
				|| "name".equals(lowerType)
				|| "uuid".equals(lowerType)
				|| "bytea".equals(lowerType)
				|| "money".equals(lowerType)
				|| "smallmoney".equals(lowerType)
				|| "uniqueidentifier".equals(lowerType)
				|| "sysname".equals(lowerType)
				|| "timestamp".equals(lowerType)
				|| "rowversion".equals(lowerType)
				|| "json".equals(lowerType)
				|| "jsonb".equals(lowerType)
				|| "xml".equals(lowerType)
				|| "inet".equals(lowerType)
				|| "cidr".equals(lowerType)
				|| "macaddr".equals(lowerType)
				|| lowerType.endsWith("text")
				|| lowerType.endsWith("blob")
				|| lowerType.endsWith("serial");
	}

	private static String getPostgresSerialType(final String typeDefinition) {
		if (typeDefinition.toLowerCase(Locale.ROOT).endsWith("serial")) {
			// The driver already reports "serial", "bigserial" or "smallserial"
			return typeDefinition;
		}
		switch (typeDefinition.toLowerCase(Locale.ROOT)) {
			case "int2":
			case "smallint":
				return "smallserial";
			case "int8":
			case "bigint":
				return "bigserial";
			default:
				return "serial";
		}
	}

	/**
	 * MySQL reports string default values without quotes (e.g. abc instead of 'abc'), all other databases
	 * report the default value as SQL expression.
	 */
	private String formatDefaultValue(final String defaultValue, final int dataType) {
		if ((dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB)
				&& !defaultValue.startsWith("'")
				&& !defaultValue.startsWith("(")
				&& !defaultValue.contains("(")
				&& !defaultValue.toUpperCase(Locale.ROOT).startsWith("CURRENT_")
				&& !"NULL".equalsIgnoreCase(defaultValue)
				&& !isNumericType(dataType)) {
			return "'" + defaultValue.replace("'", "''") + "'";
		} else {
			return defaultValue;
		}
	}

	private static boolean isNumericType(final int dataType) {
		switch (dataType) {
			case Types.BIT:
			case Types.BOOLEAN:
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.FLOAT:
			case Types.REAL:
			case Types.DOUBLE:
			case Types.NUMERIC:
			case Types.DECIMAL:
				return true;
			default:
				return false;
		}
	}

	private PrimaryKey readPrimaryKey(final String tableName) throws SQLException {
		final TreeMap<Integer, String> columnNamesBySequence = new TreeMap<>();
		String primaryKeyName = null;
		try (ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(catalog, schema, tableName)) {
			while (primaryKeyResultSet.next()) {
				columnNamesBySequence.put(primaryKeyResultSet.getInt("KEY_SEQ"), primaryKeyResultSet.getString("COLUMN_NAME"));
				primaryKeyName = primaryKeyResultSet.getString("PK_NAME");
			}
		}
		if (columnNamesBySequence.isEmpty()) {
			return null;
		} else {
			return new PrimaryKey(primaryKeyName, new ArrayList<>(columnNamesBySequence.values()));
		}
	}

	/**
	 * Check constraints are not available via JDBC metadata, so they are only read for PostgreSQL from its catalog.
	 */
	private List<String> readCheckConstraintDefinitions(final String tableName) throws SQLException {
		final List<String> checkConstraintDefinitions = new ArrayList<>();
		if (dbVendor == DbVendor.PostgreSQL) {
			final String sql = "SELECT con.conname, pg_get_constraintdef(con.oid) FROM pg_constraint con"
					+ " JOIN pg_class rel ON rel.oid = con.conrelid"
					+ " JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace"
					+ " WHERE con.contype = 'c' AND rel.relname = ? AND nsp.nspname = COALESCE(?, current_schema())"
					+ " ORDER BY con.conname";
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				statement.setString(1, tableName);
				statement.setString(2, schema);
				try (ResultSet resultSet = statement.executeQuery()) {
					while (resultSet.next()) {
						checkConstraintDefinitions.add("CONSTRAINT " + quoteIdentifier(resultSet.getString(1)) + " " + resultSet.getString(2));
					}
				}
			}
		}
		return checkConstraintDefinitions;
	}

	private List<String> readForeignKeyDefinitions(final String tableName) throws SQLException {
		// Grouped by the foreign key name (or by the start of a new key sequence for unnamed foreign keys)
		final Map<String, ForeignKey> foreignKeys = new LinkedHashMap<>();
		ForeignKey currentForeignKey = null;
		try (ResultSet foreignKeyResultSet = metaData.getImportedKeys(catalog, schema, tableName)) {
			while (foreignKeyResultSet.next()) {
				final String foreignKeyName = foreignKeyResultSet.getString("FK_NAME");
				final int keySequence = foreignKeyResultSet.getInt("KEY_SEQ");
				final String groupKey = foreignKeyName != null && !foreignKeyName.isEmpty() ? foreignKeyName : null;

				if (groupKey != null && foreignKeys.containsKey(groupKey)) {
					currentForeignKey = foreignKeys.get(groupKey);
				} else if (groupKey != null || keySequence <= 1 || currentForeignKey == null) {
					final String referencedSchema = foreignKeyResultSet.getString("PKTABLE_SCHEM");
					String quotedReferencedTableName = quoteIdentifier(foreignKeyResultSet.getString("PKTABLE_NAME"));
					if (referencedSchema != null && schema != null && !referencedSchema.equalsIgnoreCase(schema)) {
						// Referenced table in another schema
						quotedReferencedTableName = quoteIdentifier(referencedSchema) + "." + quotedReferencedTableName;
					}
					currentForeignKey = new ForeignKey(groupKey, quotedReferencedTableName, foreignKeyResultSet.getShort("UPDATE_RULE"), foreignKeyResultSet.getShort("DELETE_RULE"));
					foreignKeys.put(groupKey != null ? groupKey : "#unnamed" + foreignKeys.size(), currentForeignKey);
				}

				currentForeignKey.columnNames.put(keySequence, foreignKeyResultSet.getString("FKCOLUMN_NAME"));
				currentForeignKey.referencedColumnNames.put(keySequence, foreignKeyResultSet.getString("PKCOLUMN_NAME"));
			}
		}

		final List<String> foreignKeyDefinitions = new ArrayList<>();
		for (final ForeignKey foreignKey : foreignKeys.values()) {
			final StringBuilder definition = new StringBuilder();
			if (!isSystemGeneratedName(foreignKey.name)) {
				definition.append("CONSTRAINT ").append(quoteIdentifier(foreignKey.name)).append(" ");
			}
			definition.append("FOREIGN KEY (").append(joinQuoted(new ArrayList<>(foreignKey.columnNames.values()))).append(")");
			definition.append(" REFERENCES ").append(foreignKey.quotedReferencedTableName);
			definition.append(" (").append(joinQuoted(new ArrayList<>(foreignKey.referencedColumnNames.values()))).append(")");
			if (dbVendor != DbVendor.Oracle && dbVendor != DbVendor.Derby) {
				// Oracle has no ON UPDATE, Derby only supports NO ACTION and RESTRICT for ON UPDATE
				definition.append(formatForeignKeyRule("ON UPDATE", foreignKey.updateRule));
			}
			definition.append(formatForeignKeyRule("ON DELETE", foreignKey.deleteRule));
			foreignKeyDefinitions.add(definition.toString());
		}
		return foreignKeyDefinitions;
	}

	private String formatForeignKeyRule(final String ruleType, final int rule) {
		switch (rule) {
			case DatabaseMetaData.importedKeyCascade:
				return " " + ruleType + " CASCADE";
			case DatabaseMetaData.importedKeySetNull:
				return " " + ruleType + " SET NULL";
			case DatabaseMetaData.importedKeySetDefault:
				// Oracle does not support SET DEFAULT
				return dbVendor == DbVendor.Oracle ? "" : " " + ruleType + " SET DEFAULT";
			default:
				// NO ACTION and RESTRICT are the default behaviour
				return "";
		}
	}

	private List<String> readIndexStatements(final String tableName, final String quotedTableName, final PrimaryKey primaryKey) throws SQLException {
		final Map<String, Index> indices = new LinkedHashMap<>();
		// "approximate" must be true, otherwise the Oracle driver executes "ANALYZE TABLE ... COMPUTE STATISTICS"
		try (ResultSet indexResultSet = metaData.getIndexInfo(catalog, schema, tableName, false, true)) {
			while (indexResultSet.next()) {
				final String indexName = indexResultSet.getString("INDEX_NAME");
				if (indexName == null || indexResultSet.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
					continue;
				}
				Index index = indices.get(indexName);
				if (index == null) {
					index = new Index(indexName, !indexResultSet.getBoolean("NON_UNIQUE"));
					indices.put(indexName, index);
				}
				final String columnName = indexResultSet.getString("COLUMN_NAME");
				final String ascOrDesc = indexResultSet.getString("ASC_OR_DESC");
				if (columnName == null) {
					index.isSupported = false;
				} else {
					index.columns.put((int) indexResultSet.getShort("ORDINAL_POSITION"), new IndexColumn(columnName, "D".equalsIgnoreCase(ascOrDesc)));
				}
			}
		}

		final List<String> statements = new ArrayList<>();
		for (final Index index : indices.values()) {
			final List<String> columnNames = new ArrayList<>();
			final List<String> columnDefinitions = new ArrayList<>();
			for (final IndexColumn indexColumn : index.columns.values()) {
				columnNames.add(indexColumn.name);
				if (indexColumn.name.toUpperCase(Locale.ROOT).startsWith("SYS_NC")) {
					// Oracle function based index, the expression is not available via JDBC
					index.isSupported = false;
				}
				// An expression (e.g. PostgreSQL "lower((name)::text)") is written as it is
				columnDefinitions.add((indexColumn.name.contains("(") ? indexColumn.name : quoteIdentifier(indexColumn.name)) + (indexColumn.descending ? " DESC" : ""));
			}

			if (!index.isSupported || columnNames.isEmpty()) {
				statements.add("-- Index " + index.name + " on " + tableName + " skipped: its definition is not available via JDBC");
				continue;
			} else if (primaryKey != null && index.unique && columnNames.equals(primaryKey.columnNames)) {
				// The index of the primary key is created with the primary key
				continue;
			}

			String indexName = index.name;
			if (isSystemGeneratedName(indexName)) {
				if (!index.unique) {
					// Created implicitly for a foreign key constraint, so it is created again with the foreign key
					continue;
				}
				// Index of a unique constraint, the system generated name cannot be used for CREATE INDEX (e.g. SQLite
				// "sqlite_autoindex_..." is reserved)
				indexName = tableName + "_" + String.join("_", columnNames) + "_uk";
			}

			statements.add("CREATE " + (index.unique ? "UNIQUE " : "") + "INDEX " + quoteIdentifier(indexName) + " ON " + quotedTableName + " (" + String.join(", ", columnDefinitions) + ");");
		}
		return statements;
	}

	/**
	 * Checks for names the database generated for unnamed constraints and indices, which should not be written,
	 * so the database generates them again.
	 */
	private static boolean isSystemGeneratedName(final String name) {
		if (name == null || name.isEmpty()) {
			return true;
		}
		final String upperName = name.toUpperCase(Locale.ROOT);
		return "PRIMARY".equals(upperName)
				|| upperName.startsWith("SYS_")
				|| upperName.startsWith("SQLITE_")
				|| upperName.startsWith("RDB$")
				|| upperName.startsWith("INTEG_")
				|| DERBY_SYSTEM_NAME_PATTERN.matcher(upperName).matches()
				|| MSSQL_SYSTEM_NAME_PATTERN.matcher(name).matches();
	}

	/**
	 * Quotes an identifier if needed: for special characters, for a case which the database would change for an
	 * unquoted name (e.g. "MyTable" in PostgreSQL), and for reserved words of the vendor.
	 *
	 * @param identifier the table, column, index or constraint name
	 * @return the identifier as it can be used in a statement
	 */
	public String quoteIdentifier(final String identifier) {
		final boolean needsQuoting = !SIMPLE_IDENTIFIER_PATTERN.matcher(identifier).matches() || isCaseChangedWhenUnquoted(identifier);
		if (!needsQuoting) {
			return DbUtilities.escapeVendorReservedNames(dbVendor, identifier);
		}

		switch (dbVendor) {
			case MySQL:
			case MariaDB:
				return "`" + identifier.replace("`", "``") + "`";
			case MsSQL:
				return "[" + identifier.replace("]", "]]") + "]";
			default:
				return "\"" + identifier.replace("\"", "\"\"") + "\"";
		}
	}

	private boolean isCaseChangedWhenUnquoted(final String identifier) {
		switch (dbVendor) {
			case PostgreSQL:
				// Unquoted names are lower case
				return !identifier.equals(identifier.toLowerCase(Locale.ROOT));
			case Oracle:
			case Derby:
			case HSQL:
			case Firebird:
				// Unquoted names are upper case
				return !identifier.equals(identifier.toUpperCase(Locale.ROOT));
			default:
				return false;
		}
	}

	private String joinQuoted(final List<String> identifiers) {
		final List<String> quotedIdentifiers = new ArrayList<>();
		for (final String identifier : identifiers) {
			quotedIdentifiers.add(quoteIdentifier(identifier));
		}
		return String.join(", ", quotedIdentifiers);
	}

	/**
	 * Escapes the wildcards "_" and "%" of a name used as metadata search pattern.
	 */
	private String escapeMetaDataPattern(final String name) {
		try {
			final String escape = metaData.getSearchStringEscape();
			if (escape != null && !escape.isEmpty()) {
				return name.replace(escape, escape + escape).replace("_", escape + "_").replace("%", escape + "%");
			}
		} catch (@SuppressWarnings("unused") final SQLException e) {
			// Then the name is used unescaped and the result is filtered by the exact name
		}
		return name;
	}

	private static String getCurrentCatalog(final Connection connection) {
		try {
			return connection.getCatalog();
		} catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	private static String getCurrentSchema(final Connection connection) {
		try {
			return connection.getSchema();
		} catch (@SuppressWarnings("unused") final Throwable e) {
			// Older drivers without JDBC 4.1 throw AbstractMethodError
			return null;
		}
	}

	private static class PrimaryKey {
		private final String name;
		private final List<String> columnNames;
		/** SQLite: defined as "INTEGER PRIMARY KEY AUTOINCREMENT" at the column */
		private boolean isDefinedInline = false;

		private PrimaryKey(final String name, final List<String> columnNames) {
			this.name = name;
			this.columnNames = columnNames;
		}
	}

	private static class ForeignKey {
		private final String name;
		private final String quotedReferencedTableName;
		private final int updateRule;
		private final int deleteRule;
		private final TreeMap<Integer, String> columnNames = new TreeMap<>();
		private final TreeMap<Integer, String> referencedColumnNames = new TreeMap<>();

		private ForeignKey(final String name, final String quotedReferencedTableName, final int updateRule, final int deleteRule) {
			this.name = name;
			this.quotedReferencedTableName = quotedReferencedTableName;
			this.updateRule = updateRule;
			this.deleteRule = deleteRule;
		}
	}

	private static class Index {
		private final String name;
		private final boolean unique;
		private final TreeMap<Integer, IndexColumn> columns = new TreeMap<>();
		private boolean isSupported = true;

		private Index(final String name, final boolean unique) {
			this.name = name;
			this.unique = unique;
		}
	}

	private static class IndexColumn {
		private final String name;
		private final boolean descending;

		private IndexColumn(final String name, final boolean descending) {
			this.name = name;
			this.descending = descending;
		}
	}
}
