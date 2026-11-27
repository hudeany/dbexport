package de.soderer.dbexport.worker;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.soderer.dbexport.DbExportException;
import de.soderer.utilities.DateUtilities;

/**
 * Writes the structure of PostgreSQL tables as SQL DDL statements in the format of "pg_dump --schema-only".
 *
 * <p>
 * The definitions are read from the PostgreSQL system catalog with the functions pg_dump uses as well
 * (format_type, pg_get_expr, pg_get_constraintdef, pg_get_indexdef, pg_get_triggerdef, pg_get_viewdef,
 * pg_get_functiondef), so data types and expressions are written exactly like by pg_dump. Each object gets the
 * comment header of pg_dump ("Name: ...; Type: ...; Schema: ...; Owner: ..."), only without the TOC entry and OID
 * lines, which refer to the internal archive of pg_dump.
 * </p>
 *
 * <p>
 * Contained objects, in the order of pg_dump:
 * </p>
 * <ul>
 * <li>schemas of the exported tables, extensions</li>
 * <li>enum types and domains, functions and procedures of these schemas</li>
 * <li>tables of the export (with identity columns, owner and comments), sequences of these schemas</li>
 * <li>views and materialized views of these schemas</li>
 * <li>partition attachments, serial default values, primary key/unique/exclusion constraints, indices, triggers and
 * foreign keys of the exported tables</li>
 * </ul>
 *
 * <p>
 * Not contained: grants (ACL), row level security policies, rules, publications, composite and range types,
 * aggregates and operators, tablespaces and statistics objects. Requires PostgreSQL 10 or later (pg_sequence).
 * </p>
 */
public class PostgreSqlStructureWriter implements SqlStructureWriter {
	private final Connection connection;
	private final Charset encoding;
	private final int serverMajorVersion;
	private final String serverVersion;

	private Writer writer;

	/** Whether the settings for tables were written before the first table */
	private boolean tableSettingsWritten = false;

	/** Names of the tables already written ("schema.table"), to decide where "OWNED BY" can be written */
	private final Set<String> writtenTables = new HashSet<>();

	// Deferred entries, written after all tables, in the order of pg_dump
	private final List<DumpEntry> deferredSequenceOwnedByEntries = new ArrayList<>();
	private final List<DumpEntry> tableAttachEntries = new ArrayList<>();
	private final List<DumpEntry> defaultEntries = new ArrayList<>();
	private final List<DumpEntry> constraintEntries = new ArrayList<>();
	private final List<DumpEntry> indexEntries = new ArrayList<>();
	private final List<DumpEntry> indexAttachEntries = new ArrayList<>();
	private final List<DumpEntry> triggerEntries = new ArrayList<>();
	private final List<DumpEntry> foreignKeyEntries = new ArrayList<>();

	/**
	 * Creates the writer.
	 *
	 * @param connection connection to the PostgreSQL database
	 * @param encoding encoding of the output file, written as client_encoding
	 * @throws Exception if the server version cannot be read or is older than 10
	 */
	public PostgreSqlStructureWriter(final Connection connection, final Charset encoding) throws Exception {
		this.connection = connection;
		this.encoding = encoding == null ? StandardCharsets.UTF_8 : encoding;
		serverMajorVersion = connection.getMetaData().getDatabaseMajorVersion();
		serverVersion = connection.getMetaData().getDatabaseProductVersion();
		if (serverMajorVersion < 10) {
			throw new DbExportException("SQL structure export needs PostgreSQL 10 or later, but database version is " + serverVersion);
		}
	}

	@Override
	public void writeStructure(final List<String> tableNames, final Writer outputWriter, final String creatorText, final ProgressListener progressListener) throws Exception {
		writer = outputWriter;

		final List<TableInfo> tables = new ArrayList<>();
		for (final String tableName : tableNames) {
			tables.add(resolveTable(tableName));
		}
		// pg_dump sorts by schema and name
		tables.sort(Comparator.comparing((final TableInfo table) -> table.schemaName).thenComparing(table -> table.tableName));

		final Set<String> schemaNames = new LinkedHashSet<>();
		for (final TableInfo table : tables) {
			schemaNames.add(table.schemaName);
		}
		final List<String> sortedSchemaNames = new ArrayList<>(schemaNames);
		sortedSchemaNames.sort(null);

		// Tables, sequences and views are sorted together by schema and name like by pg_dump. Objects a view, an
		// inheriting table or a function with an SQL standard body depends on are moved in front of it, like the
		// dependency sorting of pg_dump does.
		final List<RelationItem> relations = new ArrayList<>();
		relations.addAll(tables);
		relations.addAll(readSequences(sortedSchemaNames));
		relations.addAll(readViews(sortedSchemaNames));
		relations.sort(Comparator.comparing((final RelationItem relation) -> relation.getSchemaName()).thenComparing(relation -> relation.getRelationName()));
		final Map<String, RelationItem> relationsByKey = new HashMap<>();
		for (final RelationItem relation : relations) {
			relationsByKey.put(relation.getKey(), relation);
		}

		writeHeader(creatorText);
		writeSchemas(sortedSchemaNames);
		writeExtensions();
		writeTypes(sortedSchemaNames);
		writeFunctions(sortedSchemaNames, relationsByKey, progressListener);

		for (final RelationItem relation : relations) {
			if (progressListener != null && progressListener.isCanceled()) {
				return;
			}
			writeRelation(relation, relationsByKey, progressListener);
		}

		writeEntries(deferredSequenceOwnedByEntries);
		writeEntries(tableAttachEntries);
		writeEntries(defaultEntries);
		writeEntries(constraintEntries);
		writeEntries(indexEntries);
		writeEntries(indexAttachEntries);
		writeEntries(triggerEntries);
		writeEntries(foreignKeyEntries);

		writeFooter();
	}

	/**
	 * Writes a table, sequence or view, but first the not yet written objects it depends on.
	 */
	private void writeRelation(final RelationItem relation, final Map<String, RelationItem> relationsByKey, final ProgressListener progressListener) throws Exception {
		if (relation.written) {
			return;
		}
		// Marked before writing the dependencies, so cyclic dependencies cannot cause an endless recursion
		relation.written = true;
		for (final String dependencyKey : relation.getDependencyKeys()) {
			final RelationItem dependency = relationsByKey.get(dependencyKey);
			if (dependency != null) {
				writeRelation(dependency, relationsByKey, progressListener);
			}
		}

		if (!tableSettingsWritten && (relation instanceof TableInfo || (relation instanceof final ViewInfo view && view.materialized))) {
			// Like pg_dump, before the first object with storage
			write("SET default_tablespace = '';\n\n");
			write("SET default_table_access_method = heap;\n\n");
			tableSettingsWritten = true;
		}

		if (relation instanceof final TableInfo table) {
			writeTable(table);
			if (progressListener != null) {
				progressListener.tableDone();
			}
		} else if (relation instanceof final SequenceInfo sequence) {
			writeSequence(sequence);
		} else if (relation instanceof final ViewInfo view) {
			writeView(view);
		}
	}

	private void writeHeader(final String creatorText) throws IOException {
		write("--\n");
		write("-- PostgreSQL database dump\n");
		write("--\n\n");
		write("-- Dumped from database version " + serverVersion + "\n");
		write("-- Dumped by " + (creatorText == null ? "DbExport" : creatorText) + "\n\n");
		write("-- Started on " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + "\n\n");
		write("SET statement_timeout = 0;\n");
		write("SET lock_timeout = 0;\n");
		write("SET idle_in_transaction_session_timeout = 0;\n");
		write("SET client_encoding = '" + getPostgresEncodingName(encoding) + "';\n");
		write("SET standard_conforming_strings = on;\n");
		write("SELECT pg_catalog.set_config('search_path', '', false);\n");
		write("SET check_function_bodies = false;\n");
		write("SET xmloption = content;\n");
		write("SET client_min_messages = warning;\n");
		write("SET row_security = off;\n\n");
	}

	private void writeFooter() throws IOException {
		write("-- Completed on " + DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, LocalDateTime.now()) + "\n\n");
		write("--\n");
		write("-- PostgreSQL database dump complete\n");
		write("--\n\n");
	}

	private void writeSchemas(final List<String> schemaNames) throws Exception {
		final String sql = "SELECT n.nspname, quote_ident(n.nspname), pg_get_userbyid(n.nspowner), quote_ident(pg_get_userbyid(n.nspowner)), obj_description(n.oid, 'pg_namespace')"
				+ " FROM pg_namespace n WHERE n.nspname = ANY(?) ORDER BY n.nspname";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setArray(1, createTextArray(schemaNames));
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final String schemaName = resultSet.getString(1);
					final String quotedSchemaName = resultSet.getString(2);
					final String owner = resultSet.getString(3);
					final String comment = resultSet.getString(5);
					if ("public".equals(schemaName)) {
						// Exists in every database, pg_dump only writes its comment, if it was changed
						if (comment != null && !"standard public schema".equals(comment)) {
							writeEntry(new DumpEntry("SCHEMA public", "COMMENT", "-", owner, "COMMENT ON SCHEMA public IS " + quoteLiteral(comment) + ";\n", null));
						}
						continue;
					}
					writeEntry(new DumpEntry(schemaName, "SCHEMA", "-", owner, "CREATE SCHEMA " + quotedSchemaName + ";\n", "ALTER SCHEMA " + quotedSchemaName + " OWNER TO " + resultSet.getString(4) + ";\n"));
					if (comment != null) {
						writeEntry(new DumpEntry("SCHEMA " + schemaName, "COMMENT", "-", owner, "COMMENT ON SCHEMA " + quotedSchemaName + " IS " + quoteLiteral(comment) + ";\n", null));
					}
				}
			}
		}
	}

	private void writeExtensions() throws Exception {
		final String sql = "SELECT e.extname, quote_ident(e.extname), quote_ident(n.nspname), obj_description(e.oid, 'pg_extension')"
				+ " FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace"
				+ " WHERE e.extname <> 'plpgsql' ORDER BY e.extname";
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(sql)) {
			while (resultSet.next()) {
				final String extensionName = resultSet.getString(1);
				final String quotedExtensionName = resultSet.getString(2);
				writeEntry(new DumpEntry(extensionName, "EXTENSION", "-", "-", "CREATE EXTENSION IF NOT EXISTS " + quotedExtensionName + " WITH SCHEMA " + resultSet.getString(3) + ";\n", null));
				final String comment = resultSet.getString(4);
				if (comment != null) {
					writeEntry(new DumpEntry("EXTENSION " + quotedExtensionName, "COMMENT", "-", "", "COMMENT ON EXTENSION " + quotedExtensionName + " IS " + quoteLiteral(comment) + ";\n", null));
				}
			}
		}
	}

	/**
	 * Enum types and domains, which may be used by the columns of the tables.
	 */
	private void writeTypes(final List<String> schemaNames) throws Exception {
		final String sql = "SELECT t.oid, n.nspname, t.typname, quote_ident(n.nspname) || '.' || quote_ident(t.typname), pg_get_userbyid(t.typowner), quote_ident(pg_get_userbyid(t.typowner)), t.typtype,"
				+ " (SELECT string_agg(quote_literal(e.enumlabel), E',\\n    ' ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid = t.oid),"
				+ " format_type(t.typbasetype, t.typtypmod), t.typnotnull, t.typdefault,"
				+ " CASE WHEN t.typcollation <> 0 AND t.typcollation <> bt.typcollation THEN (SELECT quote_ident(cn.nspname) || '.' || quote_ident(c.collname) FROM pg_collation c JOIN pg_namespace cn ON cn.oid = c.collnamespace WHERE c.oid = t.typcollation) END,"
				+ " obj_description(t.oid, 'pg_type')"
				+ " FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace LEFT JOIN pg_type bt ON bt.oid = t.typbasetype"
				+ " WHERE n.nspname = ANY(?) AND t.typtype IN ('e', 'd')"
				+ " AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e')"
				+ " ORDER BY n.nspname, t.typname";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setArray(1, createTextArray(schemaNames));
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final long typeOid = resultSet.getLong(1);
					final String schemaName = resultSet.getString(2);
					final String typeName = resultSet.getString(3);
					final String qualifiedTypeName = resultSet.getString(4);
					final String owner = resultSet.getString(5);
					final String quotedOwner = resultSet.getString(6);
					final String comment = resultSet.getString(13);
					if ("e".equals(resultSet.getString(7))) {
						final String labels = resultSet.getString(8);
						writeEntry(new DumpEntry(typeName, "TYPE", schemaName, owner,
								"CREATE TYPE " + qualifiedTypeName + " AS ENUM (\n" + (labels == null ? "" : "    " + labels + "\n") + ");\n",
								"ALTER TYPE " + qualifiedTypeName + " OWNER TO " + quotedOwner + ";\n"));
						if (comment != null) {
							writeEntry(new DumpEntry("TYPE " + typeName, "COMMENT", schemaName, owner, "COMMENT ON TYPE " + qualifiedTypeName + " IS " + quoteLiteral(comment) + ";\n", null));
						}
					} else {
						final StringBuilder definition = new StringBuilder("CREATE DOMAIN " + qualifiedTypeName + " AS " + resultSet.getString(9));
						if (resultSet.getString(12) != null) {
							definition.append(" COLLATE ").append(resultSet.getString(12));
						}
						if (resultSet.getString(11) != null) {
							definition.append(" DEFAULT ").append(resultSet.getString(11));
						}
						if (resultSet.getBoolean(10)) {
							definition.append(" NOT NULL");
						}
						for (final String[] constraint : readDomainConstraints(typeOid)) {
							definition.append("\n\tCONSTRAINT ").append(constraint[0]).append(" ").append(constraint[1]);
						}
						definition.append(";\n");
						writeEntry(new DumpEntry(typeName, "DOMAIN", schemaName, owner, definition.toString(), "ALTER DOMAIN " + qualifiedTypeName + " OWNER TO " + quotedOwner + ";\n"));
						if (comment != null) {
							writeEntry(new DumpEntry("DOMAIN " + typeName, "COMMENT", schemaName, owner, "COMMENT ON DOMAIN " + qualifiedTypeName + " IS " + quoteLiteral(comment) + ";\n", null));
						}
					}
				}
			}
		}
	}

	private List<String[]> readDomainConstraints(final long typeOid) throws SQLException {
		final List<String[]> constraints = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement("SELECT quote_ident(conname), pg_get_constraintdef(oid) FROM pg_constraint WHERE contypid = ? ORDER BY conname")) {
			statement.setLong(1, typeOid);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					constraints.add(new String[] { resultSet.getString(1), resultSet.getString(2) });
				}
			}
		}
		return constraints;
	}

	private void writeFunctions(final List<String> schemaNames, final Map<String, RelationItem> relationsByKey, final ProgressListener progressListener) throws Exception {
		final String sql = "SELECT n.nspname, p.proname, quote_ident(n.nspname) || '.' || quote_ident(p.proname), pg_get_function_identity_arguments(p.oid),"
				+ " pg_get_userbyid(p.proowner), quote_ident(pg_get_userbyid(p.proowner)), p.prokind, obj_description(p.oid, 'pg_proc'),"
				+ " oidvectortypes(p.proargtypes), pg_get_function_arguments(p.oid), pg_get_function_result(p.oid), l.lanname, quote_ident(l.lanname),"
				+ " p.provolatile, p.proisstrict, p.prosecdef, p.proleakproof, p.procost::text, p.prorows::text, p.proretset, p.proparallel, p.proconfig, p.prosrc, p.probin,"
				+ (serverMajorVersion >= 14 ? " CASE WHEN p.prosqlbody IS NOT NULL THEN pg_get_function_sqlbody(p.oid) END," : " NULL,")
				// Tables and views used by an SQL standard body must exist before the function
				+ " ARRAY(SELECT DISTINCT rn.nspname || '.' || rc.relname FROM pg_depend d JOIN pg_class rc ON rc.oid = d.refobjid JOIN pg_namespace rn ON rn.oid = rc.relnamespace"
				+ "   WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.refclassid = 'pg_class'::regclass)"
				+ " FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_language l ON l.oid = p.prolang"
				+ " WHERE n.nspname = ANY(?) AND p.prokind IN ('f', 'p', 'w')"
				+ " AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')"
				+ " ORDER BY n.nspname, p.proname, 9";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setArray(1, createTextArray(schemaNames));
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final String schemaName = resultSet.getString(1);
					final String functionName = resultSet.getString(2);
					final String qualifiedSignature = resultSet.getString(3) + "(" + resultSet.getString(4) + ")";
					final String owner = resultSet.getString(5);
					final boolean isProcedure = "p".equals(resultSet.getString(7));
					final String objectType = isProcedure ? "PROCEDURE" : "FUNCTION";
					final String comment = resultSet.getString(8);
					// pg_dump names a function by its argument types only, e.g. "add(integer, integer)"
					final String signature = functionName + "(" + resultSet.getString(9) + ")";

					final String definition = createFunctionDefinition(resultSet, objectType, resultSet.getString(3), isProcedure);

					final Array dependencies = resultSet.getArray(26);
					if (dependencies != null) {
						for (final String dependencyKey : (String[]) dependencies.getArray()) {
							final RelationItem dependency = relationsByKey.get(dependencyKey);
							if (dependency != null) {
								writeRelation(dependency, relationsByKey, progressListener);
							}
						}
					}

					writeEntry(new DumpEntry(signature, objectType, schemaName, owner, definition, "ALTER " + objectType + " " + qualifiedSignature + " OWNER TO " + resultSet.getString(6) + ";\n"));
					if (comment != null) {
						writeEntry(new DumpEntry(objectType + " " + signature, "COMMENT", schemaName, owner, "COMMENT ON " + objectType + " " + qualifiedSignature + " IS " + quoteLiteral(comment) + ";\n", null));
					}
				}
			}
		}
	}

	/**
	 * Creates the CREATE FUNCTION/PROCEDURE statement in the format of pg_dump.
	 */
	private static String createFunctionDefinition(final ResultSet resultSet, final String objectType, final String qualifiedName, final boolean isProcedure) throws SQLException {
		final String language = resultSet.getString(12);
		final StringBuilder definition = new StringBuilder();
		definition.append("CREATE ").append(objectType).append(" ").append(qualifiedName).append("(").append(resultSet.getString(10)).append(")");
		if (!isProcedure) {
			definition.append(" RETURNS ").append(resultSet.getString(11));
		}
		definition.append("\n    LANGUAGE ").append(resultSet.getString(13));
		if ("w".equals(resultSet.getString(7))) {
			definition.append(" WINDOW");
		}
		if ("i".equals(resultSet.getString(14))) {
			definition.append(" IMMUTABLE");
		} else if ("s".equals(resultSet.getString(14))) {
			definition.append(" STABLE");
		}
		if (resultSet.getBoolean(15)) {
			definition.append(" STRICT");
		}
		if (resultSet.getBoolean(16)) {
			definition.append(" SECURITY DEFINER");
		}
		if (resultSet.getBoolean(17)) {
			definition.append(" LEAKPROOF");
		}
		final String cost = resultSet.getString(18);
		final String defaultCost = "internal".equals(language) || "c".equals(language) ? "1" : "100";
		if (cost != null && !"0".equals(cost) && !defaultCost.equals(cost)) {
			definition.append(" COST ").append(cost);
		}
		final String rows = resultSet.getString(19);
		if (resultSet.getBoolean(20) && rows != null && !"0".equals(rows) && !"1000".equals(rows)) {
			definition.append(" ROWS ").append(rows);
		}
		if ("s".equals(resultSet.getString(21))) {
			definition.append(" PARALLEL SAFE");
		} else if ("r".equals(resultSet.getString(21))) {
			definition.append(" PARALLEL RESTRICTED");
		}

		final Array configArray = resultSet.getArray(22);
		if (configArray != null) {
			for (final String configItem : (String[]) configArray.getArray()) {
				final int equalsIndex = configItem.indexOf('=');
				if (equalsIndex > 0) {
					final String configName = configItem.substring(0, equalsIndex);
					final String configValue = configItem.substring(equalsIndex + 1);
					definition.append("\n    SET ").append(configName).append(" TO ");
					if ("search_path".equalsIgnoreCase(configName)) {
						// A list of schemas, each quoted
						final List<String> quotedSchemas = new ArrayList<>();
						for (final String schemaName : configValue.split(",")) {
							quotedSchemas.add(quoteLiteral(schemaName.trim()));
						}
						definition.append(String.join(", ", quotedSchemas));
					} else {
						definition.append(quoteLiteral(configValue));
					}
				}
			}
		}

		final String sqlBody = resultSet.getString(25);
		final String source = resultSet.getString(23);
		final String binary = resultSet.getString(24);
		if (sqlBody != null) {
			// SQL standard body (PostgreSQL 14+), e.g. "BEGIN ATOMIC ... END"
			definition.append("\n    ").append(sqlBody);
		} else if (binary != null && !binary.isEmpty() && !"-".equals(binary)) {
			// C function: library file and link symbol
			definition.append("\n    AS ").append(quoteLiteral(binary));
			if (source != null && !source.equals(resultSet.getString(2))) {
				definition.append(", ").append(quoteLiteral(source));
			}
		} else {
			definition.append("\n    AS ").append(quoteDollar(source == null ? "" : source));
		}
		definition.append(";\n");
		return definition.toString();
	}

	/**
	 * Dollar quoting like pg_dump: "$$", or "$_$", "$__$", ... if the text contains the quote.
	 */
	private static String quoteDollar(final String text) {
		String quote = "$$";
		while (text.contains(quote)) {
			quote = quote.substring(0, quote.length() - 1) + "_$";
		}
		return quote + text + quote;
	}

	/**
	 * Sequences of the schemas, except the ones of identity columns (written with their table) and of extensions.
	 */
	private List<SequenceInfo> readSequences(final List<String> schemaNames) throws Exception {
		final String sql = "SELECT n.nspname, c.relname, quote_ident(n.nspname) || '.' || quote_ident(c.relname), pg_get_userbyid(c.relowner), quote_ident(pg_get_userbyid(c.relowner)),"
				+ " format_type(s.seqtypid, NULL), s.seqstart, s.seqincrement, s.seqmin, s.seqmax, s.seqcache, s.seqcycle, obj_description(c.oid, 'pg_class'),"
				+ " (SELECT tn.nspname || '.' || t.relname FROM pg_depend d JOIN pg_class t ON t.oid = d.refobjid JOIN pg_namespace tn ON tn.oid = t.relnamespace"
				+ "   WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.refclassid = 'pg_class'::regclass AND d.deptype = 'a' AND d.refobjsubid > 0 LIMIT 1),"
				+ " (SELECT quote_ident(tn.nspname) || '.' || quote_ident(t.relname) || '.' || quote_ident(a.attname) FROM pg_depend d JOIN pg_class t ON t.oid = d.refobjid"
				+ "   JOIN pg_namespace tn ON tn.oid = t.relnamespace JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid"
				+ "   WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.refclassid = 'pg_class'::regclass AND d.deptype = 'a' AND d.refobjsubid > 0 LIMIT 1)"
				+ " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_sequence s ON s.seqrelid = c.oid"
				+ " WHERE c.relkind = 'S' AND n.nspname = ANY(?)"
				+ " AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('i', 'e'))"
				+ " ORDER BY n.nspname, c.relname";
		final List<SequenceInfo> sequences = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setArray(1, createTextArray(schemaNames));
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final SequenceInfo sequence = new SequenceInfo();
					sequence.schemaName = resultSet.getString(1);
					sequence.sequenceName = resultSet.getString(2);
					sequence.qualifiedName = resultSet.getString(3);
					sequence.owner = resultSet.getString(4);
					sequence.quotedOwner = resultSet.getString(5);
					sequence.definition = createSequenceOptions(resultSet.getString(6), resultSet.getLong(7), resultSet.getLong(8), resultSet.getLong(9), resultSet.getLong(10), resultSet.getLong(11), resultSet.getBoolean(12));
					sequence.comment = resultSet.getString(13);
					sequence.ownedByTable = resultSet.getString(14);
					sequence.ownedByColumn = resultSet.getString(15);
					sequences.add(sequence);
				}
			}
		}
		return sequences;
	}

	private void writeSequence(final SequenceInfo sequence) throws IOException {
		writeEntry(new DumpEntry(sequence.sequenceName, "SEQUENCE", sequence.schemaName, sequence.owner,
				"CREATE SEQUENCE " + sequence.qualifiedName + "\n" + sequence.definition + ";\n",
				"ALTER SEQUENCE " + sequence.qualifiedName + " OWNER TO " + sequence.quotedOwner + ";\n"));
		if (sequence.comment != null) {
			writeEntry(new DumpEntry("SEQUENCE " + sequence.sequenceName, "COMMENT", sequence.schemaName, sequence.owner, "COMMENT ON SEQUENCE " + sequence.qualifiedName + " IS " + quoteLiteral(sequence.comment) + ";\n", null));
		}
		if (sequence.ownedByColumn != null) {
			final DumpEntry ownedByEntry = new DumpEntry(sequence.sequenceName, "SEQUENCE OWNED BY", sequence.schemaName, sequence.owner, "ALTER SEQUENCE " + sequence.qualifiedName + " OWNED BY " + sequence.ownedByColumn + ";\n", null);
			if (writtenTables.contains(sequence.ownedByTable)) {
				writeEntry(ownedByEntry);
			} else {
				// The table is written later (or is not exported at all, then the statement is not written)
				ownedByEntry.requiredTable = sequence.ownedByTable;
				deferredSequenceOwnedByEntries.add(ownedByEntry);
			}
		}
	}

	/**
	 * Options of a sequence like pg_dump writes them, without the trailing semicolon or bracket.
	 */
	private static String createSequenceOptions(final String dataType, final long start, final long increment, final long minValue, final long maxValue, final long cache, final boolean cycle) {
		long typeMinValue = Long.MIN_VALUE;
		long typeMaxValue = Long.MAX_VALUE;
		if ("integer".equals(dataType)) {
			typeMinValue = Integer.MIN_VALUE;
			typeMaxValue = Integer.MAX_VALUE;
		} else if ("smallint".equals(dataType)) {
			typeMinValue = Short.MIN_VALUE;
			typeMaxValue = Short.MAX_VALUE;
		}
		final long defaultMinValue = increment > 0 ? 1 : typeMinValue;
		final long defaultMaxValue = increment > 0 ? typeMaxValue : -1;

		final StringBuilder options = new StringBuilder();
		if (!"bigint".equals(dataType)) {
			options.append("    AS ").append(dataType).append("\n");
		}
		options.append("    START WITH ").append(start).append("\n");
		options.append("    INCREMENT BY ").append(increment).append("\n");
		options.append(minValue == defaultMinValue ? "    NO MINVALUE\n" : "    MINVALUE " + minValue + "\n");
		options.append(maxValue == defaultMaxValue ? "    NO MAXVALUE\n" : "    MAXVALUE " + maxValue + "\n");
		options.append("    CACHE ").append(cache);
		if (cycle) {
			options.append("\n    CYCLE");
		}
		return options.toString();
	}

	private TableInfo resolveTable(final String tableName) throws Exception {
		final int separatorIndex = tableName.lastIndexOf('.');
		final String schemaName = separatorIndex < 0 ? null : tableName.substring(0, separatorIndex);
		final String plainTableName = separatorIndex < 0 ? tableName : tableName.substring(separatorIndex + 1);

		// Case insensitive like the table list, the exact name is preferred
		final String sql = "SELECT c.oid, n.nspname, c.relname, quote_ident(n.nspname) || '.' || quote_ident(c.relname), pg_get_userbyid(c.relowner), quote_ident(pg_get_userbyid(c.relowner)),"
				+ " c.relkind, c.relpersistence, c.reloptions, c.relispartition, obj_description(c.oid, 'pg_class'),"
				+ " CASE WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid) END,"
				+ " CASE WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid) END"
				+ " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
				+ " WHERE c.relkind IN ('r', 'p') AND lower(n.nspname) = lower(COALESCE(?, current_schema())) AND lower(c.relname) = lower(?)"
				+ " ORDER BY (n.nspname = COALESCE(?, current_schema()) AND c.relname = ?) DESC LIMIT 1";
		final TableInfo table = new TableInfo();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, schemaName);
			statement.setString(2, plainTableName);
			statement.setString(3, schemaName);
			statement.setString(4, plainTableName);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (!resultSet.next()) {
					throw new DbExportException("Table not found: " + tableName);
				}
				table.oid = resultSet.getLong(1);
				table.schemaName = resultSet.getString(2);
				table.tableName = resultSet.getString(3);
				table.qualifiedName = resultSet.getString(4);
				table.owner = resultSet.getString(5);
				table.quotedOwner = resultSet.getString(6);
				table.isPartitioned = "p".equals(resultSet.getString(7));
				table.isUnlogged = "u".equals(resultSet.getString(8));
				final Array reloptions = resultSet.getArray(9);
				table.options = reloptions == null ? null : (String[]) reloptions.getArray();
				table.isPartition = resultSet.getBoolean(10);
				table.comment = resultSet.getString(11);
				table.partitionKey = resultSet.getString(12);
				table.partitionBound = resultSet.getString(13);
			}
		}
		table.parents = readParentTables(table.oid, table.parentKeys);
		if (!table.isPartition) {
			// An inheriting table needs its parent tables (partitions are attached later)
			table.dependencyKeys.addAll(table.parentKeys);
		}
		return table;
	}

	private void writeTable(final TableInfo table) throws Exception {
		final List<String> parents = table.parents;
		final boolean isInheritanceChild = !table.isPartition && !parents.isEmpty();

		final List<String> tableElements = new ArrayList<>();
		final List<DumpEntry> columnComments = new ArrayList<>();
		final List<DumpEntry> identityEntries = new ArrayList<>();
		final String columnsSql = "SELECT a.attname, quote_ident(a.attname), format_type(a.atttypid, a.atttypmod), a.attnotnull, pg_get_expr(d.adbin, d.adrelid), a.attidentity, a.attgenerated, a.attislocal,"
				+ " CASE WHEN a.attcollation <> 0 AND a.attcollation <> t.typcollation THEN (SELECT quote_ident(cn.nspname) || '.' || quote_ident(c.collname) FROM pg_collation c JOIN pg_namespace cn ON cn.oid = c.collnamespace WHERE c.oid = a.attcollation) END,"
				+ " col_description(a.attrelid, a.attnum),"
				+ " CASE WHEN a.attidentity <> '' THEN pg_get_serial_sequence(?, a.attname) END"
				+ " FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum"
				+ " WHERE a.attrelid = ? AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";
		try (PreparedStatement statement = connection.prepareStatement(columnsSql)) {
			statement.setString(1, table.qualifiedName);
			statement.setLong(2, table.oid);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final String columnName = resultSet.getString(1);
					final String quotedColumnName = resultSet.getString(2);
					final String defaultValue = resultSet.getString(5);
					final String identity = resultSet.getString(6);
					final String generated = resultSet.getString(7);

					if (resultSet.getString(10) != null) {
						columnComments.add(new DumpEntry("COLUMN " + table.tableName + "." + columnName, "COMMENT", table.schemaName, table.owner,
								"COMMENT ON COLUMN " + table.qualifiedName + "." + quotedColumnName + " IS " + quoteLiteral(resultSet.getString(10)) + ";\n", null));
					}

					if (isInheritanceChild && !resultSet.getBoolean(8)) {
						// Inherited column, created by INHERITS
						continue;
					}

					final StringBuilder columnDefinition = new StringBuilder("    " + quotedColumnName + " " + resultSet.getString(3));
					if (resultSet.getString(9) != null) {
						columnDefinition.append(" COLLATE ").append(resultSet.getString(9));
					}
					if ("s".equals(generated) && defaultValue != null) {
						columnDefinition.append(" GENERATED ALWAYS AS (").append(defaultValue).append(") STORED");
					} else if (defaultValue != null) {
						if (defaultValue.contains("nextval(")) {
							// Like pg_dump: set after all tables and sequences, so the sequence exists
							defaultEntries.add(new DumpEntry(table.tableName + " " + columnName, "DEFAULT", table.schemaName, table.owner,
									"ALTER TABLE ONLY " + table.qualifiedName + " ALTER COLUMN " + quotedColumnName + " SET DEFAULT " + defaultValue + ";\n", null));
						} else {
							columnDefinition.append(" DEFAULT ").append(defaultValue);
						}
					}
					if (resultSet.getBoolean(4)) {
						columnDefinition.append(" NOT NULL");
					}
					tableElements.add(columnDefinition.toString());

					if (identity != null && !identity.isEmpty() && resultSet.getString(11) != null) {
						identityEntries.add(createIdentityEntry(table, quotedColumnName, "a".equals(identity), resultSet.getString(11)));
					}
				}
			}
		}

		readConstraints(table, tableElements);

		final StringBuilder definition = new StringBuilder();
		definition.append("CREATE ").append(table.isUnlogged ? "UNLOGGED " : "").append("TABLE ").append(table.qualifiedName).append(" (\n");
		definition.append(String.join(",\n", tableElements));
		definition.append(tableElements.isEmpty() ? "" : "\n").append(")");
		if (isInheritanceChild) {
			definition.append("\nINHERITS (").append(String.join(", ", parents)).append(")");
		}
		if (table.isPartitioned && table.partitionKey != null) {
			definition.append("\nPARTITION BY ").append(table.partitionKey);
		}
		if (table.options != null && table.options.length > 0) {
			final List<String> options = new ArrayList<>();
			for (final String option : table.options) {
				final int equalsIndex = option.indexOf('=');
				options.add(equalsIndex < 0 ? option : option.substring(0, equalsIndex + 1) + quoteLiteral(option.substring(equalsIndex + 1)));
			}
			definition.append("\nWITH (").append(String.join(", ", options)).append(")");
		}
		definition.append(";\n");

		writeEntry(new DumpEntry(table.tableName, "TABLE", table.schemaName, table.owner, definition.toString(), "ALTER TABLE " + table.qualifiedName + " OWNER TO " + table.quotedOwner + ";\n"));
		if (table.comment != null) {
			writeEntry(new DumpEntry("TABLE " + table.tableName, "COMMENT", table.schemaName, table.owner, "COMMENT ON TABLE " + table.qualifiedName + " IS " + quoteLiteral(table.comment) + ";\n", null));
		}
		writeEntries(columnComments);
		writeEntries(identityEntries);
		writtenTables.add(table.schemaName + "." + table.tableName);

		if (table.isPartition && table.partitionBound != null && !parents.isEmpty()) {
			tableAttachEntries.add(new DumpEntry(table.tableName, "TABLE ATTACH", table.schemaName, table.owner,
					"ALTER TABLE ONLY " + parents.get(0) + " ATTACH PARTITION " + table.qualifiedName + " " + table.partitionBound + ";\n", null));
		}

		readIndices(table);
		readTriggers(table);
	}

	private DumpEntry createIdentityEntry(final TableInfo table, final String quotedColumnName, final boolean always, final String sequenceName) throws SQLException {
		final String sql = "SELECT c.relname, quote_ident(n.nspname) || '.' || quote_ident(c.relname), format_type(s.seqtypid, NULL), s.seqstart, s.seqincrement, s.seqmin, s.seqmax, s.seqcache, s.seqcycle"
				+ " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_sequence s ON s.seqrelid = c.oid WHERE c.oid = ?::regclass";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, sequenceName);
			try (ResultSet resultSet = statement.executeQuery()) {
				resultSet.next();
				final String definition = "ALTER TABLE " + table.qualifiedName + " ALTER COLUMN " + quotedColumnName + " ADD GENERATED " + (always ? "ALWAYS" : "BY DEFAULT") + " AS IDENTITY (\n"
						+ "    SEQUENCE NAME " + resultSet.getString(2) + "\n"
						+ createSequenceOptions(resultSet.getString(3), resultSet.getLong(4), resultSet.getLong(5), resultSet.getLong(6), resultSet.getLong(7), resultSet.getLong(8), resultSet.getBoolean(9)) + "\n"
						+ ");\n";
				return new DumpEntry(resultSet.getString(1), "SEQUENCE", table.schemaName, table.owner, definition, null);
			}
		}
	}

	/**
	 * Reads the parent tables (inheritance or partitioned table).
	 *
	 * @return the quoted qualified names of the parent tables
	 */
	private List<String> readParentTables(final long tableOid, final List<String> parentKeys) throws SQLException {
		final List<String> parents = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement("SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname), n.nspname || '.' || c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhparent JOIN pg_namespace n ON n.oid = c.relnamespace WHERE i.inhrelid = ? ORDER BY i.inhseqno")) {
			statement.setLong(1, tableOid);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					parents.add(resultSet.getString(1));
					parentKeys.add(resultSet.getString(2));
				}
			}
		}
		return parents;
	}

	/**
	 * Check constraints are added to the table elements (like pg_dump), all other constraints are collected for their
	 * sections after the tables.
	 */
	private void readConstraints(final TableInfo table, final List<String> tableElements) throws SQLException {
		final String sql = "SELECT con.conname, quote_ident(con.conname), con.contype, pg_get_constraintdef(con.oid), con.conislocal, con.convalidated, con.conparentid, obj_description(con.oid, 'pg_constraint'), con.oid"
				+ " FROM pg_constraint con WHERE con.conrelid = ? ORDER BY con.conname";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setLong(1, table.oid);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final String constraintName = resultSet.getString(1);
					final String quotedConstraintName = resultSet.getString(2);
					final String constraintType = resultSet.getString(3);
					final String constraintDefinition = resultSet.getString(4);
					final boolean isLocal = resultSet.getBoolean(5);
					final String comment = resultSet.getString(8);
					final String alterTableStatement = "ALTER TABLE ONLY " + table.qualifiedName + "\n    ADD CONSTRAINT " + quotedConstraintName + " " + constraintDefinition + ";\n";

					final List<DumpEntry> targetEntries;
					if ("c".equals(constraintType)) {
						if (!isLocal) {
							// Inherited from the parent table
							continue;
						} else if (resultSet.getBoolean(6)) {
							tableElements.add("    CONSTRAINT " + quotedConstraintName + " " + constraintDefinition);
							targetEntries = null;
						} else {
							// NOT VALID check constraints are added after the tables by pg_dump
							targetEntries = constraintEntries;
						}
					} else if ("p".equals(constraintType) || "u".equals(constraintType) || "x".equals(constraintType)) {
						if (!isLocal && !table.isPartition) {
							continue;
						}
						targetEntries = constraintEntries;
					} else if ("f".equals(constraintType)) {
						if (resultSet.getLong(7) != 0) {
							// Foreign key of a partition, created by the foreign key of the partitioned table
							continue;
						}
						targetEntries = foreignKeyEntries;
					} else {
						// Constraint triggers are written as triggers
						continue;
					}

					if (targetEntries != null) {
						final DumpEntry constraintEntry = new DumpEntry(table.tableName + " " + constraintName, targetEntries == foreignKeyEntries ? "FK CONSTRAINT" : "CONSTRAINT", table.schemaName, table.owner, alterTableStatement, null);
						constraintEntry.key = "constraint:" + resultSet.getLong(9);
						if (resultSet.getLong(7) != 0) {
							// Constraint of a partition after the one of the partitioned table
							constraintEntry.dependencyKey = "constraint:" + resultSet.getLong(7);
						}
						targetEntries.add(constraintEntry);
					}
					if (comment != null) {
						final DumpEntry commentEntry = new DumpEntry("CONSTRAINT " + constraintName + " ON " + table.tableName, "COMMENT", table.schemaName, table.owner,
								"COMMENT ON CONSTRAINT " + quotedConstraintName + " ON " + table.qualifiedName + " IS " + quoteLiteral(comment) + ";\n", null);
						(targetEntries == null ? constraintEntries : targetEntries).add(commentEntry);
					}
				}
			}
		}
	}

	/**
	 * Indices, which do not belong to a constraint, and the attachment of partition indices to the index of the
	 * partitioned table.
	 */
	private void readIndices(final TableInfo table) throws SQLException {
		final String sql = "SELECT ic.relname, pg_get_indexdef(i.indexrelid), obj_description(i.indexrelid, 'pg_class'), quote_ident(n.nspname) || '.' || quote_ident(ic.relname),"
				+ " EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid AND c.conrelid = i.indrelid AND c.contype IN ('p', 'u', 'x')),"
				+ " (SELECT quote_ident(pn.nspname) || '.' || quote_ident(pc.relname) FROM pg_inherits inh JOIN pg_class pc ON pc.oid = inh.inhparent JOIN pg_namespace pn ON pn.oid = pc.relnamespace WHERE inh.inhrelid = i.indexrelid),"
				+ " i.indexrelid, (SELECT inh.inhparent FROM pg_inherits inh WHERE inh.inhrelid = i.indexrelid)"
				+ " FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_namespace n ON n.oid = ic.relnamespace"
				+ " WHERE i.indrelid = ? ORDER BY ic.relname";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setLong(1, table.oid);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final String indexName = resultSet.getString(1);
					final String qualifiedIndexName = resultSet.getString(4);
					if (!resultSet.getBoolean(5)) {
						final DumpEntry indexEntry = new DumpEntry(indexName, "INDEX", table.schemaName, table.owner, resultSet.getString(2) + ";\n", null);
						indexEntry.key = "index:" + resultSet.getLong(7);
						if (resultSet.getLong(8) != 0) {
							// Index of a partition after the one of the partitioned table
							indexEntry.dependencyKey = "index:" + resultSet.getLong(8);
						}
						indexEntries.add(indexEntry);
						if (resultSet.getString(3) != null) {
							indexEntries.add(new DumpEntry("INDEX " + indexName, "COMMENT", table.schemaName, table.owner, "COMMENT ON INDEX " + qualifiedIndexName + " IS " + quoteLiteral(resultSet.getString(3)) + ";\n", null));
						}
					}
					if (resultSet.getString(6) != null) {
						indexAttachEntries.add(new DumpEntry(indexName, "INDEX ATTACH", table.schemaName, table.owner, "ALTER INDEX " + resultSet.getString(6) + " ATTACH PARTITION " + qualifiedIndexName + ";\n", null));
					}
				}
			}
		}
	}

	private void readTriggers(final TableInfo table) throws SQLException {
		// Triggers of partitions cloned from the partitioned table (PostgreSQL 13+) are created by the parent's trigger
		final String sql = "SELECT t.tgname, quote_ident(t.tgname), pg_get_triggerdef(t.oid, false), obj_description(t.oid, 'pg_trigger')"
				+ " FROM pg_trigger t WHERE t.tgrelid = ? AND NOT t.tgisinternal" + (serverMajorVersion >= 13 ? " AND t.tgparentid = 0" : "")
				+ " ORDER BY t.tgname";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setLong(1, table.oid);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final String triggerName = resultSet.getString(1);
					triggerEntries.add(new DumpEntry(table.tableName + " " + triggerName, "TRIGGER", table.schemaName, table.owner, resultSet.getString(3) + ";\n", null));
					if (resultSet.getString(4) != null) {
						triggerEntries.add(new DumpEntry("TRIGGER " + triggerName + " ON " + table.tableName, "COMMENT", table.schemaName, table.owner,
								"COMMENT ON TRIGGER " + resultSet.getString(2) + " ON " + table.qualifiedName + " IS " + quoteLiteral(resultSet.getString(4)) + ";\n", null));
					}
				}
			}
		}
	}

	/**
	 * Views and materialized views of the schemas with the tables and views they use.
	 */
	private List<ViewInfo> readViews(final List<String> schemaNames) throws Exception {
		final String sql = "SELECT c.oid, n.nspname, c.relname, quote_ident(n.nspname) || '.' || quote_ident(c.relname), pg_get_userbyid(c.relowner), quote_ident(pg_get_userbyid(c.relowner)),"
				+ " c.relkind, pg_get_viewdef(c.oid), obj_description(c.oid, 'pg_class'),"
				+ " ARRAY(SELECT DISTINCT rn.nspname || '.' || rc.relname FROM pg_rewrite r"
				+ "   JOIN pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid AND d.refclassid = 'pg_class'::regclass"
				+ "   JOIN pg_class rc ON rc.oid = d.refobjid JOIN pg_namespace rn ON rn.oid = rc.relnamespace"
				+ "   WHERE r.ev_class = c.oid AND rc.oid <> c.oid)"
				+ " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
				+ " WHERE c.relkind IN ('v', 'm') AND n.nspname = ANY(?)"
				+ " AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')"
				+ " ORDER BY n.nspname, c.relname";
		final List<ViewInfo> views = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setArray(1, createTextArray(schemaNames));
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					final ViewInfo view = new ViewInfo();
					view.oid = resultSet.getLong(1);
					view.schemaName = resultSet.getString(2);
					view.viewName = resultSet.getString(3);
					view.qualifiedName = resultSet.getString(4);
					view.owner = resultSet.getString(5);
					view.quotedOwner = resultSet.getString(6);
					view.materialized = "m".equals(resultSet.getString(7));
					view.query = resultSet.getString(8);
					view.comment = resultSet.getString(9);
					final Array dependencies = resultSet.getArray(10);
					if (dependencies != null) {
						view.dependencyKeys.addAll(Arrays.asList((String[]) dependencies.getArray()));
					}
					views.add(view);
				}
			}
		}
		return views;
	}

	private void writeView(final ViewInfo view) throws Exception {
		final String objectType = view.materialized ? "MATERIALIZED VIEW" : "VIEW";
		String query = view.query.trim();
		if (query.endsWith(";")) {
			query = query.substring(0, query.length() - 1);
		}
		final String definition = "CREATE " + objectType + " " + view.qualifiedName + " AS\n " + query + (view.materialized ? "\n  WITH NO DATA" : "") + ";\n";

		writeEntry(new DumpEntry(view.viewName, objectType, view.schemaName, view.owner, definition, "ALTER " + objectType + " " + view.qualifiedName + " OWNER TO " + view.quotedOwner + ";\n"));
		if (view.comment != null) {
			writeEntry(new DumpEntry(objectType + " " + view.viewName, "COMMENT", view.schemaName, view.owner, "COMMENT ON " + objectType + " " + view.qualifiedName + " IS " + quoteLiteral(view.comment) + ";\n", null));
		}
		writeViewColumnComments(view.oid, view.schemaName, view.viewName, view.qualifiedName, view.owner);
	}

	private void writeViewColumnComments(final long viewOid, final String schemaName, final String viewName, final String qualifiedViewName, final String owner) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement("SELECT a.attname, quote_ident(a.attname), col_description(a.attrelid, a.attnum) FROM pg_attribute a WHERE a.attrelid = ? AND a.attnum > 0 AND NOT a.attisdropped AND col_description(a.attrelid, a.attnum) IS NOT NULL ORDER BY a.attnum")) {
			statement.setLong(1, viewOid);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					writeEntry(new DumpEntry("COLUMN " + viewName + "." + resultSet.getString(1), "COMMENT", schemaName, owner,
							"COMMENT ON COLUMN " + qualifiedViewName + "." + resultSet.getString(2) + " IS " + quoteLiteral(resultSet.getString(3)) + ";\n", null));
				}
			}
		}
	}

	/**
	 * Writes deferred entries sorted by schema and name like pg_dump. Comments stay directly after their object.
	 */
	private void writeEntries(final List<DumpEntry> entries) throws IOException {
		final List<List<DumpEntry>> groups = new ArrayList<>();
		for (final DumpEntry entry : entries) {
			if ("COMMENT".equals(entry.type) && !groups.isEmpty()) {
				groups.get(groups.size() - 1).add(entry);
			} else {
				final List<DumpEntry> group = new ArrayList<>();
				group.add(entry);
				groups.add(group);
			}
		}
		groups.sort(Comparator.comparing((final List<DumpEntry> group) -> group.get(0).schema).thenComparing(group -> group.get(0).getSortName()));
		final Map<String, List<DumpEntry>> groupsByKey = new HashMap<>();
		for (final List<DumpEntry> group : groups) {
			if (group.get(0).key != null) {
				groupsByKey.put(group.get(0).key, group);
			}
		}
		final Set<List<DumpEntry>> writtenGroups = Collections.newSetFromMap(new IdentityHashMap<>());
		for (final List<DumpEntry> group : groups) {
			writeEntryGroup(group, groupsByKey, writtenGroups);
		}
		entries.clear();
	}

	/**
	 * Writes an entry with its comments, but first the entry it depends on (e.g. the constraint of the partitioned
	 * table before the one of its partition).
	 */
	private void writeEntryGroup(final List<DumpEntry> group, final Map<String, List<DumpEntry>> groupsByKey, final Set<List<DumpEntry>> writtenGroups) throws IOException {
		if (!writtenGroups.add(group)) {
			return;
		}
		final String dependencyKey = group.get(0).dependencyKey;
		if (dependencyKey != null && groupsByKey.containsKey(dependencyKey)) {
			writeEntryGroup(groupsByKey.get(dependencyKey), groupsByKey, writtenGroups);
		}
		for (final DumpEntry entry : group) {
			if (entry.requiredTable == null || writtenTables.contains(entry.requiredTable)) {
				writeEntry(entry);
			}
		}
	}

	/**
	 * Writes an entry in the format of pg_dump: comment header, definition and owner statement.
	 */
	private void writeEntry(final DumpEntry entry) throws IOException {
		write("--\n");
		write("-- Name: " + entry.name + "; Type: " + entry.type + "; Schema: " + entry.schema + "; Owner: " + entry.owner + "\n");
		write("--\n\n");
		write(entry.definition);
		write("\n\n");
		if (entry.ownerStatement != null) {
			write(entry.ownerStatement);
			write("\n");
		}
	}

	private void write(final String text) throws IOException {
		writer.write(text);
	}

	private Array createTextArray(final List<String> values) throws SQLException {
		return connection.createArrayOf("text", values.toArray(new String[0]));
	}

	/**
	 * Quotes a string literal like pg_dump with standard_conforming_strings on.
	 */
	private static String quoteLiteral(final String value) {
		return "'" + value.replace("'", "''") + "'";
	}

	private static String getPostgresEncodingName(final Charset charset) {
		switch (charset.name().toUpperCase()) {
			case "UTF-8":
				return "UTF8";
			case "ISO-8859-1":
				return "LATIN1";
			case "ISO-8859-15":
				return "LATIN9";
			case "WINDOWS-1252":
				return "WIN1252";
			case "US-ASCII":
				return "SQL_ASCII";
			default:
				return charset.name().replace("-", "").toUpperCase();
		}
	}

	/**
	 * Table, sequence or view, which are sorted together by pg_dump.
	 */
	private abstract static class RelationItem {
		/** Whether the object was already written (or is being written) */
		private boolean written = false;
		/** Keys ("schema.name") of the objects this one depends on */
		protected final List<String> dependencyKeys = new ArrayList<>();

		protected abstract String getSchemaName();

		protected abstract String getRelationName();

		private String getKey() {
			return getSchemaName() + "." + getRelationName();
		}

		private List<String> getDependencyKeys() {
			return dependencyKeys;
		}
	}

	private static class TableInfo extends RelationItem {
		private long oid;
		private String schemaName;
		private String tableName;
		private String qualifiedName;
		private String owner;
		private String quotedOwner;
		private boolean isPartitioned;
		private boolean isPartition;
		private boolean isUnlogged;
		private String[] options;
		private String comment;
		private String partitionKey;
		private String partitionBound;
		/** Quoted qualified names of the parent tables */
		private List<String> parents = new ArrayList<>();
		/** "schema.table" of the parent tables */
		private final List<String> parentKeys = new ArrayList<>();

		@Override
		protected String getSchemaName() {
			return schemaName;
		}

		@Override
		protected String getRelationName() {
			return tableName;
		}
	}

	private static class ViewInfo extends RelationItem {
		private long oid;
		private String schemaName;
		private String viewName;
		private String qualifiedName;
		private String owner;
		private String quotedOwner;
		private boolean materialized;
		private String query;
		private String comment;

		@Override
		protected String getSchemaName() {
			return schemaName;
		}

		@Override
		protected String getRelationName() {
			return viewName;
		}
	}

	private static class SequenceInfo extends RelationItem {
		private String schemaName;
		private String sequenceName;
		private String qualifiedName;
		private String owner;
		private String quotedOwner;
		private String definition;
		private String comment;
		/** "schema.table" of the owning table, or null */
		private String ownedByTable;
		/** Quoted "schema.table.column" of the owning column, or null */
		private String ownedByColumn;

		@Override
		protected String getSchemaName() {
			return schemaName;
		}

		@Override
		protected String getRelationName() {
			return sequenceName;
		}
	}

	private static class DumpEntry {
		private final String name;
		private final String type;
		private final String schema;
		private final String owner;
		private final String definition;
		private final String ownerStatement;
		/** "schema.table" which must have been written before, or null */
		private String requiredTable = null;
		/** Key of this entry for dependencies, or null */
		private String key = null;
		/** Key of the entry, which must be written before this one, or null */
		private String dependencyKey = null;

		private DumpEntry(final String name, final String type, final String schema, final String owner, final String definition, final String ownerStatement) {
			this.name = name;
			this.type = type;
			this.schema = schema;
			this.owner = owner;
			this.definition = definition;
			this.ownerStatement = ownerStatement;
		}

		/**
		 * pg_dump sorts constraints and triggers by their own name, which is the second part of "table constraint".
		 */
		private String getSortName() {
			if (("CONSTRAINT".equals(type) || "FK CONSTRAINT".equals(type) || "TRIGGER".equals(type)) && name.contains(" ")) {
				final int separatorIndex = name.indexOf(' ');
				return name.substring(separatorIndex + 1) + " " + name.substring(0, separatorIndex);
			}
			return name;
		}
	}
}
