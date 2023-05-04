package de.soderer.utilities.db;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.NetworkUtilities;
import de.soderer.utilities.TextTable;
import de.soderer.utilities.TextUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.collection.CaseInsensitiveMap;
import de.soderer.utilities.collection.CaseInsensitiveSet;
import de.soderer.utilities.csv.CsvFormat;
import de.soderer.utilities.csv.CsvWriter;
import de.soderer.utilities.db.DatabaseConstraint.ConstraintType;
import de.soderer.utilities.http.HttpUtilities;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.JsonReader;

public class DbUtilities {
	public static final String DOWNLOAD_LOCATION_MYSQL = "https://dev.mysql.com/downloads/connector/j";
	public static final String DOWNLOAD_LOCATION_MARIADB = "https://downloads.mariadb.org/connector-java";
	public static final String DOWNLOAD_LOCATION_ORACLE = "http://www.oracle.com/technetwork/apps-tech/jdbc-112010-090769.html";
	public static final String DOWNLOAD_LOCATION_POSTGRESQL = "https://jdbc.postgresql.org/download.html";
	public static final String DOWNLOAD_LOCATION_FIREBIRD = "http://www.firebirdsql.org/en/jdbc-driver";
	public static final String DOWNLOAD_LOCATION_DERBY = "https://db.apache.org/derby/derby_downloads.html";
	public static final String DOWNLOAD_LOCATION_SQLITE = "https://bitbucket.org/xerial/sqlite-jdbc/downloads";
	public static final String DOWNLOAD_LOCATION_HSQL = "http://hsqldb.org/download";
	public static final String DOWNLOAD_LOCATION_MSSQL = "https://msdn.microsoft.com/de-de/library/mt683464(v=sql.110).aspx";

	public static final List<String> SQL_OPERATORS = Arrays.asList(new String[] { "+", "-", "*", "/", "%", "&", "|",
			"^", "=", "!=", ">", "<", ">=", "<=", "<>", "+=", "-=", "*=", "/=", "%=", "&=", "||", "^-=", "|*=" });

	public static final List<String> RESERVED_WORDS_ORACLE = Arrays.asList(new String[] { "access", "account",
			"activate", "add", "admin", "advise", "after", "all", "all_rows", "allocate", "alter", "analyze", "and",
			"any", "archive", "archivelog", "array", "as", "asc", "at", "audit", "authenticated", "authorization",
			"autoextend", "automatic", "backup", "become", "before", "begin", "between", "bfile", "bitmap", "blob",
			"block", "body", "by", "cache", "cache_instances", "cancel", "cascade", "cast", "cfile", "chained",
			"change", "char", "char_cs", "character", "check", "checkpoint", "choose", "chunk", "clear", "clob",
			"clone", "close", "close_cached_open_cursors", "cluster", "coalesce", "column", "columns", "comment",
			"commit", "committed", "compatibility", "compile", "complete", "composite_limit", "compress", "compute",
			"connect", "connect_time", "constraint", "constraints", "contents", "continue", "controlfile", "convert",
			"cost", "cpu_per_call", "cpu_per_session", "create", "current", "current_schema", "curren_user", "cursor",
			"cycle", "dangling", "database", "datafile", "datafiles", "dataobjno", "date", "dba", "dbhigh", "dblow",
			"dbmac", "deallocate", "debug", "dec", "decimal", "declare", "default", "deferrable", "deferred", "degree",
			"delete", "deref", "desc", "directory", "disable", "disconnect", "dismount", "distinct", "distributed",
			"dml", "double", "drop", "dump", "each", "else", "enable", "end", "enforce", "entry", "escape", "except",
			"exceptions", "exchange", "excluding", "exclusive", "execute", "exists", "expire", "explain", "extent",
			"extents", "externally", "failed_login_attempts", "false", "fast", "file", "first_rows", "flagger", "float",
			"flob", "flush", "for", "force", "foreign", "freelist", "freelists", "from", "full", "function", "global",
			"globally", "global_name", "grant", "group", "groups", "hash", "hashkeys", "having", "header", "heap",
			"identified", "idgenerators", "idle_time", "if", "immediate", "in", "including", "increment", "index",
			"indexed", "indexes", "indicator", "ind_partition", "initial", "initially", "initrans", "insert",
			"instance", "instances", "instead", "int", "integer", "intermediate", "intersect", "into", "is",
			"isolation", "isolation_level", "keep", "key", "kill", "label", "layer", "less", "level", "library", "like",
			"limit", "link", "list", "lob", "local", "lock", "locked", "log", "logfile", "logging",
			"logical_reads_per_call", "logical_reads_per_session", "long", "manage", "master", "max", "maxarchlogs",
			"maxdatafiles", "maxextents", "maxinstances", "maxlogfiles", "maxloghistory", "maxlogmembers", "maxsize",
			"maxtrans", "maxvalue", "min", "member", "minimum", "minextents", "minus", "minvalue", "mlslabel",
			"mls_label_format", "mode", "modify", "mount", "move", "mts_dispatchers", "multiset", "national", "nchar",
			"nchar_cs", "nclob", "needed", "nested", "network", "new", "next", "noarchivelog", "noaudit", "nocache",
			"nocompress", "nocycle", "noforce", "nologging", "nomaxvalue", "nominvalue", "none", "noorder",
			"nooverride", "noparallel", "noreverse", "normal", "nosort", "not", "nothing", "nowait", "null", "number",
			"numeric", "nvarchar2", "object", "objno", "objno_reuse", "of", "off", "offline", "oid", "oidindex", "old",
			"on", "online", "only", "opcode", "open", "optimal", "optimizer_goal", "option", "or", "order",
			"organization", "oslabel", "overflow", "own", "package", "parallel", "partition", "password",
			"password_grace_time", "password_life_time", "password_lock_time", "password_reuse_max",
			"password_reuse_time", "password_verify_function", "pctfree", "pctincrease", "pctthreshold", "pctused",
			"pctversion", "percent", "permanent", "plan", "plsql_debug", "post_transaction", "precision", "preserve",
			"primary", "prior", "private", "private_sga", "privilege", "privileges", "procedure", "profile", "public",
			"purge", "queue", "quota", "range", "raw", "rba", "read", "readup", "real", "rebuild", "recover",
			"recoverable", "recovery", "ref", "references", "referencing", "refresh", "rename", "replace", "reset",
			"resetlogs", "resize", "resource", "restricted", "return", "returning", "reuse", "reverse", "revoke",
			"role", "roles", "rollback", "row", "rowid", "rownum", "rows", "rule", "sample", "savepoint", "sb4",
			"scan_instances", "schema", "scn", "scope", "sd_all", "sd_inhibit", "sd_show", "segment", "seg_block",
			"seg_file", "select", "sequence", "serializable", "session", "session_cached_cursors", "sessions_per_user",
			"set", "share", "shared", "shared_pool", "shrink", "size", "skip", "skip_unusable_indexes", "smallint",
			"snapshot", "some", "sort", "specification", "split", "sql_trace", "standby", "start", "statement_id",
			"statistics", "stop", "storage", "store", "structure", "successful", "switch", "sys_op_enforce_not_null$",
			"sys_op_ntcimg$", "synonym", "sysdate", "sysdba", "sysoper", "system", "table", "tables", "tablespace",
			"tablespace_no", "tabno", "temporary", "than", "the", "then", "thread", "timestamp", "time", "to",
			"toplevel", "trace", "tracing", "transaction", "transitional", "trigger", "triggers", "true", "truncate",
			"tx", "type", "ub2", "uba", "uid", "unarchived", "undo", "union", "unique", "unlimited", "unlock",
			"unrecoverable", "until", "unusable", "unused", "updatable", "update", "usage", "use", "user", "using",
			"validate", "validation", "value", "values", "varchar", "varchar2", "varying", "view", "when", "whenever",
			"where", "with", "without", "work", "write", "writedown", "writeup", "xid", "year", "zone" });

	public static final List<String> RESERVED_WORDS_MYSSQL_MARIADB = Arrays.asList(new String[] { "accessible",
			"account", "action", "add", "after", "against", "aggregate", "algorithm", "all", "alter", "always",
			"analyse", "analyze", "and", "any", "as", "asc", "ascii", "asensitive", "at", "autoextend_size",
			"auto_increment", "avg", "avg_row_length", "backup", "before", "begin", "between", "bigint", "binary",
			"binlog", "bit", "blob", "block", "bool", "boolean", "both", "btree", "by", "byte", "cache", "call",
			"cascade", "cascaded", "case", "catalog_name", "chain", "change", "changed", "channel", "char", "character",
			"charset", "check", "checksum", "cipher", "class_origin", "client", "close", "coalesce", "code", "collate",
			"collation", "column", "columns", "column_format", "column_name", "comment", "commit", "committed",
			"compact", "completion", "compressed", "compression", "concurrent", "condition", "connection", "consistent",
			"constraint", "constraint_catalog", "constraint_name", "constraint_schema", "contains", "context",
			"continue", "convert", "cpu", "create", "cross", "cube", "current", "current_date", "current_time",
			"current_timestamp", "current_user", "cursor", "cursor_name", "data", "database", "databases", "datafile",
			"date", "datetime", "day", "day_hour", "day_microsecond", "day_minute", "day_second", "deallocate", "dec",
			"decimal", "declare", "default", "default_auth", "definer", "delayed", "delay_key_write", "delete", "desc",
			"describe", "des_key_file", "deterministic", "diagnostics", "directory", "disable", "discard", "disk",
			"distinct", "distinctrow", "div", "do", "double", "drop", "dual", "dumpfile", "duplicate", "dynamic",
			"each", "else", "elseif", "enable", "enclosed", "encryption", "end", "ends", "engine", "engines", "enum",
			"error", "errors", "escape", "escaped", "event", "events", "every", "exchange", "execute", "exists", "exit",
			"expansion", "expire", "explain", "export", "extended", "extent_size", "false", "fast", "faults", "fetch",
			"fields", "file", "file_block_size", "filter", "first", "fixed", "float", "float4", "float8", "flush",
			"follows", "for", "force", "foreign", "format", "found", "from", "full", "fulltext", "function", "general",
			"generated", "geometry", "geometrycollection", "get", "get_format", "global", "grant", "grants", "group",
			"group_replication", "handler", "hash", "having", "help", "high_priority", "host", "hosts", "hour",
			"hour_microsecond", "hour_minute", "hour_second", "identified", "if", "ignore", "ignore_server_ids",
			"import", "in", "index", "indexes", "infile", "initial_size", "inner", "inout", "insensitive", "insert",
			"insert_method", "install", "instance", "int", "int1", "int2", "int3", "int4", "int8", "integer",
			"interval", "into", "invoker", "io", "io_after_gtids", "io_before_gtids", "io_thread", "ipc", "is",
			"isolation", "issuer", "iterate", "join", "json", "key", "keys", "key_block_size", "kill", "language",
			"last", "leading", "leave", "leaves", "left", "less", "level", "like", "limit", "linear", "lines",
			"linestring", "list", "load", "local", "localtime", "localtimestamp", "lock", "locks", "logfile", "logs",
			"long", "longblob", "longtext", "loop", "low_priority", "master", "master_auto_position", "master_bind",
			"master_connect_retry", "master_delay", "master_heartbeat_period", "master_host", "master_log_file",
			"master_log_pos", "master_password", "master_port", "master_retry_count", "master_server_id", "master_ssl",
			"master_ssl_ca", "master_ssl_capath", "master_ssl_cert", "master_ssl_cipher", "master_ssl_crl",
			"master_ssl_crlpath", "master_ssl_key", "master_ssl_verify_server_cert", "master_tls_version",
			"master_user", "match", "maxvalue", "max_connections_per_hour", "max_queries_per_hour", "max_rows",
			"max_size", "max_statement_time", "max_updates_per_hour", "max_user_connections", "medium", "mediumblob",
			"mediumint", "mediumtext", "memory", "merge", "message_text", "microsecond", "middleint", "migrate",
			"minute", "minute_microsecond", "minute_second", "min_rows", "mod", "mode", "modifies", "modify", "month",
			"multilinestring", "multipoint", "multipolygon", "mutex", "mysql_errno", "name", "names", "national",
			"natural", "nchar", "ndb", "ndbcluster", "never", "new", "next", "no", "nodegroup", "nonblocking", "none",
			"not", "no_wait", "no_write_to_binlog", "null", "number", "numeric", "nvarchar", "offset", "old_password",
			"on", "one", "only", "open", "optimize", "optimizer_costs", "option", "optionally", "options", "or",
			"order", "out", "outer", "outfile", "owner", "pack_keys", "page", "parser", "parse_gcol_expr", "partial",
			"partition", "partitioning", "partitions", "password", "phase", "plugin", "plugins", "plugin_dir", "point",
			"polygon", "port", "precedes", "precision", "prepare", "preserve", "prev", "primary", "privileges",
			"procedure", "processlist", "profile", "profiles", "proxy", "purge", "quarter", "query", "quick", "range",
			"read", "reads", "read_only", "read_write", "real", "rebuild", "recover", "redofile", "redo_buffer_size",
			"redundant", "references", "regexp", "relay", "relaylog", "relay_log_file", "relay_log_pos", "relay_thread",
			"release", "reload", "remove", "rename", "reorganize", "repair", "repeat", "repeatable", "replace",
			"replicate_do_db", "replicate_do_table", "replicate_ignore_db", "replicate_ignore_table",
			"replicate_rewrite_db", "replicate_wild_do_table", "replicate_wild_ignore_table", "replication", "require",
			"reset", "resignal", "restore", "restrict", "resume", "return", "returned_sqlstate", "returns", "reverse",
			"revoke", "right", "rlike", "rollback", "rollup", "rotate", "routine", "row", "rows", "row_count",
			"row_format", "rtree", "savepoint", "schedule", "schema", "schemas", "schema_name", "second",
			"second_microsecond", "security", "select", "sensitive", "separator", "serial", "serializable", "server",
			"session", "set", "share", "show", "shutdown", "signal", "signed", "simple", "slave", "slow", "smallint",
			"snapshot", "socket", "some", "soname", "sounds", "source", "spatial", "specific", "sql", "sqlexception",
			"sqlstate", "sqlwarning", "sql_after_gtids", "sql_after_mts_gaps", "sql_before_gtids", "sql_big_result",
			"sql_buffer_result", "sql_cache", "sql_calc_found_rows", "sql_no_cache", "sql_small_result", "sql_thread",
			"sql_tsi_day", "sql_tsi_hour", "sql_tsi_minute", "sql_tsi_month", "sql_tsi_quarter", "sql_tsi_second",
			"sql_tsi_week", "sql_tsi_year", "ssl", "stacked", "start", "starting", "starts", "stats_auto_recalc",
			"stats_persistent", "stats_sample_pages", "status", "stop", "storage", "stored", "straight_join", "string",
			"subclass_origin", "subject", "subpartition", "subpartitions", "super", "suspend", "swaps", "switches",
			"table", "tables", "tablespace", "table_checksum", "table_name", "temporary", "temptable", "terminated",
			"text", "than", "then", "time", "timestamp", "timestampadd", "timestampdiff", "tinyblob", "tinyint",
			"tinytext", "to", "trailing", "transaction", "trigger", "triggers", "true", "truncate", "type", "types",
			"uncommitted", "undefined", "undo", "undofile", "undo_buffer_size", "unicode", "uninstall", "union",
			"unique", "unknown", "unlock", "unsigned", "until", "update", "upgrade", "usage", "use", "user",
			"user_resources", "use_frm", "using", "utc_date", "utc_time", "utc_timestamp", "validation", "value",
			"values", "varbinary", "varchar", "varcharacter", "variables", "varying", "view", "virtual", "wait",
			"warnings", "week", "weight_string", "when", "where", "while", "with", "without", "work", "wrapper",
			"write", "x509", "xa", "xid", "xml", "xor", "year", "year_month", "zerofill" });

	public static final List<String> RESERVED_WORDS_DERBY = Arrays.asList(new String[] { "add", "all", "allocate",
			"alter", "and", "any", "are", "as", "asc", "assertion", "at", "authorization", "avg", "begin", "between",
			"bit", "boolean", "both", "by", "call", "cascade", "cascaded", "case", "cast", "char", "character", "check",
			"close", "collate", "collation", "column", "commit", "connect", "connection", "constraint", "constraints",
			"continue", "convert", "corresponding", "count", "create", "current", "current_date", "current_time",
			"current_timestamp", "current_user", "cursor", "deallocate", "dec", "decimal", "declare", "deferrable",
			"deferred", "delete", "desc", "describe", "diagnostics", "disconnect", "distinct", "double", "drop", "else",
			"end", "endexec", "escape", "except", "exception", "exec", "execute", "exists", "explain", "external",
			"false", "fetch", "first", "float", "for", "foreign", "found", "from", "full", "function", "get",
			"get_current_connection", "global", "go", "goto", "grant", "group", "having", "hour", "identity",
			"immediate", "in", "indicator", "initially", "inner", "inout", "input", "insensitive", "insert", "int",
			"integer", "intersect", "into", "is", "isolation", "join", "key", "last", "left", "like", "longint",
			"lower", "ltrim", "match", "max", "min", "minute", "national", "natural", "nchar", "nvarchar", "next", "no",
			"not", "null", "nullif", "numeric", "of", "on", "only", "open", "option", "or", "order", "out", "outer",
			"output", "overlaps", "pad", "partial", "prepare", "preserve", "primary", "prior", "privileges",
			"procedure", "public", "read", "real", "references", "relative", "restrict", "revoke", "right", "rollback",
			"rows", "rtrim", "schema", "scroll", "second", "select", "session_user", "set", "smallint", "some", "space",
			"sql", "sqlcode", "sqlerror", "sqlstate", "substr", "substring", "sum", "system_user", "table", "temporary",
			"timezone_hour", "timezone_minute", "to", "trailing", "transaction", "translate", "translation", "true",
			"union", "unique", "unknown", "update", "upper", "user", "using", "values", "varchar", "varying", "view",
			"whenever", "where", "with", "work", "write", "xml", "xmlexists", "xmlparse", "xmlserialize", "year" });

	/**
	 * DevNull used to prevent creation of file "derby.log"
	 */
	public static final OutputStream DEV_NULL = new OutputStream() {
		@Override
		public void write(final int b) {
			// Do nothing
		}
	};

	/**
	 * In an Oracle DB the statement "SELECT CURRENT_TIMESTAMP FROM DUAL" return this special Oracle type "oracle.sql.TIMESTAMPTZ",
	 * which is not listed in java.sql.Types, but can be read via ResultSet.getTimestamp(i) into a normal java.sql.Timestamp object
	 */
	public static final int ORACLE_TIMESTAMPTZ_TYPECODE = -101;

	public enum DbVendor {
		Oracle("oracle.jdbc.OracleDriver", 1521, "SELECT 1 FROM DUAL"),
		MySQL("com.mysql.cj.jdbc.Driver", 3306, "SELECT 1"),
		MariaDB("org.mariadb.jdbc.Driver", 3306, "SELECT 1"),
		PostgreSQL("org.postgresql.Driver", 5432, "SELECT 1"),
		Firebird("org.firebirdsql.jdbc.FBDriver", 3050, "SELECT 1 FROM RDB$RELATION_FIELDS ROWS 1"),
		SQLite("org.sqlite.JDBC", 0, "SELECT 1"),
		Derby("org.apache.derby.jdbc.EmbeddedDriver", 0, "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
		HSQL("org.hsqldb.jdbc.JDBCDriver", 0, "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
		Cassandra("com.simba.cassandra.jdbc42.Driver", 9042, ""),
		MsSQL("com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433, "SELECT 1");

		public static DbVendor getDbVendorByName(final String dbVendorName) throws Exception {
			for (final DbVendor dbVendor : DbVendor.values()) {
				if (dbVendor.toString().equalsIgnoreCase(dbVendorName)) {
					return dbVendor;
				}
			}
			if ("postgres".equalsIgnoreCase(dbVendorName)) {
				return DbVendor.PostgreSQL;
			} else if ("hypersql".equalsIgnoreCase(dbVendorName)) {
				return DbVendor.HSQL;
			} else {
				throw new Exception("Invalid db vendor: " + dbVendorName);
			}
		}

		private final String driverClassName;
		private final int defaultPort;
		private final String testStatement;

		DbVendor(final String driverClassName, final int defaultPort, final String testStatement) {
			this.driverClassName = driverClassName;
			this.defaultPort = defaultPort;
			this.testStatement = testStatement;
		}

		public String getDriverClassName() {
			return driverClassName;
		}

		public int getDefaultPort() {
			return defaultPort;
		}

		public String getTestStatement() {
			return testStatement;
		}
	}

	public static String generateUrlConnectionString(final DbVendor dbVendor, String dbServerHostname, int dbServerPort, String dbName, final boolean secureConnection, final File trustStoreFile, final char[] trustStorePassword, final String trustedCN) throws Exception {
		if (secureConnection && dbVendor != DbVendor.Oracle && dbVendor != DbVendor.MySQL && dbVendor != DbVendor.MariaDB) {
			throw new Exception("Secure connection is only supported for db vendors Oracle, MySQL, MariaDB");
		}

		if (DbVendor.Oracle == dbVendor) {
			if (!secureConnection) {
				if (dbName.startsWith("/")) {
					// Some Oracle databases only accept the SID separated by a "/"
					return "jdbc:oracle:thin:@" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + dbName;
				} else {
					return "jdbc:oracle:thin:@" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + ":" + dbName;
				}
			} else {
				// For trustedCN you must also enforce server certificate DN check for oracle connections, default is false
				//props.setProperty("oracle.net.ssl_server_dn_match", "true");

				return "jdbc:oracle:thin:@"
				+ "(DESCRIPTION="
				+ "(ADDRESS=(PROTOCOL=TCPS)(HOST=" + dbServerHostname + ")(PORT=" + dbServerPort + "))"
				+ "(CONNECT_DATA=(SERVICE_NAME=" + dbName + "))"
				+ (Utilities.isNotBlank(trustedCN) ? "(SECURITY=(ssl_server_cert_dn=\"" + trustedCN + "\"))" : "")
				+ ")";
			}
		} else if (DbVendor.MySQL == dbVendor) {
			if (secureConnection) {
				if (trustStoreFile != null) {
					if (trustStorePassword != null) {
						return "jdbc:mysql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true&trustStore=" + trustStoreFile.getAbsolutePath() + "&trustStorePassword=" + new String(trustStorePassword);
					} else {
						return "jdbc:mysql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true&trustStore=" + trustStoreFile.getAbsolutePath();
					}
				} else {
					return "jdbc:mysql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true&trustServerCertificate=true";
				}
			} else {
				return "jdbc:mysql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
			}
		} else if (DbVendor.MariaDB == dbVendor) {
			if (secureConnection) {
				if (trustStoreFile != null) {
					if (trustStorePassword != null) {
						return "jdbc:mariadb://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true&trustStore=" + trustStoreFile.getAbsolutePath() + "&trustStorePassword=" + new String(trustStorePassword);
					} else {
						return "jdbc:mariadb://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true&trustStore=" + trustStoreFile.getAbsolutePath();
					}
				} else {
					return "jdbc:mariadb://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true&trustServerCertificate=true";
				}
			} else {
				return "jdbc:mariadb://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?useEncoding=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
			}
		} else if (DbVendor.PostgreSQL == dbVendor) {
			return "jdbc:postgresql://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName;
		} else if (DbVendor.SQLite == dbVendor) {
			return "jdbc:sqlite:" + Utilities.replaceUsersHome(dbName);
		} else if (DbVendor.Derby == dbVendor) {
			return "jdbc:derby:" + Utilities.replaceUsersHome(dbName);
		} else if (DbVendor.Firebird == dbVendor) {
			return "jdbc:firebirdsql:" + dbServerHostname + "/" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + ":" + Utilities.replaceUsersHome(dbName);
		} else if (DbVendor.HSQL == dbVendor) {
			dbName = Utilities.replaceUsersHome(dbName);
			if (dbName.startsWith("/") || dbName.matches(".\\:\\\\.*")) {
				return "jdbc:hsqldb:file:" + dbName + ";shutdown=true";
			} else if (Utilities.isNotBlank(dbServerHostname)) {
				if (!dbServerHostname.toLowerCase().startsWith("http")) {
					dbServerHostname = "http://" + dbServerHostname;
				}
				if (dbServerHostname.toLowerCase().startsWith("https://") && dbServerPort == 443) {
					dbServerPort = -1;
				} else if (dbServerHostname.toLowerCase().startsWith("http://") && dbServerPort == 80) {
					dbServerPort = -1;
				}
				return "jdbc:hsqldb:" + dbServerHostname + (dbServerPort <= 0 ? "" : ":" + dbServerPort) + "/" + dbName;
			} else {
				return "jdbc:hsqldb:mem:" + dbName;
			}
		} else if (DbVendor.Cassandra == dbVendor) {
			return "jdbc:cassandra://" + dbServerHostname + ":" + (dbServerPort <= 0 ? dbVendor.defaultPort : dbServerPort) + "/" + dbName + "?primarydc=DC1&backupdc=DC2&consistency=QUORUM";
		} else {
			throw new Exception("Unknown db vendor");
		}
	}

	public static Connection createNewDatabase(final DbVendor dbVendor, String dbPath) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}

		if (dbVendor == DbVendor.Derby) {
			// Prevent creation of file "derby.log"
			System.setProperty("derby.stream.error.field", "de.soderer.utilities.DbUtilities.DEV_NULL");
		}

		Class.forName(dbVendor.getDriverClassName());

		if (dbVendor == DbVendor.SQLite) {
			dbPath = Utilities.replaceUsersHome(dbPath);
			if (new File(dbPath).exists()) {
				throw new Exception("SQLite db file '" + dbPath + "' already exists");
			}
			return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbPath, false, null, null, null));
		} else if (dbVendor == DbVendor.Derby) {
			dbPath = Utilities.replaceUsersHome(dbPath);
			if (new File(dbPath).exists()) {
				throw new Exception("Derby db directory '" + dbPath + "' already exists");
			}
			return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbPath, false, null, null, null) + ";create=true");
		} else if (dbVendor == DbVendor.HSQL) {
			dbPath = Utilities.replaceUsersHome(dbPath);
			if (dbPath.startsWith("/")) {
				if (FileUtilities.getFilesByPattern(new File(dbPath.substring(0, dbPath.lastIndexOf("/"))), dbPath.substring(dbPath.lastIndexOf("/") + 1).replace(".", "\\.") + "\\..*", false).size() > 0) {
					throw new Exception("HSQL db '" + dbPath + "' already exists");
				}
			}

			// Logger must be kept in a local variable for making it work
			final Logger dbLogger = Logger.getLogger("hsqldb.db");
			dbLogger.setLevel(Level.WARNING);

			return DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbPath, false, null, null, null));
		} else {
			throw new Exception("Invalid db vendor '" + dbVendor.toString() + "'. Only SQLite, HSQL or Derby db can be created this way.");
		}
	}

	public static boolean deleteDatabase(final DbVendor dbVendor, String dbPath) throws Exception {
		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		}

		if (dbVendor == DbVendor.SQLite) {
			dbPath = Utilities.replaceUsersHome(dbPath);
			if (new File(dbPath).exists()) {
				new File(dbPath).delete();
				return true;
			} else {
				return false;
			}
		} else if (dbVendor == DbVendor.Derby) {
			dbPath = Utilities.replaceUsersHome(dbPath);
			if (new File(dbPath).exists()) {
				try {
					DbUtilities.shutDownDerbyDb(dbPath);
				} catch (@SuppressWarnings("unused") final Exception e) {
					// do nothing
				}
				return Utilities.delete(new File(dbPath));
			} else {
				return false;
			}
		} else if (dbVendor == DbVendor.HSQL) {
			dbPath = Utilities.replaceUsersHome(dbPath);
			if (dbPath.startsWith("/")) {
				final File baseDirectory = new File(dbPath.substring(0, dbPath.lastIndexOf("/")));
				final String basename = dbPath.substring(dbPath.lastIndexOf("/") + 1);
				for (final File fileToDelete : baseDirectory.listFiles()) {
					if (fileToDelete.getName().startsWith(basename)) {
						if (!Utilities.delete(fileToDelete)) {
							throw new Exception("Cannot delete db file '" + fileToDelete.getAbsolutePath() + "'");
						}
					}
				}
				return true;
			} else if (dbPath.matches(".\\:\\\\.*")) {
				final File baseDirectory = new File(dbPath.substring(0, dbPath.lastIndexOf("\\")));
				final String basename = dbPath.substring(dbPath.lastIndexOf("\\") + 1);
				for (final File fileToDelete : baseDirectory.listFiles()) {
					if (fileToDelete.getName().startsWith(basename)) {
						if (!Utilities.delete(fileToDelete)) {
							throw new Exception("Cannot delete db file '" + fileToDelete.getAbsolutePath() + "'");
						}
					}
				}
				return true;
			} else {
				return false;
			}
		} else {
			throw new Exception("Invalid db vendor '" + dbVendor.toString() + "'. Only SQLite, HSQL or Derby db can be deleted this way.");
		}
	}

	@SuppressWarnings("resource")
	public static Connection createConnection(final DbDefinition dbDefinition, final boolean retryOnError) throws Exception {
		final DbVendor dbVendor = dbDefinition.getDbVendor();
		final String hostnameAndPort = dbDefinition.getHostnameAndPort();
		String dbName = dbDefinition.getDbName();
		final String userName = dbDefinition.getUsername();
		final char[] password = dbDefinition.getPassword();
		final boolean secureConnection = dbDefinition.isSecureConnection();
		File trustStoreFile = dbDefinition.getTrustStoreFile();
		final char[] trustStorePassword = dbDefinition.getTrustStorePassword();

		if (dbVendor == null) {
			throw new Exception("Unknown db vendor");
		} else if (Utilities.isEmpty(hostnameAndPort) && dbVendor != DbVendor.HSQL && dbVendor != DbVendor.SQLite && dbVendor != DbVendor.Derby){
			throw new Exception("Cannot create db connection: Missing hostname");
		} else if (Utilities.isEmpty(dbName)){
			throw new Exception("Cannot create db connection: Missing dbName");
		} else if (trustStoreFile != null && !trustStoreFile.exists()){
			throw new Exception("Cannot create db connection: Configured trustStoreFile '" + trustStoreFile.getAbsolutePath() + "' does not exist");
		}

		if (secureConnection && dbVendor != DbVendor.Oracle && dbVendor != DbVendor.MySQL && dbVendor != DbVendor.MariaDB) {
			throw new Exception("Secure connection is only supported for db vendors Oracle, MySQL, MariaDB");
		}

		try {
			if (dbVendor == DbVendor.Derby) {
				// Prevent creation of file "derby.log"
				System.setProperty("derby.stream.error.field", "de.soderer.utilities.DbUtilities.DEV_NULL");
			}

			Class.forName(dbVendor.getDriverClassName());
		} catch (final Exception e) {
			throw new Exception("Cannot create db connection, caused by unknown DriverClassName: " + e.getMessage());
		}

		try {
			Connection connection;
			if (dbVendor == DbVendor.SQLite) {
				dbName = Utilities.replaceUsersHome(dbName);
				if (!new File(dbName).exists()) {
					throw new DbNotExistsException("SQLite db file '" + dbName + "' is not available");
				} else if (!new File(dbName).isFile()) {
					throw new Exception("SQLite db file '" + dbName + "' is not a file");
				}
				connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbName, false, null, null, null));
			} else if (dbVendor == DbVendor.Derby) {
				dbName = Utilities.replaceUsersHome(dbName);
				if (!new File(dbName).exists()) {
					throw new DbNotExistsException("Derby db directory '" + dbName + "' is not available");
				} else if (!new File(dbName).isDirectory()) {
					throw new Exception("Derby db directory '" + dbName + "' is not a directory");
				}
				connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, "", 0, dbName, false, null, null, null));
			} else if (dbVendor == DbVendor.HSQL) {
				dbName = Utilities.replaceUsersHome(dbName);
				if (dbName.startsWith("/")) {
					if (FileUtilities.getFilesByPattern(new File(dbName.substring(0, dbName.lastIndexOf("/"))), dbName.substring(dbName.lastIndexOf("/") + 1).replace(".", "\\.") + "\\..*", false).size() <= 0) {
						throw new DbNotExistsException("HSQL db '" + dbName + "' is not available");
					}
				}
				int port;
				final String[] hostParts = hostnameAndPort.split(":");
				if (hostParts.length == 2) {
					try {
						port = Integer.parseInt(hostParts[1]);
					} catch (@SuppressWarnings("unused") final Exception e) {
						throw new Exception("Invalid port: " + hostParts[1]);
					}
				} else {
					port = dbVendor.getDefaultPort();
				}

				// Logger must be kept in a local variable for making it work
				final Logger dbLogger = Logger.getLogger("hsqldb.db");
				dbLogger.setLevel(Level.WARNING);

				connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName, false, null, null, null), (Utilities.isNotEmpty(userName) ? userName : "SA"), (password != null ? new String(password) : ""));
			} else {
				int port;
				final String[] hostParts = hostnameAndPort.split(":");
				if (hostParts.length == 2) {
					try {
						port = Integer.parseInt(hostParts[1]);
					} catch (@SuppressWarnings("unused") final Exception e) {
						throw new Exception("Invalid port: " + hostParts[1]);
					}
				} else {
					port = dbVendor.getDefaultPort();
				}

				//				if (dbVendor == DbVendor.Oracle && new File("/dev/urandom").exists()) {
				//					// Set the alternative random generator to improve the connection creation speed, which may even cause a "I/O-Fehler: Connection reset"-error on low performance systems
				//					System.setProperty("java.security.egd", "file:///dev/./urandom");
				//
				//					// Alternatively you can change the file $JAVA_HOME/jre/lib/security/java.security and add following line:
				//					// securerandom.source=file:/dev/./urandom
				//				}

				if (!secureConnection) {
					try {
						if (userName != null && password != null) {
							connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName, false, null, null, null), userName, new String(password));
						} else {
							connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName, false, null, null, null));
						}
					} catch (final Exception e) {
						if (retryOnError && e.getCause() != null && e.getCause() instanceof SQLRecoverableException) {
							if (userName != null && password != null) {
								connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName, false, null, null, null), userName, new String(password));
							} else {
								connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName, false, null, null, null));
							}
						} else {
							throw e;
						}
					}
				} else {
					if (trustStoreFile != null) {
						final Properties props = new Properties();
						if (dbVendor == DbVendor.Oracle) {
							props.setProperty("javax.net.ssl.trustStore", trustStoreFile.getAbsolutePath());
							props.setProperty("javax.net.ssl.trustStoreType", "JKS");
							if (trustStorePassword != null && trustStorePassword.length > 0) {
								props.setProperty("javax.net.ssl.trustStorePassword", new String(trustStorePassword));
							}
						} else {
							// MariaDB and MySQLDB ignore trustStore settings in the temporary properties, so system properties must be used
							System.setProperty("javax.net.ssl.trustStore", trustStoreFile.getAbsolutePath());
							System.setProperty("javax.net.ssl.trustStoreType", "JKS");
							if (trustStorePassword != null && trustStorePassword.length > 0) {
								System.setProperty("javax.net.ssl.trustStorePassword", new String(trustStorePassword));
							}
						}

						props.setProperty("user", userName);
						props.setProperty("password", new String(password));

						connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName, true, trustStoreFile, trustStorePassword, null), props);
					} else {
						final Properties props = new Properties();
						props.setProperty("user", userName);
						props.setProperty("password", new String(password));

						String temporaryTrustStoreFilePath = null;
						if (dbVendor == DbVendor.Oracle) {
							// Create a temporary trustStore
							temporaryTrustStoreFilePath = Files.createTempFile("TempTrustStore", ".jks").toString();
							new File(temporaryTrustStoreFilePath).delete();
							trustStoreFile = new File(temporaryTrustStoreFilePath);
							HttpUtilities.createTrustStoreFile(hostParts[0], port, trustStoreFile, null);

							props.setProperty("javax.net.ssl.trustStore", trustStoreFile.getAbsolutePath());
							props.setProperty("javax.net.ssl.trustStoreType", "JKS");
							if (trustStorePassword != null && trustStorePassword.length > 0) {
								props.setProperty("javax.net.ssl.trustStorePassword", new String(trustStorePassword));
							}
						}

						try {
							connection = DriverManager.getConnection(generateUrlConnectionString(dbVendor, hostParts[0], port, dbName, true, trustStoreFile, trustStorePassword, null), props);
						} finally {
							if (dbVendor == DbVendor.Oracle) {
								// Remove temporary trustStore
								new File(temporaryTrustStoreFilePath).delete();
							}
						}
					}
				}

				//				if (dbVendor == DbVendor.Oracle && new File("/dev/random").exists()) {
				//					// Reset the alternative random generator
				//					System.clearProperty("java.security.egd");
				//				}
			}

			if (dbVendor == DbVendor.Cassandra) {
				try (Statement statement = connection.createStatement()) {
					statement.execute("USE " + dbName);
				}
			}

			return connection;
		} catch (final DbNotExistsException e) {
			throw e;
		} catch (final Exception e) {
			try {
				int port;
				if (Utilities.isNotEmpty(hostnameAndPort)) {
					if (hostnameAndPort.contains(":")) {
						try {
							port = Integer.parseInt(hostnameAndPort.substring(hostnameAndPort.indexOf(":") + 1));
						} catch (@SuppressWarnings("unused") final Exception e1) {
							throw new Exception("Invalid port: " + hostnameAndPort.substring(hostnameAndPort.indexOf(":") + 1));
						}
						NetworkUtilities.testConnection(hostnameAndPort.substring(0, hostnameAndPort.indexOf(":")), port);
					} else {
						port = dbVendor.getDefaultPort();
						NetworkUtilities.testConnection(hostnameAndPort, port);
					}
				}

				// No Exception from testConnection, so it must be some other problem
				throw new Exception("Cannot create db connection: " + e.getMessage(), e);
			} catch (final Exception e1) {
				throw new Exception("Cannot create db connection, caused by Url (" + hostnameAndPort + "): " + e1.getMessage(), e1);
			}
		}
	}

	public static int readoutInOutputStream(final DataSource dataSource, final String statementString, final OutputStream outputStream, final Charset encoding, final char separator, final Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readoutInOutputStream(connection, statementString, outputStream, encoding, separator, stringQuote);
		}
	}

	public static int readoutInOutputStream(final Connection connection, final String statementString, final OutputStream outputStream, final Charset encoding, final char separator, final Character stringQuote) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(statementString)) {
			statement.setFetchSize(100);
			try (CsvWriter csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote))) {
				final ResultSetMetaData metaData = resultSet.getMetaData();

				final List<String> headers = new ArrayList<>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					headers.add(metaData.getColumnLabel(i));
				}
				csvWriter.writeValues(headers);

				while (resultSet.next()) {
					final List<String> values = new ArrayList<>();
					for (int i = 1; i <= metaData.getColumnCount(); i++) {
						if (metaData.getColumnType(i) == Types.BLOB
								|| metaData.getColumnType(i) == Types.BINARY
								|| metaData.getColumnType(i) == Types.VARBINARY
								|| metaData.getColumnType(i) == Types.LONGVARBINARY) {
							if (dbVendor == DbVendor.SQLite || dbVendor == DbVendor.PostgreSQL || dbVendor == DbVendor.Cassandra) {
								// DB vendor does not allow "resultSet.getBlob(i)"
								try (InputStream input = resultSet.getBinaryStream(metaData.getColumnName(i))) {
									if (input != null) {
										final byte[] data = IoUtilities.toByteArray(input);
										values.add(Base64.getEncoder().encodeToString(data));
									} else {
										values.add("");
									}
								} catch (@SuppressWarnings("unused") final Exception e) {
									// NULL blobs throw a NullpointerException in SQLite
									values.add("");
								}
							} else {
								final Blob blob = resultSet.getBlob(i);
								if (resultSet.wasNull()) {
									values.add("");
								} else {
									try (InputStream input = blob.getBinaryStream()) {
										final byte[] data = IoUtilities.toByteArray(input);
										values.add(Base64.getEncoder().encodeToString(data));
									}
								}
							}
						} else if (dbVendor == DbVendor.SQLite && "DATE".equals(metaData.getColumnTypeName(i))) {
							final LocalDate extractSqliteLocalDate = extractSqliteLocalDate(resultSet.getObject(i));
							if (extractSqliteLocalDate != null) {
								values.add(extractSqliteLocalDate.toString());
							} else {
								values.add(null);
							}
						} else if (dbVendor == DbVendor.SQLite && "TIMESTAMP".equals(metaData.getColumnTypeName(i))) {
							final LocalDateTime extractSqliteLocalDateTime = extractSqliteLocalDateTime(resultSet.getObject(i));
							if (extractSqliteLocalDateTime != null) {
								values.add(extractSqliteLocalDateTime.toString());
							} else {
								values.add(null);
							}
						} else if (metaData.getColumnType(i) == Types.DATE) {
							values.add(resultSet.getString(i));
						} else if (metaData.getColumnType(i) == Types.TIMESTAMP) {
							values.add(resultSet.getString(i));
						} else {
							values.add(resultSet.getString(i));
						}
					}
					csvWriter.writeValues(values);
				}

				return csvWriter.getWrittenLines() - 1;
			}
		}
	}

	public static LocalDate extractSqliteLocalDate(final Object valueObject) throws Exception {
		if (valueObject == null) {
			return null;
		} else if (valueObject instanceof Long) {
			// java.util.Date: 1670968004453 (Long)
			return Instant.ofEpochMilli(((Long) valueObject)).atZone(ZoneId.systemDefault()).toLocalDate();
		} else if (valueObject instanceof String) {
			String valueString = (String) valueObject;
			if (valueString.contains("[")) {
				valueString = valueString.substring(0, valueString.indexOf(("[")));
			}

			final String[] datePatterns = new String[] {
					// ZonedDateTime: "2022-12-13T22:46:44.463491400+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSz",

					// ZonedDateTime: "2022-12-13T22:46:44.463491+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSSSSz",

					// ZonedDateTime: "2022-12-13T22:46:44.463+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSz",

					// ZonedDateTime: "2022-12-13T22:46:44+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ssz",

					// LocalDateTime: "2022-12-13T22:46:44.460515300" (String)
					DateUtilities.ISO_8601_DATETIME_WITH_NANOS_FORMAT_NO_TIMEZONE,

					// LocalDateTime: "2022-12-13T22:46:44.460515" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSSSS",

					// LocalDateTime: "2022-12-13T22:46:44.460" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSS",

					// LocalDateTime: "2022-12-13T22:46:44" (String)
					"yyyy-MM-dd'T'HH:mm:ss",

					// CURRENT_TIMESTAMP: "2022-12-13 21:46:44" (String), CURRENT_TIMESTAMP uses UTC timezone by default
					DateUtilities.YYYY_MM_DD_HHMMSS,

					// LocalDate: "2022-12-13" (String)
					// CURRENT_DATE: "2022-12-13" (String), CURRENT_DATE uses UTC timezone by default
					DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE
			};

			for (final String pattern : datePatterns) {
				try {
					return DateUtilities.parseLocalDate(pattern, valueString);
				} catch(@SuppressWarnings("unused") final DateTimeParseException e) {
					// try next pattern
				}
			}
			throw new Exception("Unparseable value for data type DATE: " + ((String) valueObject));
		} else {
			throw new Exception("Unparseable value type for data type DATE");
		}
	}

	public static LocalDateTime extractSqliteLocalDateTime(final Object valueObject) throws Exception {
		if (valueObject == null) {
			return null;
		} else if (valueObject instanceof Long) {
			// java.util.Date: 1670968004453 (Long)
			return Instant.ofEpochMilli(((Long) valueObject)).atZone(ZoneId.systemDefault()).toLocalDateTime();
		} else if (valueObject instanceof String) {
			String valueString = (String) valueObject;
			if (valueString.contains("[")) {
				valueString = valueString.substring(0, valueString.indexOf(("[")));
			}

			final String[] dateTimePatterns = new String[] {
					// ZonedDateTime: "2022-12-13T22:46:44.463491400+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSz",

					// ZonedDateTime: "2022-12-13T22:46:44.463491+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSSSSz",

					// ZonedDateTime: "2022-12-13T22:46:44.463+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSz",

					// ZonedDateTime: "2022-12-13T22:46:44+01:00[Europe/Berlin]" (String)
					"yyyy-MM-dd'T'HH:mm:ssz",

					// LocalDateTime: "2022-12-13T22:46:44.460515300" (String)
					DateUtilities.ISO_8601_DATETIME_WITH_NANOS_FORMAT_NO_TIMEZONE,

					// LocalDateTime: "2022-12-13T22:46:44.460515" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSSSSS",

					// LocalDateTime: "2022-12-13T22:46:44.460" (String)
					"yyyy-MM-dd'T'HH:mm:ss.SSS",

					// LocalDateTime: "2022-12-13T22:46:44" (String)
					"yyyy-MM-dd'T'HH:mm:ss",

					// CURRENT_TIMESTAMP: "2022-12-13 21:46:44" (String), CURRENT_TIMESTAMP uses UTC timezone by default
					DateUtilities.YYYY_MM_DD_HHMMSS
			};

			for (final String pattern : dateTimePatterns) {
				try {
					return DateUtilities.parseLocalDateTime(pattern, valueString);
				} catch(@SuppressWarnings("unused") final DateTimeParseException e) {
					// try next pattern
				}
			}

			if (valueString.length() == 10) {
				// LocalDate: "2022-12-13" (String)
				// CURRENT_DATE: "2022-12-13" (String), CURRENT_DATE uses UTC timezone by default
				return DateUtilities.parseLocalDateTime(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE, valueString + "T00:00:00");
			} else {
				throw new Exception("Unparseable value for data type TIMESTAMP: " + ((String) valueObject));
			}
		} else {
			throw new Exception("Unparseable value type for data type TIMESTAMP");
		}
	}

	public static String readout(final DataSource dataSource, final String statementString, final char separator, final Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readout(connection, statementString, separator, stringQuote);
		}
	}

	public static String readout(final Connection connection, final String statementString, final char separator, final Character stringQuote) throws Exception {
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		readoutInOutputStream(connection, statementString, outputStream, StandardCharsets.UTF_8, separator, stringQuote);
		return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
	}

	public static String readoutTable(final DataSource dataSource, final String tableName, final char separator, final Character stringQuote) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return readoutTable(connection, tableName, separator, stringQuote);
		}
	}

	public static String readoutTable(final Connection connection, final String tableName) throws Exception {
		return readoutTable(connection, tableName, ';', '"');
	}

	public static String readoutTable(final Connection connection, final String tableName, final char separator, final Character stringQuote) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			final DbVendor dbVendor = getDbVendor(connection);
			final List<String> columnNames = new ArrayList<>(getColumnNames(connection, tableName));
			Collections.sort(columnNames);
			final List<String> keyColumnNames = new ArrayList<>(getPrimaryKeyColumns(connection, tableName));
			Collections.sort(keyColumnNames);
			final List<String> readoutColumns = new ArrayList<>();
			readoutColumns.addAll(keyColumnNames);
			for (final String columnName : columnNames) {
				if (!Utilities.containsIgnoreCase(readoutColumns, columnName)) {
					readoutColumns.add(columnName);
				}
			}
			String orderPart = "";
			if (!keyColumnNames.isEmpty()) {
				orderPart = " ORDER BY " + joinColumnVendorEscaped(dbVendor, keyColumnNames);
			}
			return readout(connection, "SELECT " + joinColumnVendorEscaped(dbVendor, readoutColumns) + " FROM " + tableName + orderPart, separator, stringQuote);
		}
	}

	public static DbVendor getDbVendor(final DataSource dataSource) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return getDbVendor(connection);
		} catch (final SQLException e) {
			throw new Exception("Cannot check db vendor: " + e.getMessage(), e);
		}
	}

	public static DbVendor getDbVendor(final Connection connection) throws Exception {
		try {
			final DatabaseMetaData databaseMetaData = connection.getMetaData();
			if (databaseMetaData != null) {
				final String productName = databaseMetaData.getDatabaseProductName();
				if (productName != null && productName.toLowerCase().contains("oracle")) {
					return DbVendor.Oracle;
				} else if (productName != null && productName.toLowerCase().contains("mysql")) {
					return DbVendor.MySQL;
				} else if (productName != null && productName.toLowerCase().contains("maria")) {
					return DbVendor.MariaDB;
				} else if (productName != null && productName.toLowerCase().contains("postgres")) {
					return DbVendor.PostgreSQL;
				} else if (productName != null && productName.toLowerCase().contains("sqlite")) {
					return DbVendor.SQLite;
				} else if (productName != null && productName.toLowerCase().contains("derby")) {
					return DbVendor.Derby;
				} else if (productName != null && productName.toLowerCase().contains("hsql")) {
					return DbVendor.HSQL;
				} else if (productName != null && productName.toLowerCase().contains("firebird")) {
					return DbVendor.Firebird;
				} else if (productName != null && productName.toLowerCase().contains("cassandra")) {
					return DbVendor.Cassandra;
				} else {
					throw new Exception("Unknown db vendor: " + productName);
				}
			} else {
				throw new Exception("Undetectable db vendor");
			}
		} catch (final SQLException e) {
			throw new Exception("Error while detecting db vendor: " + e.getMessage(), e);
		}
	}

	public static String getDbUrl(final DataSource dataSource) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			return getDbUrl(connection);
		}
	}

	public static String getDbUrl(final Connection connection) {
		try {
			final DatabaseMetaData databaseMetaData = connection.getMetaData();
			if (databaseMetaData != null) {
				return databaseMetaData.getURL();
			} else {
				return null;
			}
		} catch (@SuppressWarnings("unused") final SQLException e) {
			return null;
		}
	}

	public static boolean checkTableAndColumnsExist(final Connection connection, final String tableName, final String... columns) throws Exception {
		return checkTableAndColumnsExist(connection, tableName, false, columns);
	}

	public static boolean checkTableAndColumnsExist(final DataSource dataSource, final String tableName, final boolean throwExceptionOnError, final String... columns) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return checkTableAndColumnsExist(connection, tableName, throwExceptionOnError, columns);
		}
	}

	public static boolean checkTableAndColumnsExist(final Connection connection, final String tableName, final boolean throwExceptionOnError, final String... columns) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);
		final CaseInsensitiveSet dbTableColumns = getColumnNames(connection, tableName);
		if (columns != null) {
			for (final String column : columns) {
				if (column != null && !dbTableColumns.contains(unescapeVendorReservedNames(dbVendor, column))) {
					if (throwExceptionOnError) {
						throw new Exception("Column '" + column + "' does not exist in table '" + tableName + "'");
					} else {
						return false;
					}
				}
			}
		}
		return true;
	}

	public static boolean checkTableExist(final Connection connection, final String tableName) throws Exception {
		return checkTableExist(connection, tableName, false);
	}

	public static boolean checkTableExist(final Connection connection, final String tableName, final boolean throwExceptionOnError) throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.setFetchSize(100);
			try (ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0")) {
				return true;
			} catch (@SuppressWarnings("unused") final Exception e) {
				if (throwExceptionOnError) {
					throw new Exception("Table '" + tableName + "' does not exist");
				} else {
					return false;
				}
			}
		}
	}

	public static String callStoredProcedureWithDbmsOutput(final Connection connection, final String procedureName, final Object... parameters) throws SQLException {
		try (CallableStatement callableStatement = connection.prepareCall("begin dbms_output.enable(:1); end;")) {
			callableStatement.setLong(1, 10000);
			callableStatement.executeUpdate();
		}

		if (parameters != null) {
			try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "(" + TextUtilities.repeatString("?", parameters.length, ", ") + ")}")) {
				for (int i = 0; i < parameters.length; i++) {
					if (parameters[i].getClass() == Date.class) {
						parameters[i] = new java.sql.Date(((Date) parameters[i]).getTime());
					}
				}
				for (int i = 0; i < parameters.length; i++) {
					callableStatement.setObject(i + 1, parameters[i]);
				}
				callableStatement.execute();
			}
		} else {
			try (CallableStatement callableStatement = connection.prepareCall("{call " + procedureName + "()}")) {
				callableStatement.execute();
			}
		}

		final StringBuffer dbmsOutput = new StringBuffer(1024);
		try (CallableStatement callableStatement = connection.prepareCall(
				"declare "
						+ "    l_line varchar2(255); "
						+ "    l_done number; "
						+ "    l_buffer long; "
						+ "begin "
						+ "  loop "
						+ "    exit when length(l_buffer)+255 > :maxbytes OR l_done = 1; "
						+ "    dbms_output.get_line( l_line, l_done ); "
						+ "    l_buffer := l_buffer || l_line || chr(10); "
						+ "  end loop; "
						+ " :done := l_done; "
						+ " :buffer := l_buffer; "
						+ "end;")) {
			callableStatement.registerOutParameter(2, Types.INTEGER);
			callableStatement.registerOutParameter(3, Types.VARCHAR);
			while (true) {
				callableStatement.setInt(1, 32000);
				callableStatement.executeUpdate();
				dbmsOutput.append(callableStatement.getString(3).trim());
				if (callableStatement.getInt(2) == 1) {
					break;
				}
			}
		}

		try (CallableStatement callableStatement = connection.prepareCall("begin dbms_output.disable; end;")) {
			callableStatement.executeUpdate();
		}

		return dbmsOutput.toString();
	}

	public static String getResultAsTextTable(final DataSource datasource, final String selectString) throws Exception {
		try (Connection connection = datasource.getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(selectString)) {
			preparedStatement.setFetchSize(100);
			try (final ResultSet resultSet = preparedStatement.executeQuery()) {
				final TextTable textTable = new TextTable();
				for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
					textTable.addColumn(resultSet.getMetaData().getColumnLabel(columnIndex));
				}
				while (resultSet.next()) {
					textTable.startNewLine();
					for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
						if (resultSet.getString(columnIndex) != null) {
							textTable.addValueToCurrentLine(resultSet.getString(columnIndex));
						} else {
							textTable.addValueToCurrentLine("<null>");
						}
					}
				}
				return textTable.toString();
			}
		}
	}

	public static CaseInsensitiveSet getColumnNames(final DataSource dataSource, final String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnNames");
		}

		try (Connection connection = dataSource.getConnection()) {
			return getColumnNames(connection, tableName);
		} catch (final SQLException e) {
			throw new Exception("Cannot read columns for table " + tableName + ": " + e.getMessage(), e);
		}
	}

	public static CaseInsensitiveSet getColumnNames(final Connection connection, final String tableName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnNames");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnNames");
		} else {
			try (Statement statement = connection.createStatement();
					ResultSet resultSet = statement.executeQuery("SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0")) {
				final CaseInsensitiveSet columnNamesList = new CaseInsensitiveSet();
				for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
					columnNamesList.add(resultSet.getMetaData().getColumnName(i));
				}
				return columnNamesList;
			}
		}
	}

	public static CaseInsensitiveMap<DbColumnType> getColumnDataTypes(final DataSource dataSource, final String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDataTypes");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataTypes");
		} else {
			try (Connection connection = dataSource.getConnection()) {
				return getColumnDataTypes(connection, tableName);
			}
		}
	}

	public static CaseInsensitiveMap<DbColumnType> getColumnDataTypes(final Connection connection, final String tableName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnDataTypes");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDataTypes");
		} else {
			final CaseInsensitiveMap<DbColumnType> returnMap = new CaseInsensitiveMap<>();
			final DbVendor dbVendor = getDbVendor(connection);
			if (DbVendor.Oracle == dbVendor) {
				// Watchout: Oracle's timestamp datatype is "TIMESTAMP(6)", so remove the bracket value
				final String sql = "SELECT column_name, NVL(substr(data_type, 1, instr(data_type, '(') - 1), data_type) AS data_type, data_length, data_precision, data_scale, nullable FROM user_tab_columns WHERE LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						while (resultSet.next()) {
							int characterLength = resultSet.getInt("data_length");
							if (resultSet.wasNull()) {
								characterLength = -1;
							}
							int numericPrecision = resultSet.getInt("data_precision");
							if (resultSet.wasNull()) {
								numericPrecision = -1;
							}
							int numericScale = resultSet.getInt("data_scale");
							if (resultSet.wasNull()) {
								numericScale = -1;
							}
							final boolean isNullable = "y".equalsIgnoreCase(resultSet.getString("nullable"));

							// TODO AutoIncrements will be introduced with Oracle 12
							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, false));
						}
					}
				}
			} else if (DbVendor.MySQL == dbVendor) {
				final String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, extra FROM information_schema.columns WHERE table_schema = schema() AND LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery() ) {
						while (resultSet.next()) {
							long characterLength = resultSet.getLong("character_maximum_length");
							if (resultSet.wasNull()) {
								characterLength = -1;
							}
							int numericPrecision = resultSet.getInt("numeric_precision");
							if (resultSet.wasNull()) {
								numericPrecision = -1;
							}
							int numericScale = resultSet.getInt("numeric_scale");
							if (resultSet.wasNull()) {
								numericScale = -1;
							}
							final boolean isNullable = "yes".equalsIgnoreCase(resultSet.getString("is_nullable"));
							final boolean isAutoIncrement = "auto_increment".equalsIgnoreCase(resultSet.getString("extra"));

							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.MariaDB == dbVendor) {
				final String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, extra FROM information_schema.columns WHERE table_schema = schema() AND LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery() ) {
						while (resultSet.next()) {
							long characterLength = resultSet.getLong("character_maximum_length");
							if (resultSet.wasNull()) {
								characterLength = -1;
							}
							int numericPrecision = resultSet.getInt("numeric_precision");
							if (resultSet.wasNull()) {
								numericPrecision = -1;
							}
							int numericScale = resultSet.getInt("numeric_scale");
							if (resultSet.wasNull()) {
								numericScale = -1;
							}
							final boolean isNullable = "yes".equalsIgnoreCase(resultSet.getString("is_nullable"));
							final boolean isAutoIncrement = "auto_increment".equalsIgnoreCase(resultSet.getString("extra"));

							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.HSQL == dbVendor) {
				final String sql = "SELECT column_name, type_name, column_size, decimal_digits, is_nullable, is_autoincrement FROM information_schema.system_columns WHERE LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						while (resultSet.next()) {
							long characterLength = resultSet.getLong("column_size");
							if (resultSet.wasNull()) {
								characterLength = -1;
							}
							int numericPrecision = resultSet.getInt("column_size");
							if (resultSet.wasNull()) {
								numericPrecision = -1;
							}
							int numericScale = resultSet.getInt("decimal_digits");
							if (resultSet.wasNull()) {
								numericScale = -1;
							}
							final boolean isNullable = "yes".equalsIgnoreCase(resultSet.getString("is_nullable"));
							final boolean isAutoIncrement = "yes".equalsIgnoreCase(resultSet.getString("is_autoincrement"));

							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("type_name"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.Derby == dbVendor) {
				final String sql = "SELECT columnname, columndatatype, autoincrementvalue FROM sys.systables, sys.syscolumns WHERE tableid = referenceid AND LOWER(tablename) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						while (resultSet.next()) {
							String type = resultSet.getString("columndatatype");

							long characterLength = -1;
							final int numericPrecision = -1;
							final int numericScale = -1;

							boolean isNullable;
							if (type.toLowerCase().endsWith("not null")) {
								isNullable = false;
								type = type.substring(0, type.length() - 8).trim();
							} else {
								isNullable = true;
							}

							final boolean autoincrement = !Utilities.isBlank(resultSet.getString("autoincrementvalue"));

							if (type.contains("(")) {
								characterLength = Long.parseLong(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
								type = type.substring(0, type.indexOf("("));
							}

							returnMap.put(resultSet.getString("columnname"), new DbColumnType(type, characterLength, numericPrecision, numericScale, isNullable, autoincrement));
						}
					}
				}
			} else if (DbVendor.Firebird == dbVendor) {
				final String sql = "SELECT rf.rdb$field_name, f.rdb$field_type, f.rdb$field_sub_type, f.rdb$field_length, f.rdb$field_scale, f.rdb$null_flag"
						+ " FROM rdb$fields f JOIN rdb$relation_fields rf ON rf.rdb$field_source = f.rdb$field_name WHERE rf.rdb$relation_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName.toUpperCase());
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						while (resultSet.next()) {
							long characterLength = resultSet.getLong("rdb$field_length");
							if (resultSet.wasNull()) {
								characterLength = -1;
							}
							int numericPrecision = resultSet.getInt("rdb$field_length");
							if (resultSet.wasNull()) {
								numericPrecision = -1;
							}
							int numericScale = resultSet.getInt("rdb$field_scale");
							if (resultSet.wasNull()) {
								numericScale = -1;
							}
							final boolean isNullable = resultSet.getObject("rdb$null_flag") == null;

							String dataType;
							switch (resultSet.getInt("rdb$field_type")) {
								case 7: dataType = "SMALLINT";
								break;
								case 8: dataType = "INTEGER";
								break;
								case 10: dataType = "FLOAT";
								break;
								case 12: dataType = "DATE";
								break;
								case 13: dataType = "TIME";
								break;
								case 14: dataType = "CHAR";
								break;
								case 16: dataType = "BIGINT";
								break;
								case 27: dataType = "DOUBLE PRECISION";
								break;
								case 35: dataType = "TIMESTAMP";
								break;
								case 37: dataType = "VARCHAR";
								break;
								case 261:
									if (resultSet.getInt("rdb$field_sub_type") == 1) {
										dataType = "CLOB";
									} else {
										dataType = "BLOB";
									}
									break;
								default: dataType = getTypeNameById(resultSet.getInt("rdb$field_type"));
							}

							// TODO check autoincrement
							returnMap.put(resultSet.getString("rdb$field_name").trim(), new DbColumnType(dataType, characterLength, numericPrecision, numericScale, isNullable, false));
						}
					}
				}
			} else if (DbVendor.SQLite == dbVendor) {
				boolean hasAutoIncrement = false;
				if (checkTableExist(connection, "sqlite_sequence")) {
					// sqlite_sequence only exists if there is any table with auto_increment
					try (PreparedStatement preparedStatement2 = connection.prepareStatement("SELECT COUNT(*) FROM sqlite_sequence WHERE LOWER(name) = LOWER(?)")) {
						preparedStatement2.setFetchSize(100);
						preparedStatement2.setString(1, tableName);
						try (ResultSet resultSet2 = preparedStatement2.executeQuery()) {
							if (resultSet2.next()) {
								hasAutoIncrement = resultSet2.getInt(1) > 0;
							}
						}
					}
				}

				final String sql = "PRAGMA table_info(" + tableName + ")";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						while (resultSet.next()) {
							long characterLength = -1;
							final int numericPrecision = -1;
							final int numericScale = -1;
							final boolean isNullable = resultSet.getInt("notnull") == 0;
							// Only the primary key can be auto incremented in SQLite
							final boolean isAutoIncrement = hasAutoIncrement && resultSet.getInt("pk") > 0;

							String type = resultSet.getString("type");

							if (type.contains("(")) {
								characterLength = Long.parseLong(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
								type = type.substring(0, type.indexOf("("));
							}

							returnMap.put(resultSet.getString("name"), new DbColumnType(type, characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.PostgreSQL == dbVendor) {
				final String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA() AND LOWER(table_name) = LOWER(?)";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						while (resultSet.next()) {
							long characterLength = resultSet.getLong("character_maximum_length");
							if (resultSet.wasNull()) {
								characterLength = -1;
							}
							int numericPrecision = resultSet.getInt("numeric_precision");
							if (resultSet.wasNull()) {
								numericPrecision = -1;
							}
							int numericScale = resultSet.getInt("numeric_scale");
							if (resultSet.wasNull()) {
								numericScale = -1;
							}
							final boolean isNullable = "yes".equalsIgnoreCase(resultSet.getString("is_nullable"));

							final String defaultValue = resultSet.getString("column_default");
							boolean isAutoIncrement = false;
							if (defaultValue != null && defaultValue.toLowerCase().startsWith("nextval(")) {
								isAutoIncrement = true;
							}

							returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("data_type"), characterLength, numericPrecision, numericScale, isNullable, isAutoIncrement));
						}
					}
				}
			} else if (DbVendor.Cassandra == dbVendor) {
				if (tableName.contains(".")) {
					final String keySpaceName = tableName.substring(0, tableName.indexOf("."));
					final String tableNameInKeySpace = tableName.substring(tableName.indexOf(".") + 1);
					final String sql = "SELECT column_name, kind, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?";
					try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
						preparedStatement.setFetchSize(100);
						preparedStatement.setString(1, keySpaceName);
						preparedStatement.setString(2, tableNameInKeySpace);
						try (ResultSet resultSet = preparedStatement.executeQuery() ) {
							while (resultSet.next()) {
								returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("type"), -1, -1, -1, !resultSet.getString("kind").equalsIgnoreCase("partition_key"), false));
							}
						}
					}
				} else {
					final String sql = "SELECT keyspace_name, column_name, kind, type FROM system_schema.columns WHERE table_name = ? ALLOW FILTERING";
					try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
						preparedStatement.setFetchSize(100);
						preparedStatement.setString(1, tableName);
						try (ResultSet resultSet = preparedStatement.executeQuery() ) {
							String keyspace = null;
							while (resultSet.next()) {
								final String nextKeyspace = resultSet.getString("keyspace_name");
								if (keyspace == null) {
									keyspace = nextKeyspace;
								} else if (!keyspace.equals(nextKeyspace)) {
									throw new Exception("Multiple tables for table name '" + tableName + "' found. Please specify keyspace by keyspace_name prefix.");
								}
								returnMap.put(resultSet.getString("column_name"), new DbColumnType(resultSet.getString("type"), -1, -1, -1, !resultSet.getString("kind").equalsIgnoreCase("partition_key"), false));
							}
						}
					}
				}
			} else {
				throw new Exception("Unsupported db vendor");
			}
			return returnMap;
		}
	}

	public static int getTableEntriesCount(final Connection connection, final String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getTableEntriesNumber");
		} else {
			try (Statement statement = connection.createStatement();
					ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + getSQLSafeString(tableName))) {
				if (resultSet.next()) {
					return resultSet.getInt(1);
				} else {
					return 0;
				}
			}
		}
	}

	public static boolean containsColumnName(final DataSource dataSource, final String tableName, final String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for containsColumnName");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for containsColumnName");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for containsColumnName");
		} else {
			try (Connection connection = dataSource.getConnection();
					Statement statement = connection.createStatement();
					ResultSet resultSet = statement.executeQuery("SELECT * FROM " + getSQLSafeString(tableName) + " WHERE 1 = 0")) {
				for (int columnIndex = 1; columnIndex <= resultSet.getMetaData().getColumnCount(); columnIndex++) {
					if (resultSet.getMetaData().getColumnName(columnIndex).equalsIgnoreCase(columnName.trim())) {
						return true;
					}
				}
				return false;
			}
		}
	}

	public static String getColumnDefaultValue(final DataSource dataSource, final String tableName, final String columnName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDefaultValue");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDefaultValue");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getColumnDefaultValue");
		} else {
			try (Connection connection = dataSource.getConnection()) {
				return getColumnDefaultValue(connection, tableName, columnName);
			}
		}
	}

	public static String getColumnDefaultValue(final Connection connection, final String tableName, final String columnName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnDefaultValue");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDefaultValue");
		} else if (Utilities.isBlank(columnName)) {
			throw new Exception("Invalid empty columnName for getColumnDefaultValue");
		} else {
			final DbVendor dbVendor = getDbVendor(connection);
			if (DbVendor.Oracle == dbVendor) {
				final String sql = "SELECT data_default FROM user_tab_cols WHERE table_name = ? AND column_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName.toUpperCase());
					preparedStatement.setString(2, columnName.toUpperCase());
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						if (resultSet.next()) {
							final String defaultvalue = resultSet.getString(1);
							String returnValue;
							if (defaultvalue == null || "null".equalsIgnoreCase(defaultvalue)) {
								returnValue = null;
							} else if (defaultvalue.startsWith("'") && defaultvalue.endsWith("'")) {
								returnValue = defaultvalue.substring(1, defaultvalue.length() - 1);
							} else {
								returnValue = defaultvalue;
							}
							return returnValue;
						} else {
							return null;
						}
					}
				}
			} else if (DbVendor.HSQL == dbVendor) {
				final String sql = "SELECT column_default FROM information_schema.columns WHERE table_name = ? AND column_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						if (resultSet.next()) {
							final String returnValue = resultSet.getString(1);
							if ("NULL".equalsIgnoreCase(returnValue)) {
								return null;
							} else {
								return returnValue;
							}
						} else {
							return null;
						}
					}
				}
			} else {
				final String sql = "SELECT column_default FROM information_schema.columns WHERE table_schema = (SELECT schema()) AND table_name = ? AND column_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, columnName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						if (resultSet.next()) {
							final String returnValue = resultSet.getString(1);
							if ("NULL".equalsIgnoreCase(returnValue)) {
								return null;
							} else {
								return returnValue;
							}
						} else {
							return null;
						}
					}
				}
			}
		}
	}


	public static CaseInsensitiveMap<String> getColumnDefaultValues(final DataSource dataSource, final String tableName) throws Exception {
		if (dataSource == null) {
			throw new Exception("Invalid empty dataSource for getColumnDefaultValue");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDefaultValue");
		} else {
			try (Connection connection = dataSource.getConnection()) {
				return getColumnDefaultValues(connection, tableName);
			}
		}
	}

	public static CaseInsensitiveMap<String> getColumnDefaultValues(final Connection connection, final String tableName) throws Exception {
		if (connection == null) {
			throw new Exception("Invalid empty connection for getColumnDefaultValue");
		} else if (Utilities.isBlank(tableName)) {
			throw new Exception("Invalid empty tableName for getColumnDefaultValues");
		} else {
			final CaseInsensitiveMap<String> returnMap = new CaseInsensitiveMap<>();
			final DbVendor dbVendor = getDbVendor(connection);
			if (DbVendor.Oracle == dbVendor) {
				final String sql = "SELECT column_name, data_default FROM user_tab_cols WHERE table_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName.toUpperCase());
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						if (resultSet.next()) {
							final String columnName = resultSet.getString(1);
							final String defaultvalue = resultSet.getString(2);
							String returnValue;
							if (defaultvalue == null || "null".equalsIgnoreCase(defaultvalue)) {
								returnValue = null;
							} else if (defaultvalue.startsWith("'") && defaultvalue.endsWith("'")) {
								returnValue = defaultvalue.substring(1, defaultvalue.length() - 1);
							} else {
								returnValue = defaultvalue;
							}
							returnMap.put(columnName.toLowerCase(), returnValue);
						}
					}
				}
			} else if (DbVendor.HSQL == dbVendor) {
				final String sql = "SELECT column_name, column_default FROM information_schema.columns WHERE table_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						if (resultSet.next()) {
							final String columnName = resultSet.getString(1);
							String returnValue = resultSet.getString(2);
							if ("NULL".equalsIgnoreCase(returnValue)) {
								returnValue = null;
							}
							returnMap.put(columnName.toLowerCase(), returnValue);
						}
					}
				}
			} else {
				final String sql = "SELECT column_name, column_default FROM information_schema.columns WHERE table_schema = (SELECT schema()) AND table_name = ?";
				try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
					preparedStatement.setFetchSize(100);
					preparedStatement.setString(1, tableName);
					try (ResultSet resultSet = preparedStatement.executeQuery()) {
						if (resultSet.next()) {
							final String columnName = resultSet.getString(1);
							String returnValue = resultSet.getString(2);
							if ("NULL".equalsIgnoreCase(returnValue)) {
								returnValue = null;
							}
							returnMap.put(columnName.toLowerCase(), returnValue);
						}
					}
				}
			}
			return returnMap;
		}
	}

	public static String getDateDefaultValue(final DataSource dataSource, final String fieldDefault) throws Exception {
		final DbVendor dbVendor = getDbVendor(dataSource);
		if ("sysdate".equalsIgnoreCase(fieldDefault) || "sysdate()".equalsIgnoreCase(fieldDefault) || "current_timestamp".equalsIgnoreCase(fieldDefault)) {
			if (dbVendor == DbVendor.Oracle) {
				return "current_timestamp";
			} else if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
				return "current_timestamp";
			} else {
				throw new Exception("Unsupported db vendor");
			}
		} else {
			if (dbVendor == DbVendor.Oracle) {
				return "to_date('" + fieldDefault + "', 'DD.MM.YYYY')";
			} else if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
				return "'" + fieldDefault + "'";
			} else {
				throw new Exception("Unsupported db vendor");
			}
		}
	}

	public static boolean addColumnToDbTable(final DataSource dataSource, final String tablename, final String fieldname, String fieldType, int length, final String fieldDefault, final boolean notNull) throws Exception {
		if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!tablename.equalsIgnoreCase(getSQLSafeString(tablename))) {
			return false;
		} else if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!fieldname.equalsIgnoreCase(getSQLSafeString(fieldname))) {
			return false;
		} else if (Utilities.isBlank(fieldType)) {
			return false;
		} else if (containsColumnName(dataSource, tablename, fieldname)) {
			return false;
		} else {
			fieldType = fieldType.toUpperCase().trim();

			String addColumnStatement = "ALTER TABLE " + tablename + " ADD (" + fieldname.toLowerCase() + " " + fieldType;
			if (fieldType.startsWith("VARCHAR")) {
				if (length <= 0) {
					length = 100;
				}
				addColumnStatement += "(" + length + ")";
			}

			// Default Value
			if (Utilities.isNotEmpty(fieldDefault)) {
				if (fieldType.startsWith("VARCHAR")) {
					addColumnStatement += " DEFAULT '" + fieldDefault + "'";
				} else if ("DATE".equalsIgnoreCase(fieldType)) {
					addColumnStatement += " DEFAULT " + getDateDefaultValue(dataSource, fieldDefault);
				} else {
					addColumnStatement += " DEFAULT " + fieldDefault;
				}
			}

			// Maybe null
			if (notNull) {
				addColumnStatement += " NOT NULL";
			}

			addColumnStatement += ")";

			try (Connection connection = dataSource.getConnection();
					Statement statement = connection.createStatement()) {
				statement.executeUpdate(addColumnStatement);
				return true;
			} catch (@SuppressWarnings("unused") final Exception e) {
				return false;
			}
		}
	}

	public static boolean alterColumnTypeInDbTable(final DataSource dataSource, final String tablename, final String fieldname, String fieldType, int length, final String fieldDefault, final boolean notNull) throws Exception {
		if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!tablename.equalsIgnoreCase(getSQLSafeString(tablename))) {
			return false;
		} else if (Utilities.isBlank(fieldname)) {
			return false;
		} else if (!fieldname.equalsIgnoreCase(getSQLSafeString(fieldname))) {
			return false;
		} else if (Utilities.isBlank(fieldType)) {
			return false;
		} else if (!containsColumnName(dataSource, tablename, fieldname)) {
			return false;
		} else {
			fieldType = fieldType.toUpperCase().trim();

			String changeColumnStatementPart = fieldname.toLowerCase() + " " + fieldType;
			if (fieldType.startsWith("VARCHAR")) {
				if (length <= 0) {
					length = 100;
				}
				changeColumnStatementPart += "(" + length + ")";
			}

			// Default Value
			if (Utilities.isNotEmpty(fieldDefault)) {
				if ("VARCHAR".equalsIgnoreCase(fieldType)) {
					changeColumnStatementPart += " DEFAULT '" + fieldDefault + "'";
				} else if ("DATE".equalsIgnoreCase(fieldType)) {
					changeColumnStatementPart += " DEFAULT " + getDateDefaultValue(dataSource, fieldDefault);
				} else {
					changeColumnStatementPart += " DEFAULT " + fieldDefault;
				}
			}

			// Maybe null
			if (notNull) {
				changeColumnStatementPart += " NOT NULL";
			}

			String changeColumnStatement;
			final DbVendor dbVendor = getDbVendor(dataSource);
			if (DbVendor.Oracle == dbVendor) {
				changeColumnStatement = "ALTER TABLE " + tablename + " MODIFY (" + changeColumnStatementPart + ")";
			} else if (DbVendor.MySQL == dbVendor || DbVendor.MariaDB == dbVendor) {
				changeColumnStatement = "ALTER TABLE " + tablename + " MODIFY " + changeColumnStatementPart;
			} else {
				throw new Exception("Unsupported db vendor");
			}

			try (Connection connection = dataSource.getConnection();
					Statement statement = connection.createStatement()) {
				statement.executeUpdate(changeColumnStatement);
				return true;
			} catch (@SuppressWarnings("unused") final Exception e) {
				return false;
			}
		}
	}

	private static String getSQLSafeString(final String value) {
		if (value == null) {
			return null;
		} else {
			return value.replace("'", "''");
		}
	}

	public static boolean checkOracleTablespaceExists(final DataSource dataSource, final String tablespaceName) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return checkOracleTablespaceExists(connection, tablespaceName);
		} catch (final SQLException e) {
			throw new Exception("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
		}
	}

	/**
	 * Check if oracle tablespace exists
	 *
	 * @param connection
	 * @param tablespaceName
	 * @return
	 * @throws Exception
	 */
	public static boolean checkOracleTablespaceExists(final Connection connection, final String tablespaceName) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);
		if (dbVendor == DbVendor.Oracle && tablespaceName != null) {
			try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM dba_tablespaces WHERE LOWER(tablespace_name) = ?")) {
				preparedStatement.setFetchSize(100);
				preparedStatement.setString(1, tablespaceName.toLowerCase());
				try (ResultSet resultSet = preparedStatement.executeQuery()) {
					return resultSet.getInt(1) > 0;
				}
			} catch (final Exception e) {
				throw new Exception("Cannot check db tablespace " + tablespaceName + ": " + e.getMessage(), e);
			}
		} else {
			return false;
		}
	}

	public static CaseInsensitiveSet getPrimaryKeyColumns(final DataSource dataSource, final String tableName) throws Exception {
		try (Connection connection = dataSource.getConnection()) {
			return getPrimaryKeyColumns(connection, tableName);
		} catch (final SQLException e) {
			throw new Exception("Cannot read primarykey columns for table " + tableName + ": " + e.getMessage(), e);
		}
	}

	public static CaseInsensitiveSet getPrimaryKeyColumns(final Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			final DbVendor dbVendor = getDbVendor(connection);
			try {
				if (DbVendor.Cassandra == dbVendor) {
					if (tableName.contains(".")) {
						final String keySpaceName = tableName.substring(0, tableName.indexOf("."));
						final String tableNameInKeySpace = tableName.substring(tableName.indexOf(".") + 1);
						final String sql = "SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key'";
						try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
							preparedStatement.setFetchSize(100);
							preparedStatement.setString(1, keySpaceName);
							preparedStatement.setString(2, tableNameInKeySpace);
							final CaseInsensitiveSet returnList = new CaseInsensitiveSet();
							try (ResultSet resultSet = preparedStatement.executeQuery() ) {
								while (resultSet.next()) {
									returnList.add(resultSet.getString("column_name"));
								}
							}
							return returnList;
						}
					} else {
						final String sql = "SELECT keyspace_name, column_name FROM system_schema.columns WHERE table_name = ? AND kind = 'partition_key' ALLOW FILTERING";
						try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
							preparedStatement.setFetchSize(100);
							preparedStatement.setString(1, tableName);
							try (ResultSet resultSet = preparedStatement.executeQuery() ) {
								String keyspace = null;
								final CaseInsensitiveSet returnList = new CaseInsensitiveSet();
								while (resultSet.next()) {
									final String nextKeyspace = resultSet.getString("keyspace_name");
									if (keyspace == null) {
										keyspace = nextKeyspace;
									} else if (!keyspace.equals(nextKeyspace)) {
										throw new Exception("Multiple tables for table name '" + tableName + "' found. Please specify keyspace by keyspace_name prefix.");
									}
									returnList.add(resultSet.getString("column_name"));
								}
								return returnList;
							}
						}
					}
				} else {
					if (dbVendor == DbVendor.Oracle || dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
						tableName = tableName.toUpperCase();
					}

					final DatabaseMetaData metaData = connection.getMetaData();
					try (ResultSet resultSet = metaData.getPrimaryKeys(connection.getCatalog(), null, tableName)) {
						final CaseInsensitiveSet returnList = new CaseInsensitiveSet();
						while (resultSet.next()) {
							returnList.add(resultSet.getString("COLUMN_NAME"));
						}
						return returnList;
					}
				}
			} catch (final Exception e) {
				throw new Exception("Cannot read primarykey columns for table " + tableName + ": " + e.getMessage(), e);
			}
		}
	}

	/**
	 * tablePatternExpression contains a comma-separated list of tablenames with wildcards *? and !(not, before tablename)
	 *
	 * @param connection
	 * @param tablePatternExpression
	 * @return
	 * @throws Exception
	 */
	public static List<String> getAvailableTables(final Connection connection, final String tablePatternExpression) throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.setFetchSize(100);
			final DbVendor dbVendor = getDbVendor(connection);

			String tableQuery;
			if (DbVendor.Oracle == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM all_tables WHERE owner NOT IN ('CTXSYS', 'DBSNMP', 'MDDATA', 'MDSYS', 'DMSYS', 'OLAPSYS', 'ORDPLUGINS', 'OUTLN', 'SI_INFORMATN_SCHEMA', 'SYS', 'SYSMAN', 'SYSTEM')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.MySQL == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = SCHEMA()";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					statement.setFetchSize(100);
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.MariaDB == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = SCHEMA()";
				final List<String> positivePatterns = new ArrayList<>();
				final List<String> negativePatterns = new ArrayList<>();
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							negativePatterns.add(tablePattern.substring(1));
						} else {
							positivePatterns.add(tablePattern);
						}
					}
				}
				if (positivePatterns.size() > 0) {
					tableQuery += " AND (";
					tableQuery += "table_name LIKE '" + Utilities.join(positivePatterns, "' OR table_name LIKE '") + "'";
					tableQuery += ")";
				}
				if (negativePatterns.size() > 0) {
					tableQuery += " AND (";
					tableQuery += "table_name NOT LIKE '" + Utilities.join(negativePatterns, "' OR table_name LIKE '") + "'";
					tableQuery += ")";
				}

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					statement.setFetchSize(100);
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.PostgreSQL == dbVendor) {
				tableQuery = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					statement.setFetchSize(100);
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.SQLite == dbVendor) {
				tableQuery = "SELECT name FROM sqlite_master WHERE type = 'table'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY name";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.Derby == dbVendor) {
				tableQuery = "SELECT tablename FROM sys.systables WHERE tabletype = 'T'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND tablename NOT LIKE '" + tablePattern.substring(1) + "' {ESCAPE '\\'}";
						} else {
							tableQuery += " AND tablename LIKE '" + tablePattern + "' {ESCAPE '\\'}";
						}
					}
				}
				tableQuery += " ORDER BY tablename";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("tablename"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.Firebird == dbVendor) {
				tableQuery = "SELECT TRIM(rdb$relation_name) AS table_name FROM rdb$relations WHERE rdb$view_blr IS NULL AND (rdb$system_flag IS NULL OR rdb$system_flag = 0)";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND TRIM(rdb$relation_name) NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND TRIM(rdb$relation_name) LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY rdb$relation_name";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.HSQL == dbVendor) {
				tableQuery = "SELECT table_name FROM information_schema.system_tables WHERE table_type = 'TABLE' AND table_schem = 'PUBLIC'";
				for (String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
					if (Utilities.isNotBlank(tablePattern)) {
						tablePattern = tablePattern.trim().toUpperCase().replace("%", "\\%").replace("_", "\\_").replace("*", "%").replace("?", "_");
						if (tablePattern.startsWith("!")) {
							tableQuery += " AND table_name NOT LIKE '" + tablePattern.substring(1) + "' ESCAPE '\\'";
						} else {
							tableQuery += " AND table_name LIKE '" + tablePattern + "' ESCAPE '\\'";
						}
					}
				}
				tableQuery += " ORDER BY table_name";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						tableNamesToExport.add(resultSet.getString("table_name"));
					}
					return tableNamesToExport;
				}
			} else if (DbVendor.Cassandra == dbVendor) {
				final List<String> systemKeySpaces = Arrays.asList(new String[] {"system", "system_auth", "system_schema", "system_distributed", "system_traces"});
				tableQuery = "SELECT keyspace_name, table_name FROM system_schema.tables";

				try (ResultSet resultSet = statement.executeQuery(tableQuery)) {
					final List<String> tableNamesToExport = new ArrayList<>();
					while (resultSet.next()) {
						final String keySpaceName = resultSet.getString("keyspace_name");
						if (!systemKeySpaces.contains(keySpaceName)) {
							final String tableName = resultSet.getString("table_name");
							boolean addTable = true;
							for (final String tablePattern : tablePatternExpression.split(",| |;|\\||\n")) {
								if (Utilities.isNotBlank(tablePattern)) {
									addTable = false;
									if (Pattern.compile(tablePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*").replace("_", ".").replace("%", ".*")).matcher(tableName).find()) {
										addTable = true;
										break;
									}
								}
							}

							if (addTable) {
								tableNamesToExport.add(tableName);
							}
						}
					}
					return tableNamesToExport;
				}

			} else {
				throw new Exception("Unknown db vendor");
			}
		}
	}

	public static void createTable(final Connection connection, final String tablename, final Map<String, DbColumnType> columnsAndTypes, final Collection<String> keyColumns) throws Exception {
		if  (keyColumns != null) {
			for (String keyColumn : keyColumns) {
				keyColumn = Utilities.trimSimultaneously(Utilities.trimSimultaneously(keyColumn, "\""), "`");
				if (!columnsAndTypes.containsKey(keyColumn)) {
					throw new Exception("Cannot create table. Keycolumn '" + keyColumn + "' is not included in column types");
				}
			}
		}

		try (Statement statement = connection.createStatement()) {
			final DbVendor dbVendor = getDbVendor(connection);

			String columnPart = "";
			for (final Entry<String, DbColumnType> columnAndType : columnsAndTypes.entrySet()) {
				if (columnPart.length() > 0) {
					columnPart += ", ";
				}
				final DbColumnType columnType = columnAndType.getValue();
				if (columnType != null) {
					final String dataType = getDataType(dbVendor, columnType.getSimpleDataType());
					int dataLength = 0;
					if (dbVendor != DbVendor.Cassandra) {
						dataLength = dataType.toLowerCase().contains("varchar") ? (int) columnType.getCharacterByteSize() : 0;
					}
					columnPart += escapeVendorReservedNames(dbVendor, columnAndType.getKey()) + " " + dataType + (dataLength > 0 ? "(" + dataLength + ")" : "");
				} else {
					final String dataType = getDataType(dbVendor, SimpleDataType.String);
					columnPart += escapeVendorReservedNames(dbVendor, columnAndType.getKey()) + " " + dataType + "(1)";
				}
			}

			String primaryKeyPart = "";
			if (Utilities.isNotEmpty(keyColumns)) {
				primaryKeyPart = ", PRIMARY KEY (" + joinColumnVendorEscaped(dbVendor, keyColumns) + ")";
			}
			statement.execute("CREATE TABLE " + tablename + " (" + columnPart + primaryKeyPart + ")");
			if (dbVendor == DbVendor.Derby) {
				connection.commit();
			}
		}
	}

	public static String getDataType(final DbVendor dbVendor, final SimpleDataType simpleDataType) throws Exception {
		if (dbVendor == DbVendor.Oracle) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case DateTime: return "TIMESTAMP";
				case Date: return "DATE";
				case Integer: return "NUMBER";
				case Float: return "NUMBER";
				case String: return "VARCHAR2";
				default: return "VARCHAR2";
			}
		} else if (dbVendor == DbVendor.MySQL) {
			switch (simpleDataType) {
				case Blob: return "LONGBLOB";
				case Clob: return "LONGTEXT";
				case DateTime: return "TIMESTAMP NULL";
				case Date: return "DATE";
				case Integer: return "INT";
				case Float: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.MariaDB) {
			switch (simpleDataType) {
				case Blob: return "LONGBLOB";
				case Clob: return "LONGTEXT";
				case DateTime: return "TIMESTAMP NULL";
				case Date: return "DATE";
				case Integer: return "INT";
				case Float: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.HSQL) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case DateTime: return "TIMESTAMP";
				case Date: return "DATE";
				case Integer: return "INTEGER";
				case Float: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.PostgreSQL) {
			switch (simpleDataType) {
				case Blob: return "BYTEA";
				case Clob: return "TEXT";
				case DateTime: return "TIMESTAMP";
				case Date: return "DATE";
				case Integer: return "INTEGER";
				case Float: return "REAL";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.SQLite) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case DateTime: return "TIMESTAMP";
				case Date: return "DATE";
				case Integer: return "INTEGER";
				case Float: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.Derby) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "CLOB";
				case DateTime: return "TIMESTAMP";
				case Date: return "DATE";
				case Integer: return "INTEGER";
				case Float: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.Firebird) {
			switch (simpleDataType) {
				case Blob: return "BLOB SUB_TYPE BINARY";
				case Clob: return "BLOB SUB_TYPE TEXT";
				case DateTime: return "TIMESTAMP";
				case Date: return "DATE";
				case Integer: return "INTEGER";
				case Float: return "DOUBLE PRECISION";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else if (dbVendor == DbVendor.Cassandra) {
			switch (simpleDataType) {
				case Blob: return "BLOB";
				case Clob: return "TEXT";
				case DateTime: return "TIMESTAMP";
				case Date: return "DATE";
				case Integer: return "INT";
				case Float: return "DOUBLE";
				case String: return "VARCHAR";
				default: return "VARCHAR";
			}
		} else {
			throw new Exception("Cannot get datatype: " + dbVendor + "/" + simpleDataType);
		}
	}

	public static String getTypeNameById(final int typeId) throws Exception {
		switch (typeId) {
			case Types.BIT: return "BIT";
			case Types.TINYINT: return "TINYINT";
			case Types.SMALLINT: return "SMALLINT";
			case Types.INTEGER: return "INTEGER";
			case Types.BIGINT: return "BIGINT";
			case Types.FLOAT: return "FLOAT";
			case Types.REAL: return "REAL";
			case Types.DOUBLE: return "DOUBLE";
			case Types.NUMERIC: return "NUMERIC";
			case Types.DECIMAL: return "DECIMAL";
			case Types.CHAR: return "CHAR";
			case Types.VARCHAR: return "VARCHAR";
			case Types.LONGVARCHAR: return "LONGVARCHAR";
			case Types.DATE: return "DATE";
			case Types.TIME: return "TIME";
			case Types.TIMESTAMP: return "TIMESTAMP";
			case Types.BINARY: return "BINARY";
			case Types.VARBINARY: return "VARBINARY";
			case Types.LONGVARBINARY: return "LONGVARBINARY";
			case Types.NULL: return "NULL";
			case Types.OTHER: return "OTHER";
			case Types.JAVA_OBJECT: return "JAVA_OBJECT";
			case Types.DISTINCT: return "DISTINCT";
			case Types.STRUCT: return "STRUCT";
			case Types.ARRAY: return "ARRAY";
			case Types.BLOB: return "BLOB";
			case Types.CLOB: return "CLOB";
			case Types.REF: return "REF";
			case Types.DATALINK: return "DATALINK";
			case Types.BOOLEAN: return "BOOLEAN";
			case Types.ROWID: return "ROWID";
			case Types.NCHAR: return "NCHAR";
			case Types.NVARCHAR: return "NVARCHAR";
			case Types.LONGNVARCHAR: return "LONGNVARCHAR";
			case Types.NCLOB: return "NCLOB";
			case Types.SQLXML: return "SQLXML";
			case Types.REF_CURSOR: return "REF_CURSOR";
			case Types.TIME_WITH_TIMEZONE: return "TIME_WITH_TIMEZONE";
			case Types.TIMESTAMP_WITH_TIMEZONE: return "TIMESTAMP_WITH_TIMEZONE";
			default: throw new Exception("Unknown type id: " + typeId);
		}
	}

	public static String getDownloadUrl(final DbVendor dbVendor) throws Exception {
		if (dbVendor == DbVendor.MySQL) {
			return DOWNLOAD_LOCATION_MYSQL;
		} else if (dbVendor == DbVendor.MariaDB) {
			return DOWNLOAD_LOCATION_MARIADB;
		} else if (dbVendor == DbVendor.Oracle) {
			return DOWNLOAD_LOCATION_ORACLE;
		} else if (dbVendor == DbVendor.PostgreSQL) {
			return DOWNLOAD_LOCATION_POSTGRESQL;
		} else if (dbVendor == DbVendor.Firebird) {
			return DOWNLOAD_LOCATION_FIREBIRD;
		} else if (dbVendor == DbVendor.Derby) {
			return DOWNLOAD_LOCATION_DERBY;
		} else if (dbVendor == DbVendor.SQLite) {
			return DOWNLOAD_LOCATION_SQLITE;
		} else if (dbVendor == DbVendor.HSQL) {
			return DOWNLOAD_LOCATION_HSQL;
		} else if (dbVendor == DbVendor.MsSQL) {
			return DOWNLOAD_LOCATION_MSSQL;
		} else {
			throw new Exception("Invalid db vendor");
		}
	}

	/**
	 * Update the duplicateIndexColumn column of all entries of a table with the minimum index value from itemIndexColumn of other duplicates.
	 *
	 * @param connection
	 * @param tableName
	 * @param keyColumns
	 * @param keyColumnsWithFunctions
	 * @param itemIndexColumn
	 * @param duplicateIndexColumn
	 * @throws Exception
	 */
	public static void markDuplicates(final Connection connection, final String tableName, final Collection<String> keyColumnsWithFunctions, String itemIndexColumn, String duplicateIndexColumn) throws Exception {
		if (Utilities.isNotEmpty(keyColumnsWithFunctions)) {
			final DbVendor dbVendor = getDbVendor(connection);
			itemIndexColumn = escapeVendorReservedNames(dbVendor, itemIndexColumn);
			duplicateIndexColumn = escapeVendorReservedNames(dbVendor, duplicateIndexColumn);

			final StringBuilder selectPart = new StringBuilder();
			final StringBuilder wherePart = new StringBuilder();
			int columnIndex = 0;
			for (String columnWithFunction : keyColumnsWithFunctions) {
				columnWithFunction = escapeVendorReservedNames(dbVendor, columnWithFunction.trim());

				if (selectPart.length() > 0) {
					selectPart.append(", ");
					wherePart.append(", ");
				}

				selectPart.append(columnWithFunction + " AS col" + columnIndex);

				wherePart.append("subselect.col" + columnIndex);

				wherePart.append(" = ");

				if (columnWithFunction.contains("(")) {
					wherePart.append(columnWithFunction.replace("(", "(" + tableName + "."));
				} else {
					wherePart.append(tableName + "." + columnWithFunction);
				}

				columnIndex++;
			}

			try (Statement statement = connection.createStatement()) {
				// The indirection with a subselect is needed for MySQL (You can't specify target table for update in FROM clause)
				final String setDuplicateReferences = "UPDATE " + tableName + " SET " + duplicateIndexColumn + " = (SELECT subselect." + itemIndexColumn + " FROM"
						+ " (SELECT " + selectPart.toString() + ", MIN(" + itemIndexColumn + ") AS " + itemIndexColumn + " FROM " + tableName + " GROUP BY " + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + ") subselect"
						+ " WHERE " + wherePart.toString() + ")";
				statement.executeUpdate(setDuplicateReferences);
				connection.commit();
			} catch (final Exception e) {
				connection.rollback();
				throw new Exception("Cannot markTrailingDuplicates: " + e.getMessage(), e);
			}
		} else {
			throw new Exception("Cannot markDuplicates: Missing keycolumns");
		}
	}

	public static String getKeyColumnEquationList(final DbVendor dbVendor, final Collection<String> columnNamesWithFunctions, final String tableAlias1, final String tableAlias2) {
		final StringBuilder returnValue = new StringBuilder();
		for (String columnName : columnNamesWithFunctions) {
			columnName = escapeVendorReservedNames(dbVendor, columnName.trim());

			if (returnValue.length() > 0) {
				returnValue.append(", ");
			}

			if (Utilities.isNotBlank(tableAlias1)) {
				if (columnName.contains("(")) {
					returnValue.append(columnName.replace("(", "(" + tableAlias1 + "."));
				} else {
					returnValue.append(tableAlias1 + "." + columnName);
				}
			} else {
				returnValue.append(columnName);
			}

			returnValue.append(" = ");

			if (Utilities.isNotBlank(tableAlias2)) {
				if (columnName.contains("(")) {
					returnValue.append(columnName.replace("(", "(" + tableAlias2 + "."));
				} else {
					returnValue.append(tableAlias2 + "." + columnName);
				}
			} else {
				returnValue.append(columnName);
			}
		}
		return returnValue.toString();
	}

	public static int detectDuplicates(final Connection connection, final String tableName, final Collection<String> keyColumnsWithFunctions) throws Exception {
		try (Statement statement = connection.createStatement()) {
			final String countDuplicatesStatement = "SELECT COUNT(*) FROM (SELECT COUNT(*) FROM " + tableName + " GROUP BY " + joinColumnVendorEscaped(getDbVendor(connection), keyColumnsWithFunctions) + " HAVING COUNT(*) > 1) subsel";
			try (ResultSet resultSet = statement.executeQuery(countDuplicatesStatement)) {
				if (resultSet.next()) {
					return resultSet.getInt(1);
				} else {
					return 0;
				}
			}
		} catch (final Exception e) {
			throw new Exception("Cannot detectDuplicates: " + e.getMessage(), e);
		}
	}

	public static int detectDuplicatesCrossTables(final Connection connection, final String detectTableName, final String fromTableName, final List<String> keyColumnsWithFunctions) throws Exception {
		try (Statement statement = connection.createStatement()) {
			final String selectDuplicatesNumber = "SELECT COUNT(*) FROM " + detectTableName + " a WHERE EXISTS (SELECT 1 FROM " + fromTableName + " b WHERE " + getKeyColumnEquationList(getDbVendor(connection), keyColumnsWithFunctions, "a", "b") + ")";
			try (ResultSet resultSet = statement.executeQuery(selectDuplicatesNumber)) {
				resultSet.next();
				return resultSet.getInt(1);
			}
		} catch (final Exception e) {
			throw new Exception("Cannot detectDuplicatesCrossTables: " + e.getMessage(), e);
		}
	}

	public static String addIndexedIntegerColumn(final Connection connection, final String tableName, final String columnBaseName) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);

		String columnName = columnBaseName;
		int i = 0;
		while (checkTableAndColumnsExist(connection, tableName, columnName) && i < 10) {
			i++;
			columnName = columnBaseName + "_" + i;
		}
		if (i >= 10) {
			throw new Exception("Cannot create columnBaseName " + columnBaseName + " in table " + tableName);
		}

		try (Statement statement = connection.createStatement()) {
			statement.execute("ALTER TABLE " + tableName + " ADD " + columnName + " " + DbUtilities.getDataType(dbVendor, SimpleDataType.Integer));
			if (dbVendor != DbVendor.Cassandra) {
				try {
					statement.execute("CREATE INDEX tmp" + new Random().nextInt(100000000) + "_idx ON " + tableName + " (" + columnName + ")");
				} catch (final Exception e) {
					e.printStackTrace();
					// Work without index. Maybe it already exists or it is a not allowed function based index
				}
			}
		}

		return columnName;
	}

	public static int dropDuplicatesCrossTable(final Connection connection, final String keepInTableName, final String deleteInTableName, final List<String> keyColumnsWithFunctions) throws Exception {
		if (Utilities.isEmpty(keyColumnsWithFunctions)) {
			final DbVendor dbVendor = getDbVendor(connection);
			try (Statement statement = connection.createStatement()) {
				final String deleteDuplicates = "DELETE FROM " + deleteInTableName + " WHERE " + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + " IN (SELECT " + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + " FROM " + keepInTableName + ")";
				final int numberOfDeletedDuplicates = statement.executeUpdate(deleteDuplicates);
				connection.commit();
				return numberOfDeletedDuplicates;
			} catch (final Exception e) {
				connection.rollback();
				throw new Exception("Cannot deleteTableCrossDuplicates: " + e.getMessage(), e);
			}
		} else {
			return 0;
		}
	}

	public static int dropDuplicates(final Connection connection, final String tableName, final Collection<String> keyColumns) throws Exception {
		if (detectDuplicates(connection, tableName, keyColumns) > 0) {
			final DbVendor dbVendor = getDbVendor(connection);

			String originalItemIndexColumn = null;
			String originalDuplicateIndexColumn = null;
			try (Statement statement = connection.createStatement()) {
				originalItemIndexColumn = createLineNumberIndexColumn(connection, tableName, "drop_idx");
				originalDuplicateIndexColumn = addIndexedIntegerColumn(connection, tableName, "drop_dpl");

				// Try to create an additional column index
				if (Utilities.isNotEmpty(keyColumns) && dbVendor != DbVendor.Cassandra) {
					try {
						statement.execute("CREATE INDEX tmp" + new Random().nextInt(100000000) + "_idx ON " + tableName + " (" + joinColumnVendorEscaped(getDbVendor(connection), keyColumns) + ")");
					} catch (@SuppressWarnings("unused") final Exception e) {
						// Work without index. Maybe it already exists or it is a not allowed function based index
					}
				}

				markDuplicates(connection, tableName, keyColumns, originalItemIndexColumn, originalDuplicateIndexColumn);

				final int numberOfDeletedDuplicates = statement.executeUpdate("DELETE FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn);
				connection.commit();
				return numberOfDeletedDuplicates;
			} catch (final Exception e) {
				connection.rollback();
				throw new Exception("Cannot dropDuplicates: " + e.getMessage(), e);
			} finally {
				dropColumnIfExists(connection, tableName, originalItemIndexColumn);
				dropColumnIfExists(connection, tableName, originalDuplicateIndexColumn);
			}
		} else {
			return 0;
		}
	}

	public static int joinDuplicates(final Connection connection, final String tableName, final Collection<String> keyColumnsWithFunctions, final boolean updateWithNullValues) throws Exception {
		if (detectDuplicates(connection, tableName, keyColumnsWithFunctions) > 0) {
			final DbVendor dbVendor = getDbVendor(connection);
			final String randomSuffix = "" + new Random().nextInt(100000000);
			final String interimTableName = "tmp_join_" + randomSuffix;
			String originalItemIndexColumn = null;
			String originalDuplicateIndexColumn = null;

			// Join all duplicates in destination table
			try (Statement statement = connection.createStatement()) {
				// Create additional index columns
				if (Utilities.isNotEmpty(keyColumnsWithFunctions) && dbVendor != DbVendor.Cassandra) {
					try {
						statement.execute("CREATE INDEX tmp" + randomSuffix + "_idx ON " + tableName + " (" + joinColumnVendorEscaped(dbVendor, keyColumnsWithFunctions) + ")");
						connection.commit();
					} catch (@SuppressWarnings("unused") final Exception e) {
						// Work without index. Maybe it already exists or it is a not allowed function based index
					}
				}

				originalItemIndexColumn = createLineNumberIndexColumn(connection, tableName, "join_idx");
				originalDuplicateIndexColumn = addIndexedIntegerColumn(connection, tableName, "join_dpl");

				markDuplicates(connection, tableName, keyColumnsWithFunctions, originalItemIndexColumn, originalDuplicateIndexColumn);
				connection.commit();

				// Create temp table
				if (dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
					statement.execute("CREATE TABLE " + interimTableName + " AS (SELECT * FROM " + tableName + ") WITH NO DATA");
					statement.executeUpdate("INSERT INTO " + interimTableName + " (SELECT * FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn + ")");
				} else if (dbVendor == DbVendor.PostgreSQL) {
					// Close a maybe open transaction to allow DDL-statement
					connection.rollback();
					statement.execute("CREATE TABLE " + interimTableName + " AS SELECT * FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn);
				} else if (dbVendor == DbVendor.Firebird) {
					// There is no "create table as select"-statement in firebird
					createTable(connection, interimTableName, getColumnDataTypes(connection, tableName), null);
				} else if (dbVendor == DbVendor.Cassandra) {
					// There is no "create table as select"-statement in Cassandra
					createTable(connection, interimTableName, getColumnDataTypes(connection, tableName), getPrimaryKeyColumns(connection, tableName));
				} else {
					statement.execute("CREATE TABLE " + interimTableName + " AS SELECT * FROM " + tableName + " WHERE " + originalItemIndexColumn + " != " + originalDuplicateIndexColumn);
				}
				connection.commit();

				final int deletedDuplicatesInDB = dropDuplicates(connection, tableName, keyColumnsWithFunctions);

				final List<String> columnsWithoutAutoIncrement = new ArrayList<>();
				for (final Entry<String, DbColumnType> column : getColumnDataTypes(connection, tableName).entrySet()) {
					if (!column.getValue().isAutoIncrement()) {
						columnsWithoutAutoIncrement.add(column.getKey());
					}
				}

				updateAllExistingItems(connection, interimTableName, tableName, columnsWithoutAutoIncrement, keyColumnsWithFunctions, originalItemIndexColumn, updateWithNullValues, null);
				connection.commit();
				return deletedDuplicatesInDB;
			} catch (final Exception e) {
				throw new Exception("Cannot joinDuplicates: " + e.getMessage(), e);
			} finally {
				dropTableIfExists(connection, interimTableName);
				dropColumnIfExists(connection, tableName, originalItemIndexColumn);
				dropColumnIfExists(connection, tableName, originalDuplicateIndexColumn);
			}
		} else {
			return 0;
		}
	}

	private static String createLineNumberIndexColumn(final Connection connection, final String tableName, final String indexColumnNameBaseName) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);
		final String indexColumnName = addIndexedIntegerColumn(connection, tableName, indexColumnNameBaseName);

		try (Statement statement = connection.createStatement()) {
			if (dbVendor == DbVendor.MySQL) {
				statement.execute("SELECT @n := 0");
				statement.execute("UPDATE " + tableName + " SET " + indexColumnName + " = @n := @n + 1");
			} else if (dbVendor == DbVendor.MariaDB) {
				statement.execute("SELECT @n := 0");
				statement.execute("UPDATE " + tableName + " SET " + indexColumnName + " = @n := @n + 1");
			} else if (dbVendor == DbVendor.Oracle) {
				statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = ROWNUM");
			} else if (dbVendor == DbVendor.SQLite) {
				statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = ROWID");
			} else if (dbVendor == DbVendor.HSQL) {
				statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = ROWNUM()");
			} else if (dbVendor == DbVendor.Derby) {
				String autoIncrementColumn = null;
				final List<String> columnsWithoutAutoIncrement = new ArrayList<>();
				for (final Entry<String, DbColumnType> column : getColumnDataTypes(connection, tableName).entrySet()) {
					if (!column.getValue().isAutoIncrement()) {
						columnsWithoutAutoIncrement.add(column.getKey());
					} else {
						autoIncrementColumn = column.getKey();
					}
				}
				columnsWithoutAutoIncrement.remove(indexColumnName);
				if (autoIncrementColumn != null) {
					statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = " + autoIncrementColumn);
				} else {
					statement.executeUpdate("INSERT INTO " + tableName + " (" + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", " + indexColumnName + ") (SELECT " + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", ROW_NUMBER() OVER() FROM " + tableName + ")");
					statement.executeUpdate("DELETE FROM " + tableName + " WHERE " + indexColumnName + " IS NULL");
				}
			} else if (dbVendor == DbVendor.PostgreSQL) {
				String autoIncrementColumn = null;
				final List<String> columnsWithoutAutoIncrement = new ArrayList<>();
				for (final Entry<String, DbColumnType> column : getColumnDataTypes(connection, tableName).entrySet()) {
					if (!column.getValue().isAutoIncrement()) {
						columnsWithoutAutoIncrement.add(column.getKey());
					} else {
						autoIncrementColumn = column.getKey();
					}
				}
				columnsWithoutAutoIncrement.remove(indexColumnName);
				if (autoIncrementColumn != null) {
					statement.executeUpdate("UPDATE " + tableName + " SET " + indexColumnName + " = " + autoIncrementColumn);
				} else {
					statement.executeUpdate("INSERT INTO " + tableName + " (" + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", " + indexColumnName + ") (SELECT " + joinColumnVendorEscaped(dbVendor, columnsWithoutAutoIncrement) + ", ROW_NUMBER() OVER() FROM " + tableName + ")");
					statement.executeUpdate("DELETE FROM " + tableName + " WHERE " + indexColumnName + " IS NULL");
				}
			} else if (dbVendor == DbVendor.Cassandra) {
				statement.execute("UPDATE " + tableName + " SET " + indexColumnName + " = " + getPrimaryKeyColumns(connection, tableName).iterator().next());
			} else {
				throw new Exception("Unsupported db vendor");
			}
			connection.commit();
		} catch (final Exception e) {
			throw new Exception("Cannot create lineNumberIndexColumn: " + e.getMessage(), e);
		}
		return indexColumnName;
	}

	public static boolean dropColumnIfExists(final Connection connection, final String tableName, final String columnName) throws Exception {
		if (connection != null && tableName != null && columnName != null && checkTableAndColumnsExist(connection, tableName, columnName)) {
			final DbVendor dbVendor = getDbVendor(connection);
			if (dbVendor == DbVendor.SQLite) {
				// SQLite cannot drop columns
				try (Statement statement = connection.createStatement()) {
					final String randomSuffix = "" + new Random().nextInt(100000000);

					String createTableStatement = null;
					try (ResultSet resultSet = statement.executeQuery("SELECT sql FROM sqlite_master WHERE type = 'table' AND LOWER(name) = LOWER('" + tableName + "')")) {
						if (resultSet.next()) {
							createTableStatement = resultSet.getString("sql");
						} else {
							throw new Exception("Cannot find create table sql");
						}
					}

					createTableStatement = createTableStatement.replaceAll(", " + columnName + " [a-zA-Z0-9()]+\\)", ")").replaceAll(", " + columnName + " [a-zA-Z0-9()]+,", ",");

					statement.execute("ALTER TABLE " + tableName + " RENAME TO tmp" + randomSuffix + "_old");
					statement.execute(createTableStatement);
					final Set<String> columns = getColumnNames(connection, tableName);
					columns.remove(columnName);
					statement.execute("INSERT INTO " + tableName + " (" + joinColumnVendorEscaped(dbVendor, columns) + ") SELECT " + joinColumnVendorEscaped(dbVendor, columns) + " FROM tmp" + randomSuffix + "_old");
					statement.execute("DROP TABLE tmp" + randomSuffix + "_old");
					connection.commit();
					return true;
				} catch (final Exception e) {
					connection.rollback();
					throw e;
				}
			} else {
				try (Statement statement = connection.createStatement()) {
					statement.execute("ALTER TABLE " + tableName + " DROP " + (dbVendor == DbVendor.Cassandra ? "" :  "COLUMN ") + columnName);
					connection.commit();
					return true;
				} catch (final Exception e) {
					connection.rollback();
					throw e;
				}
			}
		}
		return false;
	}

	public static boolean dropTableIfExists(final Connection connection, final String tableName) throws Exception {
		if (connection != null && tableName != null && checkTableExist(connection, tableName)) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("DROP TABLE " + tableName);
				connection.commit();
				return true;
			}
		}
		return false;
	}

	public static void copyTableStructure(final Connection connection, final String sourceTableName, final List<String> columnNames, final List<String> keyColumns, final String destinationTableName) throws Exception {
		try (Statement statement = connection.createStatement()) {
			final DbVendor dbVendor = getDbVendor(connection);
			if (dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
				statement.execute("CREATE TABLE " + destinationTableName + " AS (SELECT " + joinColumnVendorEscaped(dbVendor, columnNames) + " FROM " + sourceTableName + ") WITH NO DATA");
			} else if (dbVendor == DbVendor.PostgreSQL) {
				// Close a maybe open transaction to allow DDL-statement
				connection.rollback();
				statement.execute("CREATE TABLE " + destinationTableName + " AS SELECT " + joinColumnVendorEscaped(dbVendor, columnNames) + " FROM " + sourceTableName + " WHERE 1 = 0");
			} else if (dbVendor == DbVendor.Firebird) {
				// There is no "create table as select"-statement in firebird
				createTable(connection, destinationTableName, getColumnDataTypes(connection, sourceTableName), null);
			} else if (dbVendor == DbVendor.Cassandra) {
				// There is no "create table as select"-statement in Cassandra
				createTable(connection, destinationTableName, getColumnDataTypes(connection, sourceTableName), getPrimaryKeyColumns(connection, sourceTableName));
			} else {
				statement.execute("CREATE TABLE " + destinationTableName + " AS SELECT " + joinColumnVendorEscaped(dbVendor, columnNames) + " FROM " + sourceTableName + " WHERE 1 = 0");
			}

			if (dbVendor != DbVendor.Cassandra) {
				// Make all columns nullable
				final CaseInsensitiveMap<DbColumnType> columnDataTypes = getColumnDataTypes(connection, destinationTableName);
				for (final Entry<String, DbColumnType> columnDataType : columnDataTypes.entrySet()) {
					if (!columnDataType.getValue().isNullable() && (keyColumns == null || !keyColumns.contains(columnDataType.getKey()))) {
						String typeString = columnDataType.getValue().getTypeName();
						if (columnDataType.getValue().getSimpleDataType() == SimpleDataType.String) {
							typeString += "(" + (Long.toString(columnDataType.getValue().getCharacterByteSize())) + ")";
						} else if (columnDataType.getValue().getSimpleDataType() == SimpleDataType.Integer) {
							typeString += "(" + (Integer.toString(columnDataType.getValue().getNumericPrecision())) + ")";
						} else if (columnDataType.getValue().getSimpleDataType() == SimpleDataType.Float) {
							typeString += "(" + (Integer.toString(columnDataType.getValue().getNumericPrecision())) + ")";
						}
						statement.execute("ALTER TABLE " + destinationTableName + " MODIFY " + columnDataType.getKey() + " " + typeString + " NULL");
					}
				}
			}

			if (dbVendor == DbVendor.PostgreSQL || dbVendor == DbVendor.Firebird) {
				connection.commit();
			}
		}
	}

	public static String createIndex(final Connection connection, final String tableName, final List<String> columns) throws Exception {
		try (Statement statement = connection.createStatement()) {
			final String indexNameSuffix = "_" + new Random().nextInt(100000000) + "_ix";
			final String indexName = Utilities.shortenStringToMaxLengthCutRight(tableName, 30 - indexNameSuffix.length(), "") + indexNameSuffix;
			statement.execute("CREATE INDEX " + indexName + " ON " + tableName + " (" + joinColumnVendorEscaped(getDbVendor(connection), columns) + ")");
			return indexName;
		} catch (final Exception e) {
			throw new Exception("Cannot create index: " + e.getMessage(), e);
		}
	}

	public static int clearTable(final Connection connection, final String tableName) throws Exception {
		try (Statement statement = connection.createStatement()) {
			return statement.executeUpdate("DELETE FROM " + tableName);
		} catch (final Exception e) {
			throw new Exception("Cannot clear table: " + e.getMessage(), e);
		}
	}

	public static int insertNotExistingItems(final Connection connection, final String sourceTableName, final String destinationTableName, final List<String> insertColumns, final List<String> keyColumnsWithFunctions, final String additionalInsertValues) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);
		try (Statement statement = connection.createStatement()) {
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

			String insertDataStatement = "INSERT INTO " + destinationTableName + " (" + additionalInsertValuesSqlColumns + joinColumnVendorEscaped(dbVendor, insertColumns) + ") SELECT " + additionalInsertValuesSqlValues + joinColumnVendorEscaped(dbVendor, insertColumns) + " FROM " + sourceTableName + " a";
			if (Utilities.isNotEmpty(keyColumnsWithFunctions)) {
				insertDataStatement += " WHERE NOT EXISTS (SELECT 1 FROM " + destinationTableName + " b WHERE " + getKeyColumnEquationList(dbVendor, keyColumnsWithFunctions, "a", "b") + ")";
			}
			final int numberOfInserts = statement.executeUpdate(insertDataStatement);
			connection.commit();
			return numberOfInserts;
		} catch (final Exception e) {
			connection.rollback();
			throw new Exception("Cannot insert: " + e.getMessage(), e);
		}
	}

	public static int insertAllItems(final Connection connection, final String sourceTableName, final String destinationTableName, final List<String> insertColumns, final String additionalInsertValues) throws Exception {
		return insertNotExistingItems(connection, sourceTableName, destinationTableName, insertColumns, null, additionalInsertValues);
	}

	public static int updateAllExistingItems(final Connection connection, final String sourceTableName, final String destinationTableName, Collection<String> updateColumns, final Collection<String> keyColumns, String itemIndexColumn, final boolean updateWithNullValues, final String additionalUpdateValues) throws Exception {
		if (keyColumns == null || keyColumns.isEmpty()) {
			throw new Exception("Missing keycolumns");
		}

		// Do not update the keycolumns
		updateColumns = new ArrayList<>(updateColumns);
		updateColumns.removeAll(keyColumns);

		int updatedItems = 0;

		if (!updateColumns.isEmpty()) {
			try (Statement statement = connection.createStatement()) {
				String additionalUpdateValuesSql = "";
				if (Utilities.isNotBlank(additionalUpdateValues)) {
					for (final String line : Utilities.splitAndTrimListQuoted(additionalUpdateValues, '\n', '\r', ';')) {
						final String columnName = line.substring(0, line.indexOf("=")).trim();
						final String columnvalue = line.substring(line.indexOf("=") + 1).trim();
						additionalUpdateValuesSql += columnName + " = " + columnvalue + ", ";
					}
				}

				updateColumns = new ArrayList<>(updateColumns);
				updateColumns.removeAll(keyColumns);
				if (updateColumns.size() > 0 || additionalUpdateValuesSql.length() > 0) {
					final DbVendor dbVendor = getDbVendor(connection);
					itemIndexColumn = escapeVendorReservedNames(dbVendor, itemIndexColumn);
					String updatedIndexColumn = null;
					try {
						if (updateWithNullValues) {
							String updateSetPart = "";
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								if (updateSetPart.length() > 0) {
									updateSetPart += ", ";
								}
								updateSetPart += updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
										+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))";
							}
							final String updateAllAtOnce = "UPDATE " + destinationTableName + " SET " + additionalUpdateValuesSql + updateSetPart
									+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "b") + ")";
							updatedItems = statement.executeUpdate(updateAllAtOnce);
						} else {
							updatedIndexColumn = addIndexedIntegerColumn(connection, destinationTableName, "updatedindex");
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								final String updateSingleColumn = "UPDATE " + destinationTableName
										+ " SET " + additionalUpdateValuesSql + updatedIndexColumn + " = 1, " + updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
										+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + updateColumn + " IS NOT NULL AND " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))"
										+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + updateColumn + " IS NOT NULL AND " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "b") + ")";
								statement.executeUpdate(updateSingleColumn);
							}

							try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + destinationTableName + " WHERE " + updatedIndexColumn + " = 1")) {
								resultSet.next();
								updatedItems = resultSet.getInt(1);
							}
						}
						connection.commit();
					} catch (final Exception e) {
						connection.rollback();
						throw e;
					} finally {
						dropColumnIfExists(connection, destinationTableName, updatedIndexColumn);
					}
				}
			} catch (final Exception e) {
				throw new Exception("Cannot update: " + e.getMessage(), e);
			}
		}

		return updatedItems;
	}

	public static int updateFirstExistingItems(final Connection connection, final String sourceTableName, final String destinationTableName, Collection<String> updateColumns, final Collection<String> keyColumns, final String itemIndexColumn, final boolean updateWithNullValues, final String additionalUpdateValues) throws Exception {
		if (keyColumns == null || keyColumns.isEmpty()) {
			throw new Exception("Missing keycolumns");
		}

		// Do not update the keycolumns
		updateColumns = new ArrayList<>(updateColumns);
		updateColumns.removeAll(keyColumns);

		int updatedItems = 0;

		if (!updateColumns.isEmpty()) {
			try (Statement statement = connection.createStatement()) {
				String additionalUpdateValuesSql = "";
				if (Utilities.isNotBlank(additionalUpdateValues)) {
					for (final String line : Utilities.splitAndTrimListQuoted(additionalUpdateValues, '\n', '\r', ';')) {
						final String columnName = line.substring(0, line.indexOf("=")).trim();
						final String columnvalue = line.substring(line.indexOf("=") + 1).trim();
						additionalUpdateValuesSql += columnName + " = " + columnvalue + ", ";
					}
				}

				updateColumns = new ArrayList<>(updateColumns);
				updateColumns.removeAll(keyColumns);
				if (updateColumns.size() > 0 || additionalUpdateValuesSql.length() > 0) {
					String originalItemIndexColumn = null;
					String updatedIndexColumn = null;
					try {
						final DbVendor dbVendor = getDbVendor(connection);
						originalItemIndexColumn = createLineNumberIndexColumn(connection, destinationTableName, "itemindex");

						// Mark duplicates in temp table
						final List<String> keycolumnParts = new ArrayList<>();
						for (final String keyColumn : keyColumns) {
							keycolumnParts.add("src." + escapeVendorReservedNames(dbVendor, keyColumn) + " = " + sourceTableName + "." + escapeVendorReservedNames(dbVendor, keyColumn) + " AND src." + escapeVendorReservedNames(dbVendor, keyColumn) + " IS NOT NULL");
						}
						final String updateStatement = "UPDATE " + sourceTableName + " SET " + itemIndexColumn + " = COALESCE((SELECT MIN(src." + originalItemIndexColumn + ") FROM " + destinationTableName + " src WHERE " + Utilities.join(keycolumnParts, " AND ") + "), 0)";
						statement.executeUpdate(updateStatement);
						connection.commit();

						// Update with marked items
						if (updateWithNullValues) {
							String updateSetPart = "";
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								if (updateSetPart.length() > 0) {
									updateSetPart += ", ";
								}
								updateSetPart += updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
										+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))";
							}
							final String updateAllAtOnce = "UPDATE " + destinationTableName + " SET " + additionalUpdateValuesSql + updateSetPart
									+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + originalItemIndexColumn + " = b." + itemIndexColumn + ")";
							updatedItems = statement.executeUpdate(updateAllAtOnce);
						} else {
							updatedIndexColumn = addIndexedIntegerColumn(connection, destinationTableName, "updatedindex");
							for (String updateColumn : updateColumns) {
								updateColumn = escapeVendorReservedNames(dbVendor, updateColumn);
								final String updateSingleColumn = "UPDATE " + destinationTableName
										+ " SET " + additionalUpdateValuesSql + updatedIndexColumn + " = 1, " + updateColumn + " = (SELECT " + updateColumn + " FROM " + sourceTableName + " WHERE " + itemIndexColumn + " ="
										+ " (SELECT MAX(" + itemIndexColumn + ") FROM " + sourceTableName + " c WHERE " + updateColumn + " IS NOT NULL AND " + getKeyColumnEquationList(dbVendor, keyColumns, destinationTableName, "c") + "))"
										+ " WHERE EXISTS (SELECT 1 FROM " + sourceTableName + " b WHERE " + originalItemIndexColumn + " = b." + itemIndexColumn + ")";
								statement.executeUpdate(updateSingleColumn);
							}

							try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + destinationTableName + " WHERE " + updatedIndexColumn + " = 1")) {
								resultSet.next();
								updatedItems = resultSet.getInt(1);
							}
						}
						connection.commit();
					} catch (final Exception e) {
						connection.rollback();
						throw e;
					} finally {
						dropColumnIfExists(connection, destinationTableName, originalItemIndexColumn);
						dropColumnIfExists(connection, destinationTableName, updatedIndexColumn);
					}
				}
			} catch (final Exception e) {
				throw new Exception("Cannot update first entries: " + e.getMessage(), e);
			}
		}

		return updatedItems;
	}

	/**
	 * Check for existing index
	 * Returns null, if check cannot be executed (happens on some db vendors)
	 *
	 * @param connection
	 * @param tableName
	 * @param keyColumns
	 * @return
	 * @throws Exception
	 */
	public static Boolean checkForIndex(final Connection connection, final String tableName, final List<String> keyColumns) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);
		if (dbVendor == DbVendor.Oracle) {
			try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM user_ind_columns WHERE LOWER(table_name) = ? AND LOWER(column_name) = ?")) {
				for (String keyColumn : keyColumns) {
					keyColumn = escapeVendorReservedNames(dbVendor, keyColumn);
					statement.setString(1, tableName.toLowerCase());
					statement.setString(2, keyColumn.toLowerCase());
					try (ResultSet resultSet = statement.executeQuery()) {
						if (!resultSet.next() || resultSet.getInt(1) <= 0) {
							return false;
						}
					}
				}
				return true;
			}
		} else if (dbVendor == DbVendor.MySQL) {
			try (PreparedStatement statement = connection.prepareStatement("SHOW INDEX FROM " + tableName.toLowerCase() + " WHERE column_name = ?")) {
				for (String keyColumn : keyColumns) {
					keyColumn = escapeVendorReservedNames(dbVendor, keyColumn);
					statement.setString(1, keyColumn.toLowerCase());
					try (ResultSet resultSet = statement.executeQuery()) {
						if (!resultSet.next()) {
							return false;
						}
					}
				}
				return true;
			}
		} else if (dbVendor == DbVendor.MariaDB) {
			try (PreparedStatement statement = connection.prepareStatement("SHOW INDEX FROM " + tableName.toLowerCase() + " WHERE column_name = ?")) {
				for (String keyColumn : keyColumns) {
					keyColumn = escapeVendorReservedNames(dbVendor, keyColumn);
					statement.setString(1, keyColumn.toLowerCase());
					try (ResultSet resultSet = statement.executeQuery()) {
						if (!resultSet.next()) {
							return false;
						}
					}
				}
				return true;
			}
		} else if (dbVendor == DbVendor.Cassandra) {
			final List<String> indexedColumns = new ArrayList<>();
			indexedColumns.addAll(getPrimaryKeyColumns(connection, tableName));

			try (PreparedStatement statement = connection.prepareStatement("SELECT options FROM system_schema.indexes WHERE table_name = ? ALLOW FILTERING")) {
				statement.setString(1, tableName);
				try (ResultSet resultSet = statement.executeQuery()) {
					while (resultSet.next()) {
						final String options = resultSet.getString("options");
						indexedColumns.add((String) ((JsonObject) JsonReader.readJsonItemString(options).getValue()).get("target"));
					}
				}

				for (final String keyColumn : keyColumns) {
					if (!indexedColumns.contains(keyColumn)) {
						return false;
					}
				}
				return true;
			}
		} else {
			return null;
		}
	}

	public static String unescapeVendorReservedNames(final DbVendor dbVendor, final String value) {
		if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
			return Utilities.trimSimultaneously(value, "`");
		} else if (dbVendor == DbVendor.Oracle) {
			return Utilities.trimSimultaneously(value, "\"");
		} else if (dbVendor == DbVendor.Derby) {
			return Utilities.trimSimultaneously(value, "\"");
		} else {
			return value;
		}
	}

	public static String escapeVendorReservedNames(final DbVendor dbVendor, final String value) {
		if (Utilities.isBlank(value)) {
			return value;
		} else if ((dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) && RESERVED_WORDS_MYSSQL_MARIADB.contains(value.toLowerCase())) {
			return "`" + value + "`";
		} else if (dbVendor == DbVendor.Oracle && RESERVED_WORDS_ORACLE.contains(value.toLowerCase())) {
			return "\"" + value.toUpperCase() + "\"";
		} else if (dbVendor == DbVendor.Derby && RESERVED_WORDS_DERBY.contains(value.toLowerCase())) {
			return "\"" + value + "\"";
		} else {
			return value;
		}
	}

	public static String joinColumnVendorEscaped(final DbVendor dbVendor, final Collection<String> columnNames) {
		final StringBuilder returnValue = new StringBuilder();
		for (final String columnName : columnNames) {
			if (returnValue.length() > 0) {
				returnValue.append(", ");
			}
			returnValue.append(escapeVendorReservedNames(dbVendor, columnName));
		}
		return returnValue.toString();
	}

	/**
	 * Special shutdown command to free single user derby db for next connection maybe of another thread
	 *
	 * @param dbName
	 * @throws Exception
	 */
	public static void shutDownDerbyDb(String dbName) throws Exception {
		dbName = Utilities.replaceUsersHome(dbName);
		if (!new File(dbName).exists()) {
			throw new DbNotExistsException("Derby db directory '" + dbName + "' is not available");
		} else if (!new File(dbName).isDirectory()) {
			throw new Exception("Derby db directory '" + dbName + "' is not a directory");
		}
		try (Connection connection = DriverManager.getConnection("jdbc:derby:" + Utilities.replaceUsersHome(dbName) + ";shutdown=true")) {
			// do nothing
		} catch (@SuppressWarnings("unused") final SQLNonTransientConnectionException e) {
			// Derby shutdown ALWAYS throws a SQLNonTransientConnectionException by intention (see also: http://db.apache.org/derby/docs/10.3/devguide/tdevdvlp20349.html)
		}
	}

	public static int getMysqlMaxAllowedPacketSize(final Connection connection) throws Exception {
		try (PreparedStatement preparedStatement = connection.prepareStatement("SHOW VARIABLES like 'max_allowed_packet'");
				ResultSet resultSet = preparedStatement.executeQuery()) {
			if (resultSet.next()) {
				final Object value = resultSet.getObject("value");
				if (value != null) {
					return Integer.parseInt(value.toString());
				} else {
					return 0;
				}
			} else {
				return 0;
			}
		}
	}

	public static boolean isValidIdentifier(final String identifier) {
		final Pattern pattern = Pattern.compile("[0-9A-Za-z_]{1,30}");
		return pattern.matcher(identifier).matches();
	}

	public static boolean isValidAlias(final String alias) {
		final Pattern pattern = Pattern.compile("[0-9A-Za-z_]{1,30}");
		return pattern.matcher(alias).matches();
	}

	public static void updateBlob(final DbDefinition dbDefinition, final String sqlUpdateStatementWithPlaceholder, final String filePath) throws Exception {
		try (Connection connection = createConnection(dbDefinition, false)) {
			try (FileInputStream inputStream = new FileInputStream(new File(filePath))) {
				try (PreparedStatement preparedStatement = connection.prepareStatement(sqlUpdateStatementWithPlaceholder)) {
					preparedStatement.setBinaryStream(1, inputStream);
					preparedStatement.execute();
				}
			}
		}
	}

	/**
	 * Demand recalculation of Oracle Table stats to enable table indices after creation of big temp tables
	 *
	 * @param connection
	 * @param tableName
	 * @throws Exception
	 */
	public static void gatherTableStats(final Connection connection, final String tableName) throws Exception {
		if (getDbVendor(connection) == DbVendor.Oracle) {
			try (Statement statement = connection.createStatement()) {
				String username;
				try (ResultSet resultSet = statement.executeQuery("SELECT USER FROM DUAL")) {
					if (resultSet.next()) {
						username = resultSet.getString(1);
					} else {
						throw new Exception("Cannot detect oracle db username");
					}
				}

				final String executeStatement =
						"begin\n"
								+ " dbms_stats.gather_table_stats(\n"
								+ " ownname => '" + username.toUpperCase() + "',\n"
								+ " tabname => '" + tableName.toUpperCase() + "',\n"
								+ " estimate_percent => 30,\n"
								+ " method_opt => 'for all columns size 254',\n"
								+ " cascade => true,\n"
								+ " no_invalidate => FALSE\n"
								+ " );\n"
								+ " end;";

				final boolean success = statement.execute(executeStatement);
				if (!success) {
					throw new Exception("Cannot gatherTableStats");
				}
			}
		}
	}

	public static long getTableStorageSize(final Connection connection, final String tableName) throws Exception {
		if (getDbVendor(connection) == DbVendor.Oracle) {
			final List<String> indexNames = getTableIndexNames(connection, tableName);
			final String sql = "SELECT SUM(bytes) FROM user_segments WHERE segment_name in (SELECT segment_name FROM user_lobs WHERE table_name = ?) OR segment_name IN (?" + Utilities.repeat(", ?", indexNames.size()) + ")";
			try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
				preparedStatement.setString(1, tableName.toUpperCase());
				preparedStatement.setString(2, tableName.toUpperCase());
				for (int i = 0; i < indexNames.size(); i++) {
					preparedStatement.setString(3 + i, indexNames.get(i).toUpperCase());
				}
				try (final ResultSet resultSet = preparedStatement.executeQuery()) {
					if (resultSet.next()) {
						return resultSet.getLong(1);
					} else {
						throw new Exception("Invalid data for: " + tableName);
					}
				}
			}
		} else if (getDbVendor(connection) == DbVendor.MariaDB || getDbVendor(connection) == DbVendor.MySQL) {
			final String sql = "SELECT data_length + data_free FROM information_schema.tables WHERE LOWER(table_name) = ?";
			try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
				preparedStatement.setString(1, tableName.toLowerCase());
				try (final ResultSet resultSet = preparedStatement.executeQuery()) {
					if (resultSet.next()) {
						return resultSet.getLong(1);
					} else {
						throw new Exception("Invalid data for: " + tableName);
					}
				}
			}
		} else {
			throw new Exception("getTableStorageSize is not supported by this db vendor");
		}
	}

	public static List<String> getTableIndexNames(final Connection connection, final String tableName) throws Exception {
		if (getDbVendor(connection) == DbVendor.Oracle) {
			final String query = "SELECT index_name FROM user_ind_columns WHERE LOWER(table_name) = ?";
			try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
				preparedStatement.setString(1, tableName.toLowerCase());
				try (final ResultSet resultSet = preparedStatement.executeQuery()) {
					final List<String> resultList = new ArrayList<>();
					while (resultSet.next()) {
						resultList.add(resultSet.getString(1));
					}
					return resultList;
				}
			}
		} else if (getDbVendor(connection) == DbVendor.MariaDB || getDbVendor(connection) == DbVendor.MySQL) {
			final String query = "SHOW INDEX FROM " + tableName.toLowerCase();
			try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
				try (final ResultSet resultSet = preparedStatement.executeQuery()) {
					final List<String> resultList = new ArrayList<>();
					while (resultSet.next()) {
						resultList.add(resultSet.getString(1));
					}
					return resultList;
				}
			}
		} else {
			throw new Exception("getTableIndexNames is not supported by this db vendor");
		}
	}

	public static void setForeignKeyConstraintStatus(final DbVendor dbVendor, final Connection connection, final boolean activated) throws Exception {
		if (dbVendor == DbVendor.Oracle) {
			final List<String> sqlListToExecute = new ArrayList<>();
			try (Statement statement = connection.createStatement()) {
				try (ResultSet result = statement.executeQuery("SELECT table_name, constraint_name FROM all_constraints WHERE constraint_type = 'R' AND status = '" + (activated ? "DISABLED" : "ENABLED") + "' AND UPPER(owner) NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'SITE_SYS', 'MDSYS')")) {
					while (result.next()) {
						final String tableName = result.getString("table_name");
						final String constraintName = result.getString("constraint_name");

						sqlListToExecute.add("ALTER TABLE " + tableName + " " + (activated ? "ENABLE" : "DISABLE") + " CONSTRAINT " + constraintName);
					}
				}

				for (final String sqlToExecute : sqlListToExecute) {
					try {
						statement.execute(sqlToExecute);
					} catch (final SQLException e) {
						e.printStackTrace();
					}
				}

				if (activated) {
					final List<String> notEnabledConstraints = new ArrayList<>();
					try (ResultSet result = statement.executeQuery("SELECT table_name, constraint_name FROM all_constraints WHERE constraint_type = 'R' AND status != 'ENABLED' AND UPPER(owner) NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'SITE_SYS', 'MDSYS')")) {
						while (result.next()) {
							final String tableName = result.getString("table_name");
							final String constraintName = result.getString("constraint_name");
							notEnabledConstraints.add(constraintName + " (TABLE: " + tableName + ")");
						}
					}
					if (notEnabledConstraints.size() > 0) {
						throw new Exception("Cannot activate following foreign key constraints:\n" + Utilities.join(notEnabledConstraints, ",\n"));
					}
				}
			}
		} else if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
			try (Statement statement = connection.createStatement()) {
				if (activated) {
					statement.execute("SET global FOREIGN_KEY_CHECKS = 1");
				} else {
					statement.execute("SET global FOREIGN_KEY_CHECKS = 0");
				}
			}
		} else {
			throw new Exception("ForeignKeyConstraintStatus change not available for dbvendor '" + dbVendor.name() + "'");
		}
	}

	public static void setTriggerStatus(final DbVendor dbVendor, final Connection connection, final boolean activated) throws Exception {
		if (dbVendor == DbVendor.Oracle) {
			final List<String> sqlListToExecute = new ArrayList<>();
			try (Statement statement = connection.createStatement()) {
				try (ResultSet result = statement.executeQuery("SELECT trigger_name FROM user_triggers WHERE status = '" + (activated ? "DISABLED" : "ENABLED") + "'")) {
					while (result.next()) {
						final String triggerName = result.getString("trigger_name");

						sqlListToExecute.add("ALTER TRIGGER " + triggerName + " " + (activated ? "ENABLE" : "DISABLE"));
					}
				}

				for (final String sqlToExecute : sqlListToExecute) {
					try {
						statement.execute(sqlToExecute);
					} catch (final SQLException e) {
						e.printStackTrace();
					}
				}

				if (activated) {
					final List<String> notEnabledTriggers = new ArrayList<>();
					try (ResultSet result = statement.executeQuery("SELECT trigger_name FROM user_triggers WHERE status = 'DISABLED'")) {
						while (result.next()) {
							final String triggerName = result.getString("trigger_name");
							notEnabledTriggers.add(triggerName);
						}
					}
					if (notEnabledTriggers.size() > 0) {
						throw new Exception("Cannot activate following triggers:\n" + Utilities.join(notEnabledTriggers, ",\n"));
					}
				}
			}
		} else {
			throw new Exception("TriggerStatus change not available for dbvendor '" + dbVendor.name() + "'");
		}
	}

	public static Statement getStatementForLargeQuery(final Connection connection) throws Exception {
		final DbVendor dbVendor = getDbVendor(connection);
		if (DbVendor.MySQL == dbVendor) {
			final Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setFetchSize(Integer.MIN_VALUE);
			return statement;
		} else {
			final Statement statement = connection.createStatement();
			statement.setFetchSize(100);
			return statement;
		}
	}

	public static List<DatabaseForeignKey> getForeignKeys(final Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			final DbVendor dbVendor = getDbVendor(connection);
			try {
				if (dbVendor == DbVendor.Oracle || dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
					tableName = tableName.toUpperCase();
				}

				final DatabaseMetaData metaData = connection.getMetaData();
				try (ResultSet resultSet = metaData.getImportedKeys(connection.getCatalog(), null, tableName)) {
					final List<DatabaseForeignKey> returnList = new ArrayList<>();
					while (resultSet.next()) {
						String foreignKeyName = resultSet.getString("FK_NAME");
						if (foreignKeyName != null) {
							foreignKeyName = foreignKeyName.toLowerCase();
						}
						final String columnName = resultSet.getString("FKCOLUMN_NAME");
						final String referencesTableName = resultSet.getString("PKTABLE_NAME");
						final String referencedColumnName = resultSet.getString("PKCOLUMN_NAME");
						returnList.add(new DatabaseForeignKey(foreignKeyName, tableName.toLowerCase(), columnName.toLowerCase(), referencesTableName.toLowerCase(), referencedColumnName.toLowerCase()));
					}
					return returnList;
				}
			} catch (final Exception e) {
				throw new Exception("Cannot read foreign key columns for table " + tableName + ": " + e.getMessage(), e);
			}
		}
	}

	public static List<DatabaseConstraint> getConstraints(final Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			final DbVendor dbVendor = getDbVendor(connection);
			try {
				if (dbVendor == DbVendor.Oracle || dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
					tableName = tableName.toUpperCase();
				}

				if (dbVendor == DbVendor.Oracle) {
					try (PreparedStatement preparedStatement = connection.prepareStatement(
							"SELECT user_constraints.constraint_name, user_constraints.constraint_type, user_cons_columns.column_name"
									+ " FROM user_constraints JOIN user_cons_columns ON user_constraints.constraint_name = user_cons_columns.constraint_name AND user_constraints.table_name = user_cons_columns.table_name"
									+ " WHERE user_constraints.table_name = ? ORDER BY user_constraints.constraint_name")) {
						preparedStatement.setString(1, tableName);
						try (ResultSet resultSet = preparedStatement.executeQuery()) {
							final List<DatabaseConstraint> returnList = new ArrayList<>();
							while (resultSet.next()) {
								final String constraintName = resultSet.getString("constraint_name").toLowerCase();
								final String constraintType = resultSet.getString("constraint_type");
								final String columnName = resultSet.getString("column_name");

								returnList.add(new DatabaseConstraint(tableName.toLowerCase(), constraintName, ConstraintType.fromName(constraintType), columnName.toLowerCase(), null));
							}
							return returnList;
						}
					}
				} else if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
					final List<DatabaseConstraint> returnList = new ArrayList<>();
					try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = ? ORDER BY constraint_name")) {
						preparedStatement.setString(1, tableName);
						try (ResultSet resultSet = preparedStatement.executeQuery()) {
							while (resultSet.next()) {
								final String constraintName = resultSet.getString("constraint_name").toLowerCase();
								final String constraintType = resultSet.getString("constraint_type");

								returnList.add(new DatabaseConstraint(tableName.toLowerCase(), constraintName, ConstraintType.fromName(constraintType), null, null));
							}
						}
					}

					try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE table_name = ?")) {
						preparedStatement.setString(1, tableName);
						try (ResultSet resultSet = preparedStatement.executeQuery()) {
							while (resultSet.next()) {
								final String constraintName = resultSet.getString("constraint_name").toLowerCase();
								final String condition = resultSet.getString("check_clause");

								for (final DatabaseConstraint databaseConstraint : returnList) {
									if (constraintName != null && constraintName.equalsIgnoreCase(databaseConstraint.getConstraintName())) {
										databaseConstraint.setCondition(condition);
									}
								}
							}
						}
					} catch (final Exception e) {
						System.err.println("Cannot read constraint conditions of table '" + tableName + "': " + e.getMessage());
					}

					return returnList;
				} else {
					return null;
				}
			} catch (final Exception e) {
				throw new Exception("Cannot read constraints columns for table " + tableName + ": " + e.getMessage(), e);
			}
		}
	}

	public static List<DatabaseIndex> getIndices(final Connection connection, String tableName) throws Exception {
		if (Utilities.isBlank(tableName)) {
			return null;
		} else {
			final DbVendor dbVendor = getDbVendor(connection);
			try {
				if (dbVendor == DbVendor.Oracle || dbVendor == DbVendor.HSQL || dbVendor == DbVendor.Derby) {
					tableName = tableName.toUpperCase();
				}

				if (dbVendor == DbVendor.Oracle) {
					try (Statement statement = connection.createStatement()) {
						try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT index_name, column_name FROM all_ind_columns WHERE table_name = ?")) {
							preparedStatement.setString(1, tableName);
							try (ResultSet resultSet = preparedStatement.executeQuery()) {
								final Map<String, List<String>> interimMap = new HashMap<>();

								final List<DatabaseIndex> returnList = new ArrayList<>();
								while (resultSet.next()) {
									final String indexName = resultSet.getString("index_name").toLowerCase();
									final String columnName = resultSet.getString("column_name".toLowerCase());

									if (!interimMap.containsKey(indexName)) {
										interimMap.put(indexName, new ArrayList<>());
									}

									interimMap.get(indexName).add(columnName.toLowerCase());
								}

								for (final String indexName : Utilities.sortButPutItemsFirst(interimMap.keySet(), "primary")) {
									returnList.add(new DatabaseIndex(tableName.toLowerCase(), indexName, interimMap.get(indexName)));
								}
								return returnList;
							}
						}
					}
				} else if (dbVendor == DbVendor.MySQL || dbVendor == DbVendor.MariaDB) {
					try (Statement statement = connection.createStatement()) {
						try (ResultSet resultSet = statement.executeQuery("SHOW index FROM " + tableName)) {
							final Map<String, List<String>> interimMap = new HashMap<>();

							final List<DatabaseIndex> returnList = new ArrayList<>();
							while (resultSet.next()) {
								final String indexName = resultSet.getString("key_name").toLowerCase();
								final String columnName = resultSet.getString("column_name".toLowerCase());

								if (!interimMap.containsKey(indexName)) {
									interimMap.put(indexName, new ArrayList<>());
								}

								interimMap.get(indexName).add(columnName.toLowerCase());
							}

							for (final String indexName : Utilities.sortButPutItemsFirst(interimMap.keySet(), "primary")) {
								returnList.add(new DatabaseIndex(tableName.toLowerCase(), indexName, interimMap.get(indexName)));
							}
							return returnList;
						}
					}
				} else {
					return null;
				}
			} catch (final Exception e) {
				throw new Exception("Cannot read indexed columns for table " + tableName + ": " + e.getMessage(), e);
			}
		}
	}
}
