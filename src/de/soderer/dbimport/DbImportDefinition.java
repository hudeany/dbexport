package de.soderer.dbimport;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import de.soderer.dbimport.dataprovider.CsvDataProvider;
import de.soderer.dbimport.dataprovider.DataProvider;
import de.soderer.dbimport.dataprovider.ExcelDataProvider;
import de.soderer.dbimport.dataprovider.JsonDataProvider;
import de.soderer.dbimport.dataprovider.OdsDataProvider;
import de.soderer.dbimport.dataprovider.XmlDataProvider;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.db.DbUtilities.DbVendor;
import de.soderer.utilities.worker.WorkerParentSimple;

public class DbImportDefinition extends DbDefinition {
	/**
	 * The Enum DataType.
	 */
	public enum DataType {
		CSV,
		JSON,
		XML,
		SQL,
		EXCEL,
		ODS;

		/**
		 * Gets the string representation of data type.
		 *
		 * @param dataType
		 *            the data type
		 * @return the from string
		 * @throws Exception
		 *             the exception
		 */
		public static DataType getFromString(final String dataTypeString) throws Exception {
			for (final DataType dataType : DataType.values()) {
				if (dataType.toString().equalsIgnoreCase(dataTypeString)) {
					return dataType;
				}
			}
			throw new Exception("Invalid data type: " + dataTypeString);
		}
	}

	/**
	 * The Enum ImportMode.
	 */
	public enum ImportMode {
		CLEARINSERT,
		INSERT,
		UPDATE,
		UPSERT;

		public static ImportMode getFromString(final String importModeString) throws Exception {
			for (final ImportMode importMode : ImportMode.values()) {
				if (importMode.toString().equalsIgnoreCase(importModeString)) {
					return importMode;
				}
			}
			throw new Exception("Invalid import mode: " + importModeString);
		}
	}

	/**
	 * The Enum DuplicateMode.
	 */
	public enum DuplicateMode {
		NO_CHECK,
		CKECK_SOURCE_ONLY_DROP,
		CKECK_SOURCE_ONLY_JOIN,
		UPDATE_FIRST_DROP,
		UPDATE_FIRST_JOIN,
		UPDATE_ALL_DROP,
		UPDATE_ALL_JOIN,
		MAKE_UNIQUE_DROP,
		MAKE_UNIQUE_JOIN;

		public static DuplicateMode getFromString(final String duplicateModeString) throws Exception {
			for (final DuplicateMode duplicateMode : DuplicateMode.values()) {
				if (duplicateMode.toString().equalsIgnoreCase(duplicateModeString)) {
					return duplicateMode;
				}
			}
			throw new Exception("Invalid duplicate mode: " + duplicateModeString);
		}
	}

	// Mandatory parameters

	/** The tableName. */
	private String tableName = "*";

	/** The importFilePath or data. */
	private String importFilePathOrData;

	private boolean isInlineData = false;

	// Default optional parameters

	/** The data type. */
	private DataType dataType = DataType.CSV;

	/** Log activation. */
	private boolean log = false;

	/** The verbose. */
	private boolean verbose = false;

	/** The encoding. */
	private Charset encoding = StandardCharsets.UTF_8;

	/** The separator. */
	private char separator = ';';

	/** The string quote. */
	private Character stringQuote = '"';

	/** The escape string quote character. */
	private char escapeStringQuote = '"';

	/** The no headers. */
	private boolean noHeaders = false;

	/** The null value string. */
	private String nullValueString = "";

	private boolean completeCommit = false;

	private boolean allowUnderfilledLines = false;

	private boolean removeSurplusEmptyTrailingColumns = false;

	private ImportMode importMode = ImportMode.INSERT;

	private DuplicateMode duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;

	private boolean updateNullData = true;

	private List<String> keycolumns = null;

	private boolean createTable = false;

	private String structureFilePath = null;

	private boolean trimData = false;

	private String mapping = "";

	private String additionalInsertValues = null;

	private String additionalUpdateValues = null;

	private boolean logErroneousData = false;

	private boolean createNewIndexIfNeeded = true;

	private boolean deactivateForeignKeyConstraints = false;

	private String dataPath = null;

	private String schemaFilePath = null;

	private char[] zipPassword = null;

	private String databaseTimeZone = TimeZone.getDefault().getID();

	private String importDataTimeZone = TimeZone.getDefault().getID();

	private String dateFormat = null;

	private String dateTimeFormat = null;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	public String getImportFilePathOrData() {
		return importFilePathOrData;
	}

	public boolean isInlineData() {
		return isInlineData;
	}

	public void setInlineData(final boolean isInlineData) {
		this.isInlineData = isInlineData;
	}

	public void setImportFilePathOrData(final String importFilePathOrData, final boolean checkForExistingFiles) throws DbImportException {
		this.importFilePathOrData = importFilePathOrData;
		if (this.importFilePathOrData != null && !isInlineData) {
			// Check filepath syntax
			if (getImportFilePathOrData().contains("?") || getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(getImportFilePathOrData().lastIndexOf("/"), getImportFilePathOrData().lastIndexOf("\\"));
				String directoryPath = getImportFilePathOrData().substring(0, lastSeparator);
				directoryPath = Utilities.replaceUsersHome(directoryPath);
				final String filePattern = getImportFilePathOrData().substring(lastSeparator + 1);
				if (directoryPath.contains("?") || directoryPath.contains("*")) {
					throw new DbImportException("Import directory path contains wildcards, but wildcards only allowed for filenames: " + (directoryPath));
				} else if (checkForExistingFiles) {
					if (!new File(directoryPath).exists()) {
						throw new DbImportException("Import path does not exist: " + (directoryPath));
					} else if (!new File((directoryPath)).isDirectory()) {
						throw new DbImportException("Import path is not a directory: " + (directoryPath));
					} else {
						if (FileUtilities.getFilesByPattern(new File(directoryPath), filePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*"), false).size() == 0) {
							throw new DbImportException("Import file pattern has no matching files: " + (directoryPath));
						}
					}
				}
			} else {
				try {
					Paths.get(importFilePathOrData);
					isInlineData = false;
				} catch (@SuppressWarnings("unused") final Exception e) {
					isInlineData = true;
				}
				if (!isInlineData) {
					this.importFilePathOrData = Utilities.replaceUsersHome(this.importFilePathOrData.trim());
					if (this.importFilePathOrData.endsWith(File.separator)) {
						this.importFilePathOrData = this.importFilePathOrData.substring(0, this.importFilePathOrData.length() - 1);
					}
					isInlineData = false;
				}
			}
		}
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(final DataType dataType) {
		this.dataType = dataType;
	}

	public boolean isLog() {
		return log;
	}

	public void setLog(final boolean log) {
		this.log = log;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(final boolean verbose) {
		this.verbose = verbose;
	}

	public Charset getEncoding() {
		return encoding;
	}

	public void setEncoding(final Charset encoding) {
		this.encoding = encoding;
	}

	public char getSeparator() {
		return separator;
	}

	public void setSeparator(final char separator) {
		this.separator = separator;
	}

	public Character getStringQuote() {
		return stringQuote;
	}

	public void setStringQuote(final Character stringQuote) {
		this.stringQuote = stringQuote;
	}

	public char getEscapeStringQuote() {
		return escapeStringQuote;
	}

	public void setEscapeStringQuote(final char escapeStringQuote) {
		this.escapeStringQuote = escapeStringQuote;
	}

	public boolean isNoHeaders() {
		return noHeaders;
	}

	public void setNoHeaders(final boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	public String getNullValueString() {
		return nullValueString;
	}

	public void setNullValueString(final String nullValueString) {
		this.nullValueString = nullValueString;
	}

	public boolean isCompleteCommit() {
		return completeCommit;
	}

	public void setCompleteCommit(final boolean completeCommit) {
		this.completeCommit = completeCommit;
	}

	public boolean isAllowUnderfilledLines() {
		return allowUnderfilledLines;
	}

	public void setAllowUnderfilledLines(final boolean allowUnderfilledLines) {
		this.allowUnderfilledLines = allowUnderfilledLines;
	}

	public boolean isRemoveSurplusEmptyTrailingColumns() {
		return removeSurplusEmptyTrailingColumns;
	}

	public void setRemoveSurplusEmptyTrailingColumns(final boolean removeSurplusEmptyTrailingColumns) {
		this.removeSurplusEmptyTrailingColumns = removeSurplusEmptyTrailingColumns;
	}

	public ImportMode getImportMode() {
		return importMode;
	}

	public void setImportMode(final ImportMode importMode) {
		this.importMode = importMode;
	}

	public DuplicateMode getDuplicateMode() {
		return duplicateMode;
	}

	public void setDuplicateMode(final DuplicateMode duplicateMode) {
		this.duplicateMode = duplicateMode;
	}

	public boolean isUpdateNullData() {
		return updateNullData;
	}

	public void setUpdateNullData(final boolean updateNullData) {
		this.updateNullData = updateNullData;
	}

	public List<String> getKeycolumns() {
		return keycolumns;
	}

	public void setKeycolumns(final List<String> keycolumns) {
		this.keycolumns = keycolumns;
	}

	public boolean isCreateTable() {
		return createTable;
	}

	public void setCreateTable(final boolean createTable) {
		this.createTable = createTable;
	}

	public String getStructureFilePath() {
		return structureFilePath;
	}

	public void setStructureFilePath(final String structureFilePath) {
		this.structureFilePath = structureFilePath;
	}

	public String getMapping() {
		return mapping;
	}

	public void setMapping(final String mapping) {
		this.mapping = mapping;
	}

	public boolean isTrimData() {
		return trimData;
	}

	public void setTrimData(final boolean trimData) {
		this.trimData = trimData;
	}

	public void setAdditionalInsertValues(final String additionalInsertValues) {
		this.additionalInsertValues = additionalInsertValues;
	}

	public String getAdditionalInsertValues() {
		return additionalInsertValues;
	}

	public void setAdditionalUpdateValues(final String additionalUpdateValues) {
		this.additionalUpdateValues = additionalUpdateValues;
	}

	public String getAdditionalUpdateValues() {
		return additionalUpdateValues;
	}

	public boolean isLogErroneousData() {
		return logErroneousData;
	}

	public void setLogErroneousData(final boolean logErroneousData) {
		this.logErroneousData = logErroneousData;
	}

	public boolean isCreateNewIndexIfNeeded() {
		return createNewIndexIfNeeded;
	}

	public void setCreateNewIndexIfNeeded(final boolean createNewIndexIfNeeded) {
		this.createNewIndexIfNeeded = createNewIndexIfNeeded;
	}

	public boolean isDeactivateForeignKeyConstraints() {
		return deactivateForeignKeyConstraints;
	}

	public void setDeactivateForeignKeyConstraints(final boolean deactivateForeignKeyConstraints) {
		this.deactivateForeignKeyConstraints = deactivateForeignKeyConstraints;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(final String dataPath) {
		this.dataPath = dataPath;
	}

	public String getSchemaFilePath() {
		return schemaFilePath;
	}

	public void setSchemaFilePath(final String schemaFilePath) {
		this.schemaFilePath = schemaFilePath;
	}

	public void setZipPassword(final char[] zipPassword) {
		this.zipPassword = zipPassword;
	}

	public char[] getZipPassword() {
		return zipPassword;
	}

	public void setDatabaseTimeZone(final String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
		if (this.databaseTimeZone == null) {
			this.databaseTimeZone = TimeZone.getDefault().getID();
		}
	}

	public String getDatabaseTimeZone() {
		return databaseTimeZone;
	}

	public void setImportDataTimeZone(final String importDataTimeZone) {
		this.importDataTimeZone = importDataTimeZone;
		if (this.importDataTimeZone == null) {
			this.importDataTimeZone = TimeZone.getDefault().getID();
		}
	}

	public String getImportDataTimeZone() {
		return importDataTimeZone;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(final String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getDateTimeFormat() {
		return dateTimeFormat;
	}

	public void setDateTimeFormat(final String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}

	public void checkParameters() throws Exception {
		super.checkParameters(DbImport.APPLICATION_NAME, DbImport.CONFIGURATION_FILE);

		if (importFilePathOrData == null) {
			throw new DbImportException("ImportFilePath or data is missing");
		} else if (!isInlineData) {
			if (getImportFilePathOrData().contains("?") || getImportFilePathOrData().contains("*")) {
				final int lastSeparator = Math.max(getImportFilePathOrData().lastIndexOf("/"), getImportFilePathOrData().lastIndexOf("\\"));
				String directoryPath = getImportFilePathOrData().substring(0, lastSeparator);
				directoryPath = Utilities.replaceUsersHome(directoryPath);
				final String filePattern = getImportFilePathOrData().substring(lastSeparator + 1);
				if (directoryPath.contains("?") || directoryPath.contains("*")) {
					throw new DbImportException("Import directory path contains wildcards, but wildcards only allowed for filenames: " + (directoryPath));
				} else if (!new File(directoryPath).exists()) {
					throw new DbImportException("Import path does not exist: " + (directoryPath));
				} else if (!new File((directoryPath)).isDirectory()) {
					throw new DbImportException("Import path is not a directory: " + (directoryPath));
				} else {
					if (FileUtilities.getFilesByPattern(new File(directoryPath), filePattern.replace(".", "\\.").replace("?", ".").replace("*", ".*"), false).size() == 0) {
						throw new DbImportException("Import file pattern has no matching files: " + (directoryPath));
					}
				}
			} else {
				if (!new File(importFilePathOrData).exists()) {
					throw new DbImportException("ImportFilePath does not exist: " + importFilePathOrData);
				} else if (!new File(importFilePathOrData).isFile()) {
					throw new DbImportException("ImportFilePath is not a file: " + importFilePathOrData);
				}
			}
		}

		if (noHeaders && dataType != DataType.CSV && dataType != DataType.EXCEL && dataType != DataType.ODS) {
			throw new DbImportException("NoHeaders is not supported for data format " + dataType);
		}

		if (dataType != DataType.SQL) {
			if ((importMode == ImportMode.UPDATE || importMode == ImportMode.UPSERT)
					&& (keycolumns == null || keycolumns.isEmpty())) {
				throw new DbImportException("Invalid empty key column definition for import mode: " + importMode);
			}
		}

		if (Utilities.isNotEmpty(dataPath) && dataType != DataType.XML && dataType != DataType.JSON && dataType != DataType.EXCEL && dataType != DataType.ODS) {
			throw new DbImportException("DataPath is not supported for data format " + dataType);
		}

		if (Utilities.isNotEmpty(schemaFilePath) && dataType != DataType.XML && dataType != DataType.JSON) {
			throw new DbImportException("SchemaFilePath is not supported for data format " + dataType);
		}
	}

	/**
	 * Create and configure a worker according to the current configuration
	 *
	 * @param parent
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public DbImportWorker getConfiguredWorker(final WorkerParentSimple parent, final boolean analyseDataOnly, final String tableNameForImport, final String importFileOrData) throws Exception {
		DbImportWorker worker;
		if (getDataType() == DataType.SQL) {
			worker = new DbSqlWorker(parent,
					this,
					tableNameForImport,
					isInlineData(),
					importFileOrData,
					getZipPassword());
		} else {
			if (getDbVendor() == DbVendor.Cassandra) {
				worker = new DbNoSqlImportWorker(parent,
						this,
						tableNameForImport);
			} else {
				worker = new DbImportWorker(parent,
						this,
						tableNameForImport,
						getDateFormat(),
						getDateTimeFormat());
			}

			final DataProvider dataProvider;
			if (getDataType() == DataType.JSON) {
				dataProvider = new JsonDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getDataPath(),
						getSchemaFilePath());
			} else if (getDataType() == DataType.XML) {
				dataProvider = new XmlDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getNullValueString(),
						getDataPath(),
						getSchemaFilePath());
			} else if (getDataType() == DataType.EXCEL) {
				dataProvider = new ExcelDataProvider(
						importFileOrData,
						getZipPassword(),
						isAllowUnderfilledLines(),
						isNoHeaders(),
						getNullValueString(),
						isTrimData(),
						getDataPath());
			} else if (getDataType() == DataType.ODS) {
				dataProvider = new OdsDataProvider(
						importFileOrData,
						isAllowUnderfilledLines(),
						isNoHeaders(),
						getNullValueString(),
						isTrimData());
			} else {
				dataProvider = new CsvDataProvider(
						isInlineData(),
						importFileOrData,
						getZipPassword(),
						getSeparator(),
						getStringQuote(),
						getEscapeStringQuote(),
						isAllowUnderfilledLines(),
						isRemoveSurplusEmptyTrailingColumns(),
						isNoHeaders(),
						getNullValueString(),
						isTrimData());
			}
			worker.setDataProvider(dataProvider);

			worker.setAnalyseDataOnly(analyseDataOnly);
			if (isLog() && !isInlineData) {
				final File logDir = new File(new File(importFileOrData).getParentFile(), "importlogs");
				if (!logDir.exists()) {
					logDir.mkdir();
				}
				worker.setLogFile(new File(logDir, new File(importFileOrData).getName() + "." + DateUtilities.formatDate(DateUtilities.YYYYMMDDHHMMSS, LocalDateTime.now()) + ".import.log"));
			}
			worker.setTextFileEncoding(getEncoding());
			worker.setMapping(getMapping());
			worker.setImportMode(getImportMode());
			worker.setDuplicateMode(getDuplicateMode());
			worker.setKeycolumns(getKeycolumns());
			worker.setCompleteCommit(isCompleteCommit());
			worker.setCreateNewIndexIfNeeded(isCreateNewIndexIfNeeded());
			worker.setDeactivateForeignKeyConstraints(isDeactivateForeignKeyConstraints());
			worker.setAdditionalInsertValues(getAdditionalInsertValues());
			worker.setAdditionalUpdateValues(getAdditionalUpdateValues());
			worker.setUpdateNullData(isUpdateNullData());
			worker.setCreateTableIfNotExists(isCreateTable());
			worker.setStructureFilePath(getStructureFilePath());
			worker.setLogErroneousData(isLogErroneousData());
			worker.setDatabaseTimeZone(databaseTimeZone);
			worker.setImportDataTimeZone(importDataTimeZone);
		}

		return worker;
	}

	public String toParamsString() {
		String params = "";
		params += getDbVendor().name();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.HSQL && getDbVendor() != DbVendor.Derby) {
			params += " " + getHostnameAndPort();
		}
		params += " " + getDbName();
		if (getDbVendor() != DbVendor.SQLite && getDbVendor() != DbVendor.Derby) {
			if (getUsername() != null) {
				params += " " + getUsername();
			}
		}
		params += " -table '" + getTableName().replace("'", "\\'") + "'";
		params += " -import '" + getImportFilePathOrData().replace("'", "\\'") + "'";
		if (getPassword() != null) {
			params += " '" + new String(getPassword()).replace("'", "\\'") + "'";
		}

		if (isInlineData()) {
			params += " " + "-data";
		}
		if (getDataType() != DataType.CSV) {
			params += " " + "-x" + " " + getDataType().name();
		}
		if (isLog()) {
			params += " " + "-l";
		}
		if (isVerbose()) {
			params += " " + "-v";
		}
		if (getDataType() != DataType.CSV) {
			params += " " + "-zippassword" + " '" + new String(getZipPassword()).replace("'", "\\'") + "'";
		}
		if (TimeZone.getDefault().getDisplayName().equalsIgnoreCase(getDatabaseTimeZone())) {
			params += " " + "-dbtz" + " " + getDatabaseTimeZone();
		}
		if (TimeZone.getDefault().getDisplayName().equalsIgnoreCase(getImportDataTimeZone())) {
			params += " " + "-idtz" + " " + getImportDataTimeZone();
		}
		if (getEncoding() != StandardCharsets.UTF_8) {
			params += " " + "-e" + " " + getEncoding().name();
		}
		if (getSeparator() != ';') {
			params += " " + "-s" + " '" + Character.toString(getSeparator()).replace("'", "\\'") + "'";
		}
		if (getStringQuote() != '"') {
			params += " " + "-q" + " '" + Character.toString(getStringQuote()).replace("'", "\\'") + "'";
		}
		if (getEscapeStringQuote() != '"') {
			params += " " + "-qe" + " '" + Character.toString(getEscapeStringQuote()).replace("'", "\\'") + "'";
		}
		if (isNoHeaders()) {
			params += " " + "-noheaders";
		}
		if (!"".equals(getNullValueString())) {
			params += " " + "-n" + " '" + getNullValueString() + "'";
		}
		if (isCompleteCommit()) {
			params += " " + "-c";
		}
		if (isAllowUnderfilledLines()) {
			params += " " + "-a";
		}
		if (isRemoveSurplusEmptyTrailingColumns()) {
			params += " " + "-r";
		}
		if (!isUpdateNullData()) {
			params += " " + "-u";
		}
		if (isCreateTable()) {
			params += " " + "-create";
		}
		if (getStructureFilePath() != null && getStructureFilePath().length() > 0) {
			params += " " + "-structure '" + getStructureFilePath().replace("'", "\\'") + "'";
		}
		if (isTrimData()) {
			params += " " + "-t";
		}
		if (isLogErroneousData()) {
			params += " " + "-logerrors";
		}
		if (!isCreateNewIndexIfNeeded()) {
			params += " " + "-nonewindex";
		}
		if (!isDeactivateForeignKeyConstraints()) {
			params += " " + "-deactivatefk";
		}
		if (getImportMode() != ImportMode.INSERT) {
			params += " " + "-i " + getImportMode().name();
		}
		if (getDuplicateMode() != DuplicateMode.UPDATE_ALL_JOIN) {
			params += " " + "-i " + getDuplicateMode().name();
		}
		if (getKeycolumns() != null && getKeycolumns().size() > 0) {
			final List<String> escapedKeyColumns = new ArrayList<>();
			for (final String keyColumn : getKeycolumns()) {
				escapedKeyColumns.add(keyColumn.replace("'", "\\'"));
			}
			params += " " + "-k '" + Utilities.join(escapedKeyColumns, ", ") + "'";
		}
		if (getMapping() != null && getMapping().length() > 0) {
			params += " " + "-m '" + getMapping().replace("'", "\\'") + "'";
		}
		if (getAdditionalInsertValues() != null && getAdditionalInsertValues().length() > 0) {
			params += " " + "-insvalues '" + getAdditionalInsertValues().replace("'", "\\'") + "'";
		}
		if (getAdditionalUpdateValues() != null && getAdditionalUpdateValues().length() > 0) {
			params += " " + "-updvalues '" + getAdditionalUpdateValues().replace("'", "\\'") + "'";
		}
		if (getDataPath() != null && getDataPath().length() > 0) {
			params += " " + "-dp '" + getDataPath().replace("'", "\\'") + "'";
		}
		if (getSchemaFilePath() != null && getSchemaFilePath().length() > 0) {
			params += " " + "-sp '" + getSchemaFilePath().replace("'", "\\'") + "'";
		}
		if (getZipPassword() != null) {
			params += " " + "-zippassword '" + new String(getZipPassword()).replace("'", "\\'") + "'";
		}
		if (getDatabaseTimeZone() != TimeZone.getDefault().getID()) {
			params += " " + "-dbtz " + getDatabaseTimeZone() + "";
		}
		if (getImportDataTimeZone() != TimeZone.getDefault().getID()) {
			params += " " + "-idtz " + getImportDataTimeZone() + "";
		}
		if (Utilities.isNotBlank(getDateFormat())) {
			params += " " + "-dateFormat" + " " + getDateFormat();
		}
		if (Utilities.isNotBlank(getDateTimeFormat())) {
			params += " " + "-dateTimeFormat" + " " + getDateTimeFormat();
		}
		return params;
	}
}
