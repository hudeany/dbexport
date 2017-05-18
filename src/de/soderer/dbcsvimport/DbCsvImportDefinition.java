package de.soderer.dbcsvimport;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

import de.soderer.dbcsvimport.worker.AbstractDbImportWorker;
import de.soderer.dbcsvimport.worker.DbCsvImportWorker;
import de.soderer.dbcsvimport.worker.DbJsonImportWorker;
import de.soderer.dbcsvimport.worker.DbSqlImportWorker;
import de.soderer.dbcsvimport.worker.DbXmlImportWorker;
import de.soderer.utilities.DbUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.SecureDataEntry;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentSimple;

public class DbCsvImportDefinition extends SecureDataEntry {
	public static final String CONNECTIONTEST_SIGN = "connectiontest";
	
	/**
	 * The Enum DataType.
	 */
	public enum DataType {
		CSV,
		JSON,
		XML,
		SQL;

		/**
		 * Gets the string representation of data type.
		 *
		 * @param dataType
		 *            the data type
		 * @return the from string
		 * @throws Exception
		 *             the exception
		 */
		public static DataType getFromString(String dataTypeString) throws Exception {
			for (DataType dataType : DataType.values()) {
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

		public static ImportMode getFromString(String importModeString) throws Exception {
			for (ImportMode importMode : ImportMode.values()) {
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

		public static DuplicateMode getFromString(String duplicateModeString) throws Exception {
			for (DuplicateMode duplicateMode : DuplicateMode.values()) {
				if (duplicateMode.toString().equalsIgnoreCase(duplicateModeString)) {
					return duplicateMode;
				}
			}
			throw new Exception("Invalid duplicate mode: " + duplicateModeString);
		}
	}

	// Mandatory parameters
	
	/** The db vendor. */
	private DbUtilities.DbVendor dbVendor = null;

	/** The hostname. */
	private String hostname;

	/** The db name. */
	private String dbName;

	/** The username. */
	private String username;

	/** The tableName. */
	private String tableName;

	/** The importFilePath or data. */
	private String importFilePathOrData;
	
	private boolean isInlineData = false;

	/** The password, may be entered interactivly */
	private char[] password;

	// Default optional parameters
	
	/** Open a gui. */
	private boolean openGui = false;

	/** The data type. */
	private DataType dataType = DataType.CSV;

	/** Log activation. */
	private boolean log = false;

	/** The verbose. */
	private boolean verbose = false;

	/** The encoding. */
	private String encoding = "UTF-8";

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
	
	private ImportMode importMode = ImportMode.INSERT;
	
	private DuplicateMode duplicateMode = DuplicateMode.UPDATE_ALL_JOIN;
	
	private boolean updateNullData = true;
	
	private List<String> keycolumns = null;
	
	private boolean createTable = false;
	
	private boolean trimData = false;
	
	private String mapping = "";
	
	private String additionalInsertValues = null;
	
	private String additionalUpdateValues = null;
	
	private boolean logErrorneousData = false;
	
	private boolean createNewIndexIfNeeded = true;

	private String dataPath = null;

	private String schemaFilePath = null;

	public DbUtilities.DbVendor getDbVendor() {
		return dbVendor;
	}

	public void setDbVendor(DbUtilities.DbVendor dbVendor) {
		this.dbVendor = dbVendor;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getImportFilePathOrData() {
		return importFilePathOrData;
	}

	public boolean isInlineData() {
		return isInlineData;
	}

	public void setInlineData(boolean isInlineData) {
		this.isInlineData = isInlineData;
	}

	public void setImportFilePathOrData(String importFilePathOrData) {
		this.importFilePathOrData = importFilePathOrData;
		if (this.importFilePathOrData != null && !isInlineData) {
			// Check filepath syntax
			try {
                Paths.get(importFilePathOrData);
                isInlineData = false;
            } catch (Exception e) {
            	isInlineData = true;
            }
			if (!isInlineData) {
				this.importFilePathOrData = this.importFilePathOrData.trim().replace("~", System.getProperty("user.home"));
				if (this.importFilePathOrData.endsWith(File.separator)) {
					this.importFilePathOrData = this.importFilePathOrData.substring(0, this.importFilePathOrData.length() - 1);
				}
				isInlineData = false;
			}
		}
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	public boolean isOpenGui() {
		return openGui;
	}

	public void setOpenGui(boolean openGui) {
		this.openGui = openGui;
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	public boolean isLog() {
		return log;
	}

	public void setLog(boolean log) {
		this.log = log;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public char getSeparator() {
		return separator;
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}

	public Character getStringQuote() {
		return stringQuote;
	}

	public void setStringQuote(Character stringQuote) {
		this.stringQuote = stringQuote;
	}

	public char getEscapeStringQuote() {
		return escapeStringQuote;
	}

	public void setEscapeStringQuote(char escapeStringQuote) {
		this.escapeStringQuote = escapeStringQuote;
	}

	public boolean isNoHeaders() {
		return noHeaders;
	}

	public void setNoHeaders(boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	public String getNullValueString() {
		return nullValueString;
	}

	public void setNullValueString(String nullValueString) {
		this.nullValueString = nullValueString;
	}

	public boolean isCompleteCommit() {
		return completeCommit;
	}

	public void setCompleteCommit(boolean completeCommit) {
		this.completeCommit = completeCommit;
	}

	public boolean isAllowUnderfilledLines() {
		return allowUnderfilledLines;
	}

	public void setAllowUnderfilledLines(boolean allowUnderfilledLines) {
		this.allowUnderfilledLines = allowUnderfilledLines;
	}

	public ImportMode getImportMode() {
		return importMode;
	}

	public void setImportMode(ImportMode importMode) {
		this.importMode = importMode;
	}

	public DuplicateMode getDuplicateMode() {
		return duplicateMode;
	}

	public void setDuplicateMode(DuplicateMode duplicateMode) {
		this.duplicateMode = duplicateMode;
	}

	public boolean isUpdateNullData() {
		return updateNullData;
	}

	public void setUpdateNullData(boolean updateNullData) {
		this.updateNullData = updateNullData;
	}

	public List<String> getKeycolumns() {
		return keycolumns;
	}

	public void setKeycolumns(List<String> keycolumns) {
		this.keycolumns = keycolumns;
	}

	public boolean isCreateTable() {
		return createTable;
	}

	public void setCreateTable(boolean createTable) {
		this.createTable = createTable;
	}

	public String getMapping() {
		return mapping;
	}

	public void setMapping(String mapping) {
		this.mapping = mapping;
	}

	public boolean isTrimData() {
		return trimData;
	}

	public void setTrimData(boolean trimData) {
		this.trimData = trimData;
	}

	public void setAdditionalInsertValues(String additionalInsertValues) {
		this.additionalInsertValues = additionalInsertValues;
	}

	public String getAdditionalInsertValues() {
		return additionalInsertValues;
	}

	public void setAdditionalUpdateValues(String additionalUpdateValues) {
		this.additionalUpdateValues = additionalUpdateValues;
	}

	public String getAdditionalUpdateValues() {
		return additionalUpdateValues;
	}

	public boolean isLogErrorneousData() {
		return logErrorneousData;
	}

	public void setLogErrorneousData(boolean logErrorneousData) {
		this.logErrorneousData = logErrorneousData;
	}

	public boolean isCreateNewIndexIfNeeded() {
		return createNewIndexIfNeeded;
	}

	public void setCreateNewIndexIfNeeded(boolean createNewIndexIfNeeded) {
		this.createNewIndexIfNeeded = createNewIndexIfNeeded;
	}

	private String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	private String getSchemaFilePath() {
		return schemaFilePath;
	}

	public void setSchemaFilePath(String schemaFilePath) {
		this.schemaFilePath= schemaFilePath;
	}

	public void checkParameters() throws DbCsvImportException {
		if (CONNECTIONTEST_SIGN.equalsIgnoreCase(tableName)) {
			if (!NumberUtilities.isDigit(importFilePathOrData)) {
				throw new DbCsvImportException("Connection test iterations must be nummeric: " + importFilePathOrData);
			}
		} else if (importFilePathOrData == null) {
			throw new DbCsvImportException("ImportFilePath or data is missing");
		} else if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbCsvImportException("ImportFilePath does not exist: " + importFilePathOrData);
			} else if (!new File(importFilePathOrData).isFile()) {
				throw new DbCsvImportException("ImportFilePath is not a file: " + importFilePathOrData);
			}
		}
	
		if (dbVendor == DbVendor.SQLite) {
			if (Utilities.isNotBlank(hostname)) {
				throw new DbCsvImportException("SQLite db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbCsvImportException("SQLite db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbCsvImportException("SQLite db connections do not support the password parameter");
			}
		} else if (dbVendor == DbVendor.Derby) {
			if (Utilities.isNotBlank(hostname)) {
				throw new DbCsvImportException("Derby db connections do not support the hostname parameter");
			} else if (Utilities.isNotBlank(username)) {
				throw new DbCsvImportException("Derby db connections do not support the username parameter");
			} else if (Utilities.isNotBlank(password)) {
				throw new DbCsvImportException("Derby db connections do not support the password parameter");
			}
		} else if (dbVendor == DbVendor.HSQL) {
			dbName = dbName.replace("~", System.getProperty("user.home"));
			if (dbName.startsWith("/")) {
				if (Utilities.isNotBlank(hostname)) {
					throw new DbCsvImportException("HSQL file db connections do not support the hostname parameter");
				} else if (Utilities.isNotBlank(username)) {
					throw new DbCsvImportException("HSQL file db connections do not support the username parameter");
				} else if (Utilities.isNotBlank(password)) {
					throw new DbCsvImportException("HSQL file db connections do not support the password parameter");
				}
			}
		} else {
			if (Utilities.isBlank(hostname)) {
				throw new DbCsvImportException("Missing or invalid hostname");
			}
			if (Utilities.isBlank(username)) {
				throw new DbCsvImportException("Missing or invalid username");
			}
			if (Utilities.isBlank(password)) {
				throw new DbCsvImportException("Missing or invalid empty password");
			}
		}
	
		if (noHeaders && dataType != DataType.CSV) {
			throw new DbCsvImportException("NoHeaders is not supported for data format " + dataType);
		}
		
		if (dataType != DataType.SQL) {
			if ((importMode == ImportMode.UPDATE || importMode == ImportMode.UPSERT)
					&& (keycolumns == null || keycolumns.isEmpty())) {
				throw new DbCsvImportException("Invalid empty key column definition for import mode: " + importMode);
			}
		}
	
		if (Utilities.isNotEmpty(dataPath) && dataType != DataType.XML && dataType != DataType.JSON) {
			throw new DbCsvImportException("DataPath is not supported for data format " + dataType);
		}
	
		if (Utilities.isNotEmpty(schemaFilePath) && dataType != DataType.XML && dataType != DataType.JSON) {
			throw new DbCsvImportException("SchemaFilePath is not supported for data format " + dataType);
		}
	}

	/**
	 * Get the array containing all relevant configuration data to store it in a SecureKeyStore
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see de.soderer.utilities.SecureDataEntry#getStorageData()
	 */
	@Override
	public String[] getStorageData() {
		return new String[] {
			getEntryName(),
			dbVendor.toString(),
			hostname,
			dbName,
			username,
			new String(password),
			tableName,
			Boolean.toString(isInlineData),
			importFilePathOrData,
			dataType.toString(),
			Boolean.toString(log),
			encoding,
			Character.toString(separator),
			stringQuote == null ? "" : Character.toString(stringQuote),
			Character.toString(escapeStringQuote),
			Boolean.toString(noHeaders),
			nullValueString,
			Boolean.toString(completeCommit),
			Boolean.toString(allowUnderfilledLines),
			importMode.toString(),
			duplicateMode.toString(),
			Boolean.toString(updateNullData),
			Utilities.join(keycolumns, ", "),
			Boolean.toString(createTable),
			mapping,
			Boolean.toString(trimData),
			additionalInsertValues,
			additionalUpdateValues,
			Boolean.toString(logErrorneousData),
			dataPath,
			schemaFilePath
		};
	}
	
	/**
	 * Read the array given from a SecureKeyStore to get all relevant configuration data that was stored
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see de.soderer.utilities.SecureDataEntry#loadData(java.util.List)
	 */
	@Override
	public void loadData(List<String> valueStrings) throws Exception {
		int i = 0;
		setEntryName(valueStrings.get(i++));
		dbVendor = DbVendor.getDbVendorByName(valueStrings.get(i++));
		hostname = valueStrings.get(i++);
		dbName = valueStrings.get(i++);
		username = valueStrings.get(i++);
		password = valueStrings.get(i++).toCharArray();
		tableName = valueStrings.get(i++);
		isInlineData = Utilities.interpretAsBool(valueStrings.get(i++));
		importFilePathOrData = valueStrings.get(i++);
		dataType = DataType.getFromString(valueStrings.get(i++));
		log = Utilities.interpretAsBool(valueStrings.get(i++));
		encoding = valueStrings.get(i++);
		separator = valueStrings.get(i++).charAt(0);
		
		String stringQuoteVar = valueStrings.get(i++);
		if (Utilities.isEmpty(stringQuoteVar)) {
			stringQuote = null;
		} else {
			stringQuote = stringQuoteVar.charAt(0);
		}
		
		escapeStringQuote = valueStrings.get(i++).charAt(0);
		noHeaders = Utilities.interpretAsBool(valueStrings.get(i++));
		nullValueString = valueStrings.get(i++);
		completeCommit = Utilities.interpretAsBool(valueStrings.get(i++));
		allowUnderfilledLines = Utilities.interpretAsBool(valueStrings.get(i++));
		importMode = ImportMode.getFromString(valueStrings.get(i++));
		duplicateMode = DuplicateMode.getFromString(valueStrings.get(i++));
		updateNullData = Utilities.interpretAsBool(valueStrings.get(i++));
		keycolumns = Utilities.splitAndTrimList(valueStrings.get(i++));
		createTable = Utilities.interpretAsBool(valueStrings.get(i++));
		mapping = valueStrings.get(i++);
		trimData = Utilities.interpretAsBool(valueStrings.get(i++));
		additionalInsertValues = valueStrings.get(i++);
		additionalUpdateValues = valueStrings.get(i++);
		logErrorneousData = Utilities.interpretAsBool(valueStrings.get(i++));
		if (valueStrings.size() > i) {
			dataPath = valueStrings.get(i++);
		}
		if (valueStrings.size() > i) {
			schemaFilePath = valueStrings.get(i++);
		}
	}

	/**
	 * Create and configure a worker according to the current configuration
	 * 
	 * @param parent
	 * @return
	 * @throws Exception
	 */
	public AbstractDbImportWorker getConfiguredWorker(WorkerParentSimple parent, boolean analyseDataOnly) throws Exception {
		AbstractDbImportWorker worker;
		if (getDataType() == DataType.JSON) {
			worker = new DbJsonImportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				getTableName(),
				isInlineData(),
				getImportFilePathOrData());
			((DbJsonImportWorker) worker).setDataPath(getDataPath());
			((DbJsonImportWorker) worker).setSchemaFilePath(getSchemaFilePath());
		} else if (getDataType() == DataType.XML) {
			worker = new DbXmlImportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				getTableName(),
				isInlineData(),
				getImportFilePathOrData());
			((DbXmlImportWorker) worker).setNullValueText(getNullValueString());
			((DbXmlImportWorker) worker).setDataPath(getDataPath());
			((DbXmlImportWorker) worker).setSchemaFilePath(getSchemaFilePath());
		} else if (getDataType() == DataType.SQL) {
			worker = new DbSqlImportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				isInlineData(),
				getImportFilePathOrData());
		} else {
			worker = new DbCsvImportWorker(parent,
				getDbVendor(),
				getHostname(),
				getDbName(),
				getUsername(),
				getPassword(),
				getTableName(),
				isInlineData(),
				getImportFilePathOrData());
			((DbCsvImportWorker) worker).setSeparator(getSeparator());
			((DbCsvImportWorker) worker).setStringQuote(getStringQuote());
			((DbCsvImportWorker) worker).setEscapeStringQuote(getEscapeStringQuote());
			((DbCsvImportWorker) worker).setAllowUnderfilledLines(isAllowUnderfilledLines());
			((DbCsvImportWorker) worker).setNoHeaders(isNoHeaders());
			((DbCsvImportWorker) worker).setNullValueText(getNullValueString());
			((DbCsvImportWorker) worker).setTrimData(isTrimData());
		}
		worker.setAnalyseDataOnly(analyseDataOnly);
		worker.setLog(isLog());
		worker.setEncoding(getEncoding());
		worker.setMapping(getMapping());
		worker.setImportMode(getImportMode());
		worker.setDuplicateMode(getDuplicateMode());
		worker.setKeycolumns(getKeycolumns());
		worker.setCompleteCommit(isCompleteCommit());
		worker.setCreateNewIndexIfNeeded(isCreateNewIndexIfNeeded());
		worker.setAdditionalInsertValues(getAdditionalInsertValues());
		worker.setAdditionalUpdateValues(getAdditionalUpdateValues());
		worker.setUpdateNullData(isUpdateNullData());
		worker.setCreateTableIfNotExists(isCreateTable());
		worker.setLogErrorneousData(isLogErrorneousData());
		
		return worker;
	}
}
