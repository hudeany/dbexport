package de.soderer.dbexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.kdbx.KdbxDatabase;
import de.soderer.utilities.kdbx.KdbxWriter;
import de.soderer.utilities.kdbx.data.KdbxEntry;
import de.soderer.utilities.kdbx.data.KdbxGroup;
import de.soderer.utilities.worker.WorkerParentDual;

/**
 * Export worker for KeePass database files (KDBX format). Each data line becomes an entry of the database.
 * <p>
 * All entries are stored below a single root group, as expected by KeePass and KeePassXC.
 * Optionally a result column may contain the group path of each entry (e.g. "Internet/Mail"), relative to the root group.
 * Missing groups are created on the fly. The group path column itself is not stored as entry field.
 */
public class DbKdbxExportWorker extends AbstractDbExportWorker {
	/** Default name of the root group */
	public static final String DEFAULT_ROOT_GROUP_NAME = "Root";
	/** Default separator of group names within a group path */
	public static final String DEFAULT_GROUP_PATH_SEPARATOR = "/";

	private char[] kdbxPassword = null;
	private KdbxWriter kdbxWriter = null;
	private KdbxDatabase kdbxDatabase = null;
	private KdbxEntry kdbxEntry = null;

	/** Name of the result column containing the group path of each entry, null for no grouping */
	private String groupPathColumnName = null;
	/** Separator of group names within a group path */
	private String groupPathSeparator = DEFAULT_GROUP_PATH_SEPARATOR;
	/** Name of the single top level group of the database */
	private String rootGroupName = DEFAULT_ROOT_GROUP_NAME;

	/** Single top level group of the database */
	private KdbxGroup rootGroup = null;
	/** Group path of the current data line */
	private String currentGroupPath = null;
	/** Already created groups by their normalized path, to avoid searching the group tree for every line */
	private final Map<String, KdbxGroup> groupCache = new HashMap<>();

	/**
	 * Creates the worker.
	 *
	 * @param parent parent to signal the progress to
	 * @param dbDefinition the connection parameters of the database
	 * @param isStatementFile true if sqlStatementOrTablelist is the path of a file containing the statement or table list
	 * @param sqlStatementOrTablelist SQL select statement, or comma separated table name patterns
	 * @param outputpath output file (single statement) or directory (table list), or "console" or "gui"
	 * @param kdbxPassword the password of the KeePass database
	 */
	public DbKdbxExportWorker(final WorkerParentDual parent, final DbConnectionDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath, final char[] kdbxPassword) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);

		this.kdbxPassword = kdbxPassword;

		setDateFormat(DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE);
		setDateTimeFormat(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE);
	}

	/**
	 * Sets the name of the result column containing the group path of each entry.
	 *
	 * @param groupPathColumnName name of the column (case insensitive), or null or blank for no grouping
	 */
	public void setGroupPathColumnName(final String groupPathColumnName) {
		this.groupPathColumnName = groupPathColumnName == null || groupPathColumnName.isBlank() ? null : groupPathColumnName.trim();
	}

	/**
	 * Sets the separator of group names within a group path.
	 *
	 * @param groupPathSeparator the separator, default "/"
	 */
	public void setGroupPathSeparator(final String groupPathSeparator) {
		if (groupPathSeparator == null || groupPathSeparator.isEmpty()) {
			throw new IllegalArgumentException("Group path separator must not be empty");
		} else {
			this.groupPathSeparator = groupPathSeparator;
		}
	}

	/**
	 * Sets the name of the single top level group of the database.
	 *
	 * @param rootGroupName the name, default "Root"
	 */
	public void setRootGroupName(final String rootGroupName) {
		if (rootGroupName == null || rootGroupName.isBlank()) {
			throw new IllegalArgumentException("Root group name must not be empty");
		} else {
			this.rootGroupName = rootGroupName.trim();
		}
	}

	@Override
	public String getConfigurationLogString(final String fileName, final String sqlStatement) {
		String configurationLogString = "File: " + fileName + "\n"
				+ "Format: " + getFileExtension().toUpperCase() + "\n";

		if (compression == FileCompressionType.ZIP) {
			configurationLogString += "Compression: zip\n";
			if (zipPassword != null) {
				configurationLogString += "ZipPassword: true\n";
			}
		} else if (compression == FileCompressionType.TARGZ) {
			configurationLogString += "Compression: targz\n";
		} else if (compression == FileCompressionType.TGZ) {
			configurationLogString += "Compression: tgz\n";
		} else if (compression == FileCompressionType.GZ) {
			configurationLogString += "Compression: gz\n";
		}

		configurationLogString += "Encoding: " + encoding + "\n"
				+ "SqlStatement: " + sqlStatement + "\n"
				+ "OutputFormatLocale: " + dateFormatLocale.getLanguage() + "\n"
				+ "CreateBlobFiles: " + createBlobFiles + "\n"
				+ "CreateClobFiles: " + createClobFiles + "\n"
				+ "KdbxRootGroupName: " + rootGroupName + "\n"
				+ "KdbxGroupPathColumn: " + (groupPathColumnName == null ? "none" : groupPathColumnName);

		if (groupPathColumnName != null) {
			configurationLogString += "\n" + "KdbxGroupPathSeparator: " + groupPathSeparator;
		}

		return configurationLogString;
	}

	@Override
	protected String getFileExtension() {
		return "kdbx";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		kdbxWriter = new KdbxWriter(outputStream);
		kdbxDatabase = new KdbxDatabase();
		rootGroup = new KdbxGroup().withName(rootGroupName).withExpanded(true);
		kdbxDatabase.getGroups().add(rootGroup);
		groupCache.clear();
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		if (groupPathColumnName != null && columnNames.stream().noneMatch(groupPathColumnName::equalsIgnoreCase)) {
			throw new Exception("Configured KDBX group path column '" + groupPathColumnName + "' is not part of the result columns: " + String.join(", ", columnNames));
		}
	}

	@Override
	protected void startTableLine() throws Exception {
		kdbxEntry = new KdbxEntry();
		currentGroupPath = null;
	}

	/**
	 * Checks whether the column contains the group path of the entry.
	 */
	private boolean isGroupPathColumn(final String columnName) {
		return groupPathColumnName != null && groupPathColumnName.equalsIgnoreCase(columnName);
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		if (isGroupPathColumn(columnName)) {
			// The group path is structural information and is not stored as entry field
			currentGroupPath = value == null ? null : value.toString();
		} else {
			kdbxEntry.getItems().put(columnName, value);
		}
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		if (isGroupPathColumn(columnName)) {
			currentGroupPath = localDateValue == null ? null : localDateValue.toString();
		} else {
			kdbxEntry.getItems().put(columnName, localDateValue);
		}
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		if (isGroupPathColumn(columnName)) {
			currentGroupPath = localDateTimeValue == null ? null : localDateTimeValue.toString();
		} else {
			kdbxEntry.getItems().put(columnName, localDateTimeValue);
		}
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		if (isGroupPathColumn(columnName)) {
			currentGroupPath = zonedDateTimeValue == null ? null : zonedDateTimeValue.toString();
		} else {
			kdbxEntry.getItems().put(columnName, zonedDateTimeValue);
		}
	}

	@Override
	protected void endTableLine() throws Exception {
		getOrCreateGroup(currentGroupPath).getEntries().add(kdbxEntry);
		kdbxEntry = null;
		currentGroupPath = null;
	}

	/**
	 * Returns the group for the given path relative to the root group, creating all missing groups on the way.
	 * Leading, trailing and doubled separators as well as whitespace around group names are ignored.
	 *
	 * @param groupPath the group path, null or blank for the root group
	 * @return the group to store the entry in
	 */
	private KdbxGroup getOrCreateGroup(final String groupPath) {
		if (groupPath == null || groupPath.isBlank()) {
			return rootGroup;
		}

		KdbxGroup parentGroup = rootGroup;
		final StringBuilder normalizedPath = new StringBuilder();
		for (final String rawGroupName : groupPath.split(Pattern.quote(groupPathSeparator))) {
			final String groupName = rawGroupName.trim();
			if (groupName.isEmpty()) {
				continue;
			}

			// Use a separator, which can not be part of a group name, as cache key delimiter
			normalizedPath.append('\0').append(groupName);
			final String cacheKey = normalizedPath.toString();

			KdbxGroup group = groupCache.get(cacheKey);
			if (group == null) {
				group = new KdbxGroup().withName(groupName);
				parentGroup.getGroups().add(group);
				groupCache.put(cacheKey, group);
			}
			parentGroup = group;
		}
		return parentGroup;
	}

	@Override
	protected void endOutput() throws Exception {
		kdbxWriter.writeKdbxDatabase(kdbxDatabase, kdbxPassword);
		kdbxDatabase = null;
		rootGroup = null;
		groupCache.clear();
	}

	@Override
	protected void closeWriter() throws Exception {
		if (kdbxWriter != null) {
			try {
				kdbxWriter.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			kdbxWriter = null;
		}
	}
}
