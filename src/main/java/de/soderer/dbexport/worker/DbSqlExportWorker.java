package de.soderer.dbexport.worker;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.db.data.DbConnectionDefinition;
import de.soderer.utilities.worker.WorkerParentDual;

/**
 * Export worker for SQL insert statements. The table name is taken from the FROM clause of the exported statement.
 */
public class DbSqlExportWorker extends AbstractDbExportWorker {
	private Writer fileWriter = null;

	private String tableName = null;

	private List<String> columnNamesOfCurrentTableLine = null;

	private List<String> values = null;

	/**
	 * Creates the worker.
	 *
	 * @param parent parent to signal the progress to
	 * @param dbDefinition the connection parameters of the database
	 * @param isStatementFile true if sqlStatementOrTablelist is the path of a file containing the statement or table list
	 * @param sqlStatementOrTablelist SQL select statement, or comma separated table name patterns
	 * @param outputpath output file (single statement) or directory (table list), or "console" or "gui"
	 */
	public DbSqlExportWorker(final WorkerParentDual parent, final DbConnectionDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);
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
				+ "CreateClobFiles: " + createClobFiles;

		return configurationLogString;
	}

	@Override
	protected String getFileExtension() {
		return "sql";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		fileWriter = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		// Each line of the statement as comment (before, only the first line was commented out, so the further lines
		// of a multi line statement were executed on import)
		for (final String statementLine : sqlStatement.split("\r\n|\r|\n")) {
			fileWriter.write("--" + statementLine + "\n");
		}

		// FROM may also be surrounded by line breaks or tabs
		final Matcher fromMatcher = Pattern.compile("\\sFROM\\s+([^\\s,;()]+)", Pattern.CASE_INSENSITIVE).matcher(sqlStatement);
		if (fromMatcher.find()) {
			tableName = fromMatcher.group(1);
		} else {
			tableName = "export_tbl";
		}
	}

	@Override
	protected void startTableLine() throws Exception {
		columnNamesOfCurrentTableLine = new ArrayList<>();
		values = new ArrayList<>();
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		columnNamesOfCurrentTableLine.add(columnName);
		if (value == null) {
			values.add("NULL");
		} else if (value instanceof String) {
			values.add("'" + ((String) value).replace("'", "''") + "'");
		} else if (value instanceof Date) {
			values.add("'" + DateUtilities.formatDate(DateUtilities.ANSI_SQL_DATETIME_FORMAT, (Date) value) + "'");
		} else if (value instanceof Number) {
			values.add(value.toString());
		} else {
			values.add("'" + value.toString().replace("'", "''") + "'");
		}
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		columnNamesOfCurrentTableLine.add(columnName);
		if (localDateValue == null) {
			values.add("NULL");
		} else {
			values.add("'" + DateUtilities.formatDate(DateUtilities.ANSI_SQL_DATE_FORMAT, localDateValue) + "'");
		}
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		columnNamesOfCurrentTableLine.add(columnName);
		if (localDateTimeValue == null) {
			values.add("NULL");
		} else {
			values.add("'" + DateUtilities.formatDate(DateUtilities.ANSI_SQL_DATETIME_FORMAT, localDateTimeValue) + "'");
		}
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		columnNamesOfCurrentTableLine.add(columnName);
		if (zonedDateTimeValue == null) {
			values.add("NULL");
		} else {
			values.add("'" + DateUtilities.formatDate(DateUtilities.ANSI_SQL_DATETIME_FORMAT, zonedDateTimeValue) + "'");
		}
	}

	@Override
	protected void endTableLine() throws Exception {
		fileWriter.write("INSERT INTO " + tableName + " (" + Utilities.join(columnNamesOfCurrentTableLine, ", ") + ") VALUES (" + Utilities.join(values, ", ") + ");\n");
		columnNamesOfCurrentTableLine = null;
		values = null;
	}

	@Override
	protected void endOutput() throws Exception {
		// nothing to do
	}

	@Override
	protected void closeWriter() throws Exception {
		if (fileWriter != null) {
			try {
				fileWriter.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			fileWriter = null;
		}
	}
}
