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

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.worker.WorkerParentDual;

public class DbSqlExportWorker extends AbstractDbExportWorker {
	private Writer fileWriter = null;

	private String tableName = null;

	private List<String> columnNamesOfCurrentTableLine = null;

	private List<String> values = null;

	public DbSqlExportWorker(final WorkerParentDual parent, final DbVendor dbVendor, final String hostname, final String dbName, final String username, final char[] password, final boolean secureConnection, final String trustStoreFilePath, final char[] trustStorePassword, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, secureConnection, trustStoreFilePath, trustStorePassword, isStatementFile, sqlStatementOrTablelist, outputpath);
	}

	@Override
	public String getConfigurationLogString(final String fileName, final String sqlStatement) {
		return
				"File: " + fileName + "\n"
				+ "Format: " + getFileExtension().toUpperCase() + "\n"
				+ "Zip: " + zip + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "SqlStatement: " + sqlStatement + "\n"
				+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage() + "\n"
				+ "CreateBlobFiles: " + createBlobFiles + "\n"
				+ "CreateClobFiles: " + createClobFiles;
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
		fileWriter.write("--" + sqlStatement + "\n");

		if (sqlStatement.toUpperCase().startsWith("SELECT * FROM ")) {
			tableName = sqlStatement.substring(14).trim();
			if (tableName.contains(" ")) {
				tableName = tableName.substring(0, tableName.indexOf(" "));
			}
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
