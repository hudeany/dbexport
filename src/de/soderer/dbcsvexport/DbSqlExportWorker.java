package de.soderer.dbcsvexport;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.WorkerParentDual;

public class DbSqlExportWorker extends AbstractDbExportWorker {
	private Writer fileWriter = null;
	
	private String tableName = null;
	
	private List<String> colmnNames = null;
	
	private List<String> values = null;
	
	public DbSqlExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String sqlStatementOrTablelist, String outputpath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, sqlStatementOrTablelist, outputpath);
	}

	@Override
	protected String getConfigurationLogString(String fileName, String sqlStatement) {
		return
			"File: " + fileName
			+ "Format: " + getFileExtension().toUpperCase()
			+ "Zip: " + zip
			+ "Encoding: " + encoding
			+ "SqlStatement: " + sqlStatement
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "CreateBlobFiles: " + createBlobFiles
			+ "CreateClobFiles: " + createClobFiles;
	}

	@Override
	protected String getFileExtension() {
		return "sql";
	}

	@Override
	protected void openWriter(OutputStream outputStream) throws Exception {
		fileWriter = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
	}

	@Override
	protected void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception {
		if (sqlStatement.toUpperCase().startsWith("SELECT * FROM ")) {
			tableName = sqlStatement.substring(14).trim();
			if (tableName.contains(" ")) {
				tableName = tableName.substring(0,  tableName.indexOf(" "));
			}
		} else {
			tableName = "export_tbl";
		}
	}

	@Override
	protected void startTableLine() throws Exception {
		colmnNames = new ArrayList<String>();
		values = new ArrayList<String>();
	}

	@Override
	protected void writeColumn(String columnName, Object value) throws Exception {
		colmnNames.add(columnName);
		if (value == null) {
			values.add("NULL");
		} else if (value instanceof String) {
			values.add("'" + ((String) value).replace("'", "''") + "'");
		} else if (value instanceof Date) {
			values.add("'" + DateUtilities.ANSI_SQL_DATETIME_FORMAT.format(value) + "'");
		} else if (value instanceof Number) {
			values.add(value.toString());
		} else {
			values.add("'" + value.toString().replace("'", "''") + "'");
		}
	}

	@Override
	protected void endTableLine() throws Exception {
		fileWriter.write("INSERT INTO " + tableName + " (" + Utilities.join(colmnNames, ", ") + ") VALUES (" + Utilities.join(values, ", ") + ");\n");
		colmnNames = null;
		values = null;
	}

	@Override
	protected void endOutput() throws Exception {
	}

	@Override
	protected void closeWriter() throws Exception {
		if (fileWriter != null) {
			try {
				fileWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			fileWriter = null;
		}
	}
}
