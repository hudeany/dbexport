package de.soderer.dbcsvexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.List;

import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.WorkerParentDual;
import de.soderer.utilities.json.JsonWriter;

public class DbJsonExportWorker extends AbstractDbExportWorker {
	private JsonWriter jsonWriter = null;
	
	private String indentation = "\t";
	
	public DbJsonExportWorker(WorkerParentDual parent, DbVendor dbVendor, String hostname, String dbName, String username, String password, String sqlStatementOrTablelist, String outputpath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, sqlStatementOrTablelist, outputpath);
	}
	
	public void setIndentation(String indentation) {
		this.indentation = indentation;
	}

	@Override
	public String getConfigurationLogString(String fileName, String sqlStatement) {
		return
			"File: " + fileName
			+ "Format: " + getFileExtension().toUpperCase()
			+ "Zip: " + zip
			+ "Encoding: " + encoding
			+ "SqlStatement: " + sqlStatement
			+ "OutputFormatLocale: " + dateAndDecimalLocale.getLanguage()
			+ "CreateBlobFiles: " + createBlobFiles
			+ "CreateClobFiles: " + createClobFiles
			+ "Beautify: " + beautify
			+ "Indentation: \"" + indentation + "\"";
	}

	@Override
	protected String getFileExtension() {
		return "json";
	}

	@Override
	protected void openWriter(OutputStream outputStream) throws Exception {
		jsonWriter = new JsonWriter(outputStream, encoding);
		jsonWriter.setIndentation(indentation);
		jsonWriter.setUglify(!beautify);
	}

	@Override
	protected void startOutput(Connection connection, String sqlStatement, List<String> columnNames) throws Exception {
		jsonWriter.openJsonArray();
	}

	@Override
	protected void startTableLine() throws Exception {
		jsonWriter.openJsonObject();
	}

	@Override
	protected void writeColumn(String columnName, Object value) throws Exception {
		jsonWriter.openJsonObjectProperty(columnName);
		jsonWriter.addSimpleJsonObjectPropertyValue(value);
	}

	@Override
	protected void endTableLine() throws Exception {
		jsonWriter.closeJsonObject();
	}

	@Override
	protected void endOutput() throws Exception {
		jsonWriter.closeJsonArray();
	}

	@Override
	protected void closeWriter() throws Exception {
		if (jsonWriter != null) {
			try {
				jsonWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			jsonWriter = null;
		}
	}
}
