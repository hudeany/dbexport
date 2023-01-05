package de.soderer.dbexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.List;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.json.JsonWriter;
import de.soderer.utilities.worker.WorkerParentDual;

public class DbJsonExportWorker extends AbstractDbExportWorker {
	private JsonWriter jsonWriter = null;

	private String indentation = "\t";

	public DbJsonExportWorker(final WorkerParentDual parent, final DbVendor dbVendor, final String hostname, final String dbName, final String username, final char[] password, final boolean secureConnection, final String trustStoreFilePath, final char[] trustStorePassword, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) throws Exception {
		super(parent, dbVendor, hostname, dbName, username, password, secureConnection, trustStoreFilePath, trustStorePassword, isStatementFile, sqlStatementOrTablelist, outputpath);

		dateFormatter = DateTimeFormatter.ofPattern(DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE);
		dateFormatter.withResolverStyle(ResolverStyle.STRICT);

		dateTimeFormatter = DateTimeFormatter.ofPattern(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE);
		dateTimeFormatter.withResolverStyle(ResolverStyle.STRICT);
	}

	public void setIndentation(final String indentation) {
		this.indentation = indentation;
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
				+ "CreateClobFiles: " + createClobFiles + "\n"
				+ "Beautify: " + beautify + "\n"
				+ "Indentation: \"" + indentation + "\"";
	}

	@Override
	protected String getFileExtension() {
		return "json";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		jsonWriter = new JsonWriter(outputStream, encoding);
		jsonWriter.setIndentation(indentation);
		jsonWriter.setUglify(!beautify);
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		jsonWriter.openJsonArray();
	}

	@Override
	protected void startTableLine() throws Exception {
		jsonWriter.openJsonObject();
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		jsonWriter.openJsonObjectProperty(columnName);
		jsonWriter.addSimpleJsonObjectPropertyValue(value);
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		jsonWriter.openJsonObjectProperty(columnName);
		dateFormatter.format(LocalDateTime.now());
		jsonWriter.addSimpleJsonObjectPropertyValue(dateFormatter.format(localDateValue));
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		jsonWriter.openJsonObjectProperty(columnName);
		jsonWriter.addSimpleJsonObjectPropertyValue(dateTimeFormatter.format(localDateTimeValue));
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		jsonWriter.openJsonObjectProperty(columnName);
		jsonWriter.addSimpleJsonObjectPropertyValue(dateTimeFormatter.format(zonedDateTimeValue));
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
			} catch (final Exception e) {
				e.printStackTrace();
			}
			jsonWriter = null;
		}
	}
}
