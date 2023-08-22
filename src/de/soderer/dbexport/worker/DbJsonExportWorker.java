package de.soderer.dbexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.json.JsonWriter;
import de.soderer.utilities.worker.WorkerParentDual;

public class DbJsonExportWorker extends AbstractDbExportWorker {
	private JsonWriter jsonWriter = null;

	private String indentation = "\t";

	public DbJsonExportWorker(final WorkerParentDual parent, final DbDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);

		setDateFormat(DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE);
		setDateTimeFormat(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE);
	}

	public void setIndentation(final String indentation) {
		this.indentation = indentation;
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
				+ "Beautify: " + beautify + "\n"
				+ "Indentation: \"" + indentation + "\"";

		return configurationLogString;
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
		jsonWriter.addSimpleJsonObjectPropertyValue(localDateValue);
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		jsonWriter.openJsonObjectProperty(columnName);
		jsonWriter.addSimpleJsonObjectPropertyValue(localDateTimeValue);
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		jsonWriter.openJsonObjectProperty(columnName);
		jsonWriter.addSimpleJsonObjectPropertyValue(zonedDateTimeValue);
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
