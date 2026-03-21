package de.soderer.dbexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.worker.WorkerParentDual;
import de.soderer.yaml.YamlWriter;
import de.soderer.yaml.data.YamlMapping;

public class DbYamlExportWorker extends AbstractDbExportWorker {
	private YamlWriter yamlWriter = null;
	private YamlMapping nextLineYamlMapping = null;

	public DbYamlExportWorker(final WorkerParentDual parent, final DbDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);

		setDateFormat(DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE);
		setDateTimeFormat(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE);
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
		return "yaml";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		yamlWriter = new YamlWriter(outputStream, encoding);
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		// Do nothing
	}

	@Override
	protected void startTableLine() throws Exception {
		// Do nothing
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		if (nextLineYamlMapping == null) {
			nextLineYamlMapping = new YamlMapping();
		}

		if (value == null) {
			nextLineYamlMapping.add(columnName, null);
		} else if (value instanceof Boolean) {
			nextLineYamlMapping.add(columnName, value);
		} else if (value instanceof Date) {
			nextLineYamlMapping.add(columnName, value);
		} else if (value instanceof LocalDate) {
			nextLineYamlMapping.add(columnName, value);
		} else if (value instanceof LocalDateTime) {
			nextLineYamlMapping.add(columnName, value);
		} else if (value instanceof Number) {
			nextLineYamlMapping.add(columnName, value);
		} else if (value instanceof String) {
			nextLineYamlMapping.add(columnName, value);
		} else if (value instanceof ZonedDateTime) {
			nextLineYamlMapping.add(columnName, value);
		} else {
			throw new Exception("Unexpected data type: " + value.getClass().getSimpleName());
		}
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		if (nextLineYamlMapping == null) {
			nextLineYamlMapping = new YamlMapping();
		}

		nextLineYamlMapping.add(columnName, getDateFormatter().format(localDateValue));
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		if (nextLineYamlMapping == null) {
			nextLineYamlMapping = new YamlMapping();
		}

		nextLineYamlMapping.add(columnName, getDateTimeFormatter().format(localDateTimeValue));
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		if (nextLineYamlMapping == null) {
			nextLineYamlMapping = new YamlMapping();
		}

		nextLineYamlMapping.add(columnName, getDateTimeFormatter().format(zonedDateTimeValue));
	}

	@Override
	protected void endTableLine() throws Exception {
		if (nextLineYamlMapping != null) {
			yamlWriter.addSequenceItem(nextLineYamlMapping);
			nextLineYamlMapping = null;
		}
	}

	@Override
	protected void endOutput() throws Exception {
		if (nextLineYamlMapping != null) {
			yamlWriter.addSequenceItem(nextLineYamlMapping);
			nextLineYamlMapping = null;
		}
	}

	@Override
	protected void closeWriter() throws Exception {
		if (yamlWriter != null) {
			try {
				yamlWriter.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			yamlWriter = null;
		}
	}
}
