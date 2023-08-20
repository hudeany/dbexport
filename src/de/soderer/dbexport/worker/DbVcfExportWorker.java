package de.soderer.dbexport.worker;

import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.db.DbDefinition;
import de.soderer.utilities.vcf.VcfCard;
import de.soderer.utilities.vcf.VcfWriter;
import de.soderer.utilities.worker.WorkerParentDual;

public class DbVcfExportWorker extends AbstractDbExportWorker {
	private VcfWriter vcfWriter = null;
	private Map<String, Object> currentVcfCardMap = null;

	public DbVcfExportWorker(final WorkerParentDual parent, final DbDefinition dbDefinition, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent, dbDefinition, isStatementFile, sqlStatementOrTablelist, outputpath);

		setDateFormat(DateUtilities.ISO_8601_DATE_FORMAT_NO_TIMEZONE);
		setDateTimeFormat(DateUtilities.ISO_8601_DATETIME_FORMAT_NO_TIMEZONE);
	}

	@Override
	public String getConfigurationLogString(final String fileName, final String sqlStatement) {
		return
				"File: " + fileName + "\n"
				+ "Format: " + getFileExtension().toUpperCase() + "\n"
				+ "Zip: " + zip + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "SqlStatement: " + sqlStatement + "\n"
				+ "OutputFormatLocale: " + dateFormatLocale.getLanguage() + "\n"
				+ "CreateBlobFiles: " + createBlobFiles + "\n"
				+ "CreateClobFiles: " + createClobFiles;
	}

	@Override
	protected String getFileExtension() {
		return "vcf";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		vcfWriter = new VcfWriter(outputStream, encoding);
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		// Do nothing
	}

	@Override
	protected void startTableLine() throws Exception {
		currentVcfCardMap = new HashMap<>();
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		currentVcfCardMap.put(columnName, value);
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		currentVcfCardMap.put(columnName, localDateValue);
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		currentVcfCardMap.put(columnName, localDateTimeValue);
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		currentVcfCardMap.put(columnName, zonedDateTimeValue);
	}

	@Override
	protected void endTableLine() throws Exception {
		vcfWriter.writeCard(VcfCard.fromMap(currentVcfCardMap));
		currentVcfCardMap = null;
	}

	@Override
	protected void endOutput() throws Exception {
		// Do nothing
	}

	@Override
	protected void closeWriter() throws Exception {
		if (vcfWriter != null) {
			try {
				vcfWriter.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			vcfWriter = null;
		}
	}
}
