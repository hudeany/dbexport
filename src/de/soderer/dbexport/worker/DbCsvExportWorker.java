package de.soderer.dbexport.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.DbUtilities.DbVendor;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.csv.CsvFormat;
import de.soderer.utilities.csv.CsvFormat.QuoteMode;
import de.soderer.utilities.csv.CsvReader;
import de.soderer.utilities.csv.CsvWriter;
import de.soderer.utilities.worker.WorkerParentDual;

public class DbCsvExportWorker extends AbstractDbExportWorker {
	// Default optional parameters
	private char separator = ';';
	private char stringQuote = '"';
	private char stringQuoteEscapeCharacter = '"';
	private String nullValueText = "";
	private boolean alwaysQuote = false;
	private boolean noHeaders = false;

	private CsvWriter csvWriter = null;

	private CsvWriter beautifiedCsvWriter = null;
	private File temporaryUglifiedFile = null;
	private boolean[] columnPaddings = null;
	private int[] minimumColumnSizes = null;

	private List<String> values = null;

	public DbCsvExportWorker(final WorkerParentDual parent, final DbVendor dbVendor, final String hostname, final String dbName, final String username, final char[] password, final boolean secureConnection, final String trustStoreFilePath, final char[] trustStorePassword, final boolean isStatementFile, final String sqlStatementOrTablelist, final String outputpath) {
		super(parent, dbVendor, hostname, dbName, username, password, secureConnection, trustStoreFilePath, trustStorePassword, isStatementFile, sqlStatementOrTablelist, outputpath);
	}

	public void setSeparator(final char separator) {
		this.separator = separator;
	}

	public void setStringQuote(final char stringQuote) {
		this.stringQuote = stringQuote;
	}

	public void setStringQuoteEscapeCharacter(final char stringQuoteEscapeCharacter) {
		this.stringQuoteEscapeCharacter = stringQuoteEscapeCharacter;
	}

	public void setAlwaysQuote(final boolean alwaysQuote) {
		this.alwaysQuote = alwaysQuote;
	}

	public void setNoHeaders(final boolean noHeaders) {
		this.noHeaders = noHeaders;
	}

	public void setNullValueText(final String nullValueText) {
		this.nullValueText = nullValueText;
	}

	@Override
	public String getConfigurationLogString(final String fileName, final String sqlStatement) {
		return
				"File: " + fileName + "\n"
				+ "Format: " + getFileExtension().toUpperCase() + "\n"
				+ "Separator: " + separator + "\n"
				+ "Zip: " + zip + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "StringQuote: " + stringQuote + "\n"
				+ "StringQuoteEscapeCharacter: " + stringQuoteEscapeCharacter + "\n"
				+ "AlwaysQuote: " + alwaysQuote + "\n"
				+ "SqlStatement: " + sqlStatement + "\n"
				+ "OutputFormatLocale: " + dateFormatLocale.getLanguage() + "\n"
				+ "CreateBlobFiles: " + createBlobFiles + "\n"
				+ "CreateClobFiles: " + createClobFiles + "\n"
				+ "Beautify: " + beautify;
	}

	@Override
	protected String getFileExtension() {
		return "csv";
	}

	@Override
	protected void openWriter(final OutputStream outputStream) throws Exception {
		if (beautify) {
			temporaryUglifiedFile = File.createTempFile("DbExport_Uglified", ".csv", new File(System.getProperty("java.io.tmpdir")));
			csvWriter = new CsvWriter(new FileOutputStream(temporaryUglifiedFile), encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote).setStringQuoteEscapeCharacter(stringQuoteEscapeCharacter).setQuoteMode(alwaysQuote ? QuoteMode.QUOTE_ALL_DATA : QuoteMode.QUOTE_IF_NEEDED));
			beautifiedCsvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote));
		} else {
			csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote).setStringQuoteEscapeCharacter(stringQuoteEscapeCharacter).setQuoteMode(alwaysQuote ? QuoteMode.QUOTE_ALL_DATA : QuoteMode.QUOTE_IF_NEEDED));
		}
	}

	@Override
	protected void startOutput(final Connection connection, final String sqlStatement, final List<String> columnNames) throws Exception {
		if (!noHeaders) {
			csvWriter.writeValues(columnNames);
		}

		minimumColumnSizes = new int[columnNames.size()];
		for (int i = 0; i < columnNames.size(); i++) {
			minimumColumnSizes[i] = csvWriter.calculateOutputSizesOfValue(columnNames.get(i));
		}

		columnPaddings = new boolean[columnNames.size()];
		for (int i = 0; i < columnPaddings.length; i++) {
			columnPaddings[i] = true;
		}
	}

	@Override
	protected void startTableLine() throws Exception {
		values = new ArrayList<>();
	}

	@Override
	protected void writeColumn(final String columnName, final Object value) throws Exception {
		if (value == null) {
			values.add(nullValueText);
		} else if (value instanceof String) {
			values.add((String) value);
		} else if (value instanceof Date) {
			values.add(getDateFormatter().format(DateUtilities.getLocalDateTimeForDate((Date) value)));
		} else if (value instanceof Number) {
			columnPaddings[values.size()] = false;
			if (decimalSeparator != null) {
				values.add(NumberUtilities.formatNumber((Number) value, decimalSeparator, null));
			} else {
				values.add(decimalFormat.format(value));
			}
		} else {
			values.add(value.toString());
		}

		minimumColumnSizes[values.size() - 1] = Math.max(minimumColumnSizes[values.size() - 1], csvWriter.calculateOutputSizesOfValue(values.get(values.size() - 1)));
	}

	@Override
	protected void writeDateColumn(final String columnName, final LocalDate localDateValue) throws Exception {
		if (localDateValue == null) {
			values.add(nullValueText);
		} else {
			values.add(getDateFormatter().format(localDateValue));
		}

		minimumColumnSizes[values.size() - 1] = Math.max(minimumColumnSizes[values.size() - 1], csvWriter.calculateOutputSizesOfValue(values.get(values.size() - 1)));
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final LocalDateTime localDateTimeValue) throws Exception {
		if (localDateTimeValue == null) {
			values.add(nullValueText);
		} else {
			values.add(getDateTimeFormatter().format(localDateTimeValue));
		}

		minimumColumnSizes[values.size() - 1] = Math.max(minimumColumnSizes[values.size() - 1], csvWriter.calculateOutputSizesOfValue(values.get(values.size() - 1)));
	}

	@Override
	protected void writeDateTimeColumn(final String columnName, final ZonedDateTime zonedDateTimeValue) throws Exception {
		if (zonedDateTimeValue == null) {
			values.add(nullValueText);
		} else {
			values.add(getDateTimeFormatter().format(zonedDateTimeValue));
		}

		minimumColumnSizes[values.size() - 1] = Math.max(minimumColumnSizes[values.size() - 1], csvWriter.calculateOutputSizesOfValue(values.get(values.size() - 1)));
	}

	@Override
	protected void endTableLine() throws Exception {
		csvWriter.writeValues(values);
		values = null;
	}

	@Override
	protected void endOutput() throws Exception {
		// Do nothing
	}

	@Override
	protected void closeWriter() throws Exception {
		if (csvWriter != null) {
			try {
				csvWriter.flush();
				csvWriter.close();
				csvWriter = null;
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}

		// Beautify data from uglified csv file
		if (beautifiedCsvWriter != null) {
			CsvReader csvReaderFinal = null;
			try {
				beautifiedCsvWriter.setColumnPaddings(columnPaddings);
				beautifiedCsvWriter.setMinimumColumnSizes(minimumColumnSizes);
				csvReaderFinal = new CsvReader(new FileInputStream(temporaryUglifiedFile), encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote));
				List<String> nextLine;
				while ((nextLine = csvReaderFinal.readNextCsvLine()) != null) {
					beautifiedCsvWriter.writeValues(nextLine);
				}
			} finally {
				Utilities.closeQuietly(csvReaderFinal);
				Utilities.closeQuietly(beautifiedCsvWriter);
			}
			beautifiedCsvWriter = null;
		}

		if (temporaryUglifiedFile != null) {
			temporaryUglifiedFile.delete();
		}
	}
}
