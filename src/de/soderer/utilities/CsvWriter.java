package de.soderer.utilities;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import de.soderer.utilities.CsvFormat.QuoteMode;

/**
 * The Class CsvWriter.
 */
public class CsvWriter implements Closeable {
	/** CSV data format definition */
	private CsvFormat csvFormat;

	/** Default output encoding. */
	public static final String DEFAULT_ENCODING = "UTF-8";

	/** Current output separator as string for internal use. */
	private String separatorString;

	/** Current output string quote as string for internal use. */
	private String stringQuoteString;

	/** Current output string quote two times for internal use. */
	private String escapedStringQuoteString;

	/** Output stream. */
	private OutputStream outputStream;

	/** Output encoding. */
	private Charset encoding;

	/** Lines written until now. */
	private int writtenLines = 0;

	/** Number of columns to write, set by first line written. */
	private int numberOfColumns = -1;

	/** Output writer. */
	private BufferedWriter outputWriter = null;

	/** Minimum sizes of columns for beautification */
	private int[] minimumColumnSizes = null;

	/** Padding locations of columns for beautification (true = right padding = left aligned) */
	private boolean[] columnPaddings = null;

	/**
	 * CSV Writer derived constructor.
	 *
	 * @param outputStream
	 *            the output stream
	 */
	public CsvWriter(OutputStream outputStream) {
		this(outputStream, Charset.forName(DEFAULT_ENCODING));
	}

	/**
	 * CSV Writer derived constructor.
	 *
	 * @param outputStream
	 *            the output stream
	 * @param encoding
	 *            the encoding
	 */
	public CsvWriter(OutputStream outputStream, String encoding) {
		this(outputStream, Charset.forName(encoding));
	}

	/**
	 * CSV Writer derived constructor.
	 *
	 * @param outputStream
	 *            the output stream
	 * @param encoding
	 *            the encoding
	 */
	public CsvWriter(OutputStream outputStream, Charset encoding) {
		this(outputStream, encoding, new CsvFormat());
	}

	/**
	 * CSV Writer derived constructor.
	 *
	 * @param outputStream
	 *            the output stream
	 * @param separator
	 *            the separator
	 */
	public CsvWriter(OutputStream outputStream, CsvFormat csvFormat) {
		this(outputStream, Charset.forName(DEFAULT_ENCODING), csvFormat);
	}

	/**
	 * CSV Writer derived constructor.
	 *
	 * @param outputStream
	 *            the output stream
	 * @param encoding
	 *            the encoding
	 * @param separator
	 *            the separator
	 */
	public CsvWriter(OutputStream outputStream, String encoding, CsvFormat csvFormat) {
		this(outputStream, Charset.forName(encoding), csvFormat);
	}

	/**
	 * CSV Writer main constructor.
	 *
	 * @param outputStream
	 *            the output stream
	 * @param encoding
	 *            the encoding
	 * @param separator
	 *            the separator
	 * @param stringQuote
	 *            the string quote
	 * @param lineBreak
	 *            the line break
	 */
	public CsvWriter(OutputStream outputStream, Charset encoding, CsvFormat csvFormat) {
		this.csvFormat = csvFormat;
		this.outputStream = outputStream;
		this.encoding = encoding;
		separatorString = Character.toString(csvFormat.getSeparator());
		stringQuoteString = Character.toString(csvFormat.getStringQuote());
		escapedStringQuoteString = csvFormat.getStringQuoteEscapeCharacter() + stringQuoteString;

		if (this.encoding == null) {
			throw new IllegalArgumentException("Encoding is null");
		} else if (this.outputStream == null) {
			throw new IllegalArgumentException("OutputStream is null");
		}
	}

	/**
	 * Get configured csv format
	 * 
	 * @return
	 */
	public CsvFormat getCsvFormat() {
		return csvFormat;
	}

	/**
	 * Configured csv format
	 * 
	 * @param csvFormat
	 */
	public CsvWriter setCsvFormat(CsvFormat csvFormat) {
		this.csvFormat = csvFormat;
		return this;
	}

	/**
	 * Write a single line of data entries.
	 *
	 * @param values
	 *            the values
	 * @throws Exception
	 *             the exception
	 */
	public void writeValues(Object... values) throws Exception {
		writeValues(Arrays.asList(values));
	}

	/**
	 * Write a single line of data entries.
	 *
	 * @param values
	 *            the values
	 * @throws CsvDataException
	 *             the csv data exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void writeValues(List<? extends Object> values) throws CsvDataException, IOException {
		if (numberOfColumns != -1 && (values == null || numberOfColumns != values.size())) {
			throw new CsvDataException(
					"Inconsistent number of values after " + writtenLines + " written lines (expected: " + numberOfColumns + " was: " + (values == null ? "null" : values.size()) + ")", writtenLines);
		}

		if (outputWriter == null) {
			if (outputStream == null) {
				throw new IllegalStateException("CsvWriter is already closed");
			}
			outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
		}

		for (int i = 0; i < values.size(); i++) {
			if (i > 0) {
				outputWriter.write(csvFormat.getSeparator());
			}

			String escapedValue = escapeValue(values.get(i));

			if (minimumColumnSizes != null && minimumColumnSizes.length > i) {
				if (columnPaddings != null && columnPaddings.length > i && columnPaddings[i]) {
					escapedValue = rightPad(escapedValue, minimumColumnSizes[i]);
				} else {
					escapedValue = leftPad(escapedValue, minimumColumnSizes[i]);
				}
			}

			outputWriter.write(escapedValue);
		}
		outputWriter.write(csvFormat.getLineBreak());

		writtenLines++;
		numberOfColumns = values.size();
	}

	/**
	 * Write a full set of lines of data entries.
	 *
	 * @param valueLines
	 *            the value lines
	 * @throws Exception
	 *             the exception
	 */
	public void writeAll(List<List<? extends Object>> valueLines) throws Exception {
		for (List<? extends Object> valuesOfLine : valueLines) {
			writeValues(valuesOfLine);
		}
	}

	/**
	 * Escape a single data entry using stringquotes as configured.
	 *
	 * @param value
	 *            the value
	 * @return the string
	 * @throws CsvDataException
	 *             the csv data exception
	 */
	private String escapeValue(Object value) throws CsvDataException {
		String valueString = "";
		if (value != null) {
			valueString = value.toString();
		}
		
		boolean valueNeedsQuotation =
			valueString.contains(stringQuoteString)
			|| valueString.contains(separatorString)
			|| valueString.contains("\r")
			|| valueString.contains("\n");

		if (csvFormat.getQuoteMode() == QuoteMode.QUOTE_ALL_DATA
			|| (csvFormat.getQuoteMode() == QuoteMode.QUOTE_STRINGS && value instanceof String)
			|| (csvFormat.getQuoteMode() == QuoteMode.QUOTE_IF_NEEDED && valueNeedsQuotation)) {
			StringBuilder escapedValue = new StringBuilder();
			escapedValue.append(stringQuoteString);
			escapedValue.append(valueString.replace(stringQuoteString, escapedStringQuoteString));
			escapedValue.append(stringQuoteString);
			return escapedValue.toString();
		} else if (valueNeedsQuotation) {
			throw new CsvDataException("StringQuote was deactivated but is needed for csv-value after " + writtenLines + " written lines", writtenLines);
		} else {
			return valueString;
		}
	}

	/**
	 * Calculate column value output sizes for beautification of csv output.
	 *
	 * @param values
	 *            the values
	 * @throws CsvDataException
	 *             the csv data exception
	 */
	public int[] calculateOutputSizesOfValues(List<? extends Object> values) throws CsvDataException {
		int[] returnArray = new int[values.size()];
		for (int i = 0; i < values.size(); i++) {
			returnArray[i] = escapeValue(values.get(i)).length();
		}
		return returnArray;
	}

	/**
	 * Calculate column value output size for beautification of csv output.
	 *
	 * @param value
	 *            the value
	 * @throws CsvDataException
	 *             the csv data exception
	 */
	public int calculateOutputSizesOfValue(Object value) throws CsvDataException {
		return escapeValue(value).length();
	}

	/**
	 * Close this writer and its underlying stream.
	 */
	@Override
	public void close() {
		closeQuietly(outputWriter);
		outputWriter = null;
		closeQuietly(outputStream);
		outputStream = null;
	}

	/**
	 * Get number of lines written until now.
	 *
	 * @return the written lines
	 */
	public int getWrittenLines() {
		return writtenLines;
	}

	/**
	 * Flush buffered data.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void flush() throws IOException {
		if (outputWriter != null) {
			outputWriter.flush();
		}
	}

	/**
	 * Create a single csv line.
	 *
	 * @param separator
	 *            the separator
	 * @param stringQuote
	 *            the string quote
	 * @param values
	 *            the values
	 * @return the csv line
	 */
	public static String getCsvLine(char separator, Character stringQuote, List<? extends Object> values) {
		return getCsvLine(separator, stringQuote, values.toArray());
	}

	/**
	 * Create a single csv line.
	 *
	 * @param separator
	 *            the separator
	 * @param stringQuote
	 *            the string quote
	 * @param values
	 *            the values
	 * @return the csv line
	 */
	public static String getCsvLine(char separator, Character stringQuote, Object... values) {
		StringBuilder returnValue = new StringBuilder();
		String separatorString = Character.toString(separator);
		String stringQuoteString = stringQuote == null ? "" : Character.toString(stringQuote);
		String doubleStringQuoteString = stringQuoteString + stringQuoteString;
		if (values != null) {
			for (Object value : values) {
				if (returnValue.length() > 0) {
					returnValue.append(separator);
				}
				if (value != null) {
					String valueString = value.toString();
					if (valueString.contains(separatorString) || valueString.contains("\r") || valueString.contains("\n") || valueString.contains(stringQuoteString)) {
						returnValue.append(stringQuoteString);
						returnValue.append(valueString.replace(stringQuoteString, doubleStringQuoteString));
						returnValue.append(stringQuoteString);
					} else {
						returnValue.append(valueString);
					}
				}
			}
		}
		return returnValue.toString();
	}

	/**
	 * Close a Closable item and ignore any Exception thrown by its close method.
	 *
	 * @param closeableItem
	 *            the closeable item
	 */
	private static void closeQuietly(Closeable closeableItem) {
		if (closeableItem != null) {
			try {
				closeableItem.close();
			} catch (IOException e) {
				// Do nothing
			}
		}
	}

	/**
	 * Set minimumColumnSizes for beautification
	 *
	 * @param minimumColumnSizes
	 */
	public void setMinimumColumnSizes(int[] minimumColumnSizes) {
		this.minimumColumnSizes = minimumColumnSizes;
	}

	/**
	 * Set columnPaddings for beautification (true = right padding = left aligned)
	 *
	 * @param columnPaddings
	 */
	public void setColumnPaddings(boolean[] columnPaddings) {
		this.columnPaddings = columnPaddings;
	}

	/**
	 * Append blanks at the left of a string to make if fit the given minimum
	 *
	 * @param escapedValue
	 * @param i
	 * @return
	 */
	private String leftPad(String value, int minimumLength) {
		try {
			return String.format("%1$" + minimumLength + "s", value);
		} catch (Exception e) {
			return value;
		}
	}

	/**
	 * Append blanks at the right of a string to make if fit the given minimum
	 *
	 * @param escapedValue
	 * @param i
	 * @return
	 */
	private String rightPad(String value, int minimumLength) {
		try {
			return String.format("%1$-" + minimumLength + "s", value);
		} catch (Exception e) {
			return value;
		}
	}
}
