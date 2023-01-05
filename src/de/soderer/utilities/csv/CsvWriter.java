package de.soderer.utilities.csv;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import de.soderer.utilities.csv.CsvFormat.QuoteMode;

/**
 * The Class CsvWriter.
 */
public class CsvWriter implements Closeable {
	/** CSV data format definition */
	private CsvFormat csvFormat;

	/** Default output encoding. */
	public static final Charset DEFAULT_ENCODING = StandardCharsets.UTF_8;

	/** Current output separator as string for internal use. */
	private final String separatorString;

	/** Current output string quote as string for internal use. */
	private final String stringQuoteString;

	/** Current output string quote two times for internal use. */
	private final String escapedStringQuoteString;

	/** Output stream. */
	private OutputStream outputStream;

	/** Output encoding. */
	private final Charset encoding;

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
	public CsvWriter(final OutputStream outputStream) {
		this(outputStream, DEFAULT_ENCODING);
	}

	/**
	 * CSV Writer derived constructor.
	 *
	 * @param outputStream
	 *            the output stream
	 * @param encoding
	 *            the encoding
	 */
	public CsvWriter(final OutputStream outputStream, final Charset encoding) {
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
	public CsvWriter(final OutputStream outputStream, final CsvFormat csvFormat) {
		this(outputStream, DEFAULT_ENCODING, csvFormat);
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
	public CsvWriter(final OutputStream outputStream, final Charset encoding, final CsvFormat csvFormat) {
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
	public CsvWriter setCsvFormat(final CsvFormat csvFormat) {
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
	public void writeValues(final Object... values) throws Exception {
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
	public void writeValues(final List<? extends Object> values) throws CsvDataException, IOException {
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
	public void writeAll(final List<List<? extends Object>> valueLines) throws Exception {
		for (final List<? extends Object> valuesOfLine : valueLines) {
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
	private String escapeValue(final Object value) throws CsvDataException {
		String valueString = "";
		if (value != null) {
			valueString = value.toString();
		}

		final boolean valueNeedsQuotation =
				valueString.contains(stringQuoteString)
				|| valueString.contains(separatorString)
				|| valueString.contains("\r")
				|| valueString.contains("\n");

		if (csvFormat.getQuoteMode() == QuoteMode.QUOTE_ALL_DATA
				|| (csvFormat.getQuoteMode() == QuoteMode.QUOTE_STRINGS && value instanceof String)
				|| (csvFormat.getQuoteMode() == QuoteMode.QUOTE_IF_NEEDED && valueNeedsQuotation)) {
			final StringBuilder escapedValue = new StringBuilder();
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
	public int[] calculateOutputSizesOfValues(final List<? extends Object> values) throws CsvDataException {
		final int[] returnArray = new int[values.size()];
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
	public int calculateOutputSizesOfValue(final Object value) throws CsvDataException {
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
	public static String getCsvLine(final char separator, final Character stringQuote, final List<? extends Object> values) {
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
	public static String getCsvLine(final char separator, final Character stringQuote, final Object... values) {
		final StringBuilder returnValue = new StringBuilder();
		final String separatorString = Character.toString(separator);
		final String stringQuoteString = stringQuote == null ? "" : Character.toString(stringQuote);
		final String doubleStringQuoteString = stringQuoteString + stringQuoteString;
		if (values != null) {
			for (final Object value : values) {
				if (returnValue.length() > 0) {
					returnValue.append(separator);
				}
				if (value != null) {
					final String valueString = value.toString();
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
	private static void closeQuietly(final Closeable closeableItem) {
		if (closeableItem != null) {
			try {
				closeableItem.close();
			} catch (@SuppressWarnings("unused") final IOException e) {
				// Do nothing
			}
		}
	}

	/**
	 * Set minimumColumnSizes for beautification
	 *
	 * @param minimumColumnSizes
	 */
	public void setMinimumColumnSizes(final int[] minimumColumnSizes) {
		this.minimumColumnSizes = minimumColumnSizes;
	}

	/**
	 * Set columnPaddings for beautification (true = right padding = left aligned)
	 *
	 * @param columnPaddings
	 */
	public void setColumnPaddings(final boolean[] columnPaddings) {
		this.columnPaddings = columnPaddings;
	}

	/**
	 * Append blanks at the left of a string to make if fit the given minimum
	 *
	 * @param escapedValue
	 * @param i
	 * @return
	 */
	private static String leftPad(final String value, final int minimumLength) {
		try {
			return String.format("%1$" + minimumLength + "s", value);
		} catch (@SuppressWarnings("unused") final Exception e) {
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
	private static String rightPad(final String value, final int minimumLength) {
		try {
			return String.format("%1$-" + minimumLength + "s", value);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return value;
		}
	}
}
