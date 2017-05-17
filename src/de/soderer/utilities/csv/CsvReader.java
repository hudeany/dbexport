package de.soderer.utilities.csv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import de.soderer.utilities.BasicReader;
import de.soderer.utilities.csv.CsvFormat.QuoteMode;

/**
 * The Class CsvReader.
 */
public class CsvReader extends BasicReader {
	/** CSV data format definition */
	private CsvFormat csvFormat = new CsvFormat();
	
	/** If a single read was done, it is impossible to make a full read at once with readAll(). */
	private boolean singleReadStarted = false;

	/** Number of columns expected (set by first read line). */
	private int numberOfColumns = -1;

	/** Number of lines read until now. */
	private int readLines = 0;

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 */
	public CsvReader(InputStream inputStream) throws Exception {
		this(inputStream, Charset.forName(DEFAULT_ENCODING));
	}

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param encoding
	 *            the encoding
	 */
	public CsvReader(InputStream inputStream, String encoding) throws Exception {
		this(inputStream, Charset.forName(encoding));
	}

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param encoding
	 *            the encoding
	 */
	public CsvReader(InputStream inputStream, Charset encoding) throws Exception {
		super(inputStream, encoding);
	}

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param separator
	 *            the separator
	 */
	public CsvReader(InputStream inputStream, CsvFormat csvFormat) throws Exception {
		this(inputStream, Charset.forName(DEFAULT_ENCODING), csvFormat);
	}

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param encoding
	 *            the encoding
	 * @param separator
	 *            the separator
	 */
	public CsvReader(InputStream inputStream, String encoding, CsvFormat csvFormat) throws Exception {
		this(inputStream, Charset.forName(encoding), csvFormat);
	}

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param encoding
	 *            the encoding
	 * @param separator
	 *            the separator
	 */
	public CsvReader(InputStream inputStream, Charset encoding, CsvFormat csvFormat) throws Exception {
		this(inputStream, encoding);
		
		this.csvFormat = csvFormat;
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
	public CsvReader setCsvFormat(CsvFormat csvFormat) {
		this.csvFormat = csvFormat;
		return this;
	}

	/**
	 * Get lines read until now.
	 *
	 * @return the read lines
	 */
	public int getReadLines() {
		return readLines;
	}

	/**
	 * Read the next line of csv data.
	 *
	 * @return the list
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws CsvDataException
	 *             the csv data exception
	 */
	public List<String> readNextCsvLine() throws IOException, CsvDataException {
		readLines++;
		singleReadStarted = true;
		List<String> returnList = new ArrayList<String>();
		StringBuilder nextValue = new StringBuilder();
		boolean insideString = false;
		boolean isQuotedString = false;
		Character nextCharacter;
		char previousCharacter = (char) -1;
		
		while ((nextCharacter = readNextCharacter()) != null) {
			char nextChar = (char) nextCharacter;
			if (csvFormat.getQuoteMode() != QuoteMode.NO_QUOTE && nextChar == csvFormat.getStringQuote()) {
				if (csvFormat.getStringQuoteEscapeCharacter() != csvFormat.getStringQuote()) {
					if (previousCharacter != csvFormat.getStringQuoteEscapeCharacter()) {
						insideString = !insideString;
					}
				} else {
					insideString = !insideString;
				}
				nextValue.append(nextChar);
				isQuotedString = true;
			} else if (!insideString) {
				if (nextChar == '\r' || nextChar == '\n') {
					if (nextValue.length() > 0 || previousCharacter == csvFormat.getSeparator()) {
						returnList.add(parseValue(nextValue.toString()));
					}

					if (returnList.size() > 0) {
						if (numberOfColumns != -1 && numberOfColumns != returnList.size()) {
							if (numberOfColumns > returnList.size() && csvFormat.isFillMissingTrailingColumnsWithNull()) {
								while (returnList.size() < numberOfColumns) {
									returnList.add(null);
								}
							} else {
								throw new CsvDataException("Inconsistent number of values in line " + readLines + " (expected: " + numberOfColumns + " actually: " + returnList.size() + ")", readLines);
							}
						}
						numberOfColumns = returnList.size();
						return returnList;
					}
				} else if (nextChar == csvFormat.getSeparator()) {
					returnList.add(parseValue(nextValue.toString()));
					nextValue = new StringBuilder();
					isQuotedString = false;
				} else if (isQuotedString) {
					if (!Character.isWhitespace(nextChar)) {
						throw new CsvDataException("Not allowed textdata '" + nextChar + "' after quoted text in data in line " + readLines, readLines);
					}
				} else {
					nextValue.append(nextChar);
				}
			} else { // insideString
				if ((nextChar == '\r' || nextChar == '\n') && !csvFormat.isLineBreakInDataAllowed()) {
					throw new CsvDataException("Not allowed linebreak in data in line " + readLines, readLines);
				} else {
					nextValue.append(nextChar);
				}
			}

			previousCharacter = nextCharacter;
		}

		if (insideString) {
			close();
			throw new IOException("Unexpected end of data after quoted csv-value was started in line " + readLines);
		} else {
			if (nextValue.length() > 0 || previousCharacter == csvFormat.getSeparator()) {
				returnList.add(parseValue(nextValue.toString()));
			}

			if (returnList.size() > 0) {
				if (numberOfColumns != -1 && numberOfColumns != returnList.size()) {
					if (numberOfColumns > returnList.size() && csvFormat.isFillMissingTrailingColumnsWithNull()) {
						while (returnList.size() < numberOfColumns) {
							returnList.add(null);
						}
					} else {
						throw new CsvDataException("Inconsistent number of values in line " + readLines + " (expected: " + numberOfColumns + " actually: " + returnList.size() + ")", readLines);
					}
				}
				numberOfColumns = returnList.size();
				return returnList;
			} else {
				close();
				return null;
			}
		}
	}

	/**
	 * Read all csv data at once. This can only be done before readNextCsvLine() was called for the first time
	 *
	 * @return the list
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws CsvDataException
	 *             the csv data exception
	 */
	public List<List<String>> readAll() throws IOException, CsvDataException {
		if (singleReadStarted) {
			throw new IllegalStateException("Single readNextCsvLine was called before readAll");
		}

		try {
			List<List<String>> csvValues = new ArrayList<List<String>>();
			List<String> lineValues;
			while ((lineValues = readNextCsvLine()) != null) {
				csvValues.add(lineValues);
			}
			return csvValues;
		} finally {
			close();
		}
	}

	/**
	 * Parse a single value to applicate allowed double stringquotes.
	 *
	 * @param rawValue
	 *            the raw value
	 * @return the string
	 * @throws CsvDataException
	 *             the csv data exception
	 */
	private String parseValue(String rawValue) throws CsvDataException {
		String returnValue = rawValue;

		if (isNotEmpty(returnValue)) {
			if (csvFormat.getQuoteMode() != QuoteMode.NO_QUOTE) {
				String stringQuoteString = Character.toString(csvFormat.getStringQuote());
				if (returnValue.contains(stringQuoteString)) {
					returnValue = returnValue.trim();
				}
				if (returnValue.charAt(0) == csvFormat.getStringQuote() && returnValue.charAt(returnValue.length() - 1) == csvFormat.getStringQuote()) {
					returnValue = returnValue.substring(1, returnValue.length() - 1);
					returnValue = returnValue.replace(csvFormat.getStringQuoteEscapeCharacter() + stringQuoteString, stringQuoteString);
				}
			}
			returnValue = returnValue.replace("\r\n", "\n").replace('\r', '\n');

			if (!csvFormat.isEscapedStringQuoteInDataAllowed() && returnValue.indexOf(csvFormat.getStringQuote()) >= 0) {
				throw new CsvDataException("Not allowed stringquote in data in line " + readLines, readLines);
			}
			
			if (csvFormat.isAlwaysTrim()) {
				returnValue = returnValue.trim();
			}
		}

		return returnValue;
	}

	/**
	 * This method reads the stream to the end and counts all csv value lines, which can be less than the absolute linebreak count of the stream for the reason of quoted linebreaks. The result also
	 * contains the first line, which may consist of columnheaders.
	 *
	 * @return the csv line count
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws CsvDataException
	 *             the csv data exception
	 */
	public int getCsvLineCount() throws IOException, CsvDataException {
		if (singleReadStarted) {
			throw new IllegalStateException("Single readNextCsvLine was called before getCsvLineCount");
		}

		try {
			int csvLineCount = 0;
			while (readNextCsvLine() != null) {
				csvLineCount++;
			}
			return csvLineCount;
		} finally {
			close();
		}
	}

	/**
	 * Parse a single csv data line for data entries.
	 * 
	 * @param csvLine
	 * @return
	 * @throws Exception
	 */
	public static List<String> parseCsvLine(String csvLine) throws Exception {
		return parseCsvLine(new CsvFormat(), csvLine);
	}

	/**
	 * Parse a single csv data line for data entries.
	 * 
	 * @param csvFormat
	 * @param csvLine
	 * @return
	 * @throws Exception
	 */
	public static List<String> parseCsvLine(CsvFormat csvFormat, String csvLine) throws Exception {
		CsvReader reader = null;
		try {
			reader = new CsvReader(new ByteArrayInputStream(csvLine.getBytes("UTF-8")), "UTF-8", csvFormat);
			List<List<String>> fullData = reader.readAll();
			if (fullData.size() != 1) {
				throw new Exception("Too many csv lines in data");
			} else {
				return fullData.get(0);
			}
		} catch (CsvDataException e) {
			throw e;
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}
