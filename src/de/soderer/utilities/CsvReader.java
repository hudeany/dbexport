package de.soderer.utilities;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class CsvReader.
 */
public class CsvReader extends BasicReader {
	/** The Constant DEFAULT_SEPARATOR. */
	public static final char DEFAULT_SEPARATOR = ',';

	/** The Constant DEFAULT_STRING_QUOTE. */
	public static final char DEFAULT_STRING_QUOTE = '"';

	/** Mandatory separating charactor. */
	private char separator;

	/** Character for stringquotes: if set to null, no quoting will be done. */
	private char stringQuote;

	/**
	 * Character to escape the stringquote character within quoted strings. By default this is the stringquote character itself, so it is doubled in quoted string, but may also be a backslash '\'.
	 */
	private char stringQuoteEscapeCharacter;

	/** Since stringQuote is a simple char this activates or deactivates the quoting. */
	private boolean useStringQuote;

	/** Allow linebreaks in data texts without the effect of a new data set line. */
	private boolean lineBreakInDataAllowed = true;

	/** Allow double stringquotes to use it as a character in data text. */
	private boolean escapedStringQuoteInDataAllowed = true;

	/** If a single read was done, it is impossible to make a full read at once with readAll(). */
	private boolean singleReadStarted = false;

	/** Number of columns expected (set by first read line). */
	private int numberOfColumns = -1;

	/** Number of lines read until now. */
	private int readLines = 0;

	/** Allow lines with less than the expected number of data entries per line. */
	private boolean fillMissingTrailingColumnsWithNull = false;
	
	/** Trim all data values */
	private boolean alwaysTrim = false;

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 */
	public CsvReader(InputStream inputStream) throws Exception {
		this(inputStream, Charset.forName(DEFAULT_ENCODING), DEFAULT_SEPARATOR, DEFAULT_STRING_QUOTE);
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
		this(inputStream, Charset.forName(encoding), DEFAULT_SEPARATOR, DEFAULT_STRING_QUOTE);
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
		this(inputStream, encoding, DEFAULT_SEPARATOR, DEFAULT_STRING_QUOTE);
	}

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param separator
	 *            the separator
	 */
	public CsvReader(InputStream inputStream, char separator) throws Exception {
		this(inputStream, Charset.forName(DEFAULT_ENCODING), separator, DEFAULT_STRING_QUOTE);
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
	public CsvReader(InputStream inputStream, String encoding, char separator) throws Exception {
		this(inputStream, Charset.forName(encoding), separator, DEFAULT_STRING_QUOTE);
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
	public CsvReader(InputStream inputStream, Charset encoding, char separator) throws Exception {
		this(inputStream, encoding, separator, DEFAULT_STRING_QUOTE);
	}

	/**
	 * CSV Reader derived constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param separator
	 *            the separator
	 * @param stringQuote
	 *            the string quote
	 */
	public CsvReader(InputStream inputStream, char separator, Character stringQuote) throws Exception {
		this(inputStream, Charset.forName(DEFAULT_ENCODING), separator, stringQuote);
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
	 * @param stringQuote
	 *            the string quote
	 */
	public CsvReader(InputStream inputStream, String encoding, char separator, Character stringQuote) throws Exception {
		this(inputStream, Charset.forName(encoding), separator, stringQuote);
	}

	/**
	 * CSV Reader main constructor.
	 *
	 * @param inputStream
	 *            the input stream
	 * @param encoding
	 *            the encoding
	 * @param separator
	 *            the separator
	 * @param stringQuote
	 *            the string quote
	 * @throws Exception 
	 */
	public CsvReader(InputStream inputStream, Charset encoding, char separator, Character stringQuote) throws Exception {
		super(inputStream, encoding);
		
		this.separator = separator;
		if (stringQuote != null) {
			this.stringQuote = stringQuote;
			stringQuoteEscapeCharacter = stringQuote;
			useStringQuote = true;
		} else {
			useStringQuote = false;
		}

		if (anyCharsAreEqual(this.separator, '\r', '\n')) {
			throw new IllegalArgumentException("Separator '" + this.separator + "' is invalid");
		} else if (useStringQuote && anyCharsAreEqual(this.separator, this.stringQuote, '\r', '\n')) {
			throw new IllegalArgumentException("Stringquote '" + this.stringQuote + "' is invalid");
		}
	}

	/**
	 * Getter for property fillMissingTrailingColumnsWithNull.
	 *
	 * @return true, if is fill missing trailing columns with null
	 */
	public boolean isFillMissingTrailingColumnsWithNull() {
		return fillMissingTrailingColumnsWithNull;
	}

	/**
	 * Setter for property fillMissingTrailingColumnsWithNull.
	 *
	 * @param fillMissingTrailingColumnsWithNull
	 *            the new fill missing trailing columns with null
	 */
	public void setFillMissingTrailingColumnsWithNull(boolean fillMissingTrailingColumnsWithNull) {
		this.fillMissingTrailingColumnsWithNull = fillMissingTrailingColumnsWithNull;
	}

	/**
	 * Getter for property alwaysTrim.
	 *
	 * @return true, if is alwaysTrim
	 */
	public boolean isAlwaysTrim() {
		return alwaysTrim;
	}

	/**
	 * Setter for property alwaysTrim.
	 *
	 * @param alwaysTrim
	 *            trim all values
	 */
	public void setAlwaysTrim(boolean alwaysTrim) {
		this.alwaysTrim = alwaysTrim;
	}

	/**
	 * Setter for property stringQuoteEscapeCharacter. Character to escape the stringquote character within quoted strings. By default this is the stringquote character itself, so it is doubled in
	 * quoted string, but may also be a backslash '\'.
	 *
	 * @param stringQuoteEscapeCharacter
	 *            the new fill missing trailing columns with null
	 */
	public void setStringQuoteEscapeCharacter(char stringQuoteEscapeCharacter) {
		this.stringQuoteEscapeCharacter = stringQuoteEscapeCharacter;
		if (useStringQuote && anyCharsAreEqual(separator, stringQuote, '\r', '\n', stringQuoteEscapeCharacter)) {
			throw new IllegalArgumentException("Stringquote escape character '" + this.stringQuoteEscapeCharacter + "' is invalid");
		}
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
	 * Getter for property lineBreakInDataAllowed.
	 *
	 * @return true, if is line break in data allowed
	 */
	public boolean isLineBreakInDataAllowed() {
		return lineBreakInDataAllowed;
	}

	/**
	 * Setter for property lineBreakInDataAllowed.
	 *
	 * @param lineBreakInDataAllowed
	 *            the new line break in data allowed
	 */
	public void setLineBreakInDataAllowed(boolean lineBreakInDataAllowed) {
		this.lineBreakInDataAllowed = lineBreakInDataAllowed;
	}

	/**
	 * Getter for property escapedStringQuoteInDataAllowed.
	 *
	 * @return true, if is escaped string quote in data allowed
	 */
	public boolean isEscapedStringQuoteInDataAllowed() {
		return escapedStringQuoteInDataAllowed;
	}

	/**
	 * Setter for property escapedStringQuoteInDataAllowed.
	 *
	 * @param escapedStringQuoteInDataAllowed
	 *            the new escaped string quote in data allowed
	 */
	public void setEscapedStringQuoteInDataAllowed(boolean escapedStringQuoteInDataAllowed) {
		this.escapedStringQuoteInDataAllowed = escapedStringQuoteInDataAllowed;
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
		Character nextCharacter;
		char previousCharacter = (char) -1;
		
		while ((nextCharacter = readNextCharacter()) != null) {
			char nextChar = (char) nextCharacter;
			if (useStringQuote && nextChar == stringQuote) {
				if (stringQuoteEscapeCharacter != stringQuote) {
					if (previousCharacter != stringQuoteEscapeCharacter) {
						insideString = !insideString;
					}
				} else {
					insideString = !insideString;
				}
				nextValue.append(nextChar);
			} else if (!insideString) {
				if (nextChar == '\r' || nextChar == '\n') {
					if (nextValue.length() > 0 || previousCharacter == separator) {
						returnList.add(parseValue(nextValue.toString()));
					}

					if (returnList.size() > 0) {
						if (numberOfColumns != -1 && numberOfColumns != returnList.size()) {
							if (numberOfColumns > returnList.size() && fillMissingTrailingColumnsWithNull) {
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
				} else if (nextChar == separator) {
					returnList.add(parseValue(nextValue.toString()));
					nextValue = new StringBuilder();
				} else {
					nextValue.append(nextChar);
				}
			} else { // insideString
				if ((nextChar == '\r' || nextChar == '\n') && !lineBreakInDataAllowed) {
					throw new CsvDataException("Not allowed linebreak in data in line " + readLines, readLines);
				} else {
					nextValue.append(nextChar);
				}
			}

			previousCharacter = nextCharacter;
		}

		if (insideString) {
			close();
			throw new IOException("Unexpected end of data after quoted csv-value was started");
		} else {
			if (nextValue.length() > 0 || previousCharacter == separator) {
				returnList.add(parseValue(nextValue.toString()));
			}

			if (returnList.size() > 0) {
				if (numberOfColumns != -1 && numberOfColumns != returnList.size()) {
					if (numberOfColumns > returnList.size() && fillMissingTrailingColumnsWithNull) {
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
			if (useStringQuote) {
				String stringQuoteString = Character.toString(stringQuote);
				if (returnValue.contains(stringQuoteString)) {
					returnValue = returnValue.trim();
				}
				if (returnValue.charAt(0) == stringQuote && returnValue.charAt(returnValue.length() - 1) == stringQuote) {
					returnValue = returnValue.substring(1, returnValue.length() - 1);
					returnValue = returnValue.replace(stringQuoteEscapeCharacter + stringQuoteString, stringQuoteString);
				}
			}
			returnValue = returnValue.replace("\r\n", "\n").replace('\r', '\n');

			if (!escapedStringQuoteInDataAllowed && returnValue.indexOf(stringQuote) >= 0) {
				throw new CsvDataException("Not allowed stringquote in data in line " + readLines, readLines);
			}
			
			if (alwaysTrim) {
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
	 * @param separator
	 *            the separator
	 * @param stringQuote
	 *            the string quote
	 * @param csvLine
	 *            the csv line
	 * @return the list
	 * @throws Exception
	 *             the exception
	 */
	public static List<String> parseCsvLine(char separator, Character stringQuote, String csvLine) throws Exception {
		CsvReader reader = null;
		try {
			reader = new CsvReader(new ByteArrayInputStream(csvLine.getBytes("UTF-8")), "UTF-8", separator, stringQuote);
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

	/**
	 * Check if any characters in a list are equal.
	 *
	 * @param values
	 *            the values
	 * @return true, if successful
	 */
	private static boolean anyCharsAreEqual(char... values) {
		for (int i = 0; i < values.length; i++) {
			for (int j = i + 1; j < values.length; j++) {
				if (values[i] == values[j]) {
					return true;
				}
			}
		}
		return false;
	}
}
