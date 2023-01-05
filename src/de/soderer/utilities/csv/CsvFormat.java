package de.soderer.utilities.csv;

public class CsvFormat {
	/** The Constant DEFAULT_SEPARATOR. */
	public static final char DEFAULT_SEPARATOR = ',';

	/** The Constant DEFAULT_STRING_QUOTE. */
	public static final char DEFAULT_STRING_QUOTE = '"';

	/** Default output linebreak. */
	public static final String DEFAULT_LINEBREAK = "\n";

	/** Mandatory separating charactor */
	private char separator = DEFAULT_SEPARATOR;

	/** Character for stringquotes */
	private char stringQuote = DEFAULT_STRING_QUOTE;

	/**
	 * Character to escape the stringquote character within quoted strings.
	 * By default this is the stringquote character itself, so it is doubled in quoted strings,
	 * but may also be configured to a backslash '\'.
	 */
	private char stringQuoteEscapeCharacter = DEFAULT_STRING_QUOTE;

	/** Allow linebreaks in data texts without the effect of a new data set line. */
	private boolean lineBreakInDataAllowed = true;

	/** Allow escaped stringquotes to use them as a character in data text.
	 * Maybe turned off for data consistency checks. */
	private boolean escapedStringQuoteInDataAllowed = true;

	/** Allow lines with less than the expected number of data entries per line. */
	private boolean fillMissingTrailingColumnsWithNull = false;

	/** Allow lines with more than the expected number of data entries per line, if those are empty. */
	private boolean removeSurplusEmptyTrailingColumns = false;

	/** Trim all data values */
	private boolean alwaysTrim = false;

	/** Quote data entries. */
	private QuoteMode quoteMode = QuoteMode.QUOTE_IF_NEEDED;

	/** Linebreak for output only. */
	private String lineBreak = DEFAULT_LINEBREAK;

	/** Ignore empty lines */
	private boolean ignoreEmptyLines = false;

	/**
	 * The Enum QuoteMode.
	 */
	public enum QuoteMode {
		/** Throw an error, when any quotation is needed */
		NO_QUOTE,

		/** Do only quote, when a quotation is needed */
		QUOTE_IF_NEEDED,

		/** Quote all strings, quote other data, when a quotation is needed */
		QUOTE_STRINGS,

		/** Quote all data */
		QUOTE_ALL_DATA;

		public static QuoteMode getFromString(final String quoteModeString) throws Exception {
			for (final QuoteMode quoteMode : QuoteMode.values()) {
				if (quoteMode.toString().equalsIgnoreCase(quoteModeString)) {
					return quoteMode;
				}
			}
			throw new Exception("Invalid quote mode: " + quoteModeString);
		}
	}

	public CsvFormat() {
	}

	public CsvFormat(final char separator, final char stringQuote, final char stringQuoteEscapeCharacter, final boolean lineBreakInDataAllowed, final boolean escapedStringQuoteInDataAllowed, final boolean fillMissingTrailingColumnsWithNull, final boolean removeSurplusEmptyTrailingColumns, final boolean alwaysTrim, final QuoteMode quoteMode, final String lineBreak) {
		this.separator = separator;
		this.stringQuote = stringQuote;
		this.stringQuoteEscapeCharacter = stringQuoteEscapeCharacter;
		this.lineBreakInDataAllowed = lineBreakInDataAllowed;
		this.escapedStringQuoteInDataAllowed = escapedStringQuoteInDataAllowed;
		this.fillMissingTrailingColumnsWithNull = fillMissingTrailingColumnsWithNull;
		this.removeSurplusEmptyTrailingColumns = removeSurplusEmptyTrailingColumns;
		this.alwaysTrim = alwaysTrim;
		this.quoteMode = quoteMode;
		this.lineBreak = lineBreak;

		// Use setters to validate parameters
		setSeparator(separator);
		setStringQuote(stringQuote);
		setStringQuoteEscapeCharacter(stringQuoteEscapeCharacter);
		setLineBreakInDataAllowed(lineBreakInDataAllowed);
		setEscapedStringQuoteInDataAllowed(escapedStringQuoteInDataAllowed);
		setFillMissingTrailingColumnsWithNull(fillMissingTrailingColumnsWithNull);
		setAlwaysTrim(alwaysTrim);
		setQuoteMode(quoteMode);
		setLineBreak(lineBreak);
	}

	public char getSeparator() {
		return separator;
	}

	public CsvFormat setSeparator(final char separator) {
		if (separator == '\r' || separator == '\n') {
			throw new IllegalArgumentException("Separator '" + separator + "' is invalid");
		} else if (quoteMode != QuoteMode.NO_QUOTE && separator == stringQuote) {
			throw new IllegalArgumentException("Separator '" + separator + "' is invalid");
		} else {
			this.separator = separator;
			return this;
		}
	}

	public char getStringQuote() {
		return stringQuote;
	}

	/**
	 * Setter for stringQuote character.
	 * Also sets stringQuoteEscape character.
	 *
	 * @param stringQuote
	 */
	public CsvFormat setStringQuote(final Character stringQuote) {
		if (stringQuote != null) {
			if (stringQuote == '\r' || stringQuote == '\n' || separator == stringQuote) {
				throw new IllegalArgumentException("StringQuote '" + stringQuote + "' is invalid");
			} else {
				this.stringQuote = stringQuote;
				stringQuoteEscapeCharacter = stringQuote;
				quoteMode = QuoteMode.QUOTE_IF_NEEDED;
				return this;
			}
		} else {
			quoteMode = QuoteMode.NO_QUOTE;
			return this;
		}
	}

	public char getStringQuoteEscapeCharacter() {
		return stringQuoteEscapeCharacter;
	}

	public CsvFormat setStringQuoteEscapeCharacter(final char stringQuoteEscapeCharacter) {
		if (stringQuoteEscapeCharacter == separator || stringQuoteEscapeCharacter == '\r' || stringQuoteEscapeCharacter == '\n') {
			throw new IllegalArgumentException("Stringquote escape character '" + stringQuoteEscapeCharacter + "' is invalid");
		} else {
			this.stringQuoteEscapeCharacter = stringQuoteEscapeCharacter;
			return this;
		}
	}

	public boolean isLineBreakInDataAllowed() {
		return lineBreakInDataAllowed;
	}

	public CsvFormat setLineBreakInDataAllowed(final boolean lineBreakInDataAllowed) {
		this.lineBreakInDataAllowed = lineBreakInDataAllowed;
		return this;
	}

	public boolean isEscapedStringQuoteInDataAllowed() {
		return escapedStringQuoteInDataAllowed;
	}

	public CsvFormat setEscapedStringQuoteInDataAllowed(final boolean escapedStringQuoteInDataAllowed) {
		this.escapedStringQuoteInDataAllowed = escapedStringQuoteInDataAllowed;
		return this;
	}

	public boolean isFillMissingTrailingColumnsWithNull() {
		return fillMissingTrailingColumnsWithNull;
	}

	public CsvFormat setFillMissingTrailingColumnsWithNull(final boolean fillMissingTrailingColumnsWithNull) {
		this.fillMissingTrailingColumnsWithNull = fillMissingTrailingColumnsWithNull;
		return this;
	}

	public boolean isRemoveSurplusEmptyTrailingColumns() {
		return removeSurplusEmptyTrailingColumns;
	}

	public CsvFormat setRemoveSurplusEmptyTrailingColumns(final boolean removeSurplusEmptyTrailingColumns) {
		this.removeSurplusEmptyTrailingColumns = removeSurplusEmptyTrailingColumns;
		return this;
	}

	public boolean isAlwaysTrim() {
		return alwaysTrim;
	}

	public CsvFormat setAlwaysTrim(final boolean alwaysTrim) {
		this.alwaysTrim = alwaysTrim;
		return this;
	}

	public boolean isIgnoreEmptyLines() {
		return ignoreEmptyLines;
	}

	public CsvFormat setIgnoreEmptyLines(final boolean ignoreEmptyLines) {
		this.ignoreEmptyLines = ignoreEmptyLines;
		return this;
	}

	public QuoteMode getQuoteMode() {
		return quoteMode;
	}

	public CsvFormat setQuoteMode(final QuoteMode quoteMode) {
		if (quoteMode == null) {
			throw new IllegalArgumentException("Given quoteMode is invalid");
		} else if (quoteMode != QuoteMode.NO_QUOTE && separator == stringQuote) {
			throw new IllegalArgumentException("StringQuote '" + stringQuote + "' is invalid");
		} else {
			this.quoteMode = quoteMode;
			return this;
		}
	}

	public String getLineBreak() {
		return lineBreak;
	}

	public CsvFormat setLineBreak(final String lineBreak) {
		if (!"\r".equals(lineBreak) && !"\n".equals(lineBreak) && !"\r\n".equals(lineBreak)) {
			throw new IllegalArgumentException("Given linebreak is invalid");
		} else {
			this.lineBreak = lineBreak;
			return this;
		}
	}
}
