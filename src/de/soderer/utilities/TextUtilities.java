package de.soderer.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Text Utilities
 *
 * This class does no Logging via Log4J, because it is often used before its initialisation
 */
public class TextUtilities {
	/**
	 * A simple string for testing, which includes all ASCII characters
	 */
	public static final String ASCII_CHARACTERS_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

	/**
	 * A simple string for testing, which includes all german characters
	 */
	public static final String GERMAN_TEST_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 äöüßÄÖÜµ!?§@€$%&/\\<>(){}[]'\"´`^°¹²³*#.,;:=+-~_|½¼¬";

	/**
	 * A simple string for testing, which includes all special characters
	 */
	public static final String SPECIAL_TEST_STRING = "\n\r\t\b\f\u00c4\u00e4\u00d6\u00f6\u00dc\u00fc\u00df";

	/**
	 * Enum to represent a linebreak type of an text
	 */
	public enum LineBreak {
		/**
		 * No linebreak
		 */
		Unknown(null),

		/**
		 * Multiple linebreak types
		 */
		Mixed(null),

		/**
		 * Unix/Linux linebreak ("\n")
		 */
		Unix("\n"),

		/**
		 * Mac/Apple linebreak ("\r")
		 */
		Mac("\r"),

		/**
		 * Windows linebreak ("\r\n")
		 */
		Windows("\r\n");

		private final String representationString;

		@Override
		public String toString() {
			return representationString;
		}

		LineBreak(final String representationString) {
			this.representationString = representationString;
		}

		public static LineBreak getLineBreakTypeByName(final String lineBreakTypeName) {
			if ("WINDOWS".equalsIgnoreCase(lineBreakTypeName)) {
				return LineBreak.Windows;
			} else if ("UNIX".equalsIgnoreCase(lineBreakTypeName)) {
				return LineBreak.Unix;
			} else if ("MAC".equalsIgnoreCase(lineBreakTypeName)) {
				return LineBreak.Mac;
			} else {
				return LineBreak.Unknown;
			}
		}
	}

	/**
	 * Trim a string to an exact length with alignment right for display purposes If the string underruns the length, it will be filled up with blanks on the left side. If the string exceeds the
	 * length, it will be cut from the left side.
	 *
	 * @param inputString
	 * @param length
	 * @return
	 */
	public static String trimStringToLengthAlignedRight(final String inputString, final int length) {
		if (inputString.length() > length) {
			// Takes the last characters of length
			return inputString.subSequence(inputString.length() - length, inputString.length()).toString();
		} else {
			return String.format("%" + length + "s", inputString);
		}
	}

	/**
	 * Trim a string to a exact length with alignment left for display purposes. If the string underruns the length, it will be filled up with blanks on the right side. If the string exceeds the
	 * length, it will be cut from the right side.
	 *
	 * @param inputString
	 * @param length
	 * @return
	 */
	public static String trimStringToLengthAlignedLeft(final String inputString, final int length) {
		if (inputString.length() > length) {
			// Takes the first characters of length
			return inputString.subSequence(0, length).toString();
		} else {
			return String.format("%-" + length + "s", inputString);
		}
	}

	/**
	 * Build a string with repetitions. 0 repetitions returns an empty string.
	 *
	 * @param itemString
	 * @param repeatTimes
	 * @return
	 */
	public static String repeatString(final String itemString, final int repeatTimes) {
		return repeatString(itemString, repeatTimes, null);
	}

	/***
	 * Build a string with repetitions. 0 repetitions returns an empty string. In other cases there will be put a glue string between the reptitions, which can be left empty.
	 *
	 * @param itemString
	 *            string to be repeated
	 * @param separatorString
	 *            glue string
	 * @param repeatTimes
	 *            Number of repetitions
	 * @return
	 */
	public static String repeatString(final String itemString, final int repeatTimes, final String separatorString) {
		final StringBuilder returnStringBuilder = new StringBuilder();
		for (int i = 0; i < repeatTimes; i++) {
			if (separatorString != null && returnStringBuilder.length() > 0) {
				returnStringBuilder.append(separatorString);
			}
			returnStringBuilder.append(itemString);
		}
		return returnStringBuilder.toString();
	}

	/**
	 * Split a string into smaller strings of a maximum size
	 *
	 * @param text
	 * @param chunkSize
	 * @return
	 */
	public static List<String> chopToChunks(final String text, final int chunkSize) {
		final List<String> returnList = new ArrayList<>((text.length() + chunkSize - 1) / chunkSize);

		for (int start = 0; start < text.length(); start += chunkSize) {
			returnList.add(text.substring(start, Math.min(text.length(), start + chunkSize)));
		}

		return returnList;
	}

	/**
	 * Reduce a text to a maximum number of lines. The type of linebreaks in the result string will not be changed. If the text has less lines then maximum it will be returned unchanged.
	 *
	 * @param value
	 * @param maxLines
	 * @return
	 */
	public static String trimToMaxNumberOfLines(final String value, final int maxLines) {
		final String normalizedValue = normalizeLineBreaks(value, LineBreak.Unix);
		int count = 0;
		int nextLinebreak = 0;
		while (nextLinebreak != -1 && count < maxLines) {
			nextLinebreak = normalizedValue.indexOf(LineBreak.Unix.toString(), nextLinebreak + 1);
			count++;
		}

		if (nextLinebreak != -1) {
			final LineBreak originalLineBreakType = detectLinebreakType(value);
			return normalizeLineBreaks(normalizedValue.substring(0, nextLinebreak + 1), originalLineBreakType) + "...";
		} else {
			return value;
		}
	}

	/**
	 * Detect the type of linebreaks in a text.
	 *
	 * @param value
	 * @return
	 */
	public static LineBreak detectLinebreakType(final String value) {
		final TextPropertiesReader textPropertiesReader = new TextPropertiesReader(value);
		textPropertiesReader.readProperties();
		return textPropertiesReader.getLinebreakType();
	}

	/**
	 * Change all linebreaks of a text, no matter what type they are, to a given type
	 *
	 * @param value
	 * @param type
	 * @return
	 */
	public static String normalizeLineBreaks(final String value, final LineBreak type) {
		if (value == null) {
			return value;
		} else {
			final String returnString = value.replace(LineBreak.Windows.toString(), LineBreak.Unix.toString()).replace(LineBreak.Mac.toString(), LineBreak.Unix.toString());
			if (type == LineBreak.Mac) {
				return returnString.replace(LineBreak.Unix.toString(), LineBreak.Mac.toString());
			} else if (type == LineBreak.Windows) {
				return returnString.replace(LineBreak.Unix.toString(), LineBreak.Windows.toString());
			} else {
				return returnString;
			}
		}
	}

	public static String normalizeLineBreaksForCurrentSystem(final String value) {
		if (SystemUtilities.isWindowsSystem()) {
			return normalizeLineBreaks(value, LineBreak.Windows);
		} else {
			return normalizeLineBreaks(value, LineBreak.Unix);
		}
	}

	/**
	 * Get the text lines of a text as a list of strings
	 *
	 * @param dataString
	 * @return
	 * @throws IOException
	 */
	public static List<String> getLines(final String dataString) throws IOException {
		try (LineNumberReader lineNumberReader = new LineNumberReader(new StringReader(dataString))) {
			final List<String> list = new ArrayList<>();

			String line;
			while ((line = lineNumberReader.readLine()) != null) {
				list.add(line);
			}

			return list;
		}
	}

	/**
	 * Get the number of lines in a text
	 *
	 * @param dataString
	 * @return
	 * @throws IOException
	 */
	public static int getLineCount(final String dataString) throws IOException {
		if (dataString == null) {
			return 0;
		} else if ("".equals(dataString)) {
			return 1;
		} else {
			try (LineNumberReader lineNumberReader = new LineNumberReader(new StringReader(dataString))) {
				while (lineNumberReader.readLine() != null) {
					// do nothing
				}

				return lineNumberReader.getLineNumber();
			}
		}
	}

	/**
	 * Count the number of words by whitespaces in a text separated
	 *
	 * @param dataString
	 * @return
	 */
	public static int countWords(final String dataString) {
		int counter = 0;
		boolean isWord = false;
		final int endOfLine = dataString.length() - 1;

		for (int i = 0; i < dataString.length(); i++) {
			if (!Character.isWhitespace(dataString.charAt(i)) && i != endOfLine) {
				isWord = true;
			} else if (Character.isWhitespace(dataString.charAt(i)) && isWord) {
				counter++;
				isWord = false;
			} else if (!Character.isWhitespace(dataString.charAt(i)) && i == endOfLine) {
				counter++;
			}
		}
		return counter;
	}

	/**
	 * Get the start index of a line within a text
	 *
	 * @param dataString
	 * @param lineNumber
	 * @return
	 */
	public static int getStartIndexOfLine(final String dataString, final int lineNumber) {
		if (dataString == null || lineNumber < 0) {
			return -1;
		} else if (lineNumber == 1) {
			return 0;
		} else {
			int lineCount = 1;
			int position = 0;
			while (lineCount < lineNumber) {
				final int nextLineBreakMac = dataString.indexOf(LineBreak.Mac.toString(), position);
				final int nextLineBreakUnix = dataString.indexOf(LineBreak.Unix.toString(), position);
				final int nextLineBreakWindows = dataString.indexOf(LineBreak.Windows.toString(), position);
				int nextPosition = -1;
				int lineBreakSize = 0;
				if (nextLineBreakMac >= 0 && (nextLineBreakUnix < 0 || nextLineBreakMac < nextLineBreakUnix) && (nextLineBreakWindows < 0 || nextLineBreakMac < nextLineBreakWindows)) {
					nextPosition = nextLineBreakMac;
					lineBreakSize = 1;
				} else if (nextLineBreakUnix >= 0 && (nextLineBreakWindows < 0 || nextLineBreakUnix < nextLineBreakWindows)) {
					nextPosition = nextLineBreakUnix;
					lineBreakSize = 1;
				} else if (nextLineBreakWindows >= 0) {
					nextPosition = nextLineBreakWindows;
					lineBreakSize = 2;
				}

				if (nextPosition >= 0) {
					position = nextPosition + lineBreakSize;
					if (position >= dataString.length()) {
						break;
					}
				} else {
					break;
				}
				lineCount++;
			}
			return position;
		}
	}

	/**
	 * Get the startindex of the line at a given position within the text
	 *
	 * @param dataString
	 * @param index
	 * @return
	 */
	public static int getStartIndexOfLineAtIndex(final String dataString, final int index) {
		if (dataString == null || index < 0) {
			return -1;
		} else if (index == 0) {
			return 0;
		} else {
			final int nextLineBreakMac = dataString.lastIndexOf(LineBreak.Mac.toString(), index);
			final int nextLineBreakUnix = dataString.lastIndexOf(LineBreak.Unix.toString(), index);
			final int nextLineBreakWindows = dataString.lastIndexOf(LineBreak.Windows.toString(), index);

			if (nextLineBreakMac >= 0 && (nextLineBreakUnix < 0 || nextLineBreakMac < nextLineBreakUnix) && (nextLineBreakWindows < 0 || nextLineBreakMac < nextLineBreakWindows)) {
				return nextLineBreakMac + LineBreak.Mac.toString().length();
			} else if (nextLineBreakUnix >= 0 && (nextLineBreakWindows < 0 || nextLineBreakUnix < nextLineBreakWindows)) {
				return nextLineBreakUnix + LineBreak.Unix.toString().length();
			} else if (nextLineBreakWindows >= 0) {
				return nextLineBreakWindows + LineBreak.Windows.toString().length();
			} else {
				return 0;
			}
		}
	}

	/**
	 * Get the end index of a line within a text
	 *
	 * @param dataString
	 * @param index
	 * @return
	 */
	public static int getEndIndexOfLineAtIndex(final String dataString, final int index) {
		if (dataString == null || index < 0) {
			return -1;
		} else if (index == 0) {
			return 0;
		} else {
			final int nextLineBreakMac = dataString.indexOf(LineBreak.Mac.toString(), index);
			final int nextLineBreakUnix = dataString.indexOf(LineBreak.Unix.toString(), index);
			final int nextLineBreakWindows = dataString.indexOf(LineBreak.Windows.toString(), index);

			if (nextLineBreakMac >= 0 && (nextLineBreakUnix < 0 || nextLineBreakMac > nextLineBreakUnix) && (nextLineBreakWindows < 0 || nextLineBreakMac > nextLineBreakWindows)) {
				return nextLineBreakMac;
			} else if (nextLineBreakUnix >= 0 && (nextLineBreakWindows < 0 || nextLineBreakUnix - 1 > nextLineBreakWindows)) {
				return nextLineBreakUnix;
			} else if (nextLineBreakWindows >= 0) {
				return nextLineBreakWindows;
			} else {
				return dataString.length();
			}
		}
	}

	/**
	 * Searching the next position of a string or searchPattern beginning at a startIndex. Searched text can be a simple string or a regex pattern, switching this with the flag
	 * searchTextIsRegularExpression. Search can be done caseinsensitive or caseinsensitive by the flag searchCaseSensitive. Search can be done from end to start, backwards by the flag
	 * searchReversely.
	 *
	 *
	 * @param text
	 * @param startPosition
	 * @param searchTextOrPattern
	 * @param searchTextIsRegularExpression
	 * @param searchCaseSensitive
	 * @param searchReversely
	 * @return Tuple First = Startindex, Second = Length of found substring
	 */
	public static Tuple<Integer, Integer> searchNextPosition(final String text, final int startPosition, final String searchTextOrPattern, final boolean searchTextIsRegularExpression, final boolean searchCaseSensitive,
			final boolean searchReversely) {
		if (Utilities.isBlank(text) || startPosition < 0 || text.length() < startPosition || Utilities.isEmpty(searchTextOrPattern)) {
			return null;
		} else {
			final String fullText = text;
			int nextPosition = -1;
			int length = -1;

			Pattern searchPattern;
			if (searchTextIsRegularExpression) {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
				}
			} else {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.LITERAL | Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.MULTILINE);
				}
			}

			final Matcher matcher = searchPattern.matcher(fullText);

			if (searchReversely) {
				while (matcher.find()) {
					if (matcher.start() < startPosition) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					} else {
						break;
					}
				}
				if (nextPosition < 0) {
					if (matcher.start() > 0) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					}
					while (matcher.find()) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					}
				}
			} else {
				if (matcher.find(startPosition)) {
					nextPosition = matcher.start();
					length = matcher.end() - nextPosition;
				}
				if (startPosition > 0 && nextPosition < 0) {
					if (matcher.find(0)) {
						nextPosition = matcher.start();
						length = matcher.end() - nextPosition;
					}
				}
			}

			if (nextPosition < 0) {
				return null;
			} else {
				return new Tuple<>(nextPosition, length);
			}
		}
	}

	/**
	 * Count the occurences of a string or pattern within a text
	 *
	 * @param text
	 * @param searchTextOrPattern
	 * @param searchTextIsRegularExpression
	 * @param searchCaseSensitive
	 * @return
	 */
	public static int countOccurences(final String text, final String searchTextOrPattern, final boolean searchTextIsRegularExpression, final boolean searchCaseSensitive) {
		if (text != null && searchTextOrPattern != null) {
			final String fullText = text;
			int occurences = 0;

			Pattern searchPattern;
			if (searchTextIsRegularExpression) {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
				}
			} else {
				if (searchCaseSensitive) {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.LITERAL | Pattern.MULTILINE);
				} else {
					searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.MULTILINE);
				}
			}

			final Matcher matcher = searchPattern.matcher(fullText);
			while (matcher.find()) {
				occurences++;
			}
			return occurences;
		} else {
			return -1;
		}
	}

	/**
	 * Get line number at a given text position
	 *
	 * @param dataString
	 * @param textPosition
	 * @return
	 */
	public static int getLineNumberOfTextposition(final String dataString, final int textPosition) {
		if (dataString == null) {
			return -1;
		} else {
			try {
				String textPart = dataString;
				if (dataString.length() > textPosition) {
					textPart = dataString.substring(0, textPosition);
				}
				int lineNumber = getLineCount(textPart);
				if (textPart.endsWith(LineBreak.Unix.toString()) || textPart.endsWith(LineBreak.Mac.toString())) {
					lineNumber++;
				}
				return lineNumber;
			} catch (final IOException e) {
				e.printStackTrace();
				return -1;
			} catch (final Exception e) {
				e.printStackTrace();
				return -1;
			}
		}
	}

	/**
	 * Get the number of the first line containing a string or pattern
	 *
	 * @param dataString
	 * @param searchTextOrPattern
	 * @param searchCaseSensitive
	 * @param searchTextIsRegularExpression
	 * @return
	 * @throws IOException
	 */
	public static int getNumberOfLineContainingText(final String dataString, final String searchTextOrPattern, final boolean searchCaseSensitive, final boolean searchTextIsRegularExpression) throws IOException {
		Pattern searchPattern;
		if (searchTextIsRegularExpression) {
			if (searchCaseSensitive) {
				searchPattern = Pattern.compile(searchTextOrPattern, Pattern.MULTILINE);
			} else {
				searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
			}
		} else {
			if (searchCaseSensitive) {
				searchPattern = Pattern.compile(searchTextOrPattern, Pattern.LITERAL | Pattern.MULTILINE);
			} else {
				searchPattern = Pattern.compile(searchTextOrPattern, Pattern.CASE_INSENSITIVE | Pattern.LITERAL | Pattern.MULTILINE);
			}
		}

		try (LineNumberReader lineNumberReader = new LineNumberReader(new StringReader(dataString))) {
			String lineContent;
			while ((lineContent = lineNumberReader.readLine()) != null) {
				final Matcher matcher = searchPattern.matcher(lineContent);
				if (matcher.find()) {
					return lineNumberReader.getLineNumber();
				}
			}
			return -1;
		}
	}

	/**
	 * Insert some string before a given line into a text
	 *
	 * @param dataString
	 * @param insertText
	 * @param lineNumber
	 * @return
	 * @throws IOException
	 */
	public static String insertTextBeforeLine(final String dataString, final String insertText, final int lineNumber) throws IOException {
		if (lineNumber > -1) {
			final int startIndex = TextUtilities.getStartIndexOfLine(dataString, lineNumber);
			final StringBuilder dataBuilder = new StringBuilder(dataString);
			dataBuilder.insert(startIndex, insertText);
			return dataBuilder.toString();
		} else {
			return dataString;
		}
	}

	/**
	 * Insert some string at the end of a given line into a text
	 *
	 * @param dataString
	 * @param insertText
	 * @param lineNumber
	 * @return
	 * @throws IOException
	 */
	public static String insertTextAfterLine(final String dataString, final String insertText, final int lineNumber) throws IOException {
		if (lineNumber > -1) {
			final int startIndex = TextUtilities.getStartIndexOfLine(dataString, lineNumber + 1);
			final StringBuilder dataBuilder = new StringBuilder(dataString);
			dataBuilder.insert(startIndex, insertText);
			return dataBuilder.toString();
		} else {
			return dataString;
		}
	}

	/**
	 * Try to detect the encoding of a byte array xml data.
	 *
	 *
	 * @param data
	 * @return
	 */
	public static Tuple<String, Boolean> detectEncoding(final byte[] data) {
		if (data.length > 2 && data[0] == BOM.UTF_16_BE.getBytes()[0] && data[1] == BOM.UTF_16_BE.getBytes()[1]) {
			return new Tuple<>("UTF-16BE", true);
		} else if (data.length > 2 && data[0] == BOM.UTF_16_LE.getBytes()[0] && data[1] == BOM.UTF_16_LE.getBytes()[1]) {
			return new Tuple<>("UTF-16LE", true);
		} else if (data.length > 3 && data[0] == BOM.UTF_8.getBytes()[0] && data[1] == BOM.UTF_8.getBytes()[1] && data[2] == BOM.UTF_8.getBytes()[2]) {
			return new Tuple<>(StandardCharsets.UTF_8.name(), true);
		} else {
			// Detect Xml Encoding
			try {
				// Use first data part only to speed up
				final String interimString = new String(data, 0, Math.min(data.length, 100), StandardCharsets.UTF_8).toLowerCase();
				final String reducedInterimString = interimString.replace("\u0000", "");
				int encodingStart = reducedInterimString.indexOf("encoding");
				if (encodingStart >= 0) {
					encodingStart = reducedInterimString.indexOf("=", encodingStart);
					if (encodingStart >= 0) {
						encodingStart++;
						while (reducedInterimString.charAt(encodingStart) == ' ') {
							encodingStart++;
						}
						if (reducedInterimString.charAt(encodingStart) == '"' || reducedInterimString.charAt(encodingStart) == '\'') {
							encodingStart++;
						}
						final StringBuilder encodingString = new StringBuilder();
						while (Character.isLetter(reducedInterimString.charAt(encodingStart)) || Character.isDigit(reducedInterimString.charAt(encodingStart))
								|| reducedInterimString.charAt(encodingStart) == '-') {
							encodingString.append(reducedInterimString.charAt(encodingStart));
							encodingStart++;
						}
						Charset.forName(encodingString.toString());
						if (encodingString.toString().toLowerCase().startsWith("utf-16") && data[0] == 0) {
							return new Tuple<>("UTF-16BE", false);
						} else if (encodingString.toString().toLowerCase().startsWith("utf-16") && data[1] == 0) {
							return new Tuple<>("UTF-16LE", false);
						} else {
							return new Tuple<>(encodingString.toString().toUpperCase(), false);
						}
					}
				}
			} catch (final Exception e) {
				e.printStackTrace();
			}

			final String interimString = new String(data, StandardCharsets.UTF_8).toLowerCase();

			final int zeroIndex = interimString.indexOf('\u0000');
			if (zeroIndex >= 0 && zeroIndex <= 100) {
				if (zeroIndex % 2 == 0) {
					return new Tuple<>("UTF-16BE", false);
				} else {
					return new Tuple<>("UTF-16LE", false);
				}
			}

			if (interimString.contains("ä") || interimString.contains("ö") || interimString.contains("ü") || interimString.contains("Ä") || interimString.contains("Ö") || interimString.contains("Ü") || interimString.contains("ß")) {
				return new Tuple<>(StandardCharsets.UTF_8.name(), false);
			}

			return null;
		}
	}

	/**
	 * Replace the leading indentation by double blanks of a text by some other character like tab
	 *
	 * @param data
	 * @param blankCount
	 * @param replacement
	 * @return
	 */
	public static String replaceIndentingBlanks(final String data, final int blankCount, final String replacement) {
		String result = data;
		final Pattern pattern = Pattern.compile("^(  )+", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(result);
		while (matcher.find()) {
			final int replacementCount = (matcher.end() - matcher.start()) / blankCount;
			result = matcher.replaceFirst(Utilities.repeat(replacement, replacementCount));
			matcher = pattern.matcher(result);
		}
		return result;
	}

	/**
	 * Remove the first leading whitespace of a string
	 *
	 * @param data
	 * @return
	 */
	public static String removeFirstLeadingWhitespace(final String data) {
		final Pattern pattern = Pattern.compile("^[\\t ]", Pattern.MULTILINE);
		final Matcher matcher = pattern.matcher(data);
		return matcher.replaceAll("");
	}

	/**
	 * Indent the lines of a string by one tab
	 *
	 * @param data
	 * @return
	 */
	public static String addLeadingTab(final String data) {
		return addLeadingTabs(data, 1);
	}

	/**
	 * Indent the lines of a string by some tabs
	 *
	 * @param data
	 * @param numberOfTabs
	 * @return
	 */
	public static String addLeadingTabs(final String data, final int numberOfTabs) {
		final Pattern pattern = Pattern.compile("^", Pattern.MULTILINE);
		final Matcher matcher = pattern.matcher(data);
		return matcher.replaceAll(Utilities.repeat("\t", numberOfTabs));
	}

	/**
	 * Chop a string into pieces of maximum length
	 *
	 * @param value
	 * @param length
	 * @return
	 */
	public static List<String> truncate(final String value, final int length) {
		final List<String> parts = new ArrayList<>();
		final int valueLength = value.length();
		for (int i = 0; i < valueLength; i += length) {
			parts.add(value.substring(i, Math.min(valueLength, i + length)));
		}
		return parts;
	}

	/**
	 * Trim all strings in an array of strings to a maximum length and add the cut off rest as new lines
	 *
	 * @param lines
	 * @param length
	 * @return
	 */
	public static List<String> truncate(final String[] lines, final int length) {
		final List<String> linesTruncated = new ArrayList<>();
		for (final String line : lines) {
			linesTruncated.addAll(truncate(line, length));
		}
		return linesTruncated;
	}

	/**
	 * Trim all strings in a list of strings to a maximum length and add the cut off rest as new lines
	 *
	 * @param lines
	 * @param length
	 * @return
	 */
	public static List<String> truncate(final List<String> lines, final int length) {
		final List<String> linesTruncated = new ArrayList<>();
		for (final String line : lines) {
			linesTruncated.addAll(truncate(line, length));
		}
		return linesTruncated;
	}

	/**
	 * Remove all leading and trailing whispaces, not only blanks like it is done by .trim()
	 *
	 * @param data
	 * @return
	 */
	public static String removeLeadingAndTrailingWhitespaces(String data) {
		final Pattern pattern1 = Pattern.compile("^\\s*", Pattern.MULTILINE);
		final Matcher matcher1 = pattern1.matcher(data);
		data = matcher1.replaceAll("");

		final Pattern pattern2 = Pattern.compile("\\s*$", Pattern.MULTILINE);
		final Matcher matcher2 = pattern2.matcher(data);
		data = matcher2.replaceAll("");

		return data;
	}

	/**
	 * Remove all trailing whispaces
	 *
	 * @param data
	 * @return
	 */
	public static String removeTrailingWhitespaces(String data) {
		final Pattern pattern = Pattern.compile("\\s*$", Pattern.MULTILINE);
		final Matcher matcher = pattern.matcher(data);
		data = matcher.replaceAll("");

		return data;
	}

	/**
	 * Scan an inputstream for start indexes of text lines
	 *
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static List<Long> scanLineStartIndexes(final InputStream input) throws IOException {
		final List<Long> returnList = new ArrayList<>();
		long position = 0;
		int bufferPrevious = 0;
		int bufferNext;
		returnList.add(0L);
		while ((bufferNext = input.read()) != -1) {
			if ((char) bufferNext == '\n') {
				returnList.add(position + 1);
			} else if ((char) bufferPrevious == '\r') {
				returnList.add(position);
			}

			bufferPrevious = bufferNext;
			position++;
		}

		if ((char) bufferPrevious == '\r') {
			returnList.add(position);
		}

		return returnList;
	}

	/**
	 * Scan an inputstream for start indexes of text lines. Skip the first characters up to startIndex.
	 *
	 * @param input
	 * @param startIndex
	 * @return
	 * @throws IOException
	 */
	public static List<Long> scanLineStartIndexes(final InputStream input, final long startIndex) throws IOException {
		final List<Long> returnList = new ArrayList<>();
		long position = 0;
		int bufferPrevious = 0;
		int bufferNext;
		if (startIndex <= 0) {
			returnList.add(0L);
		} else {
			input.skip(startIndex);
			position = startIndex;
		}
		while ((bufferNext = input.read()) != -1) {
			if ((char) bufferNext == '\n') {
				returnList.add(position + 1);
			} else if ((char) bufferPrevious == '\r') {
				returnList.add(position);
			}

			bufferPrevious = bufferNext;
			position++;
		}

		if ((char) bufferPrevious == '\r') {
			returnList.add(position);
		}

		return returnList;
	}

	/**
	 * Scan an inputstream for positions of linebreaks.
	 *
	 * @param dataInputStream
	 * @return
	 * @throws IOException
	 */
	public static List<Long> scanLinebreakIndexes(final InputStream dataInputStream) throws IOException {
		final List<Long> returnList = new ArrayList<>();
		long position = 0;
		int bufferPrevious = 0;
		int bufferNext;
		while ((bufferNext = dataInputStream.read()) != -1) {
			if ((char) bufferNext == '\n' && (char) bufferPrevious != '\r') {
				returnList.add(position);
			} else if ((char) bufferNext == '\r') {
				returnList.add(position);
			}

			bufferPrevious = bufferNext;
			position++;
		}
		return returnList;
	}

	/**
	 * Break all lines after a maximum number of characters.
	 *
	 * @param dataString
	 * @param maximumLength
	 * @return
	 */
	public static String breakLinesAfterMaximumLength(final String dataString, final int maximumLength) {
		return Utilities.join(dataString.split("(?<=\\G.{" + maximumLength + "})"), "\n");
	}

	/**
	 * Scan a string for the characters used in it
	 *
	 * @param data
	 * @return
	 */
	public static List<Character> getUsedChars(final String data) {
		final List<Character> usedCharsList = new ArrayList<>();
		for (final char item : data.toCharArray()) {
			if (!usedCharsList.contains(item)) {
				usedCharsList.add(item);
			}
		}
		return usedCharsList;
	}

	/**
	 * Check if a string ends with a stringpart caseinsensitive
	 *
	 * @param str
	 * @param suffix
	 * @return
	 */
	public static boolean endsWithIgnoreCase(final String str, final String suffix) {
		if (str == null || suffix == null) {
			return (str == null && suffix == null);
		} else if (suffix.length() > str.length()) {
			return false;
		} else {
			final int strOffset = str.length() - suffix.length();
			return str.regionMatches(true, strOffset, suffix, 0, suffix.length());
		}
	}

	/**
	 * Check if a string starts with a stringpart caseinsensitive
	 *
	 * @param str
	 * @param prefix
	 * @return
	 */
	public static boolean startsWithIgnoreCase(final String str, final String prefix) {
		if (str == null || prefix == null) {
			return (str == null && prefix == null);
		} else if (prefix.length() > str.length()) {
			return false;
		} else {
			return str.regionMatches(true, 0, prefix, 0, prefix.length());
		}
	}

	/**
	 * Trim a string to a maximum number of characters and add a sign if it was cut off.
	 *
	 * @param inputString
	 * @param maxLength
	 * @param cutoffSign
	 * @return
	 */
	public static String trimStringToMaximumLength(final String inputString, final int maxLength, final String cutoffSign) {
		if (inputString == null || inputString.length() <= maxLength) {
			return inputString;
		} else {
			final int takeFirstLength = (maxLength - cutoffSign.length()) / 2;
			final int takeLastLength = maxLength - cutoffSign.length() - takeFirstLength;
			return inputString.substring(0, takeFirstLength) + cutoffSign + inputString.substring(inputString.length() - takeLastLength);
		}
	}

	/**
	 * Check if a string contains a stringpart caseinsensitive
	 *
	 * @param str
	 * @param substr
	 * @return
	 */
	public static boolean containsIgnoreCase(final String str, final String substr) {
		if (str == substr) {
			return true;
		} else if (str == null) {
			return false;
		} else if (substr == null) {
			return true;
		} else if (substr.length() > str.length()) {
			return false;
		} else {
			return str.regionMatches(true, 0, substr, 0, str.length());
		}
	}

	public static String mapToString(final Map<? extends Object, ? extends Object> map) {
		final StringBuilder returnValue = new StringBuilder();
		for (final Entry<? extends Object, ? extends Object> entry : map.entrySet()) {
			if (returnValue.length() > 0) {
				returnValue.append("\n");
			}
			Object key = entry.getKey();
			if (key == null) {
				key = "";
			}
			Object value = entry.getValue();
			if (value == null) {
				value = "";
			}

			returnValue.append(key.toString() + ": " + value.toString());
		}
		return returnValue.toString();
	}

	public static String mapToStringWithSortedKeys(final Map<? extends String, ? extends Object> map) {
		final StringBuilder returnValue = new StringBuilder();
		for (String key : Utilities.asSortedList(map.keySet())) {
			@SuppressWarnings("unlikely-arg-type")
			Object value = map.get(key);
			if (returnValue.length() > 0) {
				returnValue.append("\n");
			}
			if (key == null) {
				key = "";
			}
			if (value == null) {
				value = "";
			}

			returnValue.append(key.toString() + ": " + value.toString());
		}
		return returnValue.toString();
	}

	public static int getColumnNumberFromColumnChars(final String columnChars) {
		int value = 0;
		for (final char columnChar : columnChars.toLowerCase().toCharArray()) {
			final int columnIndex = (byte) columnChar - (byte) 'a' + 1;
			value = value * 26 + columnIndex;
		}
		return value;
	}

	public static boolean isValidBase64(final String value) {
		return Pattern.matches("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$", value);
	}

	public static String getLineStartingWith(final String text, final String starter) throws Exception {
		for (final String line : TextUtilities.getLines(text)) {
			if (line.startsWith(starter)) {
				return line;
			}
		}
		return null;
	}

	public static String getProsaParameter(final String text, final String parameterName) throws Exception {
		if (Utilities.isBlank(text)) {
			return null;
		} else {
			final Pattern parameterPattern = Pattern.compile("^\\s*" + parameterName + "\\s*:(.*)$", Pattern.MULTILINE);
			final Matcher parameterMatcher = parameterPattern.matcher(text);
			if (parameterMatcher.find()) {
				return parameterMatcher.group(1).trim();
			} else {
				return null;
			}
		}
	}

	public static String removeLinesContainingText(final String text, final String searchLinePart) {
		if (text == null) {
			return null;
		}

		final StringBuilder resultText = new StringBuilder();

		StringBuilder nextLine = new StringBuilder();
		boolean foundLinebreakStart = false;
		char lastChar = 'x';
		for (final char nextChar : text.toCharArray()) {
			if ((nextChar == '\r' && lastChar != '\r' && lastChar != '\n') || (nextChar == '\n' && (lastChar == '\r' || lastChar != '\n'))) {
				foundLinebreakStart = true;
			} else if (foundLinebreakStart) {
				if (!nextLine.toString().contains(searchLinePart)) {
					resultText.append(nextLine);
				}
				nextLine = new StringBuilder();
				foundLinebreakStart = false;
			}
			nextLine.append(nextChar);
			lastChar = nextChar;
		}
		if (nextLine.length() > 0) {
			if (!nextLine.toString().contains(searchLinePart)) {
				resultText.append(nextLine);
			}
		}

		return resultText.toString();
	}

	public static boolean containsNonAsciiCharacters(final char[] text) {
		for (final char nextChar : text) {
			if (!ASCII_CHARACTERS_STRING.contains(Character.toString(nextChar))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Fix the encoding of a String if it was stored in UTF-8 encoding but decoded with ISO-8859-1 encoding
	 *
	 * Examples of byte data of wrongly encoded Umlauts and other special characters:
	 *   Ä: [-61, -124]
	 *   ä: [-61, -92]
	 *   ß: [-61, -97]
	 *   è: [-61, -88]
	 */
	public static String fixEncodingErrorUTF8AsISO8859(final String text) {
		boolean wrongEncodingDetected = false;
		for (final byte nextByte : text.getBytes(StandardCharsets.ISO_8859_1)) {
			if (nextByte == -61) {
				wrongEncodingDetected = true;
				break;
			}
		}
		if (wrongEncodingDetected) {
			return new String(text.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
		} else {
			return text;
		}
	}

	public static String replaceLast(final String text, final String searchText, final String replacement) {
		return text.replaceFirst("(?s)" + Pattern.quote(searchText) + "(?!.*?" + Pattern.quote(searchText) + ")", replacement);
	}
}
