package de.soderer.utilities.json;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.Utilities;

/**
 * Differences to standard JSON:<br />
 * Objects:<br />
 * 	Unquoted Object keys<br />
 * 	Single-quoted Object keys<br />
 *  Objects properties list may have trailing comma<br />
 * Arrays:<br />
 * 	Arrays item list may have trailing comma<br />
 * Strings:<br />
 * 	Single-quoted Strings<br />
 * 	Strings can be split across multiple lines (escape newlines with backslashes)<br />
 * Numbers:<br />
 * 	Hexadecimal numbers as unquoted values<br />
 * 	Numbers may begin or end with a (leading or trailing) decimal point<br />
 * 	Numbers include Infinity, -Infinity, NaN, and -NaN<br />
 * 	Numbers may begin with an explicit plus sign<br />
 * Comments:<br />
 * 	Inline comment (single-line up to the line end)<br />
 * 	Block comment (multi-line)<br />
 */
public class Json5Reader extends JsonReader {
	public Json5Reader(final InputStream inputStream) throws Exception {
		this(inputStream, null);
	}

	public Json5Reader(final InputStream inputStream, final Charset encodingCharset) throws Exception {
		super(inputStream, encodingCharset);
	}

	@Override
	protected JsonToken readNextTokenInternal(final boolean updateJsonPath) throws Exception {
		currentObject = null;
		Character currentChar = readNextNonWhitespace();
		if (currentChar == null) {
			if (openJsonItems.size() > 0) {
				throw new Exception("Premature end of data");
			} else {
				return null;
			}
		}

		JsonToken jsonToken;
		switch (currentChar) {
			case '{': // Open JsonObject
				if (openJsonItems.size() > 0 && openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
				}
				openJsonItems.push(JsonToken.JsonObject_Open);
				jsonToken = JsonToken.JsonObject_Open;
				break;
			case '}': // Close JsonObject
				if (openJsonItems.pop() != JsonToken.JsonObject_Open) {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				} else {
					jsonToken = JsonToken.JsonObject_Close;
				}
				break;
			case '[': // Open JsonArray
				if (openJsonItems.size() > 0 && openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
				}
				openJsonItems.push(JsonToken.JsonArray_Open);
				jsonToken = JsonToken.JsonArray_Open;
				break;
			case ']': // Close JsonArray
				if (openJsonItems.pop() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				} else {
					jsonToken = JsonToken.JsonArray_Close;
				}
				break;
			case ',': // Separator of JsonObject properties or JsonArray items
				jsonToken = readNextTokenInternal(false);
				break;
			case '/': // Start comment
				currentChar = readNextCharacter();
				if (currentChar == null) {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				} else if (currentChar == '/') {
					// Line comment
					readUpToNext(false, null, '\n');
				} else if (currentChar == '*') {
					// Block comment
					while ((currentChar = readNextCharacter()) != '/') {
						readUpToNext(true, null, '*');
					}
				} else {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				}
				jsonToken = readNextTokenInternal(false);
				break;
			case '"':
			case '\'': // Start JsonObject propertykey or propertyvalue or JsonArray item
				if (openJsonItems.size() == 0) {
					currentObject = readQuotedText(currentChar, '\\');
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readQuotedText(currentChar, '\\');
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					currentObject = readQuotedText(currentChar, '\\');
					currentChar = readNextNonWhitespace();
					if (currentChar != ':') {
						throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
					}
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					jsonToken = JsonToken.JsonObject_PropertyKey;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					currentObject = readQuotedText(currentChar, '\\');
					openJsonItems.pop();
					currentChar = readNextNonWhitespace();
					if (currentChar == null) {
						throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
					} else if (currentChar == '}') {
						reuseCurrentChar();
					} else if (currentChar != ',') {
						throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
					}
					jsonToken = JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				}
				break;
			default: // Start JsonObject propertykey or propertyvalue or JsonArray item
				if (openJsonItems.size() == 0) {
					currentObject = readSimpleJsonValue(readUpToNext(false, null).trim());
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					currentObject = readJsonIdentifier(readUpToNext(false, null, ':').trim());
					readNextCharacter();
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					jsonToken = JsonToken.JsonObject_PropertyKey;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', '}').trim());
					final Character nextCharAfterSimpleValue = readNextNonWhitespace();
					if (nextCharAfterSimpleValue == null) {
						throw new Exception("Invalid json data end in line " + (getReadLines() + 1) + " at overall index " + getReadCharacters());
					} else if (nextCharAfterSimpleValue == '}') {
						reuseCurrentChar();
					}
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', ']').trim());
					final char nextCharAfterSimpleValue = readNextNonWhitespace();
					if (nextCharAfterSimpleValue == ']') {
						reuseCurrentChar();
					}
					jsonToken = JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				}
				break;
		}

		if (updateJsonPath) {
			updateJsonPath(jsonToken);
		}

		return jsonToken;
	}

	private Object readSimpleJsonValue(final String valueString) throws Exception {
		if ("null".equalsIgnoreCase(valueString)) {
			return null;
		} else if ("true".equalsIgnoreCase(valueString)) {
			return true;
		} else if ("false".equalsIgnoreCase(valueString)) {
			return false;
		} else if ("Infinity".equalsIgnoreCase(valueString)) {
			return Double.POSITIVE_INFINITY;
		} else if ("-Infinity".equalsIgnoreCase(valueString)) {
			return Double.NEGATIVE_INFINITY;
		} else if ("NaN".equalsIgnoreCase(valueString)) {
			return Double.NaN;
		} else if ("-NaN".equalsIgnoreCase(valueString)) {
			return Double.NaN;
		} else if (NumberUtilities.isHexNumber(valueString)) {
			return NumberUtilities.parseHexNumber(valueString);
		} else if (NumberUtilities.isNumber(valueString)) {
			return NumberUtilities.parseNumber(valueString);
		} else {
			throw new Exception("Invalid json data '" + Utilities.shortenStringToMaxLengthCutLeft(valueString, 20) + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
		}
	}

	/**
	 * A valid identifier must
	 * 	start with a letter (A-Za-z), underscore (_) or dollar sign ($)
	 * 	subsequent characters can also be digits (0-9)
	 *
	 * @param identifierString
	 * @return
	 * @throws Exception
	 */
	private String readJsonIdentifier(final String identifierString) throws Exception {
		if (Pattern.matches("[A-Za-z_$]+[A-Za-z_$0-9]*", identifierString)) {
			return identifierString;
		} else {
			throw new Exception("Invalid json identifier '" + Utilities.shortenStringToMaxLengthCutLeft(identifierString, 20) + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
		}
	}

	/**
	 * This method should only be used to read small Json items
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static JsonNode readJsonItemString(final String data) throws Exception {
		try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))) {
			try (Json5Reader jsonReader = new Json5Reader(inputStream)) {
				return jsonReader.read();
			}
		}
	}
}
