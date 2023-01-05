package de.soderer.utilities.json;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Stack;

import de.soderer.utilities.BasicReader;
import de.soderer.utilities.NumberUtilities;
import de.soderer.utilities.Utilities;

public class JsonReader extends BasicReader {
	protected Object currentObject = null;

	protected Stack<JsonToken> openJsonItems = new Stack<>();
	protected Stack<String> currentJsonPath = new Stack<>();

	public enum JsonToken {
		JsonObject_Open,
		JsonObject_PropertyKey,
		JsonObject_Close,
		JsonArray_Open,
		JsonArray_Close,
		JsonSimpleValue,
	}

	public JsonReader(final InputStream inputStream) throws Exception {
		super(inputStream, null);
	}

	public JsonReader(final InputStream inputStream, final Charset encodingCharset) throws Exception {
		super(inputStream, encodingCharset);
	}

	public Object getCurrentObject() {
		return currentObject;
	}

	public JsonToken getCurrentToken() {
		if (openJsonItems.empty()) {
			return null;
		} else {
			return openJsonItems.peek();
		}
	}

	public JsonToken readNextToken() throws Exception {
		final JsonToken jsonToken = readNextTokenInternal();

		updateJsonPath(jsonToken);

		return jsonToken;
	}

	protected JsonToken readNextTokenInternal() throws Exception {
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
				currentChar = readNextNonWhitespace();
				if (currentChar == null || currentChar == '}' || currentChar == ']') {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				} else {
					reuseCurrentChar();
					jsonToken = readNextTokenInternal();
				}
				break;
			case '\'': // Not allowed single-quoted value
				throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
			case '"': // Start JsonObject propertykey or propertyvalue or JsonArray item
				if (openJsonItems.size() == 0) {
					currentObject = readQuotedText('"', '\\');
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readQuotedText('"', '\\');
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					currentObject = readQuotedText('"', '\\');
					if (readNextNonWhitespace() != ':') {
						throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
					}
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					jsonToken = JsonToken.JsonObject_PropertyKey;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					currentObject = readQuotedText('"', '\\');
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
			default: // Start JsonObject propertyvalue or JsonArray item
				if (openJsonItems.size() == 0) {
					currentObject = readSimpleJsonValue(readUpToNext(false, null).trim());
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', ']').trim());
					jsonToken = JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', '}').trim());
					currentChar = readNextNonWhitespace();
					if (currentChar == null) {
						throw new Exception("Invalid json data end in line " + (getReadLines() + 1) + " at overall index " + getReadCharacters());
					} else if (currentChar == '}') {
						reuseCurrentChar();
					} else {
						currentChar = readNextNonWhitespace();
						if (currentChar == null || currentChar == '}') {
							throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
						} else {
							reuseCurrentChar();
						}
					}
					jsonToken = JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data '" + currentChar + "' in line " + getReadLines() + " at overall index " + getReadCharacters());
				}
				break;
		}

		updateJsonPath(jsonToken);

		return jsonToken;
	}

	public boolean readNextJsonNode() throws Exception {
		if (getReadCharacters() == 0) {
			throw new Exception("JsonReader position was not initialized for readNextJsonItem()");
		}

		final JsonToken nextToken = readNextToken();
		if (nextToken == JsonToken.JsonObject_Open) {
			currentObject = readJsonObject();
			return true;
		} else if (nextToken == JsonToken.JsonArray_Open) {
			currentObject = readJsonArray();
			return true;
		} else if (nextToken == JsonToken.JsonSimpleValue) {
			// value was already read
			return true;
		} else if (nextToken == JsonToken.JsonObject_Close) {
			reuseCurrentChar();
			openJsonItems.push(JsonToken.JsonObject_Open);
			return false;
		} else if (nextToken == JsonToken.JsonArray_Close) {
			reuseCurrentChar();
			openJsonItems.push(JsonToken.JsonArray_Open);
			return false;
		} else {
			throw new Exception("Invalid data in line " + getReadLines() + " at overall index " + getReadCharacters());
		}
	}

	/**
	 * Read all available Json data from the input stream at once.
	 * This can only be done once and as the first action on a JsonReader.
	 *
	 * @return JsonObject or JsonArray
	 * @throws Exception
	 */
	public JsonNode read() throws Exception {
		if (getReadCharacters() != 0) {
			throw new Exception("JsonReader position was already initialized for other read operation");
		}

		final JsonToken nextToken = readNextToken();
		if (nextToken == JsonToken.JsonObject_Open) {
			return new JsonNode(readJsonObject());
		} else if (nextToken == JsonToken.JsonArray_Open) {
			return new JsonNode(readJsonArray());
		} else if (nextToken == JsonToken.JsonSimpleValue) {
			return new JsonNode(currentObject);
		} else {
			throw new Exception("Invalid json data: No JSON data found at root");
		}
	}

	private JsonObject readJsonObject() throws Exception {
		if (openJsonItems.peek() != JsonToken.JsonObject_Open) {
			throw new Exception("Invalid read position for JsonArray in line " + getReadLines() + " at overall index " + getReadCharacters());
		} else {
			final JsonObject returnObject = new JsonObject();
			JsonToken nextToken = readNextToken();
			while (nextToken != JsonToken.JsonObject_Close) {
				if (nextToken == JsonToken.JsonObject_PropertyKey && currentObject instanceof String) {
					final String propertyKey = (String) currentObject;
					nextToken = readNextToken();
					if (nextToken == JsonToken.JsonArray_Open) {
						returnObject.add(propertyKey, readJsonArray());
					} else if (nextToken == JsonToken.JsonObject_Open) {
						returnObject.add(propertyKey, readJsonObject());
					} else if (nextToken == JsonToken.JsonSimpleValue) {
						returnObject.add(propertyKey, currentObject);
					} else {
						throw new Exception("Unexpected JsonToken " + nextToken + " in line " + getReadLines() + " at overall index " + getReadCharacters());
					}
					nextToken = readNextToken();
				} else {
					throw new Exception("Unexpected JsonToken " + nextToken + " in line " + getReadLines() + " at overall index " + getReadCharacters());
				}
			}
			return returnObject;
		}
	}

	private JsonArray readJsonArray() throws Exception {
		if (openJsonItems.peek() != JsonToken.JsonArray_Open) {
			throw new Exception("Invalid read position for JsonArray in line " + getReadLines() + " at overall index " + getReadCharacters());
		} else {
			JsonToken nextToken = readNextToken();
			if (nextToken == JsonToken.JsonArray_Close
					|| nextToken == JsonToken.JsonObject_Open
					|| nextToken == JsonToken.JsonArray_Open
					|| nextToken == JsonToken.JsonSimpleValue) {
				final JsonArray returnArray = new JsonArray();
				while (nextToken != JsonToken.JsonArray_Close) {
					if (nextToken == JsonToken.JsonArray_Open) {
						returnArray.add(readJsonArray());
					} else if (nextToken == JsonToken.JsonObject_Open) {
						returnArray.add(readJsonObject());
					} else if (nextToken == JsonToken.JsonSimpleValue) {
						returnArray.add(currentObject);
					}
					nextToken = readNextToken();
				}
				return returnArray;
			} else {
				throw new Exception("Unexpected JsonToken " + nextToken + " in line " + getReadLines() + " at overall index " + getReadCharacters());
			}
		}
	}

	private Object readSimpleJsonValue(final String valueString) throws Exception {
		if (valueString == null) {
			throw new Exception("Invalid empty json data");
		} else if (valueString.startsWith("\"")) {
			return readQuotedText('"', '\\');
		} else if ("null".equalsIgnoreCase(valueString)) {
			return null;
		} else if ("true".equalsIgnoreCase(valueString)) {
			return true;
		} else if ("false".equalsIgnoreCase(valueString)) {
			return false;
		} else if (NumberUtilities.isNumber(valueString)) {
			return NumberUtilities.parseNumber(valueString);
		} else {
			throw new Exception("Invalid json data in line " + getReadLines() + " at overall index " + getReadCharacters());
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
			try (JsonReader jsonReader = new JsonReader(inputStream)) {
				return jsonReader.read();
			}
		}
	}

	protected void updateJsonPath(final JsonToken jsonToken) throws Exception {
		if (jsonToken != null) {
			switch(jsonToken) {
				case JsonArray_Open:
					if (currentJsonPath.size() > 0 && currentJsonPath.peek().startsWith("[")) {
						riseArrayIndex();
					}
					currentJsonPath.push("[]");
					break;
				case JsonArray_Close:
					if (currentJsonPath.size() > 0 && currentJsonPath.peek().startsWith("[")) {
						currentJsonPath.pop();
					}
					if (currentJsonPath.size() > 0 && currentJsonPath.peek().startsWith(".")) {
						currentJsonPath.pop();
					}
					break;
				case JsonObject_Open:
					if (currentJsonPath.size() > 0 && currentJsonPath.peek().startsWith("[")) {
						riseArrayIndex();
					}
					break;
				case JsonObject_PropertyKey:
					currentJsonPath.push("." + (String) getCurrentObject());
					break;
				case JsonSimpleValue:
					if (currentJsonPath.size() > 0) {
						if (currentJsonPath.peek().startsWith("[")) {
							riseArrayIndex();
						} else if (currentJsonPath.peek().startsWith(".")) {
							currentJsonPath.pop();
						}
					}
					break;
				case JsonObject_Close:
					if (currentJsonPath.size() > 0 && currentJsonPath.peek().startsWith(".")) {
						currentJsonPath.pop();
					}
					break;
				default:
					throw new Exception("Invalid jsonToken");
			}
		}
	}

	private void riseArrayIndex() {
		String currentArrayIndexString = currentJsonPath.pop();
		currentArrayIndexString = currentArrayIndexString.substring(1, currentArrayIndexString.length() - 1);
		int newArrayIndex = 0;
		if (currentArrayIndexString.length() > 0) {
			newArrayIndex = Integer.parseInt(currentArrayIndexString) + 1;
		}
		currentJsonPath.push("[" + newArrayIndex + "]");
	}

	/**
	 *
	 * JsonPath syntax:<br />
	 *	$ : root<br />
	 *	. : child separator<br />
	 *	[n] : array operator<br />
	 *<br />
	 * JsonPath example:<br />
	 * 	"$.list.customer[0].name"<br />
	 */
	public String getCurrentJsonPath() {
		return "$" + Utilities.join(currentJsonPath, "");
	}
}
