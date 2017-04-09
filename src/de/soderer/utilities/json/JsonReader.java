package de.soderer.utilities.json;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Stack;
import java.util.regex.Pattern;

import de.soderer.utilities.BasicReader;

public class JsonReader extends BasicReader {
	protected Object currentObject = null;
	
	protected Stack<JsonToken> openJsonItems = new Stack<JsonToken>();
	
	public enum JsonToken {
		JsonObject_Open,
		JsonObject_PropertyKey,
		JsonObject_Close,
		JsonArray_Open,
		JsonArray_Close,
		JsonSimpleValue,
	}

	public JsonReader(InputStream inputStream) throws Exception {
		super(inputStream, (String) null);
	}
	
	public JsonReader(InputStream inputStream, String encoding) throws Exception {
		super(inputStream, encoding);
	}
	
	public JsonReader(InputStream inputStream, Charset encodingCharset) throws Exception {
		super(inputStream, encodingCharset);
	}
	
	public Object getCurrentObject() {
		return currentObject;
	}
	
	public JsonToken readNextToken() throws Exception {
		currentObject = null;
		Character currentChar = readNextNonWhitespace();
		if (currentChar == null) {
			throw new Exception("Premature end of data");
		}
		switch (currentChar) {
			case '{': // Open JsonObject
				if (openJsonItems.size() > 0 && openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
				}
				openJsonItems.push(JsonToken.JsonObject_Open);
				return JsonToken.JsonObject_Open;
			case '}': // Close JsonObject
				if (openJsonItems.pop() != JsonToken.JsonObject_Open) {
					throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
				} else {
					return JsonToken.JsonObject_Close;
				}
			case '[': // Open JsonArray
				if (openJsonItems.size() > 0 && openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
				}
				openJsonItems.push(JsonToken.JsonArray_Open);
				return JsonToken.JsonArray_Open;
			case ']': // Close JsonArray
				if (openJsonItems.pop() != JsonToken.JsonArray_Open) {
					throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
				} else {
					return JsonToken.JsonArray_Close;
				}
			case ',': // Separator of JsonObject properties or JsonArray items
				currentChar = readNextNonWhitespace();
				if (currentChar == '}' || currentChar == ']') {
					throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
				} else {
					reuseCurrentChar();
					return readNextToken();
				}
			case '\'': // Not allowed single-quoted value
				throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
			case '"': // Start JsonObject propertykey or propertyvalue or JsonArray item
				if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readQuotedText('"', '\\');
					return JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_Open) {
					currentObject = readQuotedText('"', '\\');
					if (readNextNonWhitespace() != ':') {
						throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
					}
					openJsonItems.push(JsonToken.JsonObject_PropertyKey);
					return JsonToken.JsonObject_PropertyKey;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					currentObject = readQuotedText('"', '\\');
					openJsonItems.pop();
					currentChar = readNextNonWhitespace();
					if (currentChar == '}') {
						reuseCurrentChar();
					} else if (currentChar != ',') {
						throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
					}
					return JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
				}
			default: // Start JsonObject propertyvalue or JsonArray item
				if (openJsonItems.peek() == JsonToken.JsonArray_Open) {
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', ']').trim());
					return JsonToken.JsonSimpleValue;
				} else if (openJsonItems.peek() == JsonToken.JsonObject_PropertyKey) {
					openJsonItems.pop();
					currentObject = readSimpleJsonValue(readUpToNext(false, null, ',', '}').trim());
					currentChar = readNextNonWhitespace();
					if (currentChar == '}') {
						reuseCurrentChar();
					} else {
						currentChar = readNextNonWhitespace();
						if (currentChar == '}') {
							throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
						} else {
							reuseCurrentChar();
						}
					}
					return JsonToken.JsonSimpleValue;
				} else {
					throw new Exception("Invalid json data '" + currentChar + "' at index " + getReadCharacters());
				}
		}
	}
	
	public boolean readNextJsonItem() throws Exception {
		if (getReadCharacters() == 0) {
			throw new Exception("JsonReader position was not initialized for readNextJsonItem()");
		}
		
		JsonToken nextToken = readNextToken();
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
			throw new Exception("Invalid data at index " + getReadCharacters());
		} 
	}
	
	/**
	 * Read all available Json data from the input stream at once.
	 * This can only be done once and as the first action on a JsonReader.
	 * 
	 * @return JsonObject or JsonArray
	 * @throws Exception
	 */
	public JsonItem read() throws Exception {
		if (getReadCharacters() != 0) {
			throw new Exception("JsonReader position was already initialized for other read operation");
		}
		
		JsonToken nextToken = readNextToken();
		if (nextToken == JsonToken.JsonObject_Open) {
			return readJsonObject();
		} else if (nextToken == JsonToken.JsonArray_Open) {
			return readJsonArray();
		} else {
			throw new Exception("Invalid json data: No JsonObject or JsonArray at root");
		}
	}
	
	private JsonObject readJsonObject() throws Exception {
		if (openJsonItems.peek() != JsonToken.JsonObject_Open) {
			throw new Exception("Invalid read position for JsonArray at index " + getReadCharacters());
		} else {
			JsonObject returnObject = new JsonObject();
			JsonToken nextToken = readNextToken();
			while (nextToken != JsonToken.JsonObject_Close) {
				if (nextToken == JsonToken.JsonObject_PropertyKey && currentObject instanceof String) {
					String propertyKey = (String) currentObject;
					nextToken = readNextToken();
					if (nextToken == JsonToken.JsonArray_Open) {
						returnObject.add(propertyKey, readJsonArray());
					} else if (nextToken == JsonToken.JsonObject_Open) {
						returnObject.add(propertyKey, readJsonObject());
					} else if (nextToken == JsonToken.JsonSimpleValue) {
						returnObject.add(propertyKey, currentObject);
					} else {
						throw new Exception("Unexpected JsonToken " + nextToken + " at index " + getReadCharacters());
					}
					nextToken = readNextToken();
				} else {
					throw new Exception("Unexpected JsonToken " + nextToken + " at index " + getReadCharacters());
				}
			}
			return returnObject;
		}
	}
	
	private JsonArray readJsonArray() throws Exception {
		if (openJsonItems.peek() != JsonToken.JsonArray_Open) {
			throw new Exception("Invalid read position for JsonArray at index " + getReadCharacters());
		} else {
			JsonToken nextToken = readNextToken();
			if (nextToken == JsonToken.JsonArray_Close
					|| nextToken == JsonToken.JsonObject_Open
					|| nextToken == JsonToken.JsonArray_Open
					|| nextToken == JsonToken.JsonSimpleValue) {
				JsonArray returnArray = new JsonArray();
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
				throw new Exception("Unexpected JsonToken " + nextToken + " at index " + getReadCharacters());
			}
		}
	}

	private Object readSimpleJsonValue(String valueString) throws Exception {
		if (valueString.equalsIgnoreCase("null")) {
			return null;
		} else if (valueString.equalsIgnoreCase("true")) {
			return true;
		} else if (valueString.equalsIgnoreCase("false")) {
			return false;
		} else if (Pattern.matches("[+|-]?[0-9]*(\\.[0-9]*)?([e|E][+|-]?[0-9]*)?", valueString)) {
			if (valueString.contains(".")) {
				return new Double(valueString);
			} else {
				Long value = new Long(valueString);
				if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE ) {
					return new Integer(valueString);
				} else {
					return value;
				}
			}
		} else {
			throw new Exception("Invalid json data at index " + getReadCharacters());
		}
	}
	
	/**
	 * This method should only be used to read small Json items
	 * 
	 * @param data
	 * @return
	 * @throws IOException 
	 * @throws UnsupportedEncodingException 
	 */
	public static JsonItem readJsonItemString(String data) throws Exception {
		try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes("UTF-8"))) {
			try (JsonReader jsonReader = new JsonReader(inputStream)) {
				return jsonReader.read();
			}
		}
	}
}
