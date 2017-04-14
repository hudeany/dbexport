package de.soderer.utilities.json;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Stack;

import de.soderer.utilities.BasicReader;

public class JsonPath {
	private Stack<Object> pathParts = new Stack<Object>();

	/**
	 * Allowed syntax for JSON path:
	 * 
	 * dot-notation:
	 * 	$.store.customer[5].item[2]
	 * 
	 * bracket-notation:
	 * 	$['store']['customer'][5]['item'][2]
	 * 
	 * schema-reference-notation:
	 * 	#/store/customer/item
	 * 
	 * @param jsonPathString
	 * @throws Exception 
	 * @throws UnsupportedEncodingException 
	 */
	public JsonPath(String jsonPathString) throws Exception {
		try (JsonPathReader jsonPathReader = new JsonPathReader(jsonPathString)) {
		}
	}
	
	public JsonPath() {
	}
	
	public String getDotFormattedPath() {
		StringBuilder returnValue = new StringBuilder("$");
		for (Object pathPart : pathParts) {
			if (pathPart instanceof String) {
				returnValue.append(".").append(((String) pathPart).replace(".", "\\."));
			} else {
				returnValue.append("[").append(pathPart).append("]");
			}
		}
		return returnValue.toString();
	}
	
	public String getBracketFormattedPath() {
		StringBuilder returnValue = new StringBuilder("$");
		for (Object pathPart : pathParts) {
			if (pathPart instanceof String) {
				returnValue.append("['").append(((String) pathPart).replace("'", "\\'")).append("']");
			} else {
				returnValue.append("[").append(pathPart).append("]");
			}
		}
		return returnValue.toString();
	}
	
	public String getReferenceFormattedPath() throws Exception {
		StringBuilder returnValue = new StringBuilder("#");
		for (Object pathPart : pathParts) {
			if (pathPart instanceof String) {
				returnValue.append("/").append(((String) pathPart).replace("/", "\\/"));
			} else {
				throw new Exception("JSON path array index cannot be handled in reference format '" + pathPart + "'");
			}
		}
		return returnValue.toString();
	}
	
	public JsonPath appendPropertyKey(String propertyKey) {
		pathParts.push(propertyKey);
		return this;
	}
	
	public JsonPath appendArrayIndex(int arrayIndex) {
		pathParts.push(arrayIndex);
		return this;
	}
	
	private class JsonPathReader extends BasicReader {
		public JsonPathReader(String jsonPathString) throws Exception {
			super(new ByteArrayInputStream(jsonPathString.getBytes("UTF-8")));
			
			pathParts = new Stack<Object>();
			
			Character nextChar = readNextNonWhitespace();
			if (nextChar == null) {
				// Empty json path
				return;
			} else if (nextChar == '#' || nextChar == '$') {
				// Skip root element
				nextChar = readNextNonWhitespace();
			}

			while (nextChar != null) {
				String nextJsonPathPart;
				switch (nextChar) {
					case '.':
						nextJsonPathPart = readUpToNext(false, '\\', '.', '[');
						pathParts.push(replaceEscapedCharacers(nextJsonPathPart.substring(1).trim()));
						reuseCurrentChar();
						break;
					case '/':
						nextJsonPathPart = readUpToNext(false, '\\', '/', '[');
						pathParts.push(replaceEscapedCharacers(nextJsonPathPart.substring(1).trim()));
						reuseCurrentChar();
						break;
					case '[':
						nextJsonPathPart = readUpToNext(true, '\\', ']');
						nextJsonPathPart = nextJsonPathPart.substring(1, nextJsonPathPart.length() - 1);
						if (nextJsonPathPart.startsWith("'") && nextJsonPathPart.endsWith("'")) {
							pathParts.push(replaceEscapedCharacers(nextJsonPathPart.substring(1, nextJsonPathPart.length() - 1)));
						} else {
							pathParts.push(Integer.parseInt(nextJsonPathPart));
						}
						break;
					default:
						throw new Exception("Invalid JSON path data at '" + nextChar + "'");
				}
				
				nextChar = readNextNonWhitespace();
			}
		}
	}
	
	private String replaceEscapedCharacers(String value) {
		return value.replace("~0", "~").replace("~1", "/").replace("%25", "%");
	}
	
	public Stack<Object> getPathParts() {
		return pathParts;
	}
}