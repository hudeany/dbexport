package de.soderer.utilities;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class MapStringReader extends BasicReader {
	public MapStringReader(InputStream inputStream) throws Exception {
		super(inputStream, (String) null);
	}
	
	public MapStringReader(InputStream inputStream, String encoding) throws Exception {
		super(inputStream, encoding);
	}
	
	public MapStringReader(InputStream inputStream, Charset encodingCharset) throws Exception {
		super(inputStream, encodingCharset);
	}
	
	public Map<String, String> readMap() throws Exception {
		Map<String, String> returnMap = new HashMap<String, String>();
		
		boolean inValue = false;
		
		StringBuilder nextKey = new StringBuilder();
		StringBuilder nextValue = new StringBuilder();

		Character currentChar = readNextNonWhitespace();
		while (currentChar != null) {
			switch (currentChar) {
				case ' ':
					currentChar = readNextNonWhitespace();
					reuseCurrentChar();
					if ('=' == currentChar) {
						break;
					}
				case ',':
				case ';':
				case '\n':
				case '\r':
				case '\t':
					if (nextKey.length() > 0 || nextValue.length() > 0) {
						returnMap.put(nextKey.toString(), nextValue.toString());
						inValue = false;
						nextKey = new StringBuilder();
						nextValue = new StringBuilder();
					}
					currentChar = readNextNonWhitespace();
					break;
				case '\'':
				case '"':
					// Start quoted value
					String quotedText = readQuotedText(currentChar, '\\');
					if (inValue) {
						nextValue.append(quotedText);
					} else {
						nextKey.append(quotedText);
					}
					// Check for two-times-quote-char as escape char
					if (currentChar == readNextCharacter()) {
						if (inValue) {
							nextValue.append(currentChar);
						} else {
							nextKey.append(currentChar);
						}
					}
					reuseCurrentChar();
					currentChar = readNextCharacter();
					break;
				case '=':
					// Key value separator
					inValue = !inValue;
					currentChar = readNextNonWhitespace();
					break;
				default:
					// Item content, maybe quoted
					if (inValue) {
						nextValue.append(currentChar);
					} else {
						nextKey.append(currentChar);
					}
					currentChar = readNextCharacter();
					break;
			}
		}
		
		if (inValue || nextKey.length() > 0) {
			returnMap.put(nextKey.toString(), nextValue.toString());
			inValue = false;
			nextKey = new StringBuilder();
			nextValue = new StringBuilder();
		}
		
		return returnMap;
	}
	
	public static Map<String, String> readMap(String mapString) throws Exception {
		try (MapStringReader mapStringReader = new MapStringReader(new ByteArrayInputStream(mapString.getBytes("UTF-8")), "UTF-8")) {
			return mapStringReader.readMap();
		}
	}
}
