package de.soderer.utilities;

import java.io.InputStream;
import java.nio.charset.Charset;

public class SqlScriptReader extends BasicReader {
	public SqlScriptReader(InputStream inputStream) throws Exception {
		super(inputStream, (String) null);
	}
	
	public SqlScriptReader(InputStream inputStream, String encoding) throws Exception {
		super(inputStream, encoding);
	}
	
	public SqlScriptReader(InputStream inputStream, Charset encodingCharset) throws Exception {
		super(inputStream, encodingCharset);
	}
	
	public String readNextStatement() throws Exception {
		StringBuilder nextStatement = new StringBuilder();
		boolean withinString = false;
		boolean withinSingleLineComment = false;
		boolean withinMultiLineComment = false;
		
		Character nextCharacter;
		while ((nextCharacter = readNextCharacter()) != null) {
			if (withinString) {
				if (nextCharacter == '\'') {
					withinString = false;
				}
				nextStatement.append(nextCharacter);
			} else if (withinSingleLineComment) {
				if (nextCharacter == '\n' || nextCharacter == '\r') {
					withinSingleLineComment = false;
				}
			} else if (withinMultiLineComment) {
				if (nextCharacter == '*') {
					nextCharacter = readNextCharacter();
					if (nextCharacter == '/') {
						withinMultiLineComment = false;
					} else {
						reuseCurrentChar();
					}
				}
			} else if (nextCharacter == '\'') {
				withinString = true;
				nextStatement.append(nextCharacter);
			} else if (nextCharacter == '-') {
				nextCharacter = readNextCharacter();
				if (nextCharacter == '-') {
					withinSingleLineComment = true;
				} else {
					reuseCurrentChar();
					nextStatement.append('-');
				}
			} else if (nextCharacter == '/') {
				nextCharacter = readNextCharacter();
				if (nextCharacter == '*') {
					withinMultiLineComment = true;
				} else {
					reuseCurrentChar();
					nextStatement.append('/');
				}
			} else if (nextCharacter == ';') {
				break;
			} else {
				nextStatement.append(nextCharacter);
			}
		}
		
		if (withinString) {
			throw new Exception("Unclosed sql string");
		} else if (withinMultiLineComment) {
			throw new Exception("Unclosed multiline comment");
		} else if (Utilities.isNotBlank(nextStatement.toString())) {
			return nextStatement.toString().trim();
		} else if (nextCharacter == null) {
			return null;
		} else {
			// Skip empty statement
			return readNextStatement();
		}
	}
}
