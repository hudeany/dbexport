package de.soderer.utilities;

public class StringUtilities {
	public static boolean equals(String string1, String string2) {
		if (string1 == string2) {
			return true;
		} else if (string1 == null || string2 == null) {
			return false;
		} else if (string1.length() != string2.length()) {
			return false;
		} else {
			return string1.equals(string2);
		}
	}
	
	public static boolean equalsIgnoreCase(String string1, String string2) {
		if (string1 == string2) {
			return true;
		} else if (string1 == null || string2 == null) {
			return false;
		} else if (string1.length() != string2.length()) {
			return false;
		} else {
			return string1.toLowerCase().equals(string2.toLowerCase());
		}
	}
}
