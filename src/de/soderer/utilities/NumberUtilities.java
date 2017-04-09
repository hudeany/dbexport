package de.soderer.utilities;

import java.text.DecimalFormat;

public class NumberUtilities {
	public static DecimalFormat NUMBER_WITH_POINTS = new DecimalFormat("###,##0");
	public static DecimalFormat NUMBER_WITH_MIN_2_DIGITS = new DecimalFormat("00");
	public static DecimalFormat NUMBER_WITH_MIN_4_DIGITS = new DecimalFormat("0000");
	public static DecimalFormat NUMBER_WITH_MIN_6_DIGITS = new DecimalFormat("000000");
	public static DecimalFormat NUMBER_WITH_MIN_7_DIGITS = new DecimalFormat("0000000");

	public static boolean isDigit(String digitString) {
		for (char character : digitString.toCharArray()) {
			if (!Character.isDigit(character)) {
				return false;
			}
		}
		return true;
	}
}
