package de.soderer.utilities;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class BitUtilities {
	private static Pattern BITSTRINGPATTERN_BYTE = Pattern.compile("^[10]{1,8}$");
	private static Pattern BITSTRINGPATTERN_INTEGER = Pattern.compile("^[10]{1,32}$");

	/**
	 * Returns values from 0 to 255 (value range of byte)
	 *
	 * @param value
	 * @return
	 */
	public static int getUnsignedValue(byte value) {
		if (value < 0) {
			return (value) + 256;
		} else {
			return value;
		}
	}

	/**
	 * Returns values from 0 to 255 (value range of byte)
	 *
	 * @param value
	 * @return
	 */
	public static int[] getUnsignedValue(byte[] value) {
		int[] returnValue = new int[value.length];
		for (int i = 0; i < value.length; i++) {
			returnValue[i] = getUnsignedValue(value[i]);
		}
		return returnValue;
	}

	/**
	 * Returns the value as a Byte which contains only signed values in Java
	 *
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static byte getUnsignedAsByte(int value) throws Exception {
		if (value < 0 || value > 255) {
			throw new Exception("Unsigned value out of range for byte");
		} else if (value > 127) {
			return (byte) (value - 256);
		} else {
			return (byte) value;
		}
	}

	public static String getBitString(byte byteItem) {
		StringBuilder result = new StringBuilder();
		for (int i = 7; i >= 0; i--) {
			result.append((byte) ((byteItem >>> i) & 0x01));
		}
		return result.toString();
	}

	public static String getBitString(int intItem) {
		StringBuilder result = new StringBuilder();
		for (int i = 31; i >= 0; i--) {
			result.append((byte) ((intItem >>> i) & 0x01));
		}
		return result.toString();
	}

	public static String getBitString(byte[] byteArray) {
		return getBitString(byteArray, " ");
	}

	public static String getBitString(byte[] byteArray, String byteSeparator) {
		StringBuilder result = new StringBuilder();
		for (byte byteItem : byteArray) {
			if (result.length() > 0) {
				result.append(byteSeparator);
			}

			for (int i = 7; i >= 0; i--) {
				result.append((byte) ((byteItem >>> i) & 0x01));
			}
		}
		return result.toString();
	}

	public static byte getByteFromBitString(String bitString) throws Exception {
		if (Utilities.isEmpty(bitString)) {
			throw new Exception("Invalid bitstring for byte");
		}

		bitString = bitString.trim();

		if (!BITSTRINGPATTERN_BYTE.matcher(bitString).matches()) {
			throw new Exception("Invalid bitstring for byte");
		}

		byte result = 0;
		int posValue = 1;
		char[] characters = bitString.toCharArray();
		for (int i = characters.length - 1; i >= 0; i--) {
			if (characters[i] == '1') {
				result += posValue;
			}
			posValue *= 2;
		}
		return result;
	}

	public static int getIntegerFromBitString(String bitString) throws Exception {
		if (Utilities.isEmpty(bitString)) {
			throw new Exception("Invalid bitstring for integer");
		}

		bitString = bitString.trim();

		if (!BITSTRINGPATTERN_INTEGER.matcher(bitString).matches()) {
			throw new Exception("Invalid bitstring for integer");
		}

		int result = 0;
		int posValue = 1;
		char[] characters = bitString.toCharArray();
		for (int i = characters.length - 1; i >= 0; i--) {
			if (characters[i] == '1') {
				result += posValue;
			}
			posValue *= 2;
		}
		return result;
	}

	public static String getHexString(byte[] data) {
		char[] hexArray = "0123456789ABCDEF".toCharArray();

		char[] hexChars = new char[data.length * 2];
		for (int j = 0; j < data.length; j++) {
			int v = data[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static String getHexString(byte[] data, String separator, int bytesBeforeLinebreak, boolean insertHexLineNumber, boolean insertAsciiText) {
		StringBuilder returnString = new StringBuilder();
		int byteCount = 0;
		StringBuilder asciiTextLine = new StringBuilder();
		for (byte dataByte : data) {
			if (byteCount > 0) {
				if (bytesBeforeLinebreak > 0 && byteCount % bytesBeforeLinebreak == 0) {
					if (insertAsciiText) {
						returnString.append(" ; ");
						returnString.append(asciiTextLine);
						asciiTextLine = new StringBuilder();
					}
					returnString.append('\n');
					if (insertHexLineNumber) {
						returnString.append(String.format("%08Xh: ", byteCount));
					}
				} else {
					returnString.append(separator);
				}
			}
			returnString.append(String.format("%02X", dataByte));
			if (dataByte >= 0 && dataByte < 32) {
				asciiTextLine.append('.');
			} else {
				asciiTextLine.append(new String(new byte[] { dataByte }));
			}
			byteCount++;
		}

		if (insertAsciiText && asciiTextLine.length() > 0) {
			int bytesInLastLine = byteCount % bytesBeforeLinebreak;
			if (bytesInLastLine > 0) {
				returnString.append(TextUtilities.repeatString("   ", bytesBeforeLinebreak - bytesInLastLine));
			}
			returnString.append(" ; ");
			returnString.append(asciiTextLine.toString());
		}

		if (insertHexLineNumber) {
			return String.format("%08Xh: ", 0) + returnString.toString();
		} else {
			return returnString.toString();
		}
	}

	public static byte[] getByteArrayFromHexString(String value) {
		int length = value.length();
		byte[] data = new byte[length / 2];
		for (int i = 0; i < length; i += 2) {
			data[i / 2] = (byte) ((Character.digit(value.charAt(i), 16) << 4) + Character.digit(value.charAt(i + 1), 16));
		}
		return data;
	}

	/**
	 * XOR byte values
	 *
	 * @param byteArrays
	 * @return
	 */
	public static byte[] xor(byte[]... byteArrays) {
		return xor(Arrays.asList(byteArrays));
	}

	/**
	 * XOR byte values
	 *
	 * @param byteArrays
	 * @return
	 */
	public static byte[] xor(List<byte[]> byteArrays) {
		byte[] returnArray = new byte[byteArrays.get(0).length];

		for (int i = 0; i < byteArrays.size(); i++) {
			for (int j = 0; j < returnArray.length; j++) {
				returnArray[j] = xor(returnArray[j], byteArrays.get(i)[j]);
			}
		}

		return returnArray;
	}

	public static byte xor(byte... keyData) {
		byte resultItem = 0;

		for (byte key : keyData) {
			resultItem = (byte) (resultItem ^ key);
		}

		return resultItem;
	}
}
