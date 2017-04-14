package de.soderer.utilities;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * Global Utilities
 *
 * This class does no Logging via Log4J, because it is often used before its initialisation
 */
public class Utilities {
	public static final String STANDARD_XML = "<?xml version=\"1.0\" encoding=\"<encoding>\" standalone=\"yes\"?>\n<root>\n</root>\n";
	public static final String STANDARD_HTML = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n\t<head>\n\t\t<meta http-equiv=\"Content-Type\" content=\"text/html; charset=<encoding>\" />\n\t\t<title>HtmlTitle</title>\n\t\t<meta name=\"Title\" content=\"HtmlTitle\" />\n\t</head>\n\t<body>\n\t</body>\n</html>\n";
	public static final String STANDARD_BASHSCRIPTSTART = "#!/bin/bash\n";
	public static final String STANDARD_JSON = "{\n\t\"property1\": null,\n\t\"property2\": " + Math.PI + ",\n\t\"property3\": true,\n\t\"property4\": \"Text\",\n\t\"property5\": [\n\t\tnull,\n\t\t" + Math.PI + ",\n\t\ttrue,\n\t\t\"Text\"\n\t]\n}\n";

	/** UTF-8 BOM (Byte Order Mark) character for readers. */
	public static final char BOM_UTF_8_CHAR = (char) 65279;

	/** UTF-8 BOM (Byte Order Mark) at data start (EF BB BF, "ï»¿"). */
	public static final byte[] BOM_UTF_8 = new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };

	/** UTF-16 BOM (Byte Order Mark) big endian at data start (FE FF, "þÿ"). */
	public static final byte[] BOM_UTF_16_BIG_ENDIAN = new byte[] { (byte) 0xFE, (byte) 0xFF };

	/** UTF-16 BOM (Byte Order Mark) low endian at data start (FF FE, "ÿþ"). */
	public static final byte[] BOM_UTF_16_LOW_ENDIAN = new byte[] { (byte) 0xFF, (byte) 0xFE };

	/**
	 * Generate a unique ID
	 *
	 * @return
	 */
	public static String generateUUID() {
		return UUID.randomUUID().toString().toUpperCase().replaceAll("-", "");
	}

	/**
	 * Get a UUID from a string
	 *
	 * @param value
	 * @return
	 */
	public static UUID getUUIDFromString(String value) {
		StringBuilder uuidString = new StringBuilder(value);
		uuidString.insert(20, '-');
		uuidString.insert(16, '-');
		uuidString.insert(12, '-');
		uuidString.insert(8, '-');
		return UUID.fromString(uuidString.toString());
	}

	/**
	 * Get the data of a file included in a jar file
	 *
	 * @param resourceName
	 * @return
	 */
	public static InputStream getResourceAsStream(String resourceName) {
		return Utilities.class.getResourceAsStream("/" + resourceName);
	}

	/**
	 * Zip a byteArray by GZIP-Algorithm
	 *
	 * @param clearData
	 * @return
	 */
	public static byte[] gzipByteArray(byte[] clearData) {
		GZIPOutputStream gzipCompresser = null;
		try {
			ByteArrayOutputStream encoded = new ByteArrayOutputStream();
			gzipCompresser = new GZIPOutputStream(encoded);
			gzipCompresser.write(clearData);
			gzipCompresser.close();
			return encoded.toByteArray();
		} catch (IOException e) {
			return null;
		} finally {
			closeQuietly(gzipCompresser);
		}
	}

	/**
	 * Unzip a byteArray by GZIP-Algorithm
	 *
	 * @param zippedData
	 * @return
	 * @throws Exception
	 */
	public static byte[] gunzipByteArray(byte[] zippedData) throws Exception {
		try {
			ByteArrayOutputStream decoded = new ByteArrayOutputStream();
			ByteArrayInputStream encoded = new ByteArrayInputStream(zippedData);
			copy(new GZIPInputStream(encoded), decoded);
			return decoded.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Encode a Base64 String
	 *
	 * @param clearData
	 * @return
	 */
	public static String encodeBase64(byte[] clearData) {
		return Base64.getEncoder().encodeToString(clearData);
	}

	/**
	 * Decode a Base64 String
	 *
	 * @param base64String
	 * @return
	 */
	public static byte[] decodeBase64(String base64String) {
		try {
			return Base64.getDecoder().decode(base64String.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

	/**
	 * Check a simple name string
	 *
	 * @param value
	 * @return
	 */
	public static boolean checkForValidUserName(String value) {
		return value != null && value.matches("[A-Za-z0-9_-]*");
	}

	/**
	 * Convert an ArrayList of Strings to a StringArray
	 *
	 * @param pArrayListOfStrings
	 * @return
	 */
	public static String[] convertArrayListOfStringsToStringArray(ArrayList<String> pArrayListOfStrings) {
		String[] arrayofStrings = new String[0];
		return pArrayListOfStrings.toArray(arrayofStrings);
	}

	/**
	 * Get index of an Integer within an Array of Integers
	 *
	 * @param searchInt
	 * @param intArray
	 * @return
	 */
	public static int getIndex(int searchInt, int[] intArray) {
		for (int i = 0; i < intArray.length; i++) {
			if (intArray[i] == searchInt) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Read stream data in byteArray until next linefeed or stream end
	 *
	 * @param inStream
	 * @return
	 * @throws IOException
	 */
	public static byte[] readStreamUntilEndOrLinefeed(InputStream inStream) throws IOException {
		ByteArrayOutputStream returnData = new ByteArrayOutputStream();
		int nextByte;
		while (true) {
			nextByte = inStream.read();
			if (nextByte < 0) {
				break;
			} else if (nextByte == '\n') {
				returnData.write(nextByte);
				break;
			} else {
				returnData.write(nextByte);
			}
		}
		return returnData.toByteArray();
	}

	/**
	 * Get email from X509Certificate
	 *
	 * @param cert
	 * @return
	 */
	public static String getEmailFromCertificate(X509Certificate cert) {
		String[] nameParts = cert.getSubjectX500Principal().toString().split(",");
		for (String namePart : nameParts) {
			if (namePart.matches("^[ \\t]*EMAILADDRESS=.*")) {
				return namePart.substring(namePart.indexOf("=") + 1).trim();
			}
		}

		return null;
	}

	/**
	 * Get cn from X509Certificate
	 *
	 * @param cert
	 * @return
	 */
	public static String getCnFromCertificate(X509Certificate cert) {
		String[] nameParts = cert.getSubjectX500Principal().toString().split(",");
		for (String namePart : nameParts) {
			if (namePart.matches("^[ \\t]*CN=.*")) {
				return namePart.substring(namePart.indexOf("=") + 1).trim();
			}
		}

		return null;
	}

	/***
	 * Split a list into smaller lists to a maximum chunkSize
	 *
	 * @param originalList
	 * @param chunkSize
	 * @return
	 */
	public static <E> List<List<E>> chopListToChunks(List<E> originalList, int chunkSize) {
		if (originalList == null || originalList.size() <= 0 || chunkSize <= 0) {
			return null;
		}

		List<List<E>> returnList = new ArrayList<List<E>>();
		int endIndex = 0;

		while (endIndex < originalList.size()) {
			int startIndex = endIndex;
			if (chunkSize < originalList.size() - endIndex) {
				endIndex += chunkSize;
			} else {
				endIndex = originalList.size();
			}

			returnList.add(originalList.subList(startIndex, endIndex));
		}

		return returnList;
	}

	/**
	 * List of characters for randomization
	 */
	private static final char[] randomCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜabcdefghijklmnopqrstuvwxyzäöüß".toCharArray();

	/**
	 * List of numbers and strings for randomization
	 */
	private static final char[] randomAlphaNumericCharacters = (new String(randomCharacters) + "0123456789").toCharArray();

	/**
	 * Random generator
	 */
	private static final Random random = new SecureRandom();

	/**
	 * Generate a random number up to maximum value
	 *
	 * @param excludedMaximum
	 * @return
	 */
	public static int getRandomNumber(int excludedMaximum) {
		return random.nextInt(excludedMaximum);
	}

	/**
	 * Generate a random string of given size
	 *
	 * @param length
	 * @return
	 */
	public static String getRandomString(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(randomCharacters[random.nextInt(randomCharacters.length)]);
		}
		return sb.toString();
	}

	/**
	 * Generate a random string of numbers and characters of given size
	 *
	 * @param length
	 * @return
	 */
	public static String getRandomAlphanumericString(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(randomAlphaNumericCharacters[random.nextInt(randomAlphaNumericCharacters.length)]);
		}
		return sb.toString();
	}

	/**
	 * Generate a random number of given size
	 *
	 * @param length
	 * @return
	 */
	public static String getRandomNumberString(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(random.nextInt(10));
		}
		return sb.toString();
	}

	/**
	 * Generate a random byte
	 *
	 * @return
	 */
	public static byte getRandomByte() {
		byte[] result = new byte[1];
		random.nextBytes(result);
		return result[0];
	}

	/**
	 * Generate a random byteArray
	 *
	 * @param arrayToFill
	 * @return
	 */
	public static byte[] getRandomByteArray(byte[] arrayToFill) {
		random.nextBytes(arrayToFill);
		return arrayToFill;
	}

	/**
	 * Check if a Integer is contained by an interval definition Interval definitions like -1;2-5;8+
	 *
	 * @param intervals
	 * @param item
	 * @return
	 */
	public static boolean checkForIntervalContainment(String intervals, int item) {
		if (intervals != null && intervals.length() > 0) {
			String[] blockStrings = intervals.split(";");
			for (String blockString : blockStrings) {
				if (blockString.endsWith("+")) {
					if (Integer.parseInt(blockString.substring(0, blockString.length() - 1)) <= item) {
						return true;
					}
				} else if (blockString.matches("\\d+-\\d+")) {
					int plusIndex = blockString.indexOf("-");
					int startVersion = Integer.parseInt(blockString.substring(0, plusIndex));
					int endeVersion = Integer.parseInt(blockString.substring(plusIndex + 1));
					if (startVersion <= item && endeVersion >= item) {
						return true;
					}
				} else if (blockString.matches("-\\d+")) {
					int endeVersion = Integer.parseInt(blockString.substring(1));
					if (endeVersion >= item) {
						return true;
					}
				} else {
					if (Integer.parseInt(blockString) == item) {
						return true;
					}
				}
			}
		}

		return false;
	}

	/**
	 * Get the minimum of a value list down to a valid minimum
	 *
	 * @param allowedValueMinimum
	 * @param values
	 * @return
	 */
	public static int getMinimumOfAllowedValues(int allowedValueMinimum, int... values) {
		int returnValue = Integer.MAX_VALUE;
		if (values != null) {
			for (int value : values) {
				if (value >= allowedValueMinimum) {
					returnValue = Math.min(returnValue, value);
				}
			}
		}
		return returnValue;
	}

	/**
	 * Convert a string to boolean
	 *
	 * @param value
	 * @return
	 */
	public static boolean interpretAsBool(String value) {
		if (isNotEmpty(value)) {
			value = value.trim();
			return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("+") || value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("ja") || value.equalsIgnoreCase("ok")
					|| value.equalsIgnoreCase("on") || value.equalsIgnoreCase("an");
		} else {
			return false;
		}
	}

	/**
	 * Check if any characters in a list are equal
	 *
	 * @param values
	 * @return
	 */
	public static boolean anyCharsAreEqual(char... values) {
		for (int i = 0; i < values.length; i++) {
			for (int j = i + 1; j < values.length; j++) {
				if (values[i] == values[j]) {
					return true;
				}
			}
		}
		return false;
	}

	public static boolean contains(char[] characterArray, Character searchCharacter) {
		if (characterArray == null || searchCharacter == null) {
			return false;
		}
		
		for (char character : characterArray) {
			if (character == searchCharacter) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Math.square
	 *
	 * @param value
	 * @return
	 */
	public static int square(int value) {
		return value * value;
	}

	/**
	 * Math power
	 *
	 * @param base
	 * @param exp
	 * @return
	 */
	public static int pow(int base, int exp) {
		if (exp < 0) {
			throw new IllegalArgumentException("Invalid negative exponent");
		} else if (exp == 0) {
			return 1;
		} else {
			return square(pow(base, exp / 2)) * (exp % 2 == 1 ? base : 1);
		}
	}

	/**
	 * Get a collection like a set as a ordered list
	 * 
	 * @param c
	 * @return
	 */
	public static <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c) {
		List<T> list = new ArrayList<T>(c);
		Collections.sort(list);
		return list;
	}
	
	/**
	 * Sort a map by a Comparator for the keytype
	 *
	 * @param mapToSort
	 * @param comparator
	 * @return
	 */
	public static <Key, Value> Map<Key, Value> sortMap(Map<Key, Value> mapToSort, Comparator<Key> comparator) {
		List<Key> keys = new ArrayList<Key>(mapToSort.keySet());
		Collections.sort(keys, comparator);
		LinkedHashMap<Key, Value> sortedContent = new LinkedHashMap<Key, Value>();
		for (Key key : keys) {
			sortedContent.put(key, mapToSort.get(key));
		}
		return sortedContent;
	}
	
	/**
	 * Sort a map by the String keytype
	 *
	 * @param mapToSort
	 * @param comparator
	 * @return
	 */
	public static <Value> Map<String, Value> sortMap(Map<String, Value> mapToSort) {
		List<String> keys = new ArrayList<String>(mapToSort.keySet());
		Collections.sort(keys);
		LinkedHashMap<String, Value> sortedContent = new LinkedHashMap<String, Value>();
		for (String key : keys) {
			sortedContent.put(key, mapToSort.get(key));
		}
		return sortedContent;
	}

	/**
	 * Get files of classpath
	 *
	 * @return
	 */
	public static String getClassPath() {
		ClassLoader sysClassLoader = ClassLoader.getSystemClassLoader();
		URL[] urls = ((URLClassLoader) sysClassLoader).getURLs();

		StringBuilder classpath = new StringBuilder();
		for (URL url : urls) {
			classpath.append(url.getFile() + "\n");
		}
		return classpath.toString();
	}

	/**
	 * Check array equality
	 *
	 * @param array1
	 * @param array2
	 * @return
	 */
	public static <T> boolean compare(T[] array1, T[] array2) {
		if (array1 == array2) {
			return true;
		} else if (array1 == null || array2 == null || array1.length != array2.length) {
			return false;
		} else {
			for (int i = 0; i < array1.length; i++) {
				if (array1[i] != array2[i]) {
					return false;
				}
			}
			return true;
		}
	}

	/**
	 * Get all system properties
	 *
	 * @return
	 */
	public static Map<String, String> getSystemPropertiesMap() {
		Map<String, String> propertiesMap = new HashMap<String, String>();
		for (Object key : System.getProperties().keySet()) {
			propertiesMap.put((String) key, System.getProperties().getProperty((String) key));
		}
		return propertiesMap;
	}

	/**
	 * Convert Map to String
	 *
	 * @param map
	 * @param entrySeparator
	 * @param keySeparator
	 * @param sort
	 * @return
	 */
	public static String getStringFromMap(Map<String, ? extends Object> map, String entrySeparator, String keySeparator, boolean sort) {
		List<String> keyList = new ArrayList<String>(map.keySet());
		if (sort) {
			Collections.sort(keyList);
		}

		StringBuilder builder = new StringBuilder();
		for (Object key : keyList) {
			if (builder.length() > 0) {
				builder.append(entrySeparator);
			}
			builder.append(key == null ? "" : key);
			builder.append(keySeparator);
			Object value = map.get(key);
			builder.append(value == null ? "" : value.toString());
		}
		return builder.toString();
	}

	/**
	 * Make a number human readable
	 *
	 * @param value
	 * @return
	 */
	public static String getHumanReadableNumber(Number value) {
		return getHumanReadableNumber(value, null, true);
	}

	/**
	 * Make a number with unitsign human readable
	 *
	 * @param value
	 * @param unitTypeSign
	 * @return
	 */
	public static String getHumanReadableNumber(Number value, String unitTypeSign) {
		return getHumanReadableNumber(value, unitTypeSign, true);
	}

	/**
	 * Make a number with unitsign human readable
	 *
	 * @param value
	 * @param unitTypeSign
	 * @param siUnits
	 * @return
	 */
	public static String getHumanReadableNumber(Number value, String unitTypeSign, boolean siUnits) {
		int unit = siUnits ? 1000 : 1024;
		double interimValue = value.doubleValue();
		String unitExtension = "";
		if (interimValue < unit) {
			if (isNotBlank(unitTypeSign)) {
				unitExtension = " " + unitTypeSign;
			}

			if (value instanceof Integer || value instanceof Long) {
				return value + unitExtension;
			}
		} else {
			int exp = (int) (Math.log(interimValue) / Math.log(unit));
			unitExtension = " " + (siUnits ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (siUnits ? "" : "i");
			if (isNotBlank(unitTypeSign)) {
				unitExtension += unitTypeSign;
			}
			interimValue = interimValue / Math.pow(unit, exp);
		}

		String valueString;
		if (interimValue >= 1000) {
			valueString = String.format("%.1f", interimValue);
		} else if (interimValue >= 100) {
			valueString = String.format("%.2f", interimValue);
		} else if (interimValue >= 10) {
			valueString = String.format("%.3f", interimValue);
		} else if (interimValue >= 1) {
			valueString = String.format("%.4f", interimValue);
		} else {
			valueString = String.format("%.5f", interimValue);
		}

		return valueString + unitExtension;
	}

	/**
	 * Generate MD5 from string data
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] getMD5Hash(String data) throws Exception {
		try {
			return MessageDigest.getInstance("MD5").digest(data.getBytes("UTF-8"));
		} catch (Exception e) {
			throw new Exception("Error while MD5 hashing", e);
		}
	}

	/**
	 * Generate SHA-1 from string data
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] getSHA1Hash(String data) throws Exception {
		try {
			return MessageDigest.getInstance("SHA-1").digest(data.getBytes("UTF-8"));
		} catch (Exception e) {
			throw new Exception("Error while SHA-1 hashing", e);
		}
	}

	/**
	 * Generate SHA-512 from string data
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] getSHA512Hash(String data) throws Exception {
		try {
			return MessageDigest.getInstance("SHA-512").digest(data.getBytes("UTF-8"));
		} catch (Exception e) {
			throw new Exception("Error while SHA-512 hashing", e);
		}
	}

	/**
	 * Get bytearray for list of bytes
	 *
	 * @param data
	 * @return
	 */
	public static byte[] getByteArray(List<Byte> data) {
		byte[] returnArray = new byte[data.size()];
		for (int i = 0; i < data.size(); i++) {
			returnArray[i] = data.get(i);
		}
		return returnArray;
	}

	/**
	 * Get stacktrace as string
	 *
	 * @param stackTrace
	 * @return
	 */
	public static String stacktraceToString(StackTraceElement[] stackTrace) {
		StringBuilder returnBuilder = new StringBuilder();
		if (stackTrace != null) {
			for (StackTraceElement stackTraceElement : stackTrace) {
				returnBuilder.append(stackTraceElement.toString());
				returnBuilder.append("\n");
			}
		}
		return returnBuilder.toString();
	}

	/**
	 * Download a file from url
	 *
	 * @param url
	 * @param localeDestionationPath
	 * @throws Exception
	 */
	public static void downloadFile(String url, String localeDestionationPath) throws Exception {
		BufferedInputStream bufferedInputStream = null;
		FileOutputStream fileOutputStream = null;
		try {
			bufferedInputStream = new BufferedInputStream(new URL(url).openStream());
			fileOutputStream = new FileOutputStream(localeDestionationPath);
			copy(bufferedInputStream, fileOutputStream);
		} catch (Exception e) {
			throw new Exception("Cannot download file", e);
		} finally {
			closeQuietly(fileOutputStream);
			closeQuietly(bufferedInputStream);
		}
	}

	/**
	 * Check array for duplicate strings
	 *
	 * @param inputArray
	 * @param ignoreNullValues
	 * @return
	 */
	public static boolean checkForDuplicates(String[] inputArray, boolean ignoreNullValues) {
		Set<String> tempSet = new HashSet<String>();
		for (String stringItem : inputArray) {
			if (!ignoreNullValues || stringItem != null) {
				if (!tempSet.add(stringItem)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Filter all Objects of given class
	 *
	 * @param collection
	 * @param classToSelect
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> List<T> selectItems(Collection<?> collection, Class<T> classToSelect) {
		List<T> list = new ArrayList<T>();
		for (Object item : collection) {
			if (classToSelect.isInstance(item)) {
				list.add((T) item);
			}
		}
		return list;
	}

	/**
	 * Filter all Objects of given class
	 *
	 * @param array
	 * @param classToSelect
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> List<T> selectItems(Object[] array, Class<T> classToSelect) {
		List<T> list = new ArrayList<T>();
		for (Object item : array) {
			if (classToSelect.isInstance(item)) {
				list.add((T) item);
			}
		}
		return list;
	}

	public static <T> T[] revertArray(T[] array) {
		@SuppressWarnings("unchecked")
		T[] returnValue = (T[]) new Object[array.length];
		for (int i = 0; i < array.length; i++) {
			returnValue[i] = array[array.length - 1 - i];
		}
		return returnValue;
	}

	public static String getDomainFromUrl(String url) throws Exception {
		URI uri = new URI(url);
		String domain = uri.getHost();
		return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

	public static boolean isEmpty(String value) {
		return value == null || value.length() == 0;
	}

	public static boolean isNotEmpty(String value) {
		return !isEmpty(value);
	}

	public static boolean isEmpty(Collection<?> collection) {
		return collection == null || collection.isEmpty();
	}

	public static boolean isNotEmpty(Collection<?> collection) {
		return !isEmpty(collection);
	}

	public static boolean isBlank(String value) {
		return value == null || value.length() == 0 || value.trim().length() == 0;
	}

	public static boolean isNotBlank(String value) {
		return !isBlank(value);
	}

	public static boolean isEmpty(char[] value) {
		return value == null || value.length == 0;
	}

	public static boolean isNotEmpty(char[] value) {
		return !isEmpty(value);
	}

	public static boolean isBlank(char[] value) {
		if (value == null || value.length == 0) {
			return true;
		} else {
			for (char character : value) {
				if (!Character.isWhitespace(character)) {
					return false;
				}
			}
			return true;
		}
	}

	public static boolean isNotBlank(char[] value) {
		return !isBlank(value);
	}

	public static void closeQuietly(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException e) {
				// Do nothing
			}
		}
	}

	/**
	 * XMLStreamReader.close() doesn't close the underlying stream.
	 * So it must be closed separately.
	 */
	public static void closeQuietly(XMLStreamReader closeable, InputStream inputStream) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				// Do nothing
			}
		}
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (Exception e) {
				// Do nothing
			}
		}
	}

	public static void closeQuietly(XMLStreamWriter xmlWriter) {
		if (xmlWriter != null) {
			try {
				xmlWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			xmlWriter = null;
		}
	}

	public static String repeat(String value, int count) {
		return repeat(value, count, null);
	}

	public static String repeat(String value, int count, String separatorString) {
		if (value == null) {
			return null;
		} else if (value.length() == 0 || count == 0) {
			return "";
		} else {
			StringBuilder returnValue = new StringBuilder();
			for (int i = 0; i < count; i++) {
				if (separatorString != null && returnValue.length() > 0) {
					returnValue.append(separatorString);
				}
				returnValue.append(value);
			}
			return returnValue.toString();
		}
	}

	public static String join(Object[] array, String glue) {
		if (array == null) {
			return null;
		} else if (array.length == 0) {
			return "";
		} else {
			if (glue == null) {
				glue = "";
			}

			StringBuilder returnValue = new StringBuilder();
			boolean isFirst = true;
			for (Object object : array) {
				if (!isFirst) {
					returnValue.append(glue);
				}
				if (object == null) {
					object = "";
				}
				returnValue.append(object.toString());
				isFirst = false;
			}
			return returnValue.toString();
		}
	}

	public static String join(Iterable<?> iterableObject, String glue) {
		if (iterableObject == null) {
			return null;
		} else {
			if (glue == null) {
				glue = "";
			}

			StringBuilder returnValue = new StringBuilder();
			boolean isFirst = true;
			for (Object object : iterableObject) {
				if (!isFirst) {
					returnValue.append(glue);
				}
				if (object == null) {
					object = "";
				}
				returnValue.append(object.toString());
				isFirst = false;
			}
			return returnValue.toString();
		}
	}

	public static <T> T[] remove(T[] array, T itemToRemove) {
		int indexToRemove = -1;
		if (array == null) {
			return null;
		} else if (itemToRemove == null) {
		    for (int i = 0; i < array.length; i++) {
		        if (array[i] == null) {
		        	indexToRemove = i;
		        	break;
		        }
		    }
		} else {
		    for (int i = 0; i < array.length; i++) {
		        if (itemToRemove.equals(array[i])) {
		        	indexToRemove = i;
		        	break;
		        }
		    }
		}
		
		if (indexToRemove >= 0) {
			@SuppressWarnings("unchecked")
			T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);
			if (indexToRemove > 0) {
				System.arraycopy(array, 0, result, 0, indexToRemove);
			}
			if (indexToRemove < array.length - 1) {
			    System.arraycopy(array, indexToRemove + 1, result, indexToRemove, array.length - indexToRemove - 1);
			}
			return result;
		} else {
			@SuppressWarnings("unchecked")
			T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length);
			System.arraycopy(array, 0, result, 0, array.length);
			return result;
		}
	}

	public static byte[] readFileToByteArray(File file) throws FileNotFoundException, IOException {
		try (FileInputStream in = new FileInputStream(file)) {
			byte[] returnArray = new byte[(int) file.length()];
			in.read(returnArray);
			return returnArray;
		}
	}

	public static long copy(InputStream inputStream, OutputStream outputStream) throws IOException {
		byte[] buffer = new byte[4096];
		int lengthRead = -1;
		long bytesCopied = 0;
		while ((lengthRead = inputStream.read(buffer)) > -1) {
			outputStream.write(buffer, 0, lengthRead);
			bytesCopied += lengthRead;
		}
		outputStream.flush();
		return bytesCopied;
	}

	public static long copy(Reader inputReader, OutputStream outputStream, String encoding) throws UnsupportedEncodingException, IOException {
		char[] buffer = new char[4096];
		int lengthRead = -1;
		long bytesCopied = 0;
		while ((lengthRead = inputReader.read(buffer)) > -1) {
			String data = new String(buffer, 0, lengthRead);
			outputStream.write(data.getBytes(encoding));
			bytesCopied += lengthRead;
		}
		outputStream.flush();
		return bytesCopied;
	}

	public static byte[] toByteArray(InputStream inputStream) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		copy(inputStream, byteArrayOutputStream);
		byteArrayOutputStream.close();
		return byteArrayOutputStream.toByteArray();
	}

	/**
	 * Create a progress string for terminal output e.g.: "65% [=================================>                   ] 103.234 200/s eta 5m"
	 *
	 * @param start
	 * @param itemsToDo
	 * @param itemsDone
	 * @return
	 */
	public static String getConsoleProgressString(int lineLength, Date start, long itemsToDo, long itemsDone) {
		Date now = new Date();
		String itemsToDoString = "??";
		String percentageString = " 0%";
		String speedString = "???/s";
		String etaString = "eta ???";
		int percentageDone = 0;
		if (itemsToDo > 0 && itemsDone > 0) {
			itemsToDoString = NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsToDo);
			percentageDone = (int) (itemsDone * 100 / itemsToDo);
			percentageString = leftPad(percentageDone + "%", 3);
			long elapsedSeconds = (now.getTime() - start.getTime()) / 1000;
			// Prevent division by zero, when start is fast
			if (elapsedSeconds == 0) {
				elapsedSeconds = 1;
			}
			int speed = (int) (itemsDone / elapsedSeconds);
			speedString = getHumanReadableNumber(speed, "", true) + "/s";
			Date estimatedEnd = DateUtilities.calculateETA(start, itemsToDo, itemsDone);
			etaString = "eta " + DateUtilities.getShortHumanReadableTimespan(estimatedEnd.getTime() - now.getTime(), false);
		} else if (itemsToDo > 0) {
			itemsToDoString = NumberFormat.getNumberInstance(Locale.getDefault()).format(itemsToDo);
		}

		String leftPart = percentageString + " [";
		String rightPart = "] " + itemsToDoString + " " + speedString + " " + etaString;
		int barWith = lineLength - (leftPart.length() + rightPart.length());
		int barDone = barWith * percentageDone / 100;
		if (barDone < 1) {
			barDone = 1;
		} else if (barDone >= barWith) {
			barDone = barWith;
		}
		return leftPart + repeat("=", barDone - 1) + ">" + repeat(" ", barWith - barDone) + rightPart;
	}

	/**
	 * Append blanks at the left of a string to make if fit the given minimum
	 *
	 * @param escapedValue
	 * @param i
	 * @return
	 */
	public static String leftPad(String value, int minimumLength) {
		try {
			return String.format("%1$" + minimumLength + "s", value);
		} catch (Exception e) {
			return value;
		}
	}

	/**
	 * Append blanks at the right of a string to make if fit the given minimum
	 *
	 * @param escapedValue
	 * @param i
	 * @return
	 */
	public static String rightPad(String value, int minimumLength) {
		try {
			return String.format("%1$-" + minimumLength + "s", value);
		} catch (Exception e) {
			return value;
		}
	}

	/**
	 * Only trim the value when the sourrounding occures on both ends
	 * @param value
	 * @param prefix
	 * @return
	 */
	public static String trimSimultaneously(String value, String sourrounding) {
		if (value == null) {
			return null;
		} else if (isEmpty(sourrounding)) {
			return value;
		} else if (value.startsWith(sourrounding) && value.endsWith(sourrounding)) {
			return value.substring(sourrounding.length(), value.length() - sourrounding.length());
		} else {
			return value;
		}
	}

	public static String trim(String value) {
		if (value == null) {
			return null;
		} else {
			return value.trim();
		}
	}

	public static String toString(InputStream inputStream, String encoding) throws UnsupportedEncodingException, IOException {
		return new String(toByteArray(inputStream), encoding);
	}

	public static Object toString(Reader characterStream) throws IOException {
		StringBuilder returnValue = new StringBuilder();
		int characterInt;
		while ((characterInt = characterStream.read()) > -1) {
			returnValue.append((char) characterInt);
		}
		return returnValue.toString();
	}

	public static List<String> readLines(InputStream inStream, String encoding) throws IOException {
		BufferedReader reader = null;
		try {
			List<String> lines = new ArrayList<String>();
			reader = new BufferedReader(new InputStreamReader(inStream, encoding));
			String nextLine;
			while ((nextLine = reader.readLine()) != null) {
				lines.add(nextLine);
			}
			return lines;
		} finally {
			closeQuietly(reader);
		}
	}
	
	public static String trimLeft(String value) {
		return value.replaceAll("^\\s+", "");
	}
	
	public static String trimRight(String value) {
		return value.replaceAll("\\s+$", "");
	}
	
	public static void addFileToClasspath(String filePath) throws IOException {
		try {
			Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
			method.setAccessible(true);
			method.invoke(ClassLoader.getSystemClassLoader(), new Object[] { new URL(new File(filePath).toURI().toString()) });
		} catch (Throwable t) {
			throw new IOException("Error, could not add file to system classloader: " + t.getMessage(), t);
		}
	}

	public static String shortenStringToMaxLengthCutRight(String value, int maxLength, String cutSign) {
		if (value != null && value.length() > maxLength) {
			return value.substring(0, maxLength - 4) + cutSign;
		} else {
			return value;
		}
	}

	public static String shortenStringToMaxLengthCutRight(String value, int maxLength) {
		return shortenStringToMaxLengthCutRight(value, maxLength, " ...");
	}

	public static String shortenStringToMaxLengthCutMiddle(String value, int maxLength) {
		if (value != null && value.length() > maxLength) {
			int leftLength = (maxLength - 5) / 2;
			return value.substring(0, leftLength) + " ... " + value.substring(value.length() - ((maxLength - leftLength) - 5));
		} else {
			return value;
		}
	}

	public static String shortenStringToMaxLengthCutLeft(String value, int maxLength) {
		if (value != null && value.length() > maxLength) {
			return "... " + value.substring((value.length() - maxLength) + 4);
		} else {
			return value;
		}
	}
	
	public static List<String> splitAndTrimList(String stringList) {
		if (stringList == null) {
			return null;
		} else {
			List<String> returnList = new ArrayList<String>();
			String[] parts = stringList.split(",|;|\\|| |\\n|\\r|\\t");
			for (String part : parts) {
				if (isNotEmpty(part)) {
					returnList.add(part.trim());
				}
			}
			return returnList;
		}
	}
	
	public static List<String> splitAndTrimList(String stringList, Character... separatorChars) {
		if (stringList == null) {
			return null;
		} else {
			List<String> returnList = new ArrayList<String>();
			String[] parts = stringList.split(join(separatorChars, "|").replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r"));
			for (String part : parts) {
				if (isNotEmpty(part)) {
					returnList.add(part.trim());
				}
			}
			return returnList;
		}
	}

	public static boolean endsWithIgnoreCase(String data, String suffix) {
		if (data == suffix) {
			// both null or same object
			return true;
		} else if (data == null) {
			// data is null but suffix is not
			return false;
		} else if (suffix == null) {
			// suffix is null but data is not
			return true;
		} else if (data.toLowerCase().endsWith(suffix.toLowerCase())) {
			// both are set, so ignore the case for standard endsWith-method
			return true;
		} else {
			// anything else
			return false;
		}
	}

	public static int indexOfIgnoreCase(String data, String part) {
		if (data == part) {
			// both null or same object
			return 0;
		} else if (data == null || part == null) {
			// suffix is null but data is not or vice versa
			return -1;
		} else {
			// anything else
			return data.toLowerCase().indexOf(part.toLowerCase());
		}
	}

	public static List<String> splitAndTrimListQuoted(String stringList, char... separatorChars) {
		List<String> returnList = new ArrayList<String>();
		StringBuilder nextLine = new StringBuilder();
		boolean quotedBySingleQoute = false;
		boolean quotedByDoubleQoute = false;
		for (char nextChar : stringList.toCharArray()) {
			if ('\'' == nextChar) {
				if (!quotedBySingleQoute && !quotedByDoubleQoute) {
					quotedBySingleQoute = true;
				} else if (quotedBySingleQoute) {
					quotedBySingleQoute = false;
				}
			} else if ('"' == nextChar) {
				if (!quotedBySingleQoute && !quotedByDoubleQoute) {
					quotedByDoubleQoute = true;
				} else if (quotedByDoubleQoute) {
					quotedByDoubleQoute = false;
				}
			}
			
			boolean splitFound = false;
			for (char separatorChar : separatorChars) {
				if (separatorChar == nextChar && !quotedBySingleQoute && !quotedByDoubleQoute) {
					String line = nextLine.toString().trim();
					if (line.length() > 0) {
						returnList.add(line);
						splitFound = true;
					}
					nextLine = new StringBuilder();
					break;
				}
			}
			
			if (!splitFound) {
				nextLine.append(nextChar);
			}
		}
		String line = nextLine.toString().trim();
		if (line.length() > 0) {
			returnList.add(line);
		}
		return returnList;
	}

	public static boolean containsIgnoreCase(Collection<String> list, String item) {
		if (list == null) {
			return false;
		} else {
			for (String listItem : list) {
				if (listItem == item || (listItem != null && listItem.equalsIgnoreCase(item))) {
					return true;
				}
			}
			return false;
		}
	}
	
	public static boolean delete(File file) {
		if (file.isDirectory()) {
			for (File subFile : file.listFiles()) {
				if (!delete(subFile)) {
					return false;
				}
			}
		}
		return file.delete();
	}

	public static int limitValue(int minimum, int value, int maximum) {
		if (value < minimum) {
			return minimum;
		} else if (maximum < value) {
			return maximum;
		} else {
			return value;
		}
	}

	public static boolean startsWithCaseinsensitive(String data, String prefix) {
		if (data == null || prefix == null) {
			return false;
		} else {
			return data.toLowerCase().startsWith(prefix.toLowerCase());
		}
	}

	public static byte[] readStreamToByteArray(InputStream inputStream) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		copy(inputStream, byteArrayOutputStream);
		return byteArrayOutputStream.toByteArray();
	}
	
	public static Map<String, String> createMap(String... data) {
		Map<String, String> returnMap = new HashMap<String, String>();
		if (data != null && data.length > 0) {
			for (int i = 0; i < data.length / 2; i++) {
				String key = data[i * 2];
				String value = data[i * 2 + 1];
				returnMap.put(key, value);
			}
		}
		return returnMap;
	}
	
	/**
	 * Replace ~ by user.home
	 */
	public static String replaceHomeTilde(String filePath) {
		return filePath.replace("~", System.getProperty("user.home"));
	}
	
	/**
	 * Check whether an iterable collection contains a special object
	 * 
	 * @param hayshack
	 * @param needle
	 * @return
	 */
	public static boolean containsObject(Iterable<?> hayshack, Object needle) {
		for (Object item : hayshack) {
			if (item == needle) {
				return true;
			}
		}
		return false;
	}
}
