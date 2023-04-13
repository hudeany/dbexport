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
import java.io.Reader;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * Global Utilities
 */
public class Utilities {
	public static final String STANDARD_XML = "<?xml version=\"1.0\" encoding=\"<encoding>\" standalone=\"yes\"?>\n<root>\n</root>\n";
	public static final String STANDARD_HTML = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n\t<head>\n\t\t<meta http-equiv=\"Content-Type\" content=\"text/html; charset=<encoding>\" />\n\t\t<title>HtmlTitle</title>\n\t\t<meta name=\"Title\" content=\"HtmlTitle\" />\n\t</head>\n\t<body>\n\t</body>\n</html>\n";
	public static final String STANDARD_BASHSCRIPTSTART = "#!/bin/bash\n";
	public static final String STANDARD_JSON = "{\n\t\"property1\": null,\n\t\"property2\": " + Math.PI + ",\n\t\"property3\": true,\n\t\"property4\": \"Text\",\n\t\"property5\": [\n\t\tnull,\n\t\t" + Math.PI + ",\n\t\ttrue,\n\t\t\"Text\"\n\t]\n}\n";

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
	public static UUID getUUIDFromString(final String value) {
		final StringBuilder uuidString = new StringBuilder(value);
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
	public static InputStream getResourceAsStream(final String resourceName) {
		return Utilities.class.getResourceAsStream("/" + resourceName);
	}

	/**
	 * Zip a byteArray by GZIP-Algorithm
	 *
	 * @param clearData
	 * @return
	 */
	public static byte[] gzipByteArray(final byte[] clearData) {
		try (ByteArrayOutputStream encoded = new ByteArrayOutputStream()) {
			try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(encoded)) {
				gzipOutputStream.write(clearData);
			}
			return encoded.toByteArray();
		} catch (@SuppressWarnings("unused") final IOException e) {
			return null;
		}
	}

	/**
	 * Unzip a byteArray by GZIP-Algorithm
	 *
	 * @param zippedData
	 * @return
	 * @throws Exception
	 */
	public static byte[] gunzipByteArray(final byte[] zippedData) throws Exception {
		try (ByteArrayOutputStream decoded = new ByteArrayOutputStream()) {
			try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(zippedData))) {
				IoUtilities.copy(gzipInputStream, decoded);
			}
			return decoded.toByteArray();
		} catch (@SuppressWarnings("unused") final IOException e) {
			return null;
		}
	}

	/**
	 * Encode a Base64 String
	 *
	 * @param clearData
	 * @return
	 */
	public static String encodeBase64(final byte[] clearData) {
		return Base64.getEncoder().encodeToString(clearData);
	}

	/**
	 * Encode a Base64 String
	 *
	 * @param clearData
	 * @return
	 */
	public static String encodeBase64(final byte[] clearData, final int maxCharactersPerLine, final String splitCharacters) {
		final String dataBase64 = Base64.getEncoder().encodeToString(clearData);
		final StringBuilder returnString = new StringBuilder();
		final int fullLines = (int) Math.floor(dataBase64.length() / maxCharactersPerLine);
		for (int i = 0; i < fullLines; i++) {
			returnString.append(dataBase64.substring(i * maxCharactersPerLine, (i * maxCharactersPerLine) + maxCharactersPerLine));
			returnString.append(splitCharacters);
		}
		returnString.append(dataBase64.substring(fullLines * 64, (fullLines * 64) + (dataBase64.length() % 64)));
		return returnString.toString();
	}

	/**
	 * Decode a Base64 String
	 *
	 * @param base64String
	 * @return
	 */
	public static byte[] decodeBase64(final String base64String) {
		return Base64.getDecoder().decode(base64String.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Check a simple name string
	 *
	 * @param value
	 * @return
	 */
	public static boolean checkForValidUserName(final String value) {
		return value != null && value.matches("[A-Za-z0-9_-]+");
	}

	/**
	 * Convert an ArrayList of Strings to a StringArray
	 *
	 * @param pArrayListOfStrings
	 * @return
	 */
	public static String[] convertArrayListOfStringsToStringArray(final ArrayList<String> pArrayListOfStrings) {
		return pArrayListOfStrings.toArray(new String[0]);
	}

	/**
	 * Get index of an Integer within an Array of Integers
	 *
	 * @param searchInt
	 * @param intArray
	 * @return
	 */
	public static int getIndex(final int searchInt, final int[] intArray) {
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
	public static byte[] readStreamUntilEndOrLinefeed(final InputStream inStream) throws IOException {
		final ByteArrayOutputStream returnData = new ByteArrayOutputStream();
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

	/***
	 * Split a list into smaller lists to a maximum chunkSize
	 *
	 * @param originalList
	 * @param chunkSize
	 * @return
	 */
	public static <E> List<List<E>> chopListToChunks(final List<E> originalList, final int chunkSize) {
		if (originalList == null || originalList.size() <= 0 || chunkSize <= 0) {
			return null;
		}

		final List<List<E>> returnList = new ArrayList<>();
		int endIndex = 0;

		while (endIndex < originalList.size()) {
			final int startIndex = endIndex;
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
	public static int getRandomNumber(final int excludedMaximum) {
		return random.nextInt(excludedMaximum);
	}

	/**
	 * Generate a random string of given size
	 *
	 * @param length
	 * @return
	 */
	public static String getRandomString(final int length) {
		final StringBuilder sb = new StringBuilder(length);
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
	public static String getRandomAlphanumericString(final int length) {
		final StringBuilder sb = new StringBuilder(length);
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
	public static String getRandomNumberString(final int length) {
		final StringBuilder sb = new StringBuilder(length);
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
		final byte[] result = new byte[1];
		random.nextBytes(result);
		return result[0];
	}

	/**
	 * Generate a random byteArray
	 *
	 * @param arrayToFill
	 * @return
	 */
	public static byte[] getRandomByteArray(final byte[] arrayToFill) {
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
	public static boolean checkForIntervalContainment(final String intervals, final int item) {
		if (intervals != null && intervals.length() > 0) {
			final String[] blockStrings = intervals.split(";");
			for (final String blockString : blockStrings) {
				if (blockString.endsWith("+")) {
					if (Integer.parseInt(blockString.substring(0, blockString.length() - 1)) <= item) {
						return true;
					}
				} else if (blockString.matches("\\d+-\\d+")) {
					final int plusIndex = blockString.indexOf("-");
					final int startVersion = Integer.parseInt(blockString.substring(0, plusIndex));
					final int endeVersion = Integer.parseInt(blockString.substring(plusIndex + 1));
					if (startVersion <= item && endeVersion >= item) {
						return true;
					}
				} else if (blockString.matches("-\\d+")) {
					final int endeVersion = Integer.parseInt(blockString.substring(1));
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
	public static int getMinimumOfAllowedValues(final int allowedValueMinimum, final int... values) {
		int returnValue = Integer.MAX_VALUE;
		if (values != null) {
			for (final int value : values) {
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
			return "true".equalsIgnoreCase(value)
					|| "+".equalsIgnoreCase(value)
					|| "1".equalsIgnoreCase(value)
					|| "yes".equalsIgnoreCase(value)
					|| "y".equalsIgnoreCase(value)
					|| "ja".equalsIgnoreCase(value)
					|| "j".equalsIgnoreCase(value)
					|| "ok".equalsIgnoreCase(value)
					|| "on".equalsIgnoreCase(value)
					|| "an".equalsIgnoreCase(value);
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
	public static boolean anyCharsAreEqual(final char... values) {
		for (int i = 0; i < values.length; i++) {
			for (int j = i + 1; j < values.length; j++) {
				if (values[i] == values[j]) {
					return true;
				}
			}
		}
		return false;
	}

	public static boolean contains(final char[] characterArray, final Character searchCharacter) {
		if (characterArray == null || searchCharacter == null) {
			return false;
		}

		for (final char character : characterArray) {
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
	public static int square(final int value) {
		return value * value;
	}

	/**
	 * Math power
	 *
	 * @param base
	 * @param exp
	 * @return
	 */
	public static int pow(final int base, final int exp) {
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
	public static <T extends Comparable<? super T>> List<T> asSortedList(final Collection<T> c) {
		final List<T> list = new ArrayList<>(c);
		Collections.sort(list);
		return list;
	}

	/**
	 * Get a collection like a set as a ordered list
	 *
	 * @param c
	 * @return
	 */
	@SafeVarargs
	public static <T extends Comparable<? super T>> List<T> sortButPutItemsFirst(final Collection<T> collection, final T... firstItems) {
		final List<T> firstItemsList = new ArrayList<>(Arrays.asList(firstItems));
		final List<T> list = new ArrayList<>(collection);
		Collections.sort(list, new Comparator<T>() {
			@Override
			public int compare(final T o1, final T o2) {
				if (o1.equals(o2)) {
					return 0;
				} else if (firstItemsList.contains(o1)) {
					if (firstItemsList.contains(o2)) {
						return firstItemsList.indexOf(o1) < firstItemsList.indexOf(o2) ? -1 : 1;
					} else {
						return -1;
					}
				} else if (firstItemsList.contains(o2)) {
					return 1;
				} else {
					return o1.compareTo(o2);
				}
			}
		});
		return list;
	}

	/**
	 * Sort a map by a Comparator for the keytype
	 *
	 * @param mapToSort
	 * @param comparator
	 * @return
	 */
	public static <Key, Value> Map<Key, Value> sortMap(final Map<Key, Value> mapToSort, final Comparator<Key> comparator) {
		final List<Key> keys = new ArrayList<>(mapToSort.keySet());
		Collections.sort(keys, comparator);
		final LinkedHashMap<Key, Value> sortedContent = new LinkedHashMap<>();
		for (final Key key : keys) {
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
	public static <Value> Map<String, Value> sortMap(final Map<String, Value> mapToSort) {
		final List<String> keys = new ArrayList<>(mapToSort.keySet());
		Collections.sort(keys);
		final LinkedHashMap<String, Value> sortedContent = new LinkedHashMap<>();
		for (final String key : keys) {
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
		final ClassLoader sysClassLoader = ClassLoader.getSystemClassLoader();
		final URL[] urls = ((URLClassLoader) sysClassLoader).getURLs();

		final StringBuilder classpath = new StringBuilder();
		for (final URL url : urls) {
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
	public static <T> boolean compare(final T[] array1, final T[] array2) {
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
		final Map<String, String> propertiesMap = new HashMap<>();
		for (final Object key : System.getProperties().keySet()) {
			propertiesMap.put((String) key, System.getProperties().getProperty((String) key));
		}
		if (SystemUtilities.isLinuxSystem()) {
			final File distributionInfoFile = new File("/etc/os-release");
			if (distributionInfoFile.exists()) {
				try (FileInputStream inputStream = new FileInputStream(distributionInfoFile)) {
					final Properties distributionInfoProperties = new Properties();
					distributionInfoProperties.load(inputStream);
					final String distributionName = distributionInfoProperties.getProperty("NAME");
					if (distributionName != null && !"".equals(distributionName.trim())) {
						propertiesMap.put("os.distribution.name",  Utilities.trimSimultaneously(distributionName, "\""));
					} else {
						propertiesMap.put("os.distribution.name", "Unknown");
					}
					final String distributionVersion = distributionInfoProperties.getProperty("VERSION");
					if (distributionVersion != null && !"".equals(distributionVersion.trim())) {
						propertiesMap.put("os.distribution.version",  Utilities.trimSimultaneously(distributionVersion, "\""));
					} else {
						propertiesMap.put("os.distribution.version", "Unknown");
					}
				} catch (@SuppressWarnings("unused") final Exception e) {
					propertiesMap.put("os.distribution.name", "Unknown");
					propertiesMap.put("os.distribution.version", "Unknown");
				}
			} else {
				propertiesMap.put("os.distribution.name", "Unknown");
				propertiesMap.put("os.distribution.version", "Unknown");
			}
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
	public static String getStringFromMap(final Map<String, ? extends Object> map, final String entrySeparator, final String keySeparator, final boolean sort) {
		final List<String> keyList = new ArrayList<>(map.keySet());
		if (sort) {
			Collections.sort(keyList, new Comparator<String>() {
				@Override
				public int compare(final String o1, final String o2) {
					return Comparator.nullsFirst(String::compareTo).compare(o1, o2);
				}
			});
		}

		final StringBuilder builder = new StringBuilder();
		for (final String key : keyList) {
			if (builder.length() > 0) {
				builder.append(entrySeparator);
			}
			builder.append(key == null ? "" : key);
			builder.append(keySeparator);
			final Object value = map.get(key);
			builder.append(value == null ? "" : value.toString());
		}
		return builder.toString();
	}

	/**
	 * Make a number with unitsign human readable
	 *
	 * @param value
	 * @param unitTypeSign
	 * @param siUnits
	 * @return
	 */
	public static String getHumanReadableNumber(final Number value, final String unitTypeSign, final boolean siUnits, final int amountOfSignifiantDigits, final boolean keepTrailingZeros, final Locale locale) {
		final int unit = siUnits ? 1000 : 1024;
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
			final int exp = (int) (Math.log(interimValue) / Math.log(unit));
			unitExtension = " " + (siUnits ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (siUnits ? "" : "i");
			if (isNotBlank(unitTypeSign)) {
				unitExtension += unitTypeSign;
			}
			interimValue = interimValue / Math.pow(unit, exp);
		}

		final DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(locale);
		DecimalFormat numberFormat;
		if (keepTrailingZeros) {
			if (interimValue >= 1000) {
				numberFormat = new DecimalFormat("#0." + repeat("0", amountOfSignifiantDigits - 4), decimalFormatSymbols);
			} else if (interimValue >= 100) {
				numberFormat = new DecimalFormat("#0." + repeat("0", amountOfSignifiantDigits - 3), decimalFormatSymbols);
			} else if (interimValue >= 10) {
				numberFormat = new DecimalFormat("#0." + repeat("0", amountOfSignifiantDigits - 2), decimalFormatSymbols);
			} else if (interimValue >= 1) {
				numberFormat = new DecimalFormat("#0." + repeat("0", amountOfSignifiantDigits - 1), decimalFormatSymbols);
			} else {
				numberFormat = new DecimalFormat("#0." + repeat("0", amountOfSignifiantDigits), decimalFormatSymbols);
			}
		} else {
			if (interimValue >= 1000) {
				numberFormat = new DecimalFormat("#0.0" + repeat("#", amountOfSignifiantDigits - 5), decimalFormatSymbols);
			} else if (interimValue >= 100) {
				numberFormat = new DecimalFormat("#0.0" + repeat("#", amountOfSignifiantDigits - 4), decimalFormatSymbols);
			} else if (interimValue >= 10) {
				numberFormat = new DecimalFormat("#0.0" + repeat("#", amountOfSignifiantDigits - 3), decimalFormatSymbols);
			} else if (interimValue >= 1) {
				numberFormat = new DecimalFormat("#0.0" + repeat("#", amountOfSignifiantDigits - 2), decimalFormatSymbols);
			} else {
				numberFormat = new DecimalFormat("#0.0" + repeat("#", amountOfSignifiantDigits - 1), decimalFormatSymbols);
			}
		}

		return numberFormat.format(interimValue) + unitExtension;
	}

	/**
	 * Make an integer with unitsign human readable and keep all digits
	 *
	 * @param value
	 * @param unitTypeSign
	 * @param locale
	 * @return
	 */
	public static String getHumanReadableInteger(final Long value, final String unitTypeSign, final Locale locale) {
		final double interimValue = value.doubleValue();
		String unitExtension = "";
		if (isNotBlank(unitTypeSign)) {
			unitExtension = " " + unitTypeSign;
		}

		final DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(locale);
		final DecimalFormat numberFormat = new DecimalFormat("###,##0", decimalFormatSymbols);

		return numberFormat.format(interimValue) + unitExtension;
	}

	/**
	 * Generate MD5 from string data
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] getMD5Hash(final String data) throws Exception {
		try {
			return MessageDigest.getInstance("MD5").digest(data.getBytes(StandardCharsets.UTF_8));
		} catch (final Exception e) {
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
	public static byte[] getSHA1Hash(final String data) throws Exception {
		try {
			return MessageDigest.getInstance("SHA-1").digest(data.getBytes(StandardCharsets.UTF_8));
		} catch (final Exception e) {
			throw new Exception("Error while SHA-1 hashing", e);
		}
	}

	/**
	 * Generate SHA-256 from string data
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] getSHA256Hash(final String data) throws Exception {
		try {
			return MessageDigest.getInstance("SHA-256").digest(data.getBytes(StandardCharsets.UTF_8));
		} catch (final Exception e) {
			throw new Exception("Error while SHA-256 hashing", e);
		}
	}

	/**
	 * Generate SHA-384 from string data
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] getSHA384Hash(final String data) throws Exception {
		try {
			return MessageDigest.getInstance("SHA-384").digest(data.getBytes(StandardCharsets.UTF_8));
		} catch (final Exception e) {
			throw new Exception("Error while SHA-384 hashing", e);
		}
	}

	/**
	 * Generate SHA-512 from string data
	 *
	 * @param data
	 * @return
	 * @throws Exception
	 */
	public static byte[] getSHA512Hash(final String data) throws Exception {
		try {
			return MessageDigest.getInstance("SHA-512").digest(data.getBytes(StandardCharsets.UTF_8));
		} catch (final Exception e) {
			throw new Exception("Error while SHA-512 hashing", e);
		}
	}

	/**
	 * Get bytearray for list of bytes
	 *
	 * @param data
	 * @return
	 */
	public static byte[] getByteArray(final List<Byte> data) {
		final byte[] returnArray = new byte[data.size()];
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
	public static String stacktraceToString(final StackTraceElement[] stackTrace) {
		final StringBuilder returnBuilder = new StringBuilder();
		if (stackTrace != null) {
			for (final StackTraceElement stackTraceElement : stackTrace) {
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
	public static void downloadFile(final String url, final String localeDestionationPath) throws Exception {
		try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new URL(url).openStream());
				FileOutputStream fileOutputStream = new FileOutputStream(localeDestionationPath)) {
			IoUtilities.copy(bufferedInputStream, fileOutputStream);
		} catch (final Exception e) {
			throw new Exception("Cannot download file", e);
		}
	}

	/**
	 * Check array for duplicate strings
	 *
	 * @param inputArray
	 * @param ignoreNullValues
	 * @return
	 */
	public static boolean checkForDuplicates(final String[] inputArray, final boolean ignoreNullValues) {
		final Set<String> tempSet = new HashSet<>();
		for (final String stringItem : inputArray) {
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
	public static <T> List<T> selectItems(final Collection<?> collection, final Class<T> classToSelect) {
		final List<T> list = new ArrayList<>();
		for (final Object item : collection) {
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
	public static <T> List<T> selectItems(final Object[] array, final Class<T> classToSelect) {
		final List<T> list = new ArrayList<>();
		for (final Object item : array) {
			if (classToSelect.isInstance(item)) {
				list.add((T) item);
			}
		}
		return list;
	}

	public static String getDomainFromUrl(final String url) throws Exception {
		final URI uri = new URI(url);
		final String domain = uri.getHost();
		return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

	public static boolean isEmpty(final String value) {
		return value == null || value.length() == 0;
	}

	public static boolean isNotEmpty(final String value) {
		return !isEmpty(value);
	}

	public static boolean isEmpty(final Collection<?> collection) {
		return collection == null || collection.isEmpty();
	}

	public static boolean isNotEmpty(final Collection<?> collection) {
		return !isEmpty(collection);
	}

	public static boolean isBlank(final String value) {
		return value == null || value.length() == 0 || value.trim().length() == 0;
	}

	public static boolean isNotBlank(final String value) {
		return !isBlank(value);
	}

	public static boolean isEmpty(final char[] value) {
		return value == null || value.length == 0;
	}

	public static void clear(final char[] array) {
		if (array != null) {
			Arrays.fill(array, (char) 0);
		}
	}

	public static void clear(final byte[] array) {
		if (array != null) {
			Arrays.fill(array, (byte) 0);
		}
	}

	public static boolean isNotEmpty(final char[] value) {
		return !isEmpty(value);
	}

	public static boolean isBlank(final char[] value) {
		if (value == null || value.length == 0) {
			return true;
		} else {
			for (final char character : value) {
				if (!Character.isWhitespace(character)) {
					return false;
				}
			}
			return true;
		}
	}

	public static boolean isNotBlank(final char[] value) {
		return !isBlank(value);
	}

	public static void closeQuietly(final Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (@SuppressWarnings("unused") final IOException e) {
				// Do nothing
			}
		}
	}

	/**
	 * XMLStreamReader.close() doesn't close the underlying stream.
	 * So it must be closed separately.
	 */
	public static void closeQuietly(final XMLStreamReader closeable, final InputStream inputStream) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (@SuppressWarnings("unused") final Exception e) {
				// Do nothing
			}
		}
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (@SuppressWarnings("unused") final Exception e) {
				// Do nothing
			}
		}
	}

	public static void closeQuietly(XMLStreamWriter xmlWriter) {
		if (xmlWriter != null) {
			try {
				xmlWriter.close();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			xmlWriter = null;
		}
	}

	public static String repeat(final char valueChar, final int count) {
		return repeat(Character.toString(valueChar), count, null);
	}

	public static String repeat(final String value, final int count) {
		return repeat(value, count, null);
	}

	public static String repeat(final String value, final int count, final String separatorString) {
		if (value == null) {
			return null;
		} else if (value.length() == 0 || count == 0) {
			return "";
		} else {
			final StringBuilder returnValue = new StringBuilder();
			for (int i = 0; i < count; i++) {
				if (separatorString != null && returnValue.length() > 0) {
					returnValue.append(separatorString);
				}
				returnValue.append(value);
			}
			return returnValue.toString();
		}
	}

	public static String join(final Object[] array, String glue) {
		if (array == null) {
			return null;
		} else if (array.length == 0) {
			return "";
		} else {
			if (glue == null) {
				glue = "";
			}

			final StringBuilder returnValue = new StringBuilder();
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

	public static String join(final Iterable<?> iterableObject, String glue) {
		if (iterableObject == null) {
			return null;
		} else {
			if (glue == null) {
				glue = "";
			}

			final StringBuilder returnValue = new StringBuilder();
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

	public static <T> T[] remove(final T[] array, final T itemToRemove) {
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
			final T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);
			if (indexToRemove > 0) {
				System.arraycopy(array, 0, result, 0, indexToRemove);
			}
			if (indexToRemove < array.length - 1) {
				System.arraycopy(array, indexToRemove + 1, result, indexToRemove, array.length - indexToRemove - 1);
			}
			return result;
		} else {
			@SuppressWarnings("unchecked")
			final T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length);
			System.arraycopy(array, 0, result, 0, array.length);
			return result;
		}
	}

	public static <T> T[] removeItemAtIndex(final T[] array, final int itemIndexToRemove) {
		@SuppressWarnings("unchecked")
		final T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);
		for (int i = 0; i < array.length; i++) {
			if (i < itemIndexToRemove) {
				result[i] = array[i];
			} else if (i > itemIndexToRemove) {
				result[i - 1] = array[i];
			}
		}
		return result;
	}

	public static byte[] readFileToByteArray(final File file) throws FileNotFoundException, IOException {
		try (FileInputStream in = new FileInputStream(file)) {
			final byte[] returnArray = new byte[(int) file.length()];
			in.read(returnArray);
			return returnArray;
		}
	}

	/**
	 * Append blanks at the left of a string to make if fit the given minimum
	 *
	 * @param escapedValue
	 * @param i
	 * @return
	 */
	public static String leftPad(final String value, final int minimumLength) {
		try {
			return String.format("%1$" + minimumLength + "s", value);
		} catch (@SuppressWarnings("unused") final Exception e) {
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
	public static String rightPad(final String value, final int minimumLength) {
		try {
			return String.format("%1$-" + minimumLength + "s", value);
		} catch (@SuppressWarnings("unused") final Exception e) {
			return value;
		}
	}

	/**
	 * Only trim the value when the sourrounding occures on both ends
	 * @param value
	 * @param prefix
	 * @return
	 */
	public static String trimSimultaneously(final String value, final String sourrounding) {
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

	public static String trim(final String value) {
		if (value == null) {
			return null;
		} else {
			return value.trim();
		}
	}

	public static String trim(String value, final char trimChar) {
		while (value != null && value.startsWith(Character.toString(trimChar))) {
			value = value.substring(1);
		}

		while (value != null && value.endsWith(Character.toString(trimChar))) {
			value = value.substring(0, value.length() - 1);
		}

		return value;
	}

	public static String toString(final InputStream inputStream, final Charset encoding) throws IOException {
		return new String(IoUtilities.toByteArray(inputStream), encoding);
	}

	public static Object toString(final Reader characterStream) throws IOException {
		final StringBuilder returnValue = new StringBuilder();
		int characterInt;
		while ((characterInt = characterStream.read()) > -1) {
			returnValue.append((char) characterInt);
		}
		return returnValue.toString();
	}

	public static List<String> readLines(final InputStream inStream, final Charset encoding) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, encoding))) {
			final List<String> lines = new ArrayList<>();
			String nextLine;
			while ((nextLine = reader.readLine()) != null) {
				lines.add(nextLine);
			}
			return lines;
		}
	}

	public static String trimLeft(final String value) {
		return value.replaceAll("^\\s+", "");
	}

	public static String trimRight(final String value) {
		return value.replaceAll("\\s+$", "");
	}

	public static void addFileToClasspath(final String filePath) throws IOException {
		try {
			final Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
			method.setAccessible(true);
			method.invoke(ClassLoader.getSystemClassLoader(), new Object[] { new URL(new File(filePath).toURI().toString()) });
		} catch (final Throwable t) {
			throw new IOException("Error, could not add file to system classloader: " + t.getMessage(), t);
		}
	}

	public static String shortenStringToMaxLengthCutRight(final String value, final int maxLength, final String cutSign) {
		if (value != null && value.length() > maxLength) {
			return value.substring(0, maxLength - 4) + cutSign;
		} else {
			return value;
		}
	}

	public static String shortenStringToMaxLengthCutRight(final String value, final int maxLength) {
		return shortenStringToMaxLengthCutRight(value, maxLength, " ...");
	}

	public static String shortenStringToMaxLengthCutMiddle(final String value, final int maxLength) {
		if (value != null && value.length() > maxLength) {
			final int leftLength = (maxLength - 5) / 2;
			return value.substring(0, leftLength) + " ... " + value.substring(value.length() - ((maxLength - leftLength) - 5));
		} else {
			return value;
		}
	}

	public static String shortenStringToMaxLengthCutLeft(final String value, final int maxLength) {
		if (value != null && value.length() > maxLength) {
			return "... " + value.substring((value.length() - maxLength) + 4);
		} else {
			return value;
		}
	}

	public static List<String> splitAndTrimList(final String stringList) {
		if (stringList == null) {
			return null;
		} else {
			final List<String> returnList = new ArrayList<>();
			final String[] parts = stringList.split(",|;|\\|| |\\n|\\r|\\t");
			for (final String part : parts) {
				if (isNotEmpty(part)) {
					returnList.add(part.trim());
				}
			}
			return returnList;
		}
	}

	public static List<String> splitAndTrimList(final String stringList, final Character... separatorChars) {
		if (stringList == null) {
			return null;
		} else {
			final List<String> returnList = new ArrayList<>();
			final String[] parts = stringList.split(join(separatorChars, "|").replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r"));
			for (final String part : parts) {
				if (isNotEmpty(part)) {
					returnList.add(part.trim());
				}
			}
			return returnList;
		}
	}

	public static boolean endsWithIgnoreCase(final String data, final String suffix) {
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

	public static int indexOfIgnoreCase(final String data, final String part) {
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

	public static List<String> splitAndTrimListQuoted(final String stringList, final char... separatorChars) {
		final List<String> returnList = new ArrayList<>();
		StringBuilder nextLine = new StringBuilder();
		boolean quotedBySingleQoute = false;
		boolean quotedByDoubleQoute = false;
		for (final char nextChar : stringList.toCharArray()) {
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
			for (final char separatorChar : separatorChars) {
				if (separatorChar == nextChar && !quotedBySingleQoute && !quotedByDoubleQoute) {
					final String line = nextLine.toString().trim();
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
		final String line = nextLine.toString().trim();
		if (line.length() > 0) {
			returnList.add(line);
		}
		return returnList;
	}

	public static boolean containsIgnoreCase(final Collection<String> list, final String item) {
		if (list == null) {
			return false;
		} else {
			for (final String listItem : list) {
				if (listItem == item || (listItem != null && listItem.equalsIgnoreCase(item))) {
					return true;
				}
			}
			return false;
		}
	}

	public static boolean delete(final File file) {
		if (file.isDirectory()) {
			for (final File subFile : file.listFiles()) {
				if (!delete(subFile)) {
					return false;
				}
			}
		}
		return file.delete();
	}

	public static int limitValue(final int minimum, final int value, final int maximum) {
		if (value < minimum) {
			return minimum;
		} else if (maximum < value) {
			return maximum;
		} else {
			return value;
		}
	}

	public static boolean startsWithCaseinsensitive(final String data, final String prefix) {
		if (data == null || prefix == null) {
			return false;
		} else {
			return data.toLowerCase().startsWith(prefix.toLowerCase());
		}
	}

	public static Map<String, String> createMap(final String... data) throws Exception {
		final Map<String, String> returnMap = new HashMap<>();
		if (data != null && data.length > 0) {
			if (data.length % 2 != 0) {
				throw new Exception("Invalid map data: odd number of parameters, must be even");
			} else {
				for (int i = 0; i < data.length / 2; i++) {
					final String key = data[i * 2];
					final String value = data[i * 2 + 1];
					returnMap.put(key, value);
				}
			}
		}
		return returnMap;
	}

	/**
	 * Check whether an iterable collection contains a special object
	 *
	 * @param hayshack
	 * @param needle
	 * @return
	 */
	public static boolean containsObject(final Iterable<?> hayshack, final Object needle) {
		for (final Object item : hayshack) {
			if (item == needle) {
				return true;
			}
		}
		return false;
	}

	public static String replaceUsersHome(final String filePath) {
		if (filePath == null) {
			return filePath;
		}
		final String homePath = System.getProperty("user.home");
		return filePath
				.replace("~", homePath)
				.replace("$HOME", homePath)
				.replace("${HOME}", homePath);
	}

	public static String replaceUsersHomeByTilde(final String filePath) {
		if (filePath == null) {
			return filePath;
		}
		final String homePath = System.getProperty("user.home");
		return filePath.replace(homePath, "~");
	}

	public static String substring(final String text, final int startIndex) {
		if (text == null) {
			return null;
		} else {
			if (text.length() < startIndex) {
				return "";
			} else {
				return text.substring(startIndex);
			}
		}
	}

	public static String substring(final String text, final int startIndex, final int endIndex) {
		if (text == null) {
			return null;
		} else {
			if (text.length() < startIndex) {
				return "";
			} else if (text.length() < endIndex) {
				return text.substring(startIndex);
			} else {
				return text.substring(startIndex, endIndex);
			}
		}
	}

	public static boolean contains(final String text, final String searchText) {
		if (text == null) {
			return false;
		} else {
			return text.contains(searchText);
		}
	}

	public static long skipInputStreamData(final InputStream inputStream, final long bytesToSkip) throws Exception {
		long bytesSkipped = 0;
		while (bytesSkipped < bytesToSkip) {
			final long newBytesSkipped = inputStream.skip(bytesToSkip - bytesSkipped);
			if (newBytesSkipped == 0) {
				throw new Exception("Cannot skip data while reading stream");
			}
			bytesSkipped += newBytesSkipped;
		}
		return bytesSkipped;
	}

	public static byte[] convertIntToByteArray(final int value) {
		return new byte[] { (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value };
	}

	public static int convertByteArrayToInt(final byte[] bytes) throws Exception {
		if (bytes.length > 4) {
			throw new Exception("ByteArray is too big for int value");
		} else {
			return ByteBuffer.wrap(bytes).getInt();
		}
	}

	public static String[] split(final String text, final char delimiterChar, final char escapeChar, final int limit) {
		final String escapeCharString = Character.toString(escapeChar);
		final String delimiterCharString = Character.toString(delimiterChar);
		final String splitRegex = "(?<!" + Pattern.quote(escapeCharString) + ")" + Pattern.quote(delimiterCharString);
		final String[] returnParts = text.split(splitRegex, limit);
		for (int i = 0; i < returnParts.length; i++) {
			returnParts[i] = returnParts[i].replace(escapeCharString + delimiterCharString, escapeCharString);
		}
		return returnParts;
	}

	public static String[] split(final String text, final char delimiterChar, final char escapeChar) {
		final String escapeCharString = Character.toString(escapeChar);
		final String delimiterCharString = Character.toString(delimiterChar);
		final String splitRegex = "(?<!" + Pattern.quote(escapeCharString) + ")" + Pattern.quote(delimiterCharString);
		final String[] returnParts = text.split(splitRegex);
		for (int i = 0; i < returnParts.length; i++) {
			returnParts[i] = returnParts[i].replace(escapeCharString + delimiterCharString, escapeCharString);
		}
		return returnParts;
	}

	public static List<String> parseArguments(final String argumentLine) throws Exception {
		final List<String> returnList = new ArrayList<>();
		StringBuilder currentArgument = new StringBuilder();
		Character enclosingChar = null;
		boolean escapedNextChar = false;
		for (final char nextChar : argumentLine.replace("\r\n", "\n").replace("\r", "\n").toCharArray()) {
			if (enclosingChar != null) {
				if (escapedNextChar) {
					if (nextChar != enclosingChar) {
						currentArgument.append("\\");
					}
					currentArgument.append(nextChar);
					escapedNextChar = false;
				} else if (nextChar == '\\') {
					escapedNextChar = true;
				} else if (nextChar == enclosingChar) {
					returnList.add(currentArgument.toString());
					enclosingChar = null;
					currentArgument = new StringBuilder();
				} else {
					currentArgument.append(nextChar);
				}
			} else {
				if (nextChar == '\'' || nextChar == '"') {
					if (currentArgument.length() > 0) {
						returnList.add(currentArgument.toString());
						currentArgument = new StringBuilder();
					}
					enclosingChar = nextChar;
				} else if (nextChar == ' ' || nextChar == '\n' || nextChar == '\t') {
					if (currentArgument.length() > 0) {
						returnList.add(currentArgument.toString());
						currentArgument = new StringBuilder();
					}
				} else {
					currentArgument.append(nextChar);
				}
			}
		}
		if (enclosingChar != null) {
			throw new Exception("Invalid quotation. Missing closing " + enclosingChar);
		} else if (currentArgument.length() > 0) {
			returnList.add(currentArgument.toString());
			currentArgument = new StringBuilder();
		}
		return returnList;
	}

	public static List<String> parseTokens(final String line, final char... delimiters) throws Exception {
		final List<String> returnList = new ArrayList<>();
		StringBuilder currentArgument = new StringBuilder();
		Character enclosingChar = null;
		boolean escapedNextChar = false;
		for (final char nextChar : line.replace("\r\n", "\n").replace("\r", "\n").toCharArray()) {
			if (enclosingChar != null) {
				if (escapedNextChar) {
					if (nextChar != enclosingChar) {
						currentArgument.append("\\");
					}
					currentArgument.append(nextChar);
					escapedNextChar = false;
				} else if (nextChar == '\\') {
					escapedNextChar = true;
				} else if (nextChar == enclosingChar) {
					returnList.add(currentArgument.toString());
					enclosingChar = null;
					currentArgument = new StringBuilder();
				} else {
					currentArgument.append(nextChar);
				}
			} else {
				if (nextChar == '\'' || nextChar == '"') {
					if (currentArgument.length() > 0) {
						returnList.add(currentArgument.toString());
						currentArgument = new StringBuilder();
					}
					enclosingChar = nextChar;
				} else if (equalsAnyChar(nextChar, delimiters)) {
					if (currentArgument.length() > 0) {
						returnList.add(currentArgument.toString());
						currentArgument = new StringBuilder();
					}
				} else {
					currentArgument.append(nextChar);
				}
			}
		}
		if (enclosingChar != null) {
			throw new Exception("Invalid quotation. Missing closing " + enclosingChar);
		} else if (currentArgument.length() > 0) {
			returnList.add(currentArgument.toString());
			currentArgument = new StringBuilder();
		}
		return returnList;
	}

	public static boolean equalsAnyChar(final char checkChar, final char... compareChars) {
		for (final char compareChar : compareChars) {
			if (checkChar == compareChar) {
				return true;
			}
		}
		return false;
	}

	public static List<String> getList(final String... items) {
		final List<String> returnList = new ArrayList<>();
		for (final String item : items) {
			returnList.add(item);
		}
		return returnList;
	}

	public static boolean isZipArchiveFile(final File potentialZipFile) throws FileNotFoundException, IOException {
		try (FileInputStream inputStream = new FileInputStream(potentialZipFile)) {
			final byte[] magicBytes = new byte[4];
			final int readBytes = inputStream.read(magicBytes);
			return readBytes == 4 && magicBytes[0] == 0x50 && magicBytes[1] == 0x4B && magicBytes[2] == 0x03 && magicBytes[3] == 0x04;
		}
	}

	public static List<String> getFilepathsFromZipArchiveFile(final File zipFile) throws FileNotFoundException, IOException {
		try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile))) {
			final List<String> returnList = new ArrayList<>();
			ZipEntry zipEntry;
			while((zipEntry = zipInputStream.getNextEntry()) != null) {
				returnList.add(zipEntry.getName());
			}
			return returnList;
		}
	}

	public static String getSystemTempDir() {
		return System.getProperty("java.io.tmpdir");
	}

	public static <T> T[] reverseArray(final T[] arrayData) {
		for (int i = 0; i < arrayData.length / 2; i++) {
			final int j = arrayData.length - 1 - i;
			final T tmp = arrayData[i];
			arrayData[i] = arrayData[j];
			arrayData[j] = tmp;
		}
		return arrayData;
	}

	public static int[] reverseArray(final int[] arrayData) {
		for (int i = 0; i < arrayData.length / 2; i++) {
			final int j = arrayData.length - 1 - i;
			final int tmp = arrayData[i];
			arrayData[i] = arrayData[j];
			arrayData[j] = tmp;
		}
		return arrayData;
	}

	public static byte[] reverseArray(final byte[] arrayData) {
		for (int i = 0; i < arrayData.length / 2; i++) {
			final int j = arrayData.length - 1 - i;
			final byte tmp = arrayData[i];
			arrayData[i] = arrayData[j];
			arrayData[j] = tmp;
		}
		return arrayData;
	}

	/**
	 * KeyStores saved with blank password should also contain certificates when opened with a null password.
	 * Special cacerts keystore of JDKs behave this way, because they are JKS keystores, but PKCS12 keystores do not behave this way.
	 * Those are fixed by this keystore copy job to also behave in the intended way to show their certificates when openend with null password.
	 *
	 * @param keyStoreFileWithNullPassword
	 * @throws Exception
	 */
	public static void convertPkcs12TrustStoreToJKS(final File keyStoreFileWithNullPassword) throws Exception {
		final KeyStore readKeyStore = KeyStore.getInstance("PKCS12");
		try (FileInputStream inputStream = new FileInputStream(keyStoreFileWithNullPassword)) {
			readKeyStore.load(inputStream, "".toCharArray());
		} catch (@SuppressWarnings("unused") final Exception e) {
			// KeyStore password is not blank, so do not fix anything
			return;
		}

		final KeyStore writeKeyStore = KeyStore.getInstance("JKS");
		writeKeyStore.load(null, null);

		// KeyStore password is null
		for (final String alias : Collections.list(readKeyStore.aliases())) {
			writeKeyStore.setCertificateEntry(alias, readKeyStore.getCertificate(alias));
		}
		keyStoreFileWithNullPassword.delete();
		try (FileOutputStream outputStream = new FileOutputStream(keyStoreFileWithNullPassword)) {
			writeKeyStore.store(outputStream, "".toCharArray());
		}
	}

	/**
	 * Fix the encoding of a String if it was stored in UTF-8 encoding but decoded with ISO-8859-1 encoding
	 *
	 * Examples of byte data of wrongly encoded Umlauts and other special characters:
	 *	Ä: [-61, -124]
	 *	ä: [-61, -92]
	 *	ß: [-61, -97]
	 *	è: [-61, -88]
	 */
	public static String fixStringEncodingIfNeeded(final String comment) {
		boolean wrongEncodingDetected = false;
		for (final byte nextByte : comment.getBytes(StandardCharsets.ISO_8859_1)) {
			if (nextByte == -61) {
				wrongEncodingDetected = true;
				break;
			}
		}
		if (wrongEncodingDetected) {
			return new String(comment.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
		} else {
			return comment;
		}
	}
}
