package de.soderer.utilities;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

public class FileUtilities {
	/**
	 * Read a directory and return all files fitting namestart and extension
	 *
	 * @param directory
	 * @param nameStart
	 * @param extension
	 * @return
	 */
	public static File[] getSubfilesByNameAndExtension(final File directory, final String nameStart, final String extension) {
		final List<File> files = new LinkedList<>();
		for (final File file : directory.listFiles()) {
			if (file.getName().startsWith(nameStart) && file.getName().endsWith("." + extension)) {
				files.add(file);
			}
		}
		return files.toArray(new File[0]);
	}

	/**
	 * Read a directory and return all files starting with a basename form a list of basenames
	 *
	 * @param basenameList
	 * @return
	 */
	public static ArrayList<File> getArrayOfFilesByStammname(final String basenameList) {
		return getArrayOfFilesByBasename(Arrays.asList(basenameList.split(";")));
	}

	/**
	 * Read a directory and return all files starting with a basename form a list of basenames
	 *
	 * @param basenameList
	 * @return
	 */
	public static ArrayList<File> getArrayOfFilesByBasename(final List<String> basenameList) {
		final ArrayList<File> resultList = new ArrayList<>();

		for (final String basename : basenameList) {
			final int unixLastIndexOfSeparator = basename.lastIndexOf("/");
			final int dosLastIndexOfSeparator = basename.lastIndexOf("\\");
			final int lastIndexOfSeparator = Math.max(unixLastIndexOfSeparator, dosLastIndexOfSeparator);
			final String suchVerzeichnis = basename.substring(0, lastIndexOfSeparator + 1);
			final String stammFileName = basename.substring(lastIndexOfSeparator + 1);

			final File searchPath = new File(suchVerzeichnis);
			if (searchPath.isDirectory()) {
				final File[] partList = searchPath.listFiles();
				for (int i = 0; i < partList.length; i++) {
					if (partList[i].isFile() && (stammFileName.endsWith("*") && partList[i].getName().startsWith(stammFileName.substring(0, stammFileName.length() - 1))
							|| !stammFileName.endsWith("*") && partList[i].getName().equals(stammFileName))) {
						if (!resultList.contains(partList[i])) {
							resultList.add(partList[i]);
						}
					}
				}
			}
		}

		return resultList;
	}

	/**
	 * Read a directory and return all files fitting to a regex pattern
	 *
	 * @param startDirectory
	 * @param patternString
	 * @param traverseCompletely
	 * @return
	 */
	public static List<File> getFilesByPattern(final File startDirectory, final String patternString, final boolean traverseCompletely) {
		return getFilesByPattern(startDirectory, Pattern.compile(patternString), traverseCompletely);
	}

	/**
	 * Read a directory and return all files fitting to a regex pattern
	 *
	 * @param startDirectory
	 * @param pattern
	 * @param traverseCompletely
	 * @return
	 */
	public static List<File> getFilesByPattern(final File startDirectory, final Pattern pattern, final boolean traverseCompletely) {
		final List<File> files = new ArrayList<>();
		if (startDirectory.isDirectory()) {
			for (final File file : startDirectory.listFiles()) {
				if (file.isDirectory() && traverseCompletely) {
					if (traverseCompletely) {
						files.addAll(getFilesByPattern(file, pattern, traverseCompletely));
					}
				} else if (file.isFile() && pattern.matcher(file.getName()).matches()) {
					files.add(file);
				}
			}
		}
		return files;
	}

	/**
	 * Get all recursive subfiles of a directory
	 *
	 * @param directory
	 * @return
	 * @throws Exception
	 */
	public static List<File> getAllSubfiles(final File directory) throws Exception {
		try {
			final List<File> files = new LinkedList<>();
			final File[] subFiles = directory.listFiles();
			if (subFiles != null) {
				for (final File file : subFiles) {
					if (file.isDirectory()) {
						files.addAll(getAllSubfiles(file));
					} else {
						files.add(file);
					}
				}
			}
			return files;
		} catch (final Exception e) {
			throw new Exception("Error reading subfiles of: " + directory.getAbsolutePath(), e);
		}
	}

	/**
	 * Get number of all recursive subfiles of a directory
	 *
	 * @param directory
	 * @return
	 * @throws Exception
	 */
	public static int getAllSubfilesNumber(final File directory) throws Exception {
		try {
			int fileCount = 0;
			final File[] subFiles = directory.listFiles();
			if (subFiles != null) {
				for (final File file : subFiles) {
					if (file.isDirectory()) {
						fileCount += getAllSubfilesNumber(file);
					} else {
						fileCount++;
					}
				}
			}
			return fileCount;
		} catch (final Exception e) {
			throw new Exception("Error reading subfilenumber of: " + directory.getAbsolutePath(), e);
		}
	}

	/**
	 * Get size in bytes of all files and all descending subdirectories of a directory
	 *
	 * @param directory
	 * @return
	 * @throws Exception
	 */
	public static long getAllSubfilesSize(final File directory) throws Exception {
		try {
			long fileSizeSum = 0;
			final File[] subFiles = directory.listFiles();
			if (subFiles != null) {
				for (final File file : subFiles) {
					if (file.isDirectory()) {
						fileSizeSum += getAllSubfilesSize(file);
					} else {
						fileSizeSum += file.length();
					}
				}
			}
			return fileSizeSum;
		} catch (final Exception e) {
			throw new Exception("Error reading subfilesize of: " + directory.getAbsolutePath(), e);
		}
	}

	/**
	 * Get number of lines of a textfile
	 *
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int getLineCount(final File file) throws IOException {
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(file);
			if (Utilities.endsWithIgnoreCase(file.getName(), ".zip")) {
				inputStream = new ZipInputStream(inputStream);
				((ZipInputStream) inputStream).getNextEntry();
			}

			try (LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(inputStream))) {
				while (lineNumberReader.readLine() != null) {
					// do nothing
				}

				return lineNumberReader.getLineNumber();
			}
		} finally {
			Utilities.closeQuietly(inputStream);
		}
	}

	public static void write(final File file, final byte[] data) throws Exception {
		try (FileOutputStream out = new FileOutputStream(file)) {
			out.write(data);
		}
	}

	public static void writeStringToFile(final File file, final String dataString, final Charset encoding) throws Exception {
		write(file, dataString.getBytes(encoding));
	}

	public static byte[] readFileToByteArray(final File dataFile) throws Exception {
		ByteArrayOutputStream output = null;
		try (FileInputStream input = new FileInputStream(dataFile)) {
			output = new ByteArrayOutputStream();
			IoUtilities.copy(input, output);
			return output.toByteArray();
		} catch (final Exception e) {
			throw e;
		}
	}

	public static String readFileToString(final File dataFile, final Charset encoding) throws Exception {
		return new String(readFileToByteArray(dataFile), encoding);
	}

	public static List<String> readLinesFromFile(final File textFile, final boolean keepLinebreakInLines, final Charset encoding) throws Exception {
		try (InputStream inputStream = new BufferedInputStream(new FileInputStream(textFile))) {
			return readLinesFromStream(inputStream, keepLinebreakInLines, encoding);
		}
	}

	public static List<String> readLinesFromStream(final InputStream inputStream, final boolean keepLinebreakInLines, final Charset encoding) throws Exception {
		final List<String> dataLines = new ArrayList<>();
		final ByteArrayOutputStream readData = new ByteArrayOutputStream();
		int nextByte;
		int lastByte = 0;
		while ((nextByte = inputStream.read()) != -1) {
			boolean isNewLine = false;
			if (nextByte == '\r') {
				// new linebreak (Mac style, possibly Windows style)
				if (keepLinebreakInLines) {
					readData.write(nextByte);
				}
				isNewLine = true;
			} else if (nextByte == '\n') {
				if (lastByte == '\r') {
					// second part of linebreak (Windows style)
					// ignore but extend latest page end index
					if (keepLinebreakInLines) {
						dataLines.set(dataLines.size() - 1, dataLines.get(dataLines.size() - 1) + '\r');
					}
				} else {
					// new linebreak (Unix style)
					if (keepLinebreakInLines) {
						readData.write(nextByte);
					}
					isNewLine = true;
				}
			} else {
				readData.write(nextByte);
			}

			if (isNewLine) {
				dataLines.add(new String(readData.toByteArray(), encoding));
				readData.reset();
			}

			lastByte = nextByte;
		}

		if (readData.size() > 0) {
			dataLines.add(new String(readData.toByteArray(), encoding));
			readData.reset();
		}

		return dataLines;
	}

	public static void append(final File fileToAppend, final String textToAppend, final Charset encoding) throws Exception {
		try (OutputStream os = new FileOutputStream(fileToAppend, true)) {
			os.write(textToAppend.getBytes(encoding));
		}
	}

	public static void append(final File file, final byte[] data) throws Exception {
		try (FileOutputStream out = new FileOutputStream(file, true)) {
			out.write(data);
		}
	}

	public static String trimLeadingFileSeparator(final String path) {
		if (path != null && path.startsWith("/")) {
			return path.substring(1);
		} else {
			return path;
		}
	}

	public static String trimTrailingFileSeparator(final String path) {
		if (path != null && path.endsWith("/")) {
			return path.substring(0, path.length() - 1);
		} else {
			return path;
		}
	}

	public static String getFilenameFromPath(final String path) {
		if (path != null && path.contains("/")) {
			return path.substring(path.lastIndexOf("/") + 1);
		} else {
			return path;
		}
	}

	public static String getParentFromPath(final String path) {
		if (path != null && path.contains("/")) {
			final String returnPath = path.substring(0, path.lastIndexOf("/"));
			if (Utilities.isEmpty(returnPath) && path.startsWith("/")) {
				return "/";
			} else {
				return returnPath;
			}
		} else {
			if (path != null && path.startsWith("/")) {
				return "/";
			} else {
				return "";
			}
		}
	}

	/**
	 * Delete directories and all of their files in a recursive way
	 * @throws IOException
	 */
	public static void delete(final File file) throws IOException {
		if (file.isDirectory()) {
			final File[] entries = file.listFiles();
			if (entries != null) {
				for (final File entry : entries) {
					delete(entry);
				}
			}
		}
		if (!file.delete()) {
			throw new IOException("Failed to delete " + file);
		}
	}

	/**
	 * Remove all lines which start with a "#" character
	 *
	 * @param readFileToString
	 * @return
	 */
	public static String removePropertiesCommentLines(final String propertiesTextData) {
		return propertiesTextData.replaceAll("(?m)^#.*", "");
	}

	public static boolean isValidFilePath(final String path) {
		try {
			Paths.get(path);
		} catch (InvalidPathException | NullPointerException ex) {
			return false;
		}
		return true;
	}
}
