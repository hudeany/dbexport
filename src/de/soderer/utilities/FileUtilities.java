package de.soderer.utilities;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

public class FileUtilities {
	/**
	 * Delete all subfiles fitting namestart and extension
	 *
	 * @param directory
	 * @param nameStart
	 * @param extension
	 */
	public static void deleteSubfilesByNameAndExtension(File directory, String nameStart, String extension) {
		for (File file : getSubfilesByNameAndExtension(directory, nameStart, extension)) {
			file.delete();
		}
	}

	/**
	 * Read a directory and return all files fitting namestart and extension
	 *
	 * @param directory
	 * @param nameStart
	 * @param extension
	 * @return
	 */
	public static File[] getSubfilesByNameAndExtension(File directory, String nameStart, String extension) {
		List<File> files = new LinkedList<File>();
		for (File file : directory.listFiles()) {
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
	public static ArrayList<File> getArrayOfFilesByStammname(String basenameList) {
		return getArrayOfFilesByStammname(Arrays.asList(basenameList.split(";")));
	}

	/**
	 * Read a directory and return all files starting with a basename form a list of basenames
	 *
	 * @param stammNamenListe
	 * @return
	 */
	public static ArrayList<File> getArrayOfFilesByStammname(List<String> stammNamenListe) {
		ArrayList<File> ergebnisListe = new ArrayList<File>();

		for (String stammName : stammNamenListe) {
			// Suchverzeichnis und Dateistammname ermitteln
			// Achtung: stammName kann auch wildcards wie '*' enthalten daher
			// NICHT mit File-Class arbeiten
			int unixLastIndexOfSeparator = stammName.lastIndexOf("/");
			int dosLastIndexOfSeparator = stammName.lastIndexOf("\\");
			int lastIndexOfSeparator = Math.max(unixLastIndexOfSeparator, dosLastIndexOfSeparator);
			String suchVerzeichnis = stammName.substring(0, lastIndexOfSeparator + 1);
			String stammFileName = stammName.substring(lastIndexOfSeparator + 1);

			// Dateiliste erstellen
			File searchPath = new File(suchVerzeichnis);
			if (searchPath.isDirectory()) {
				File[] partList = searchPath.listFiles();
				for (int i = 0; i < partList.length; i++) {
					if (partList[i].isFile() && (stammFileName.endsWith("*") && partList[i].getName().startsWith(stammFileName.substring(0, stammFileName.length() - 1))
							|| !stammFileName.endsWith("*") && partList[i].getName().equals(stammFileName))) {
						// Datei gefunden: partList[i].getName());
						if (!ergebnisListe.contains(partList[i])) {
							ergebnisListe.add(partList[i]);
						}
					}
				}
			}
		}

		return ergebnisListe;
	}

	/**
	 * Read a directory and return all files fitting to a regex pattern
	 *
	 * @param startDirectory
	 * @param patternString
	 * @param traverseCompletely
	 * @return
	 */
	public static List<File> getFilesByPattern(File startDirectory, String patternString, boolean traverseCompletely) {
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
	public static List<File> getFilesByPattern(File startDirectory, Pattern pattern, boolean traverseCompletely) {
		List<File> files = new ArrayList<File>();
		if (startDirectory.isDirectory()) {
			for (File file : startDirectory.listFiles()) {
				if (file.isDirectory()) {
					files.add(file);
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
	public static List<File> getAllSubfiles(File directory) throws Exception {
		try {
			List<File> files = new LinkedList<File>();
			File[] subFiles = directory.listFiles();
			if (subFiles != null) {
				for (File file : subFiles) {
					if (file.isDirectory()) {
						files.addAll(getAllSubfiles(file));
					} else {
						files.add(file);
					}
				}
			}
			return files;
		} catch (Exception e) {
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
	public static int getAllSubfilesNumber(File directory) throws Exception {
		try {
			int fileCount = 0;
			File[] subFiles = directory.listFiles();
			if (subFiles != null) {
				for (File file : subFiles) {
					if (file.isDirectory()) {
						fileCount += getAllSubfilesNumber(file);
					} else {
						fileCount++;
					}
				}
			}
			return fileCount;
		} catch (Exception e) {
			throw new Exception("Error reading subfilenumber of: " + directory.getAbsolutePath(), e);
		}
	}

	/**
	 * Get size in bytes of all recursive subfiles of a directory
	 *
	 * @param directory
	 * @return
	 * @throws Exception
	 */
	public static long getAllSubfilesSize(File directory) throws Exception {
		try {
			long fileSizeSum = 0;
			File[] subFiles = directory.listFiles();
			if (subFiles != null) {
				for (File file : subFiles) {
					if (file.isDirectory()) {
						fileSizeSum += getAllSubfilesSize(file);
					} else {
						fileSizeSum += file.length();
					}
				}
			}
			return fileSizeSum;
		} catch (Exception e) {
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
	public static int getLineCount(File file) throws IOException {
		InputStream inputStream = null;
		LineNumberReader lineNumberReader = null;
		try {
			inputStream = new FileInputStream(file);
			if (Utilities.endsWithIgnoreCase(file.getName(), ".zip")) {
				inputStream = new ZipInputStream(inputStream);
				((ZipInputStream) inputStream).getNextEntry();
			}
			
			lineNumberReader = new LineNumberReader(new InputStreamReader(inputStream));
			while (lineNumberReader.readLine() != null) {
				// do nothing
			}

			return lineNumberReader.getLineNumber();
		} finally {
			Utilities.closeQuietly(lineNumberReader);
			Utilities.closeQuietly(inputStream);
		}
	}

	public static void write(File file, byte[] data) throws Exception {
		try (FileOutputStream out = new FileOutputStream(file)) {
			out.write(data);
		}
	}
	
	public static byte[] readFileToByteArray(File dataFile) throws Exception {
		ByteArrayOutputStream output = null;
		try (FileInputStream input = new FileInputStream(dataFile)) {
			output = new ByteArrayOutputStream();
			Utilities.copy(input, output);
			return output.toByteArray();
		} catch (Exception e) {
			throw e;
		}
	}
	
	public static String readFileToString(File dataFile, String encoding) throws Exception {
		return new String(readFileToByteArray(dataFile), encoding);
	}
}
