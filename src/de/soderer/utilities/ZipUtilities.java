package de.soderer.utilities;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ZipUtilities {
	/**
	 * Zip a file or recursively all files and subdirectories of a directory. The zipped file is placed in the same directory as the zipped file.
	 *
	 * @param sourceFile
	 * @return
	 * @throws IOException
	 */
	public static File zipFile(File sourceFile) throws IOException {
		File zippedFile = new File(sourceFile.getAbsolutePath() + ".zip");
		zipFile(sourceFile, zippedFile);
		return zippedFile;
	}

	/**
	 * Zip a file or recursively all files and subdirectories of a directory.
	 *
	 * @param sourceFile
	 * @param destinationZipFile
	 * @throws IOException
	 */
	public static void zipFile(File sourceFile, File destinationZipFile) throws IOException {
		ZipOutputStream zipOutputStream = null;

		if (!sourceFile.exists()) {
			throw new IOException("SourceFile does not exist");
		}

		if (destinationZipFile.exists()) {
			throw new IOException("DestinationFile already exists");
		}

		try {
			zipOutputStream = openNewZipOutputStream(destinationZipFile);

			addFileToOpenZipFileStream(sourceFile, zipOutputStream);

			Utilities.closeQuietly(zipOutputStream);
			zipOutputStream = null;
		} catch (IOException e) {
			if (destinationZipFile.exists()) {
				Utilities.closeQuietly(zipOutputStream);
				destinationZipFile.delete();
			}
			throw e;
		} finally {
			Utilities.closeQuietly(zipOutputStream);
		}
	}

	/**
	 * Zip a file or recursively all files and subdirectories of a directory. A new relative path is started.
	 *
	 * @param sourceFile
	 * @param destinationZipFileSream
	 * @throws IOException
	 */
	public static void addFileToOpenZipFileStream(File sourceFile, ZipOutputStream destinationZipFileSream) throws IOException {
		addFileToOpenZipFileStream(sourceFile, File.separator, destinationZipFileSream);
	}

	/**
	 * Zip a file or recursively all files and subdirectories of a directory.
	 *
	 * @param sourceFile
	 * @param destinationZipFileSream
	 * @throws IOException
	 */
	public static void addFileToOpenZipFileStream(File sourceFile, String relativeDirPath, ZipOutputStream destinationZipFileSream) throws IOException {
		BufferedInputStream bufferedFileInputStream = null;

		if (!sourceFile.exists()) {
			throw new IOException("SourceFile does not exist");
		}

		if (destinationZipFileSream == null) {
			throw new IOException("DestinationStream is not ready");
		}

		if (relativeDirPath == null || (!relativeDirPath.endsWith("/") && !relativeDirPath.endsWith("\\"))) {
			throw new IOException("RelativeDirPath is invalid");
		}

		try {
			if (!sourceFile.isDirectory()) {
				ZipEntry entry = new ZipEntry(relativeDirPath + sourceFile.getName());
				entry.setTime(sourceFile.lastModified());
				destinationZipFileSream.putNextEntry(entry);

				bufferedFileInputStream = new BufferedInputStream(new FileInputStream(sourceFile));
				byte[] bufferArray = new byte[4096];
				int byteBufferFillLength = bufferedFileInputStream.read(bufferArray);
				while (byteBufferFillLength > -1) {
					destinationZipFileSream.write(bufferArray, 0, byteBufferFillLength);
					byteBufferFillLength = bufferedFileInputStream.read(bufferArray);
				}
				bufferedFileInputStream.close();
				bufferedFileInputStream = null;

				destinationZipFileSream.flush();
				destinationZipFileSream.closeEntry();
			} else {
				for (File sourceSubFile : sourceFile.listFiles()) {
					addFileToOpenZipFileStream(sourceSubFile, relativeDirPath + sourceFile.getName() + File.separator, destinationZipFileSream);
				}
			}
		} catch (IOException e) {
			throw e;
		} finally {
			Utilities.closeQuietly(bufferedFileInputStream);
		}
	}

	/**
	 * Add a file to an opened ZipOutputStream
	 *
	 * @param fileData
	 * @param filename
	 * @param destinationZipFileSream
	 * @throws IOException
	 */
	public static void addFileDataToOpenZipFileStream(byte[] fileData, String filename, ZipOutputStream destinationZipFileSream) throws IOException {
		addFileDataToOpenZipFileStream(fileData, File.separator, filename, destinationZipFileSream);
	}

	/**
	 * Open a ZipOutputStream based on a file in which is written
	 *
	 * @param destinationZipFile
	 * @return
	 * @throws IOException
	 */
	public static ZipOutputStream openNewZipOutputStream(File destinationZipFile) throws IOException {
		if (destinationZipFile.exists()) {
			throw new IOException("DestinationFile already exists");
		} else if (!destinationZipFile.getParentFile().exists()) {
			throw new IOException("DestinationDirectory does not exist");
		}

		try {
			return new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(destinationZipFile)));
		} catch (IOException e) {
			if (destinationZipFile.exists()) {
				destinationZipFile.delete();
			}
			throw e;
		}
	}

	/**
	 * Open a ZipOutputStream based on a OutputStream in which is written
	 *
	 * @param destinationZipStream
	 * @return
	 * @throws IOException
	 */
	public static ZipOutputStream openNewZipOutputStream(OutputStream destinationZipStream) throws IOException {
		if (destinationZipStream == null) {
			throw new IOException("DestinationStream is missing");
		}

		return new ZipOutputStream(new BufferedOutputStream(destinationZipStream));
	}

	/**
	 * Add a file to an opened ZipOutputStream
	 *
	 * @param fileData
	 * @param relativeDirPath
	 * @param filename
	 * @param destinationZipFileStream
	 * @throws IOException
	 */
	public static void addFileDataToOpenZipFileStream(byte[] fileData, String relativeDirPath, String filename, ZipOutputStream destinationZipFileStream) throws IOException {
		if (fileData == null) {
			throw new IOException("FileData is missing");
		}

		if (Utilities.isEmpty(filename) || filename.trim().length() == 0) {
			throw new IOException("Filename is missing");
		}

		if (destinationZipFileStream == null) {
			throw new IOException("DestinationStream is not ready");
		}

		if (relativeDirPath == null || (!relativeDirPath.endsWith("/") && !relativeDirPath.endsWith("\\"))) {
			throw new IOException("RelativeDirPath is invalid");
		}

		ZipEntry entry = new ZipEntry(relativeDirPath + filename);
		entry.setTime(new Date().getTime());
		destinationZipFileStream.putNextEntry(entry);

		destinationZipFileStream.write(fileData);

		destinationZipFileStream.flush();
		destinationZipFileStream.closeEntry();
	}

	/**
	 * Add a file or recursively all files and subdirectories of a directory to an existing ZipFile. All existing entries are preserved.
	 *
	 * @param sourceFile
	 * @return
	 * @throws IOException
	 */
	public static void addFileToExistingzipFile(File sourceFile, File zipFile) throws IOException {
		ZipOutputStream zipOutputStream = openExistingZipFileForExtension(zipFile);
		try {
			addFileToOpenZipFileStream(sourceFile, zipOutputStream);
		} finally {
			Utilities.closeQuietly(zipOutputStream);
		}
	}

	/**
	 * Add a list of files or recursively all files and subdirectories of a directory to an existing ZipFile. All existing entries are preserved.
	 *
	 * @param sourceFile
	 * @return
	 * @throws IOException
	 */
	public static void addFileToExistingzipFile(List<File> sourceFiles, File zipFile) throws IOException {
		ZipOutputStream zipOutputStream = openExistingZipFileForExtension(zipFile);
		try {
			for (File file : sourceFiles) {
				addFileToOpenZipFileStream(file, zipOutputStream);
			}
		} finally {
			Utilities.closeQuietly(zipOutputStream);
		}
	}

	/**
	 * Open an exsiting ZipFile for appending new files or create a new ZipFile if it does not exist yet.
	 *
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static ZipOutputStream openExistingZipFileForExtensionOrCreateNewZipFile(File zipFile) throws IOException {
		if (zipFile.exists()) {
			return openExistingZipFileForExtension(zipFile);
		} else {
			return openNewZipOutputStream(zipFile);
		}
	}

	/**
	 * Open an exsiting ZipFile for appending new files. All existing entries are preserved.
	 *
	 * @param zipFile
	 * @return
	 * @throws IOException
	 * @throws ZipException
	 */
	public static ZipOutputStream openExistingZipFileForExtension(File zipFile) throws IOException {
		File originalFileTemp = new File(zipFile.getParentFile().getAbsolutePath() + "/" + String.valueOf(System.currentTimeMillis()));
		zipFile.renameTo(originalFileTemp);

		ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipFile)));

		BufferedInputStream bufferedInputStream = null;
		ZipFile sourceZipFile = null;
		try {
			sourceZipFile = new ZipFile(originalFileTemp);
			Enumeration<? extends ZipEntry> srcEntries = sourceZipFile.entries();
			while (srcEntries.hasMoreElements()) {
				ZipEntry sourceZipFileEntry = srcEntries.nextElement();
				zipOutputStream.putNextEntry(sourceZipFileEntry);

				bufferedInputStream = new BufferedInputStream(sourceZipFile.getInputStream(sourceZipFileEntry));

				byte[] bufferArray = new byte[4096];
				int byteBufferFillLength = bufferedInputStream.read(bufferArray);
				while (byteBufferFillLength > -1) {
					zipOutputStream.write(bufferArray, 0, byteBufferFillLength);
					byteBufferFillLength = bufferedInputStream.read(bufferArray);
				}

				zipOutputStream.closeEntry();

				bufferedInputStream.close();
				bufferedInputStream = null;
			}

			zipOutputStream.flush();
			sourceZipFile.close();
			originalFileTemp.delete();

			return zipOutputStream;
		} catch (IOException e) {
			if (zipFile.exists()) {
				Utilities.closeQuietly(zipOutputStream);
				zipFile.delete();
			}

			originalFileTemp.renameTo(zipFile);
			throw e;
		} finally {
			Utilities.closeQuietly(bufferedInputStream);
			Utilities.closeQuietly(sourceZipFile);
		}
	}

	/**
	 * Read a ZipFile
	 *
	 * @param zipFile
	 * @return All entries in a map
	 * @throws IOException
	 */
	public static Map<String, byte[]> readExistingZipFile(File zipFile) throws IOException {
		Map<String, byte[]> returnMap = new HashMap<String, byte[]>();

		ZipFile sourceZipFile = null;
		BufferedInputStream bufferedInputStream = null;
		ByteArrayOutputStream byteArrayOutputStream = null;

		try {
			sourceZipFile = new ZipFile(zipFile);

			Enumeration<? extends ZipEntry> srcEntries = sourceZipFile.entries();
			while (srcEntries.hasMoreElements()) {
				ZipEntry sourceZipFileEntry = srcEntries.nextElement();

				bufferedInputStream = new BufferedInputStream(sourceZipFile.getInputStream(sourceZipFileEntry));
				byteArrayOutputStream = new ByteArrayOutputStream();

				byte[] bufferArray = new byte[4096];
				int byteBufferFillLength = bufferedInputStream.read(bufferArray);
				while (byteBufferFillLength > -1) {
					byteArrayOutputStream.write(bufferArray, 0, byteBufferFillLength);
					byteBufferFillLength = bufferedInputStream.read(bufferArray);
				}

				returnMap.put(sourceZipFileEntry.getName(), byteArrayOutputStream.toByteArray());

				byteArrayOutputStream.close();
				byteArrayOutputStream = null;

				bufferedInputStream.close();
				bufferedInputStream = null;
			}

			sourceZipFile.close();

			return returnMap;
		} finally {
			Utilities.closeQuietly(bufferedInputStream);
			Utilities.closeQuietly(byteArrayOutputStream);
			Utilities.closeQuietly(sourceZipFile);
		}
	}

	public static String[] getZipFileEntries(File file) throws ZipException, IOException {
		try (ZipFile zipFile = new ZipFile(file)) {
			Enumeration<? extends ZipEntry> entries = zipFile.entries();
			List<String> entryList = new ArrayList<String>();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				entryList.add(entry.getName());
			}
			return entryList.toArray(new String[0]);
		}
	}
}
