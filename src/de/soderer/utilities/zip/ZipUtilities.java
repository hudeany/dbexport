package de.soderer.utilities.zip;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Utilities;

public class ZipUtilities {
	/**
	 * Zip a file or recursively all files and subdirectories of a directory. The
	 * zipped file is placed in the same directory as the zipped file.
	 *
	 * @param sourceFile
	 * @return
	 * @throws IOException
	 */
	public static File zipFile(final File sourceFile) throws IOException {
		final File zippedFile = new File(sourceFile.getAbsolutePath() + ".zip");
		zipFile(sourceFile, zippedFile, null);
		return zippedFile;
	}

	/**
	 * Zip a file or recursively all files and subdirectories of a directory.
	 *
	 * @param sourceFile
	 * @param destinationZipFile
	 * @throws IOException
	 */
	public static void zipFile(final File sourceFile, final File destinationZipFile,
			final Charset fileNameEncodingCharset) throws IOException {
		if (!sourceFile.exists()) {
			throw new IOException("SourceFile does not exist");
		}

		if (destinationZipFile.exists()) {
			throw new IOException("DestinationFile already exists");
		}

		try (ZipOutputStream zipOutputStream = openNewZipOutputStream(destinationZipFile, fileNameEncodingCharset)) {
			addFileToOpenZipFileStream(sourceFile, zipOutputStream);
		} catch (final IOException e) {
			if (destinationZipFile.exists()) {
				destinationZipFile.delete();
			}
			throw e;
		}
	}

	public static void unzipFile(final File zipFile, final File destinationDirectory) throws IOException {
		unzipFile(zipFile, destinationDirectory, null);
	}

	public static File unzipFile(final File zipFile, final File destinationFilePath, Charset fileNameEncodingCharset,
			final String filePathInZipFile) throws Exception {
		if (!zipFile.exists()) {
			throw new IOException("ZipFile '" + zipFile.getAbsolutePath() + "' does not exist");
		} else if (!zipFile.isFile()) {
			throw new IOException("ZipFile '" + zipFile.getAbsolutePath() + "' is not a file");
		} else if (destinationFilePath.exists()) {
			throw new IOException("Destination file '" + destinationFilePath.getAbsolutePath() + "' already exists");
		}

		if (fileNameEncodingCharset == null) {
			fileNameEncodingCharset = Charset.forName("Cp437");
		}

		final byte[] buffer = new byte[1024];
		try (final InputStream inputStream = new FileInputStream(zipFile);
				final ZipInputStream zis = new ZipInputStream(inputStream, fileNameEncodingCharset)) {
			ZipEntry zipEntry = zis.getNextEntry();
			while (zipEntry != null) {
				if (zipEntry.getName().equals(filePathInZipFile)) {
					final File destFile = destinationFilePath;

					final File parentDirectory = destFile.getParentFile();
					if (!parentDirectory.exists()) {
						throw new IOException(
								"Destination directory does not exist: " + parentDirectory.getAbsolutePath());
					} else if (!parentDirectory.isDirectory()) {
						throw new IOException("Destination directory exists but is not a directory: "
								+ parentDirectory.getAbsolutePath());
					}

					try (final FileOutputStream fos = new FileOutputStream(destFile)) {
						int len;
						while ((len = zis.read(buffer)) > 0) {
							fos.write(buffer, 0, len);
						}
					}

					return destFile;
				}
				zis.closeEntry();
				zipEntry = zis.getNextEntry();
			}
		}
		throw new Exception("File not found in zipfile");
	}

	public static void unzipFile(final File zipFile, final File destinationDirectory, Charset fileNameEncodingCharset)
			throws IOException {
		if (!zipFile.exists()) {
			throw new IOException("ZipFile '" + zipFile.getAbsolutePath() + "' does not exist");
		} else if (!zipFile.isFile()) {
			throw new IOException("ZipFile '" + zipFile.getAbsolutePath() + "' is not a file");
		} else if (!destinationDirectory.exists()) {
			throw new IOException(
					"Destination directory '" + destinationDirectory.getAbsolutePath() + "' does not exist");
		} else if (!destinationDirectory.isDirectory()) {
			throw new IOException(
					"Destination directory '" + destinationDirectory.getAbsolutePath() + "' is not a directory");
		}

		if (fileNameEncodingCharset == null) {
			fileNameEncodingCharset = Charset.forName("Cp437");
		}

		final String destinationDirectoryPath = destinationDirectory.getCanonicalPath();
		final byte[] buffer = new byte[1024];
		try (final InputStream inputStream = new FileInputStream(zipFile);
				final ZipInputStream zis = new ZipInputStream(inputStream, fileNameEncodingCharset)) {
			ZipEntry zipEntry = zis.getNextEntry();
			while (zipEntry != null) {
				if (zipEntry.getName().endsWith("/")) {
					final File newDirectory = new File(destinationDirectory, zipEntry.getName());
					final String newDirectoryPath = newDirectory.getCanonicalPath();
					if (!newDirectoryPath.startsWith(destinationDirectoryPath + File.separator)) {
						throw new IOException(
								"ZipEntry is outside of the destination directory: " + zipEntry.getName());
					}

					if (!newDirectory.exists()) {
						newDirectory.mkdirs();
					} else if (!newDirectory.isDirectory()) {
						throw new IOException("Destination directory exists but is not a directory: "
								+ newDirectory.getAbsolutePath());
					}
				} else {
					final File destFile = new File(destinationDirectory, zipEntry.getName());
					final String destFilePath = destFile.getCanonicalPath();
					if (!destFilePath.startsWith(destinationDirectoryPath + File.separator)) {
						throw new IOException(
								"ZipEntry is outside of the destination directory: " + zipEntry.getName());
					}

					final File parentDirectory = destFile.getParentFile();
					if (!parentDirectory.exists()) {
						parentDirectory.mkdirs();
					} else if (!parentDirectory.isDirectory()) {
						throw new IOException("Destination directory exists but is not a directory: "
								+ parentDirectory.getAbsolutePath());
					}

					try (final FileOutputStream fos = new FileOutputStream(destFile)) {
						int len;
						while ((len = zis.read(buffer)) > 0) {
							fos.write(buffer, 0, len);
						}
					}
				}
				zis.closeEntry();
				zipEntry = zis.getNextEntry();
			}
		}
	}

	public static File newFile(final File destinationDir, final ZipEntry zipEntry) throws IOException {
		final File destFile = new File(destinationDir, zipEntry.getName());

		final String destDirPath = destinationDir.getCanonicalPath();
		final String destFilePath = destFile.getCanonicalPath();

		if (!destFilePath.startsWith(destDirPath + File.separator)) {
			throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
		}

		return destFile;
	}

	/**
	 * Zip a file or recursively all files and subdirectories of a directory. A new
	 * relative path is started.
	 *
	 * @param sourceFile
	 * @param destinationZipFileSream
	 * @throws IOException
	 */
	public static void addFileToOpenZipFileStream(final File sourceFile, final ZipOutputStream destinationZipFileSream)
			throws IOException {
		addFileToOpenZipFileStream(sourceFile, null, destinationZipFileSream);
	}

	/**
	 * Zip a file or recursively all files and subdirectories of a directory.
	 *
	 * @param sourceFile
	 * @param destinationZipFileSream
	 * @throws IOException
	 */
	public static void addFileToOpenZipFileStream(final File sourceFile, String relativeDirPath,
			final ZipOutputStream destinationZipFileStream) throws IOException {
		if (!sourceFile.exists()) {
			throw new IOException("SourceFile does not exist");
		}

		if (destinationZipFileStream == null) {
			throw new IOException("DestinationStream is not ready");
		}

		if (relativeDirPath == null) {
			relativeDirPath = "";
		}

		if (relativeDirPath.length() > 0 && (!relativeDirPath.endsWith("/") && !relativeDirPath.endsWith("\\"))) {
			relativeDirPath = relativeDirPath + File.separator;
		}

		try {
			if (!sourceFile.isDirectory()) {
				final ZipEntry entry = new ZipEntry(relativeDirPath + sourceFile.getName());
				entry.setTime(sourceFile.lastModified());
				destinationZipFileStream.putNextEntry(entry);

				try (BufferedInputStream bufferedFileInputStream = new BufferedInputStream(
						new FileInputStream(sourceFile))) {
					final byte[] bufferArray = new byte[4096];
					int byteBufferFillLength;
					while ((byteBufferFillLength = bufferedFileInputStream.read(bufferArray)) > -1) {
						destinationZipFileStream.write(bufferArray, 0, byteBufferFillLength);
					}

					destinationZipFileStream.flush();
					destinationZipFileStream.closeEntry();
				}
			} else {
				for (final File sourceSubFile : sourceFile.listFiles()) {
					addFileToOpenZipFileStream(sourceSubFile, relativeDirPath + sourceFile.getName(),
							destinationZipFileStream);
				}
			}
		} catch (final IOException e) {
			throw e;
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
	public static void addFileDataToOpenZipFileStream(final byte[] fileData, final String filename,
			final ZipOutputStream destinationZipFileSream) throws IOException {
		addFileDataToOpenZipFileStream(fileData, File.separator, filename, destinationZipFileSream);
	}

	/**
	 * Open a ZipOutputStream based on a file in which is written
	 *
	 * @param destinationZipFile
	 * @return
	 * @throws IOException
	 */
	public static ZipOutputStream openNewZipOutputStream(final File destinationZipFile, Charset fileNameEncodingCharset)
			throws IOException {
		if (destinationZipFile.exists()) {
			throw new IOException("DestinationFile already exists");
		} else if (!destinationZipFile.getParentFile().exists()) {
			throw new IOException("DestinationDirectory does not exist");
		}

		if (fileNameEncodingCharset == null) {
			fileNameEncodingCharset = Charset.forName("Cp437");
		}

		try {
			return new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(destinationZipFile)));
		} catch (final IOException e) {
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
	public static ZipOutputStream openNewZipOutputStream(final OutputStream destinationZipStream) throws IOException {
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
	public static void addFileDataToOpenZipFileStream(final byte[] fileData, final String relativeDirPath,
			final String filename, final ZipOutputStream destinationZipFileStream) throws IOException {
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

		final ZipEntry entry = new ZipEntry(relativeDirPath + filename);
		entry.setTime(DateUtilities.getDateForLocalDateTime(LocalDateTime.now()).toInstant().toEpochMilli());
		destinationZipFileStream.putNextEntry(entry);

		destinationZipFileStream.write(fileData);

		destinationZipFileStream.flush();
		destinationZipFileStream.closeEntry();
	}

	/**
	 * Add a file or recursively all files and subdirectories of a directory to an
	 * existing ZipFile. All existing entries are preserved.
	 *
	 * @param sourceFile
	 * @return
	 * @throws IOException
	 */
	public static void addFileToExistingZipFile(final File sourceFile, final File zipFile) throws IOException {
		try (ZipOutputStream zipOutputStream = openExistingZipFileForExtension(zipFile)) {
			addFileToOpenZipFileStream(sourceFile, zipOutputStream);
		}
	}

	/**
	 * Add a list of files or recursively all files and subdirectories of a
	 * directory to an existing ZipFile. All existing entries are preserved.
	 *
	 * @param sourceFile
	 * @return
	 * @throws IOException
	 */
	public static void addFileToExistingZipFile(final List<File> sourceFiles, final File zipFile) throws IOException {
		try (ZipOutputStream zipOutputStream = openExistingZipFileForExtension(zipFile)) {
			for (final File file : sourceFiles) {
				addFileToOpenZipFileStream(file, zipOutputStream);
			}
		}
	}

	/**
	 * Open an exsiting ZipFile for appending new files or create a new ZipFile if
	 * it does not exist yet.
	 *
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static ZipOutputStream openExistingZipFileForExtensionOrCreateNewZipFile(final File zipFile)
			throws IOException {
		if (zipFile.exists()) {
			return openExistingZipFileForExtension(zipFile);
		} else {
			return openNewZipOutputStream(zipFile, null);
		}
	}

	/**
	 * Open an exsiting ZipFile for appending new files. All existing entries are
	 * preserved.
	 *
	 * @param zipFile
	 * @return
	 * @throws IOException
	 * @throws ZipException
	 */
	@SuppressWarnings("resource")
	public static ZipOutputStream openExistingZipFileForExtension(final File zipFile) throws IOException {
		final File originalFileTemp = new File(
				zipFile.getParentFile().getAbsolutePath() + "/" + String.valueOf(System.currentTimeMillis()));
		zipFile.renameTo(originalFileTemp);

		final ZipOutputStream zipOutputStream = new ZipOutputStream(
				new BufferedOutputStream(new FileOutputStream(zipFile)));

		ZipFile sourceZipFile = null;
		try {
			sourceZipFile = new ZipFile(originalFileTemp);
			final Enumeration<? extends ZipEntry> srcEntries = sourceZipFile.entries();
			while (srcEntries.hasMoreElements()) {
				final ZipEntry sourceZipFileEntry = srcEntries.nextElement();
				zipOutputStream.putNextEntry(sourceZipFileEntry);

				try (BufferedInputStream bufferedInputStream = new BufferedInputStream(
						sourceZipFile.getInputStream(sourceZipFileEntry))) {
					final byte[] bufferArray = new byte[4096];
					int byteBufferFillLength = bufferedInputStream.read(bufferArray);
					while (byteBufferFillLength > -1) {
						zipOutputStream.write(bufferArray, 0, byteBufferFillLength);
						byteBufferFillLength = bufferedInputStream.read(bufferArray);
					}

					zipOutputStream.closeEntry();
				}
			}

			zipOutputStream.flush();
			sourceZipFile.close();
			originalFileTemp.delete();

			return zipOutputStream;
		} catch (final IOException e) {
			if (zipFile.exists()) {
				Utilities.closeQuietly(zipOutputStream);
				zipFile.delete();
			}

			originalFileTemp.renameTo(zipFile);
			throw e;
		} finally {
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
	public static Map<String, byte[]> readExistingZipFile(final File zipFile) throws IOException {
		final Map<String, byte[]> returnMap = new HashMap<>();
		try (ZipFile sourceZipFile = new ZipFile(zipFile)) {
			final Enumeration<? extends ZipEntry> srcEntries = sourceZipFile.entries();
			while (srcEntries.hasMoreElements()) {
				final ZipEntry sourceZipFileEntry = srcEntries.nextElement();

				try (BufferedInputStream bufferedInputStream = new BufferedInputStream(
						sourceZipFile.getInputStream(sourceZipFileEntry))) {
					final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

					final byte[] bufferArray = new byte[4096];
					int byteBufferFillLength = bufferedInputStream.read(bufferArray);
					while (byteBufferFillLength > -1) {
						byteArrayOutputStream.write(bufferArray, 0, byteBufferFillLength);
						byteBufferFillLength = bufferedInputStream.read(bufferArray);
					}

					returnMap.put(sourceZipFileEntry.getName(), byteArrayOutputStream.toByteArray());
				}
			}
		}
		return returnMap;
	}

	public static String[] getZipFileEntries(final File file) throws ZipException, IOException {
		try (ZipFile zipFile = new ZipFile(file)) {
			final Enumeration<? extends ZipEntry> entries = zipFile.entries();
			final List<String> entryList = new ArrayList<>();
			while (entries.hasMoreElements()) {
				final ZipEntry entry = entries.nextElement();
				entryList.add(entry.getName());
			}
			return entryList.toArray(new String[0]);
		}
	}

	public static void removeFilesFromZipFile(final File zipFile, final Charset fileNameEncodingCharset,
			final String... filePatterns) throws Exception {
		final File tempZipFile = File.createTempFile("temp", ".zip");
		try {
			removeFilesFromZipFile(zipFile, tempZipFile, fileNameEncodingCharset, filePatterns);
		} catch (final IOException e) {
			if (tempZipFile.exists()) {
				tempZipFile.delete();
			}
			throw e;
		}
		zipFile.delete();
		tempZipFile.renameTo(zipFile);
	}

	public static void removeFilesFromZipFile(final File originalZipFile, final File shrinkedZipFile,
			final Charset fileNameEncodingCharset, final String... filePatterns) throws Exception {
		try (ZipFile sourceZipFile = new ZipFile(originalZipFile);
				ZipOutputStream destinationZipOutputStream = openNewZipOutputStream(shrinkedZipFile,
						fileNameEncodingCharset)) {
			final List<Pattern> patternsToFilter = new ArrayList<>();
			for (final String filePattern : filePatterns) {
				patternsToFilter.add(Pattern.compile(
						"^" + filePattern.replace("\\", "/").replace(".", "\\.").replace("*", ".*").replace("?", ".")
						+ "$"));
			}

			final Enumeration<? extends ZipEntry> srcEntries = sourceZipFile.entries();
			while (srcEntries.hasMoreElements()) {
				final ZipEntry sourceZipFileEntry = srcEntries.nextElement();
				boolean keepFileEntry = true;
				for (final Pattern pattern : patternsToFilter) {
					if (pattern.matcher(sourceZipFileEntry.getName()).find()) {
						keepFileEntry = false;
						break;
					}
				}
				if (keepFileEntry) {
					destinationZipOutputStream.putNextEntry(sourceZipFileEntry);

					try (BufferedInputStream bufferedInputStream = new BufferedInputStream(
							sourceZipFile.getInputStream(sourceZipFileEntry))) {
						final byte[] bufferArray = new byte[4096];
						int byteBufferFillLength = bufferedInputStream.read(bufferArray);
						while (byteBufferFillLength > -1) {
							destinationZipOutputStream.write(bufferArray, 0, byteBufferFillLength);
							byteBufferFillLength = bufferedInputStream.read(bufferArray);
						}

						destinationZipOutputStream.closeEntry();
					}
				}
			}
		} catch (final IOException e) {
			throw e;
		}
	}

	public static long getDataSizeUncompressed(final File zippedFile) throws ZipException, IOException {
		try (final ZipFile zipFile = new ZipFile(zippedFile)) {
			long uncompressedSize = 0;
			final Enumeration<? extends ZipEntry> e = zipFile.entries();
			while (e.hasMoreElements()) {
				final ZipEntry entry = e.nextElement();
				final long originalSize = entry.getSize();
				if (originalSize >= 0) {
					uncompressedSize += originalSize;
				} else {
					// -1 indicates, that size is unknown
					return originalSize;
				}
			}
			return uncompressedSize;
		}
	}
}
