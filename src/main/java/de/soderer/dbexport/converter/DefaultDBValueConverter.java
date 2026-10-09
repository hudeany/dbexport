package de.soderer.dbexport.converter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.ReaderInputStream;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.zip.TarGzUtilities;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

/**
 * Converts the values of a result set into values for the export: blobs into base64 text or separate files,
 * clobs into text or separate files, all other values as returned by the JDBC driver.
 *
 * <p>
 * Subclasses handle the special data types of single database vendors.
 * </p>
 */
public class DefaultDBValueConverter {
	/**
	 * Compression of the lob files, or null for uncompressed files.
	 */
	protected FileCompressionType compressionType;
	/**
	 * Password of zip compressed lob files, or null.
	 */
	protected char[] zipPassword;
	/**
	 * Whether zip files are encrypted with the weak ZipCrypto method instead of AES.
	 */
	protected boolean useZipCrypto = false;
	/**
	 * Whether blobs are exported as separate files instead of base64 encoded values.
	 */
	protected boolean createBlobFiles;
	/**
	 * Whether clobs are exported as separate files instead of text values.
	 */
	protected boolean createClobFiles;
	/**
	 * Path of the export file (currently not used, the path is passed to {@link #convert(ResultSetMetaData, ResultSet, int, String)}).
	 */
	protected String outputFilePath;
	/**
	 * File extension of the export format (e.g. "csv"), removed from the lob file names.
	 */
	protected String fileExtension;

	/**
	 * Creates the converter.
	 *
	 * @param compressionType compression of the lob files, or null for uncompressed files
	 * @param zipPassword password of zip compressed lob files, or null
	 * @param useZipCrypto true to encrypt zip files with the weak ZipCrypto method instead of AES
	 * @param createBlobFiles true to export blobs as separate files instead of base64 encoded values
	 * @param createClobFiles true to export clobs as separate files instead of text values
	 * @param fileExtension file extension of the export format (e.g. "csv"), removed from the lob file names
	 */
	public DefaultDBValueConverter(final FileCompressionType compressionType, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		this.compressionType = compressionType;
		this.zipPassword = zipPassword;
		this.useZipCrypto = useZipCrypto;
		this.createBlobFiles = createBlobFiles;
		this.createClobFiles = createClobFiles;
		this.fileExtension = fileExtension;
	}

	/**
	 * Reads and converts the value of a column of the current result set row.
	 *
	 * @param metaData the meta data of the result set
	 * @param resultSet the result set positioned at the row to export
	 * @param columnIndex the 1-based column index
	 * @param exportFilePath the path of the export file, next to which the lob files are created
	 * @return the value: a String, Number, Boolean, date value or File (for lob files), or null
	 * @throws Exception if the value cannot be read or a lob file cannot be written
	 */
	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == Types.BLOB) {
			final Blob blob = resultSet.getBlob(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createBlobFiles) {
				try (InputStream dataStream = blob.getBinaryStream()) {
					value = writeLobFile(exportFilePath, "blob", dataStream);
				}
			} else {
				try (InputStream input = blob.getBinaryStream()) {
					final byte[] data = IoUtilities.toByteArray(input);
					value = Base64.getEncoder().encodeToString(data);
				}
			}
		} else if (columnTypeCode == Types.CLOB) {
			final Clob clob = resultSet.getClob(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else if (createClobFiles) {
				try (Reader reader = clob.getCharacterStream();
						InputStream dataStream = new ReaderInputStream(reader, StandardCharsets.UTF_8)) {
					value = writeLobFile(exportFilePath, "clob", dataStream);
				}
			} else {
				try (Reader input = clob.getCharacterStream()) {
					value = Utilities.toString(input);
				}
			}
		} else if (columnTypeCode == Types.TIMESTAMP || columnTypeCode == Types.DATE) {
			value = resultSet.getObject(columnIndex);
			if (value != null && "0000-00-00 00:00:00".equals(value)) {
				value = null;
			}
		} else {
			value = resultSet.getObject(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			}
		}
		return value;
	}

	/**
	 * Writes the data of a lob into a new (optionally compressed) file next to the export file.
	 *
	 * @param exportFilePath the path of the export file, whose name is used as prefix of the lob file name
	 * @param lobType "blob" or "clob", used as file extension
	 * @param dataStream the lob data
	 * @return the created lob file
	 * @throws Exception if the file cannot be written
	 */
	protected File writeLobFile(final String exportFilePath, final String lobType, final InputStream dataStream) throws Exception {
		String lobOutputFilePathPrefix = exportFilePath;
		if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.ZIP.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.ZIP.getDefaultFileExtension().length());
		} else if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.TARGZ.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.TARGZ.getDefaultFileExtension().length());
		} else if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.TGZ.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.TGZ.getDefaultFileExtension().length());
		} else if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.GZ.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.GZ.getDefaultFileExtension().length());
		}
		if (lobOutputFilePathPrefix.endsWith("." + fileExtension)) {
			lobOutputFilePathPrefix = lobOutputFilePathPrefix.substring(0, lobOutputFilePathPrefix.length() - (fileExtension.length() + 1));
		}
		// Via the absolute file, so the lob files of an export file without directory are created next to it (and not in the temp directory)
		final File lobOutputFile = File.createTempFile(new File(lobOutputFilePathPrefix).getName() + "_", "." + lobType + (compressionType != null ? "." + compressionType.getDefaultFileExtension() : ""), new File(exportFilePath).getAbsoluteFile().getParentFile());

		try {
			OutputStream outputStream = null;
			File tempFile = null;
			try {
				if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.ZIP.getDefaultFileExtension())) {
					outputStream = ZipUtilities.openNewZipOutputStream(lobOutputFile, null);
					// The entry is the uncompressed lob file (without ".zip")
					final String entryName = lobOutputFile.getName().substring(0, lobOutputFile.getName().length() - 1 - FileCompressionType.ZIP.getDefaultFileExtension().length());
					((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(entryName));
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.TARGZ.getDefaultFileExtension())) {
					// The prefix must not contain a path (createTempFile() failed with "Unable to create temporary file")
					tempFile = File.createTempFile(lobOutputFile.getName(), null);
					outputStream = new FileOutputStream(tempFile);
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.TGZ.getDefaultFileExtension())) {
					tempFile = File.createTempFile(lobOutputFile.getName(), null);
					outputStream = new FileOutputStream(tempFile);
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.GZ.getDefaultFileExtension())) {
					outputStream = new GZIPOutputStream(new FileOutputStream(lobOutputFile));
				} else {
					outputStream = new FileOutputStream(lobOutputFile);
				}

				IoUtilities.copy(dataStream, outputStream);

				outputStream.close();

				if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.ZIP.getDefaultFileExtension())) {
					// Like the export file itself: only with a password, and with the configured encryption method
					if (zipPassword != null) {
						Zip4jUtilities.createPasswordSecuredZipFile(lobOutputFile.getAbsolutePath(), zipPassword, useZipCrypto);
					}
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.TARGZ.getDefaultFileExtension())) {
					TarGzUtilities.compress(lobOutputFile, tempFile, lobOutputFile.getName());
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.TGZ.getDefaultFileExtension())) {
					TarGzUtilities.compress(lobOutputFile, tempFile, lobOutputFile.getName());
				}

				return lobOutputFile;
			} finally {
				Utilities.closeQuietly(outputStream);
				if (tempFile != null && tempFile.exists()) {
					tempFile.delete();
					tempFile = null;
				}
			}
		} catch (final Exception e) {
			throw new Exception("Error creating " + lobType + " file '" + lobOutputFile.getAbsolutePath() + "': " + e.getMessage(), e);
		}
	}
}
