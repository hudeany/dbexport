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
import de.soderer.utilities.TarGzUtilities;
import de.soderer.utilities.Utilities;
import de.soderer.utilities.zip.Zip4jUtilities;
import de.soderer.utilities.zip.ZipUtilities;

public class DefaultDBValueConverter {
	protected FileCompressionType compressionType;
	protected char[] zipPassword;
	protected boolean useZipCrypto = false;
	protected boolean createBlobFiles;
	protected boolean createClobFiles;
	protected String outputFilePath;
	protected String fileExtension;

	public DefaultDBValueConverter(final FileCompressionType compressionType, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		this.compressionType = compressionType;
		this.zipPassword = zipPassword;
		this.useZipCrypto = useZipCrypto;
		this.createBlobFiles = createBlobFiles;
		this.createClobFiles = createClobFiles;
		this.fileExtension = fileExtension;
	}

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

	protected File writeLobFile(final String exportFilePath, final String lobType, final InputStream dataStream) throws Exception {
		String lobOutputFilePathPrefix = exportFilePath;
		if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.ZIP.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.ZIP.getDefaultFileExtension().length());
		} else if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.ZIP.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.TARGZ.getDefaultFileExtension().length());
		} else if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.ZIP.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.TGZ.getDefaultFileExtension().length());
		} else if (lobOutputFilePathPrefix.endsWith("." + FileCompressionType.ZIP.getDefaultFileExtension())) {
			lobOutputFilePathPrefix = exportFilePath.substring(0, exportFilePath.length() - 1 - FileCompressionType.GZ.getDefaultFileExtension().length());
		}
		if (lobOutputFilePathPrefix.endsWith("." + fileExtension)) {
			lobOutputFilePathPrefix = lobOutputFilePathPrefix.substring(0, lobOutputFilePathPrefix.length() - (fileExtension.length() + 1));
		}
		final File lobOutputFile = File.createTempFile(new File(lobOutputFilePathPrefix).getName() + "_", "." + lobType + (compressionType != null ? "." + compressionType.getDefaultFileExtension() : ""), new File(exportFilePath).getParentFile());

		try {
			OutputStream outputStream = null;
			File tempFile = null;
			try {
				if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.ZIP.getDefaultFileExtension())) {
					outputStream = ZipUtilities.openNewZipOutputStream(lobOutputFile, null);
					((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(lobOutputFile.getName()));
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.TARGZ.getDefaultFileExtension())) {
					tempFile = File.createTempFile(lobOutputFile.getAbsolutePath(), null);
					outputStream = new FileOutputStream(tempFile);
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.TGZ.getDefaultFileExtension())) {
					tempFile = File.createTempFile(lobOutputFile.getAbsolutePath(), null);
					outputStream = new FileOutputStream(tempFile);
				} else if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.GZ.getDefaultFileExtension())) {
					outputStream = new GZIPOutputStream(new FileOutputStream(lobOutputFile));
				} else {
					outputStream = new FileOutputStream(lobOutputFile);
				}

				IoUtilities.copy(dataStream, outputStream);

				outputStream.close();

				if (Utilities.endsWithIgnoreCase(lobOutputFile.getName(), "." + FileCompressionType.ZIP.getDefaultFileExtension())) {
					Zip4jUtilities.createPasswordSecuredZipFile(lobOutputFile.getAbsolutePath(), zipPassword, false);
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
			throw new Exception("Error creating blob file '" + lobOutputFile.getAbsolutePath() + "': " + e.getMessage());
		}
	}
}
