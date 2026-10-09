package de.soderer.dbexport.converter;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Base64;

import de.soderer.utilities.FileCompressionType;
import de.soderer.utilities.IoUtilities;
import de.soderer.utilities.ReaderInputStream;

/**
 * Value converter for MySQL (binary, long text and bit columns).
 */
public class MySQLDBValueConverter extends DefaultDBValueConverter {
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
	public MySQLDBValueConverter(final FileCompressionType compressionType, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(compressionType, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
	}

	@Override
	public Object convert(final ResultSetMetaData metaData, final ResultSet resultSet, final int columnIndex, final String exportFilePath) throws Exception {
		Object value;
		final int columnTypeCode = metaData.getColumnType(columnIndex);
		if (columnTypeCode == Types.LONGVARBINARY || columnTypeCode == Types.VARBINARY) {
			final Blob blob = resultSet.getBlob(columnIndex);

			if (resultSet.wasNull() || blob == null || blob.length() <= 0) {
				value = null;
			} else if (createBlobFiles) {
				try (InputStream dataStream = blob.getBinaryStream()) {
					value = writeLobFile(exportFilePath, "blob", dataStream);
				}
			} else {
				try (InputStream dataStream = blob.getBinaryStream()) {
					final byte[] data = IoUtilities.toByteArray(dataStream);
					value = Base64.getEncoder().encodeToString(data);
				}
			}
		} else if (columnTypeCode == Types.LONGVARCHAR) {
			final Clob clob = resultSet.getClob(columnIndex);

			if (resultSet.wasNull() || clob == null || clob.length() <= 0) {
				value = null;
			} else if (createClobFiles) {
				try (Reader reader = clob.getCharacterStream();
						InputStream dataStream = new ReaderInputStream(reader, StandardCharsets.UTF_8)) {
					value = writeLobFile(exportFilePath, "clob", dataStream);
				}
			} else {
				try (Reader reader = clob.getCharacterStream()) {
					final StringBuilder buffer = new StringBuilder();
					int readChars;
					final char[] cbuf = new char[1024];
					while ((readChars = reader.read(cbuf)) >= 0) {
						buffer.append(cbuf, 0, readChars);
					}
					value = buffer.toString();
				}
			}
		} else if (columnTypeCode == Types.BIT) {
			// wasNull() refers to the last read value, so the value must be read first (before, the null state of the
			// previous column was used)
			final int bitValue = resultSet.getInt(columnIndex);
			if (resultSet.wasNull()) {
				value = null;
			} else {
				value = bitValue;
			}
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
