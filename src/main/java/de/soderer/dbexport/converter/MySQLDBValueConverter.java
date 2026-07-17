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

public class MySQLDBValueConverter extends DefaultDBValueConverter {
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
			if (resultSet.wasNull()) {
				value = null;
			} else {
				value = resultSet.getInt(columnIndex);
			}
		} else {
			value = super.convert(metaData, resultSet, columnIndex, exportFilePath);
		}
		return value;
	}
}
