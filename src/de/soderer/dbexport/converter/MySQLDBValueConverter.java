package de.soderer.dbexport.converter;

import java.io.File;
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

import de.soderer.utilities.IoUtilities;

public class MySQLDBValueConverter extends DefaultDBValueConverter {
	public MySQLDBValueConverter(final boolean zip, final char[] zipPassword, final boolean useZipCrypto, final boolean createBlobFiles, final boolean createClobFiles, final String fileExtension) {
		super(zip, zipPassword, useZipCrypto, createBlobFiles, createClobFiles, fileExtension);
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
				final File blobOutputFile = new File(getLobFilePath(exportFilePath, "blob"));
				try {
					try (OutputStream outputStream = openLobOutputStream(blobOutputFile);
							InputStream dataStream = blob.getBinaryStream()) {
						IoUtilities.copy(dataStream, outputStream);
						checkAndCloseZipEntry(outputStream, blobOutputFile);
					}
					value = blobOutputFile;
				} catch (final Exception e) {
					throw new Exception("Error creating blob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
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
				final File blobOutputFile = new File(getLobFilePath(exportFilePath, "clob"));
				try {
					try (OutputStream outputStream = openLobOutputStream(blobOutputFile);
							Reader reader = clob.getCharacterStream()) {
						int readChars;
						final char[] cbuf = new char[1024];
						while ((readChars = reader.read(cbuf)) >= 0) {
							final StringBuilder buffer = new StringBuilder();
							buffer.append(cbuf, 0, readChars);
							outputStream.write(buffer.toString().getBytes(StandardCharsets.UTF_8));
						}
						checkAndCloseZipEntry(outputStream, blobOutputFile);
					}
					value = blobOutputFile;
				} catch (final Exception e) {
					throw new Exception("Error creating clob file '" + blobOutputFile.getAbsolutePath() + "': " + e.getMessage());
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
