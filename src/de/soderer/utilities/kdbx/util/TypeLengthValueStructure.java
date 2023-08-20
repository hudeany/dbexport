package de.soderer.utilities.kdbx.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TypeLengthValueStructure {
	int typeId;
	byte[] data;

	public int getTypeId() {
		return typeId;
	}

	public byte[] getData() {
		return data;
	}

	public TypeLengthValueStructure(final int typeId, final byte[] data) {
		this.typeId = typeId;
		this.data = data;
	}

	public void write(final OutputStream outputStream, final boolean useIntLength) throws IOException {
		outputStream.write(typeId);
		if (useIntLength) {
			outputStream.write(Utilities.getLittleEndianBytes(data == null ? 0 : data.length));
		} else {
			outputStream.write(Utilities.getLittleEndianBytes((short) (data == null ? 0 : data.length)));
		}
		if (data != null) {
			outputStream.write(data);
		}
	}

	public static TypeLengthValueStructure read(final InputStream inputStream, final boolean useIntLength) throws Exception {
		final int typeId = inputStream.read();

		int length;
		if (useIntLength) {
			length = Utilities.readLittleEndianIntFromStream(inputStream);
		} else {
			length = Utilities.readLittleEndianShortFromStream(inputStream);
		}
		final byte[] data;
		if (length > 0) {
			data = new byte[length];
			final int bytesRead = inputStream.read(data);
			if (bytesRead != length) {
				throw new Exception("Cannot read TypeLengthValueStructure data of length " + length);
			}
		} else {
			data = new byte[0];
		}

		return new TypeLengthValueStructure(typeId, data);
	}
}
