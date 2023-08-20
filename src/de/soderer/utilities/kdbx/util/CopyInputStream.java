package de.soderer.utilities.kdbx.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class CopyInputStream extends InputStream {
	private InputStream baseInputStream;

	private ByteArrayOutputStream bufferStream = null;

	public CopyInputStream(final InputStream inputStream) {
		if (inputStream == null) {
			throw new IllegalArgumentException("Invalid empty inputStream parameter for CopyInputStream");
		} else {
			baseInputStream = inputStream;
		}
	}

	public CopyInputStream setCopyOnRead(final boolean copyOnRead) {
		if (copyOnRead && bufferStream == null) {
			bufferStream = new ByteArrayOutputStream();
		} else {
			bufferStream = null;
		}
		return this;
	}

	public byte[] getCopiedData() {
		return bufferStream.toByteArray();
	}

	@Override
	public int read() throws IOException {
		final int readByte = baseInputStream.read();
		if (bufferStream != null) {
			bufferStream.write(readByte);
		}
		return readByte;
	}

	@Override
	public int read(final byte[] data) throws IOException {
		final int readBytes = baseInputStream.read(data);
		if (readBytes > 0 && bufferStream != null) {
			bufferStream.write(data, 0, readBytes);
		}
		return readBytes;
	}

	@Override
	public int read(final byte[] data, final int offset, final int length) throws IOException {
		final int readBytes = baseInputStream.read(data, offset, length);
		if (readBytes > 0 && bufferStream != null) {
			bufferStream.write(data, offset, readBytes);
		}
		return readBytes;
	}

	@Override
	public void close() throws IOException {
		if (baseInputStream != null) {
			try {
				baseInputStream.close();
			} finally {
				baseInputStream = null;
			}
		}
	}
}
