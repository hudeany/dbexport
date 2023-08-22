package de.soderer.utilities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.Charset;

public class IoUtilities {
	public static String toString(final InputStream inputStream, final Charset encoding) throws IOException {
		return new String(toByteArray(inputStream), encoding);
	}

	public static byte[] toByteArray(final InputStream inputStream) throws IOException {
		if (inputStream == null) {
			return null;
		} else {
			try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
				copy(inputStream, byteArrayOutputStream);
				return byteArrayOutputStream.toByteArray();
			}
		}
	}

	public static long copy(final InputStream inputStream, final OutputStream outputStream) throws IOException {
		final byte[] buffer = new byte[4096];
		int lengthRead = -1;
		long bytesCopied = 0;
		while ((lengthRead = inputStream.read(buffer)) > -1) {
			outputStream.write(buffer, 0, lengthRead);
			bytesCopied += lengthRead;
		}
		outputStream.flush();
		return bytesCopied;
	}

	public static long copy(final Reader inputReader, final OutputStream outputStream, final Charset encoding) throws IOException {
		final char[] buffer = new char[4096];
		int lengthRead = -1;
		long bytesCopied = 0;
		while ((lengthRead = inputReader.read(buffer)) > -1) {
			final String data = new String(buffer, 0, lengthRead);
			outputStream.write(data.getBytes(encoding));
			bytesCopied += lengthRead;
		}
		outputStream.flush();
		return bytesCopied;
	}

	public static long getStreamSize(final InputStream inputStream) throws IOException {
		final byte[] buffer = new byte[4096];
		int lengthRead = -1;
		long bytesCopied = 0;
		while ((lengthRead = inputStream.read(buffer)) > -1) {
			bytesCopied += lengthRead;
		}
		return bytesCopied;
	}
}
