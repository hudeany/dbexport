package de.soderer.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

public class ReaderInputStream extends InputStream {
	private static final int DEFAULT_BUFFER_SIZE = 1024;

	private final Reader reader;
	private final CharsetEncoder encoder;
	private final CharBuffer encoderIn;
	private final ByteBuffer encoderOut;
	private CoderResult lastCoderResult;
	private boolean endOfInput;

	public ReaderInputStream(final Reader reader, final CharsetEncoder encoder) {
		this(reader, encoder, DEFAULT_BUFFER_SIZE);
	}

	public ReaderInputStream(final Reader reader, final CharsetEncoder encoder, final int bufferSize) {
		this.reader = reader;
		this.encoder = encoder;
		encoderIn = CharBuffer.allocate(bufferSize);
		encoderIn.flip();
		encoderOut = ByteBuffer.allocate(128);
		encoderOut.flip();
	}

	public ReaderInputStream(final Reader reader, final Charset charset, final int bufferSize) {
		this(reader, charset.newEncoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE), bufferSize);
	}

	public ReaderInputStream(final Reader reader, final Charset charset) {
		this(reader, charset, DEFAULT_BUFFER_SIZE);
	}

	private void fillBuffer() throws IOException {
		if (!endOfInput && (lastCoderResult == null || lastCoderResult.isUnderflow())) {
			encoderIn.compact();
			final int position = encoderIn.position();
			final int nextDataSize = reader.read(encoderIn.array(), position, encoderIn.remaining());
			if (nextDataSize == -1) {
				endOfInput = true;
			} else {
				encoderIn.position(position+nextDataSize);
			}
			encoderIn.flip();
		}
		encoderOut.compact();
		lastCoderResult = encoder.encode(encoderIn, encoderOut, endOfInput);
		encoderOut.flip();
	}

	@Override
	public int read(final byte[] array, int offset, int length) throws IOException {
		if (array == null) {
			throw new IOException("Mandatory array is null");
		}
		if (length < 0 || offset < 0 || (offset + length) > array.length) {
			throw new IndexOutOfBoundsException("Array Size=" + array.length + ", offset=" + offset + ", length=" + length);
		}
		int read = 0;
		if (length == 0) {
			return 0;
		}
		while (length > 0) {
			if (encoderOut.hasRemaining()) {
				final int nextDataSize = Math.min(encoderOut.remaining(), length);
				encoderOut.get(array, offset, nextDataSize);
				offset += nextDataSize;
				length -= nextDataSize;
				read += nextDataSize;
			} else {
				fillBuffer();
				if (endOfInput && !encoderOut.hasRemaining()) {
					break;
				}
			}
		}
		return read == 0 && endOfInput ? -1 : read;
	}

	@Override
	public int read(final byte[] byteArray) throws IOException {
		return read(byteArray, 0, byteArray.length);
	}

	@Override
	public int read() throws IOException {
		while (true) {
			if (encoderOut.hasRemaining()) {
				return encoderOut.get() & 0xFF;
			}
			fillBuffer();
			if (endOfInput && !encoderOut.hasRemaining()) {
				return -1;
			}
		}
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}
}
