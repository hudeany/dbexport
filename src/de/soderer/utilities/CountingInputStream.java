package de.soderer.utilities;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CountingInputStream extends FilterInputStream {
	private static int EOF = -1;

	private long count;

	public CountingInputStream(final InputStream in) {
		super(in);
		count = 0;
	}

	protected synchronized void afterRead(final int n) {
		if (n != EOF) {
			count += n;
		}
	}

	public int getCount() {
		final long result = getByteCount();
		if (result > Integer.MAX_VALUE) {
			throw new ArithmeticException("The byte count " + result + " is too large to be converted to an int");
		}
		return (int) result;
	}

	public int resetCount() {
		final long result = resetByteCount();
		if (result > Integer.MAX_VALUE) {
			throw new ArithmeticException("The byte count " + result + " is too large to be converted to an int");
		}
		return (int) result;
	}

	public synchronized long getByteCount() {
		return count;
	}

	public synchronized long resetByteCount() {
		final long tmp = count;
		count = 0;
		return tmp;
	}

	@Override
	public int read() throws IOException {
		final int b = in.read();
		afterRead(b != EOF ? 1 : EOF);
		return b;
	}

	@Override
	public int read(final byte[] bts) throws IOException {
		final int n = in.read(bts);
		afterRead(n);
		return n;
	}

	@Override
	public int read(final byte[] bts, final int off, final int len) throws IOException {
		final int n = in.read(bts, off, len);
		afterRead(n);
		return n;
	}

	@Override
	public long skip(final long ln) throws IOException {
		final long skip = in.skip(ln);
		count += skip;
		return skip;
	}
}
