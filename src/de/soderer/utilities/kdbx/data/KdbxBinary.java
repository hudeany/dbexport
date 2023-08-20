package de.soderer.utilities.kdbx.data;

public class KdbxBinary {
	private int id;
	private boolean compressed;
	private byte[] data;

	/**
	 * Unique id of this binary
	 */
	public KdbxBinary setId(final int id) {
		this.id = id;
		return this;
	}

	/**
	 * Unique id of this binary
	 */
	public int getId() {
		return id;
	}

	/**
	 * Compression flag (GZIP)
	 */
	public KdbxBinary setCompressed(final boolean compressed) {
		this.compressed = compressed;
		return this;
	}

	/**
	 * Compression flag (GZIP)
	 */
	public boolean isCompressed() {
		return compressed;
	}

	/**
	 * Data of this binary.
	 * The same data should not be stored multiple times.
	 */
	public KdbxBinary setData(final byte[] data) {
		this.data = data;
		return this;
	}

	/**
	 * Data of this binary.
	 * The same data should not be stored multiple times.
	 */
	public byte[] getData() {
		return data;
	}
}
