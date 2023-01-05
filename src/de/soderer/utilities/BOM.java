package de.soderer.utilities;

/**
 * Byte Order Mark
 */
public final class BOM {
	/** UTF-8 BOM (Byte Order Mark) character for readers. */
	public static final char BOM_UTF_8_CHAR = (char) 65279;

	/**
	 * NONE.
	 */
	public static final BOM NONE = new BOM(new byte[] {}, "NONE");

	/**
	 * UTF-8 BOM (EF BB BF, "ï»¿", (char) 65279).
	 */
	public static final BOM UTF_8 = new BOM(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF }, "UTF-8");

	/**
	 * UTF-16, little-endian (FF FE, "ÿþ").
	 */
	public static final BOM UTF_16_LE = new BOM(new byte[] { (byte) 0xFF, (byte) 0xFE }, "UTF-16 little-endian");

	/**
	 * UTF-16, big-endian (FE FF, "þÿ").
	 */
	public static final BOM UTF_16_BE = new BOM(new byte[] { (byte) 0xFE, (byte) 0xFF }, "UTF-16 big-endian");

	/**
	 * UTF-32, little-endian (FF FE 00 00).
	 */
	public static final BOM UTF_32_LE = new BOM(new byte[] { (byte) 0xFF, (byte) 0xFE, (byte) 0x00, (byte) 0x00 }, "UTF-32 little-endian");

	/**
	 * UTF-32, big-endian (00 00 FE FF).
	 */
	public static final BOM UTF_32_BE = new BOM(new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0xFE, (byte) 0xFF }, "UTF-32 big-endian");

	final byte bytes[];
	private final String description;

	private BOM(final byte bom[], final String description) {
		bytes = bom;
		this.description = description;
	}

	public byte[] getBytes() {
		final int length = bytes.length;
		final byte[] result = new byte[length];
		System.arraycopy(bytes, 0, result, 0, length);
		return result;
	}

	@Override
	public String toString() {
		return description;
	}
}
