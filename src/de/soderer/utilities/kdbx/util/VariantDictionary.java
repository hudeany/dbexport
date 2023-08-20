package de.soderer.utilities.kdbx.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class VariantDictionary extends LinkedHashMap<String, VariantDictionaryEntry> {
	private static final long serialVersionUID = 267135612072510235L;

	/**
	 * A little-endian system stores the least-significant byte at the smallest address.
	 */
	public static final byte[] VERSION = new byte[] { 0x00, 0x01 };

	// Holds the UUID of the KeyDerivationFunction (KDF) algorithm
	public static final String KDF_UUID = "$UUID";

	// AES params
	public static final String KDF_AES_ROUNDS = "R";
	public static final String KDF_AES_SEED = "S";

	// Argon2 KDF parameters
	public static final String KDF_ARGON2_SALT = "S";
	public static final String KDF_ARGON2_PARALLELISM = "P";
	public static final String KDF_ARGON2_MEMORY_IN_BYTES = "M";
	public static final String KDF_ARGON2_ITERATIONS = "I";
	public static final String KDF_ARGON2_VERSION = "V";

	public void put(final String key, final VariantDictionaryEntry.Type type, final Object value) {
		final VariantDictionaryEntry entry = new VariantDictionaryEntry(type, new byte[0]);
		entry.setJavaValue(value);
		super.put(key, entry);
	}

	public void write(final OutputStream outputStream) throws Exception {
		outputStream.write(VERSION);

		for (final Entry<String, VariantDictionaryEntry> entry : entrySet()) {
			outputStream.write(entry.getValue().getType().getId());

			final byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
			outputStream.write(Utilities.getLittleEndianBytes(keyBytes.length));
			outputStream.write(keyBytes);

			final byte[] dataBytes = entry.getValue().getValue();
			outputStream.write(Utilities.getLittleEndianBytes(dataBytes.length));
			outputStream.write(dataBytes);
		}

		outputStream.write(VariantDictionaryEntry.Type.END.getId());
	}

	public static VariantDictionary read(final InputStream inputStream) throws Exception {
		final byte[] versionBytes = new byte[2];
		inputStream.read(versionBytes);
		if (!Arrays.equals(versionBytes, VERSION)) {
			throw new IOException("Unsupported VariantDictionary version " + Utilities.toHexString(versionBytes) + ", expected " + Utilities.toHexString(VERSION));
		}

		final VariantDictionary variantDictionary = new VariantDictionary();
		VariantDictionaryEntry.Type type;
		while ((type = VariantDictionaryEntry.Type.fromTypeId(inputStream.read())) != VariantDictionaryEntry.Type.END) {
			final int keyLen = Utilities.readLittleEndianIntFromStream(inputStream);
			final byte[] keyByteBuffer = new byte[keyLen];
			inputStream.read(keyByteBuffer);
			final String key = new String(keyByteBuffer, StandardCharsets.UTF_8);

			final int valueLen = Utilities.readLittleEndianIntFromStream(inputStream);
			final byte[] valueByteBuffer = new byte[valueLen];
			inputStream.read(valueByteBuffer);

			variantDictionary.put(key, new VariantDictionaryEntry(type, valueByteBuffer));
		}
		return variantDictionary;
	}
}
