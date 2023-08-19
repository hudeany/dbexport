package de.soderer.utilities.kdbx.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * OutputStream to create "Hashed Message Authentication Code" (HMAC) checksums.<br />
 * Default ByteOrder is Little Endian.<br />
 * Default HmacAlgorithmName is HmacSHA256.<br />
 * Default KeyHashDigestName is SHA-512.<br />
 */
public class HmacOutputStream extends OutputStream {
	private static final int BLOCK_SIZE = 1024*1024;

	private OutputStream baseOutputStream;
	private byte[] key;

	private ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
	private String hmacAlgorithmName = "HmacSHA256";
	private String keyHashDigestName = "SHA-512";

	private long hmacBlockIndex;
	private ByteArrayOutputStream bufferStream;

	public HmacOutputStream(final OutputStream outputStream, final byte[] key) {
		if (outputStream == null) {
			throw new IllegalArgumentException("Invalid empty outputStream parameter for HmacOutputStream");
		} else if (key == null || key.length <= 0) {
			throw new IllegalArgumentException("Invalid empty key parameter for HmacOutputStream");
		} else if (key.length != 64) {
			throw new IllegalArgumentException("Expected a 64-byte key but got " + key.length + " bytes");
		} else {
			baseOutputStream = outputStream;
			this.key = key;
			hmacBlockIndex = 0;
		}
	}

	public HmacOutputStream setByteOrder(final ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
		return this;
	}

	public HmacOutputStream setHmacAlgorithmName(final String hmacAlgorithmName) {
		this.hmacAlgorithmName = hmacAlgorithmName;
		return this;
	}

	public HmacOutputStream setKeyHashDigestName(final String keyHashDigestName) {
		this.keyHashDigestName = keyHashDigestName;
		return this;
	}

	@Override
	public void write(final int b) throws IOException {
		if (bufferStream == null) {
			bufferStream = new ByteArrayOutputStream();
		}
		bufferStream.write(b);
		if (bufferStream.size() == BLOCK_SIZE) {
			finalizeBlockByHmacChecksum();
		}
	}

	public void finalizeBlockByHmacChecksum() throws IOException {
		if (bufferStream != null) {
			writeHmacBlock(bufferStream.toByteArray());
			bufferStream = null;
		}
	}

	private void writeHmacBlock(final byte[] blockData) throws IOException {
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance(keyHashDigestName);
		} catch (final NoSuchAlgorithmException e) {
			throw new RuntimeException("Digest not available: " + keyHashDigestName, e);
		}
		digest.update(ByteBuffer.allocate(8).order(byteOrder).putLong(hmacBlockIndex).array());
		final byte[] hmacBlockKey = digest.digest(key);

		Mac hmac;
		try {
			hmac = Mac.getInstance(hmacAlgorithmName);
			hmac.init(new SecretKeySpec(hmacBlockKey, hmacAlgorithmName));
		} catch (final InvalidKeyException e) {
			throw new RuntimeException("Invalid key for digest: " + hmacAlgorithmName, e);
		} catch (final NoSuchAlgorithmException e) {
			throw new RuntimeException("Hmac algorithm not available: " + hmacAlgorithmName, e);
		}

		hmac.update(ByteBuffer.allocate(8).order(byteOrder).putLong(hmacBlockIndex).array());
		final byte[] blockLengthBytes = ByteBuffer.allocate(4).order(byteOrder).putInt(blockData.length).array();
		hmac.update(blockLengthBytes);
		hmac.update(blockData, 0, blockData.length);
		final byte[] blockHmacBytes = hmac.doFinal();
		baseOutputStream.write(blockHmacBytes);
		baseOutputStream.write(blockLengthBytes);
		baseOutputStream.write(blockData);

		hmacBlockIndex++;
	}

	@Override
	public void close() throws IOException {
		if (baseOutputStream != null) {
			finalizeBlockByHmacChecksum();

			// write final empty block to signal proper data end
			writeHmacBlock(new byte[0]);

			try {
				baseOutputStream.close();
			} finally {
				baseOutputStream = null;
			}
		}
	}
}
