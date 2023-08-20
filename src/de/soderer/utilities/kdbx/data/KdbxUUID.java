package de.soderer.utilities.kdbx.data;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import de.soderer.utilities.kdbx.util.Utilities;

public class KdbxUUID {
	private final byte[] data;

	public KdbxUUID() {
		data = new byte[16];
		new SecureRandom().nextBytes(data);
	}

	public KdbxUUID(final byte[] data) {
		if (data == null) {
			throw new RuntimeException("UUID data must not be null");
		} else if (data.length != 16) {
			throw new RuntimeException("UUID must have a length of 16 bytes 1, but had " + data.length);
		} else {
			this.data = data;
		}
	}

	public static KdbxUUID fromHex(final String hexString) {
		try {
			return new KdbxUUID(Utilities.fromHexString(hexString));
		} catch (final Exception e) {
			throw new RuntimeException("Invalid hey string value for UUID: " + hexString, e);
		}
	}

	public static KdbxUUID fromBase64(final String base64String) {
		if (Utilities.isBlank(base64String)) {
			return null;
		} else {
			try {
				final byte[] uuidBytes = Base64.getDecoder().decode(base64String);
				return new KdbxUUID(uuidBytes);
			} catch (final Exception e) {
				throw new RuntimeException("Invalid base64 string value for UUID: " + base64String, e);
			}
		}
	}

	public String toBase64() {
		return Base64.getEncoder().encodeToString(data);
	}

	@Override
	public boolean equals(final Object other) {
		if (other instanceof KdbxUUID) {
			final KdbxUUID otherUuid = (KdbxUUID) other;
			return Arrays.equals(otherUuid.data, data);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}

	@Override
	public String toString() {
		return toHex();
	}

	public String toHex() {
		return Utilities.toHexString(data, "");
	}
}
