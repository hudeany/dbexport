package de.soderer.utilities.kdbx.util;

import java.nio.charset.StandardCharsets;

public class VariantDictionaryEntry {
	public enum Type {
		END(0x00, Void.class),
		UINT_32(0x04, Integer.class), // unsigned
		UINT_64(0x05, Long.class), // unsigned
		BOOL(0x08, Boolean.class),
		INT_32(0x0C, Integer.class),
		INT_64(0x0D, Long.class),
		STRING(0x18, String.class), // UTF-8 String
		BYTE_ARRAY(0x42, byte[].class);

		private final int id;
		private final Class<?> javaType;

		public int getId() {
			return id;
		}

		public Class<?> getJavaType() {
			return javaType;
		}

		Type(final int id, final Class<?> javaType) {
			this.id = id;
			this.javaType = javaType;
		}

		public static Type fromTypeId(final int typeId) {
			for (final Type type : Type.values()) {
				if (type.id == typeId) {
					return type;
				}
			}
			throw new RuntimeException("Invalid type id: " + "0x" + Integer.toHexString(typeId));
		}

		public byte[] fromJavaValue(final Object value) {
			switch (this) {
				case END:
					return new byte[0];
				case STRING:
					return ((String) value).getBytes(StandardCharsets.UTF_8);
				case INT_32:
					return Utilities.getLittleEndianBytes((Integer) value);
				case INT_64:
					return Utilities.getLittleEndianBytes((Long) value);
				case UINT_32:
					return Utilities.getLittleEndianBytes((Integer) value); // unsigned
				case UINT_64:
					return Utilities.getLittleEndianBytes((Long) value); // unsigned
				case BOOL:
					return new byte[] { (byte) ((Boolean) value ? 1 : 0) };
				case BYTE_ARRAY:
					return (byte[]) value;
				default:
					throw new IllegalArgumentException("Unknown VariantDictionary type");
			}
		}

		public Object toJavaValue(final byte[] value) {
			switch (this) {
				case END:
					return null;
				case STRING:
					return new String(value, StandardCharsets.UTF_8);
				case INT_32:
					return Utilities.readIntFromLittleEndianBytes(value);
				case INT_64:
					return Utilities.readLongFromLittleEndianBytes(value);
				case UINT_32:
					return Utilities.readIntFromLittleEndianBytes(value);
				case UINT_64:
					return Utilities.readLongFromLittleEndianBytes(value);
				case BOOL:
					if (value.length != 1) {
						throw new IllegalArgumentException(this + " requires a 1-byte value, got " + value.length + " bytes");
					} else if (value[0] == 0) {
						return Boolean.FALSE;
					} else if (value[0] == 1) {
						return Boolean.TRUE;
					} else {
						throw new IllegalArgumentException(this + " requires a 1-byte value of either 0 or 1");
					}
				case BYTE_ARRAY:
					return value;
				default:
					throw new IllegalArgumentException("Unknown VariantDictionary");
			}
		}
	}

	private final Type type;
	private byte[] value;

	public Type getType() {
		return type;
	}

	public byte[] getValue() {
		return value;
	}

	public VariantDictionaryEntry(final Type type, final byte[] value) {
		this.type = type;
		this.value = value;
	}

	public Object getJavaValue() {
		return type.toJavaValue(value);
	}

	public void setJavaValue(final Object value) {
		this.value = type.fromJavaValue(value);
	}
}
