package de.soderer.utilities;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TextFilePropertiesReader extends TextPropertiesReader {
	private File file;
	private byte[] data;
	private Tuple<String, Boolean> defaultEncoding = new Tuple<>(StandardCharsets.UTF_8.name(), false);
	private Tuple<String, Boolean> encodingData = null;
	private long filesize = -1;

	public TextFilePropertiesReader(final String filePath, final Tuple<String, Boolean> encodingData) throws Exception {
		this(new File(filePath), encodingData);
	}

	public TextFilePropertiesReader(final String filePath) throws Exception {
		this(new File(filePath));
	}

	public TextFilePropertiesReader(final File file) throws Exception {
		this(file, null);
	}

	public TextFilePropertiesReader(final File file, final Tuple<String, Boolean> encodingData) throws Exception {
		this.file = file;

		if (!file.exists()) {
			throw new Exception("File does not exist");
		} else if (!file.isFile()) {
			throw new Exception("Path isn't a file");
		}

		this.encodingData = encodingData;
	}

	public TextFilePropertiesReader(final byte[] data) throws Exception {
		this(data, null);
	}

	public TextFilePropertiesReader(final byte[] data, final Tuple<String, Boolean> encodingData) throws Exception {
		this.data = data;
		filesize = data.length;
		this.encodingData = encodingData;
	}

	public String readFileToString() throws Exception {
		if (data == null) {
			filesize = file.length();
			data = Utilities.readFileToByteArray(file);
		}

		if (encodingData == null) {
			// try to detect encoding
			encodingData = TextUtilities.detectEncoding(data);

			if (encodingData == null) {
				// File encoding cannot be detected so use defaultEncoding
				encodingData = defaultEncoding;
			}

			dataString = decodeByteArray(data, encodingData);

			// Check for invalid UTF-8 character
			if (encodingData != null && StandardCharsets.UTF_8.name().equalsIgnoreCase(encodingData.getFirst()) && dataString.contains("�")) {
				// If encoding was UTF-8 (detected or default) but contains illegal chars, then try most common other encoding
				encodingData = new Tuple<>(StandardCharsets.ISO_8859_1.name(), false);
				dataString = decodeByteArray(data, encodingData);
			}
		} else {
			// only use defined encoding
			dataString = decodeByteArray(data, encodingData);
		}

		super.readProperties();
		return dataString;
	}

	public String readFileToString(long startPosition, int length) throws Exception {
		if (data == null) {
			filesize = file.length();
			if (startPosition < 0) {
				startPosition = 0;
			}
			if (filesize < length) {
				length = (int) filesize;
			}
			if (filesize < startPosition + length) {
				startPosition = filesize - length;
			}

			final byte[] subdata = new byte[length];
			try (FileInputStream inputStream = new FileInputStream(file)) {
				inputStream.skip(startPosition);
				inputStream.read(subdata);
			}

			if (encodingData == null) {
				// try to detect encoding
				encodingData = TextUtilities.detectEncoding(subdata);

				if (encodingData == null) {
					// File encoding cannot be detected so use defaultEncoding
					encodingData = defaultEncoding;
				}

				dataString = decodeByteArray(subdata, encodingData);

				// Check for invalid UTF-8 character
				if (encodingData != null && StandardCharsets.UTF_8.name().equalsIgnoreCase(encodingData.getFirst()) && dataString.contains("�")) {
					// If encoding was UTF-8 (detected or default) but contains illegal chars, then try most common other encoding
					encodingData = new Tuple<>(StandardCharsets.ISO_8859_1.name(), false);
					dataString = decodeByteArray(subdata, encodingData);
				}
			} else {
				// only use defined encoding
				dataString = decodeByteArray(subdata, encodingData);
			}

			super.readProperties();
			return dataString;
		} else {
			if (startPosition < 0) {
				startPosition = 0;
			}
			if (filesize < length) {
				length = (int) filesize;
			}
			if (filesize < startPosition + length) {
				startPosition = filesize - length;
			}

			final byte[] subdata = new byte[length];
			for (int i = 0; i < length; i++) {
				subdata[i] = data[(int) startPosition + i];
			}

			if (encodingData == null) {
				// try to detect encoding
				encodingData = TextUtilities.detectEncoding(subdata);

				if (encodingData == null) {
					// File encoding cannot be detected so use defaultEncoding
					encodingData = defaultEncoding;
				}

				dataString = decodeByteArray(subdata, encodingData);

				// Check for invalid UTF-8 character
				if (encodingData != null && StandardCharsets.UTF_8.name().equalsIgnoreCase(encodingData.getFirst()) && dataString.contains("�")) {
					// If encoding was UTF-8 (detected or default) but contains illegal chars, then try most common other encoding
					encodingData = new Tuple<>(StandardCharsets.ISO_8859_1.name(), false);
					dataString = decodeByteArray(subdata, encodingData);
				}
			} else {
				// only use defined encoding
				dataString = decodeByteArray(subdata, encodingData);
			}

			super.readProperties();
			return dataString;
		}
	}

	@Override
	public void readProperties() {
		try {
			readFileToString();
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	public long getFilesize() {
		return filesize;
	}

	public Tuple<String, Boolean> getEncoding() {
		return encodingData;
	}

	public static Tuple<String, Boolean> getEncodingData(final String encodingString) {
		if ("UnicodeBig".equalsIgnoreCase(encodingString) || "UTF-16BE-BOM".equalsIgnoreCase(encodingString)) {
			return new Tuple<>("UTF-16BE", true);
		} else if ("UnicodeLittle".equalsIgnoreCase(encodingString) || "UTF-16LE-BOM".equalsIgnoreCase(encodingString)) {
			return new Tuple<>("UTF-16LE", true);
		} else if ("UTF-8-BOM".equalsIgnoreCase(encodingString)) {
			return new Tuple<>("UTF-8", true);
		} else {
			return new Tuple<>(encodingString, false);
		}
	}

	public static String getEncodingDisplayString(final Tuple<String, Boolean> encodingData) {
		if (encodingData == null) {
			return "";
		} else {
			if ("UTF-16BE".equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond()) {
				return "UTF-16BE-BOM";
			} else if ("UTF-16BE".equalsIgnoreCase(encodingData.getFirst()) && !encodingData.getSecond()) {
				return "UTF-16BE";
			} else if ("UnicodeBig".equalsIgnoreCase(encodingData.getFirst())) {
				return "UTF-16BE-BOM";
			} else if ("UTF-16".equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond()) {
				return "UTF-16BE-BOM";
			} else if ("UTF-16".equalsIgnoreCase(encodingData.getFirst()) && !encodingData.getSecond()) {
				return "UTF-16BE";
			} else if ("UnicodeLittle".equalsIgnoreCase(encodingData.getFirst())) {
				return "UTF-16LE-BOM";
			} else if ("UTF-16LE".equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond()) {
				return "UTF-16LE-BOM";
			} else if ("UTF-16LE".equalsIgnoreCase(encodingData.getFirst()) && !encodingData.getSecond()) {
				return "UTF-16LE";
			} else if ("UTF-8".equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond()) {
				return "UTF-8-BOM";
			} else {
				return encodingData.getFirst();
			}
		}
	}

	public static byte[] encodeString(final String text, final Tuple<String, Boolean> encodingData) throws Exception {
		if (encodingData == null) {
			throw new Exception("Missing encoding data");
		} else {
			final ByteArrayOutputStream output = new ByteArrayOutputStream();
			String encoding = encodingData.getFirst();
			if ("UnicodeBig".equalsIgnoreCase(encodingData.getFirst()) || ("UTF-16BE".equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond())
					|| ("UTF-16".equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond())) {
				output.write(BOM.UTF_16_BE.getBytes());
				encoding = "UTF-16BE";
			} else if ("UnicodeLittle".equalsIgnoreCase(encodingData.getFirst()) || ("UTF-16LE".equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond())) {
				output.write(BOM.UTF_16_LE.getBytes());
				encoding = "UTF-16LE";
			} else if (StandardCharsets.UTF_8.name().equalsIgnoreCase(encodingData.getFirst()) && encodingData.getSecond()) {
				output.write(BOM.UTF_8.getBytes());
			}
			output.write(text.getBytes(encoding));
			return output.toByteArray();
		}
	}

	public static String decodeByteArray(final byte[] data, final Tuple<String, Boolean> encodingData) throws IOException {
		if (data.length > 2 && data[0] == BOM.UTF_16_BE.getBytes()[0] && data[1] == BOM.UTF_16_BE.getBytes()[1]) {
			return new String(data, 2, data.length - 2, encodingData.getFirst());
		} else if (data.length > 2 && data[0] == BOM.UTF_16_LE.getBytes()[0] && data[1] == BOM.UTF_16_LE.getBytes()[1]) {
			return new String(data, 2, data.length - 2, encodingData.getFirst());
		} else if (data.length > 3 && data[0] == BOM.UTF_8.getBytes()[0] && data[1] == BOM.UTF_8.getBytes()[1] && data[2] == BOM.UTF_8.getBytes()[2]) {
			return new String(data, 3, data.length - 3, encodingData.getFirst());
		} else {
			return new String(data, encodingData.getFirst());
		}
	}

	public void setDefaultEncodingData(final Tuple<String, Boolean> value) {
		if (value != null) {
			defaultEncoding = value;
		}
	}

	public List<Long> scanLineStartIndexes() throws Exception {
		if (file != null) {
			try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
				return TextUtilities.scanLineStartIndexes(input);
			} catch (@SuppressWarnings("unused") final Exception e) {
				return null;
			}
		} else {
			throw new Exception("Not implemented");
		}
	}

	public List<Long> scanLinebreakIndexes() throws Exception {
		if (file != null) {
			try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
				return TextUtilities.scanLinebreakIndexes(input);
			} catch (@SuppressWarnings("unused") final Exception e) {
				return null;
			}
		} else {
			throw new Exception("Not implemented");
		}
	}
}
