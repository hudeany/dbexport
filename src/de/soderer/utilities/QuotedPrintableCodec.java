package de.soderer.utilities;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

/**
 * See: https://en.wikipedia.org/wiki/Quoted-printable
 */
public class QuotedPrintableCodec {
	/**
	 * Decode text from QUOTED-PRINTABLE
	 *
	 * @param value
	 * @param charset
	 * @return
	 */
	public static String decode(final String value, final Charset charset) {
		final ByteArrayOutputStream decodedByteArray = new ByteArrayOutputStream();
		final char[] charArray = value.toCharArray();
		for (int i = 0; i < charArray.length; i++) {
			if (charArray[i] == '=') {
				if ((i + 1) < charArray.length) {
					final char nextChar = charArray[i + 1];
					if (nextChar == '\n') {
						// Ignore this '=' char and the following LF
						i++;
					} else if (nextChar == '\r' && (i + 2) < charArray.length && charArray[i + 2] == '\n') {
						// Ignore this '=' char and the following CRLF
						i = i + 2;
					} else if (nextChar == '\r') {
						// Ignore this '=' char and the following CR
						i++;
					} else if ((i + 2) < charArray.length) {
						decodedByteArray.write(BitUtilities.hexToByte(nextChar, charArray[i + 2]));
						i = i + 2;
					} else {
						// Ignore this '=' char and the following single char. Error state because hex must have 2 chars
						i++;
					}
				} else {
					// Ignore this '=' char and the following EOF
					i++;
				}
			} else {
				for (final byte data : Character.toString(charArray[i]).getBytes(charset)) {
					decodedByteArray.write(data);
				}
			}
		}
		return new String(decodedByteArray.toByteArray(), charset);
	}

	/**
	 * Encode a text in QUOTED-PRINTABLE<br />
	 * All characters are encoded<br />
	 * Maximum resulting line length is 76 characters
	 *
	 * @param value
	 * @param charset
	 * @return
	 */
	public static String encode(final String value, final Charset charset) {
		final StringBuilder encodedText = new StringBuilder();
		int currentLineLength = 0;
		for (final byte data : value.getBytes(charset)) {
			if (currentLineLength == 69) {
				encodedText.append("=\n");
				currentLineLength = 0;
			}

			encodedText.append("=").append(BitUtilities.byteToHex(data));
			currentLineLength += 3;
		}
		return encodedText.toString();
	}
}
