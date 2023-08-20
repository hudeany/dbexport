package de.soderer.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

public class BOMInputStream extends PushbackInputStream {
	private BOM bom = null;
	private boolean skipped = false;

	public BOMInputStream(final InputStream inputStream) throws IOException {
		super(inputStream, 4);

		final byte firstBytes[] = new byte[4];
		final int read = read(firstBytes);

		switch (read) {
			case 4:
				if ((firstBytes[0] == (byte) 0xFF) && (firstBytes[1] == (byte) 0xFE) && (firstBytes[2] == (byte) 0x00) && (firstBytes[3] == (byte) 0x00)) {
					bom = BOM.UTF_32_LE;
					break;
				} else if ((firstBytes[0] == (byte) 0x00) && (firstBytes[1] == (byte) 0x00) && (firstBytes[2] == (byte) 0xFE) && (firstBytes[3] == (byte) 0xFF)) {
					bom = BOM.UTF_32_BE;
					break;
				}
				//$FALL-THROUGH$
			case 3:
				if ((firstBytes[0] == (byte) 0xEF) && (firstBytes[1] == (byte) 0xBB) && (firstBytes[2] == (byte) 0xBF)) {
					bom = BOM.UTF_8;
					break;
				}
				//$FALL-THROUGH$
			case 2:
				if ((firstBytes[0] == (byte) 0xFF) && (firstBytes[1] == (byte) 0xFE)) {
					bom = BOM.UTF_16_LE;
					break;
				} else if ((firstBytes[0] == (byte) 0xFE) && (firstBytes[1] == (byte) 0xFF)) {
					bom = BOM.UTF_16_BE;
					break;
				}
				//$FALL-THROUGH$
			default:
				bom = BOM.NONE;
				break;
		}

		if (read > 0) {
			unread(firstBytes, 0, read);
		}
	}

	public final BOM getBOM() {
		return bom;
	}

	public final synchronized BOMInputStream skipBOM() throws IOException {
		if (!skipped) {
			skip(bom.bytes.length);
			skipped = true;
		}
		return this;
	}
}