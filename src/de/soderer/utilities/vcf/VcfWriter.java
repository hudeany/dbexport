package de.soderer.utilities.vcf;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.soderer.utilities.BOM;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.QuotedPrintableCodec;
import de.soderer.utilities.Utilities;

/**
 * Writer for vcf (vCard file) format
 *
 * See: https://de.wikipedia.org/wiki/VCard#Eigenschaften
 */
public class VcfWriter implements Closeable {
	private String defaultVersion = "2.1";

	/** Number of cards written until now. */
	private int writtenCards = 0;

	private int writtenLines = 0;

	private BufferedWriter bufferedWriter = null;

	public VcfWriter(final OutputStream outputStream, final boolean writeUtf8BOM) throws Exception {
		if (writeUtf8BOM) {
			outputStream.write(BOM.UTF_8.getBytes());
		}
		bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
	}

	public VcfWriter(final OutputStream outputStream, final Charset charset) {
		bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, charset));
	}

	public String getDefaultVersion() {
		return defaultVersion;
	}

	public void setDefaultVersion(final String defaultVersion) throws Exception {
		if ("2.1".equals(defaultVersion)) {
			this.defaultVersion = defaultVersion;
		} else if ("3.0".equals(defaultVersion)) {
			this.defaultVersion = defaultVersion;
		} else if ("4.0".equals(defaultVersion)) {
			this.defaultVersion = defaultVersion;
		} else {
			throw new Exception("Invalid version. Must be one of '2.1', '3.0', '4.0'");
		}
	}

	public void writeCard(final VcfCard card) throws Exception {
		writeCard(card, defaultVersion);
	}

	public void writeCard(final VcfCard card, final String version) throws Exception {
		if (!"2.1".equals(version) && !"3.0".equals(version) && !"4.0".equals(version)) {
			throw new Exception("Invalid version. Must be one of '2.1', '3.0', '4.0'");
		}

		writtenCards++;

		// Syntax:
		// PROPERTY[;PARAMETER[; ...]]:VALUE[;VALUE[; ...]]

		writeLine(VcfConstants.BEGIN_VCARD);

		writeLine(VcfConstants.VERSION_PROPERTY + ":" + version);

		if ("2.1".equals(version) || "3.0".equals(version) || card.getLastName() != null || card.getFirstName() != null || card.getAdditionalFirstName() != null || card.getNamePrefix() != null || card.getNameSuffix() != null) {
			final List<String> values = new ArrayList<>();
			values.add(card.getLastName());
			values.add(card.getFirstName());
			values.add(card.getAdditionalFirstName());
			values.add(card.getNamePrefix());
			values.add(card.getNameSuffix());
			final List<String> parametersToSet = encodeValues(version, values);
			writeLine(VcfConstants.NAME_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
		}

		if ("3.0".equals(version) || "4.0".equals(version) || card.getFormattedName() != null) {
			final List<String> values = new ArrayList<>();
			values.add(card.getFormattedName());
			final List<String> parametersToSet = encodeValues(version, values);
			writeLine(VcfConstants.FORMATTED_NAME_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
		}

		if (card.getOrganization() != null) {
			final List<String> values = new ArrayList<>();
			values.addAll(card.getOrganization());
			final List<String> parametersToSet = encodeValues(version, values);
			writeLine(VcfConstants.ORGANIZATION_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
		}

		if (card.getRole() != null) {
			final List<String> values = new ArrayList<>();
			values.add(card.getRole());
			final List<String> parametersToSet = encodeValues(version, values);
			writeLine(VcfConstants.ROLE_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
		}

		if (card.getTitle() != null) {
			final List<String> values = new ArrayList<>();
			values.add(card.getTitle());
			final List<String> parametersToSet = encodeValues(version, values);
			writeLine(VcfConstants.TITLE_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
		}

		if (card.getPhotoUrl() != null) {
			final List<String> values = new ArrayList<>();
			values.add(card.getPhotoUrl());
			final List<String> parametersToSet = encodeValues(version, values);
			if ("4.0".equals(version)) {
				writeLine(VcfConstants.PHOTO_PROPERTY + ";MEDIATYPE=image/jpeg" + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
			} else if ("3.0".equals(version)) {
				writeLine(VcfConstants.PHOTO_PROPERTY + ";VALUE=URL;TYPE=JPEG" + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
			} else {
				writeLine(VcfConstants.PHOTO_PROPERTY + ";JPEG" + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
			}
		}

		if (card.getPhotoData() != null) {
			final String value = Utilities.encodeBase64(card.getPhotoData()).replaceAll("(.{80})", "$1\n ");
			final List<String> parametersToSet = new ArrayList<>();
			parametersToSet.add("ENCODING=BASE64");
			writeLine(VcfConstants.PHOTO_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + value);
			bufferedWriter.write("\n");
		}

		if (Utilities.isNotEmpty(card.getTelephoneNumbers())) {
			for (final VcfAttributedValue telephoneNumber : card.getTelephoneNumbers()) {
				final List<String> values = new ArrayList<>();
				values.add(telephoneNumber.getValue());

				final List<String> parametersToSet = new ArrayList<>();
				parametersToSet.addAll(telephoneNumber.getAttributes());

				final List<String> additionalParametersToSet = encodeValues(version, values);
				if (additionalParametersToSet != null) {
					parametersToSet.addAll(additionalParametersToSet);
				}

				writeLine(VcfConstants.TELEPHONE_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
			}
		}

		if (Utilities.isNotEmpty(card.getEmails())) {
			for (final VcfAttributedValue email : card.getEmails()) {
				final List<String> values = new ArrayList<>();
				values.add(email.getValue());

				final List<String> parametersToSet = new ArrayList<>();
				parametersToSet.addAll(email.getAttributes());

				final List<String> additionalParametersToSet = encodeValues(version, values);
				if (additionalParametersToSet != null) {
					parametersToSet.addAll(additionalParametersToSet);
				}

				writeLine(VcfConstants.EMAIL_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
			}
		}

		if (Utilities.isNotEmpty(card.getAddresses())) {
			for (final VcfAttributedAddress address : card.getAddresses()) {
				final List<String> values = new ArrayList<>();
				values.addAll(address.getValues());

				final Set<String> parametersToSet = new HashSet<>();
				parametersToSet.addAll(address.getAttributes());

				final List<String> additionalParametersToSet = encodeValues(version, values);
				if (additionalParametersToSet != null) {
					parametersToSet.addAll(additionalParametersToSet);
				}

				writeLine(VcfConstants.ADDRESS_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
			}
		}

		if (card.getLatestUpdate() != null) {
			final String value = DateUtilities.formatDate(DateUtilities.ISO_8601_DATETIME_FORMAT, card.getLatestUpdate(), ZoneId.of("GMT"));
			writeLine(VcfConstants.REVISION_PROPERTY + ":" + value);
		}

		if (card.getUrl() != null) {
			final List<String> values = new ArrayList<>();
			values.add(card.getUrl());
			final List<String> parametersToSet = encodeValues(version, values);
			writeLine(VcfConstants.URL_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
		}

		if (card.getNote() != null) {
			final List<String> values = new ArrayList<>();
			values.add(card.getNote());
			final List<String> parametersToSet = encodeValues(version, values);
			writeLine(VcfConstants.NOTE_PROPERTY + (Utilities.isEmpty(parametersToSet) ? "" : ";" + Utilities.join(parametersToSet, ";")) + ":" + Utilities.join(values, ";"));
		}

		if (card.getBirthday() != null) {
			final String value;
			if (card.getBirthyear() == null) {
				value = card.getBirthday().format(DateTimeFormatter.ofPattern("--MM-dd"));
			} else {
				value = card.getBirthday().atYear(card.getBirthyear().getValue()).format(DateTimeFormatter.ofPattern(DateUtilities.YYYY_MM_DD));
			}
			writeLine(VcfConstants.BIRTHDAY_PROPERTY + ":" + value);
		}

		writeLine(VcfConstants.END_VCARD);
	}

	private void writeLine(final String lineContent) throws IOException {
		writtenLines++;
		bufferedWriter.write(lineContent);
		bufferedWriter.write("\n");
	}

	private static List<String> encodeValues(final String version, final List<String> values) {
		if ("2.1".equals(version)) {
			boolean mustEncode = false;
			for (final String value : values) {
				if (value != null && (!StandardCharsets.US_ASCII.newEncoder().canEncode(value) || value.contains("\n") || value.contains("\r") || value.contains("="))) {
					mustEncode = true;
					break;
				}
			}

			if (mustEncode) {
				for (int i = 0; i < values.size(); i++) {
					values.set(i, QuotedPrintableCodec.encode(values.get(i), StandardCharsets.UTF_8));
				}
				final List<String> parameterList = new ArrayList<>();
				parameterList.add("CHARSET=UTF-8");
				parameterList.add("ENCODING=QUOTED-PRINTABLE");
				return parameterList;
			} else {
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i) == null ? "" : values.get(i).replace(";", "\\;"));
				}
				return null;
			}
		} else {
			boolean mustEncode = false;
			for (final String value : values) {
				if (value != null && (value.contains("\n") || value.contains("\r") || value.contains("="))) {
					mustEncode = true;
					break;
				}
			}

			if (mustEncode) {
				for (int i = 0; i < values.size(); i++) {
					values.set(i, QuotedPrintableCodec.encode(values.get(i), StandardCharsets.UTF_8));
				}
				final List<String> parameterList = new ArrayList<>();
				parameterList.add("CHARSET=UTF-8");
				parameterList.add("ENCODING=QUOTED-PRINTABLE");
				return parameterList;
			} else {
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i) == null ? "" : values.get(i).replace(";", "\\;"));
				}
				return null;
			}
		}
	}

	/**
	 * Get cards written until now.
	 *
	 * @return the read cards
	 */
	public int getNumberOfWrittenCards() {
		return writtenCards;
	}

	public int getNumberOfWrittenLines() {
		return writtenLines;
	}

	public void writeAll(final List<VcfCard> cards) throws Exception {
		for (final VcfCard card : cards) {
			writeCard(card);
		}
	}

	public void writeAll(final VcfCard... cards) throws Exception {
		for (final VcfCard card : cards) {
			writeCard(card);
		}
	}

	@Override
	public void close() {
		if (bufferedWriter != null) {
			try {
				bufferedWriter.flush();
				bufferedWriter.close();
			} catch (@SuppressWarnings("unused") final IOException e) {
				// Do nothing
			}
		}

		bufferedWriter = null;
	}
}