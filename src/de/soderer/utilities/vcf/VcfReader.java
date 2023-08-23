package de.soderer.utilities.vcf;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.MonthDay;
import java.time.Year;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import de.soderer.utilities.BOMInputStream;
import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.QuotedPrintableCodec;
import de.soderer.utilities.Utilities;

/**
 * Reader for vcf (vCard file) format
 *
 * See: https://de.wikipedia.org/wiki/VCard#Eigenschaften
 */
public class VcfReader implements Closeable {
	private boolean strict = false;

	/** If a single read was done, it is impossible to make a full read at once with readAll(). */
	private boolean singleReadStarted = false;

	/** Number of cards read until now. */
	private int readCards = 0;

	private int readLines = 0;

	private BufferedReader inputReader = null;

	public VcfReader(final InputStream inputStream) throws Exception {
		inputReader = new BufferedReader(new InputStreamReader(new BOMInputStream(inputStream).skipBOM(), StandardCharsets.UTF_8));
	}

	public boolean isStrict() {
		return strict;
	}

	public VcfReader setStrict(final boolean strict) {
		this.strict = strict;
		return this;
	}

	public VcfCard readNextCard() throws Exception {
		readCards++;
		singleReadStarted = true;

		String nextLine;
		while ((nextLine = inputReader.readLine()) != null) {
			readLines++;
			if (nextLine.equals(VcfConstants.BEGIN_VCARD)) {
				break;
			}
		}

		if (nextLine == null) {
			return null;
		}

		final int cardStartLine = readLines;

		final List<String> vcfCardLines = new ArrayList<>();

		String lastLine = null;
		boolean lastLineWasQuotedPrintable = false;
		while ((nextLine = inputReader.readLine()) != null) {
			readLines++;

			if (nextLine.equals(VcfConstants.END_VCARD)) {
				break;
			} else if (Utilities.isBlank(nextLine)) {
				// skip empty lines
			} else if (lastLine != null && lastLine.endsWith("=") && lastLineWasQuotedPrintable) {
				// QUOTED-PRINTABLE encoded multiline
				nextLine = lastLine + "\n" + nextLine;
				vcfCardLines.set(vcfCardLines.size() - 1, nextLine);
			} else if (nextLine.startsWith(" ")) {
				// Base64 data multiline
				nextLine = lastLine + nextLine.substring(1);
				vcfCardLines.set(vcfCardLines.size() - 1, nextLine);
			} else {
				vcfCardLines.add(nextLine);
			}

			lastLine = nextLine;
			lastLineWasQuotedPrintable = lastLine.contains("ENCODING=QUOTED-PRINTABLE");
		}

		if (nextLine == null) {
			throw new Exception("Missing end sign of vcf card begin start sign in line: " + cardStartLine);
		}

		return parseCardLines(vcfCardLines, cardStartLine);
	}

	private VcfCard parseCardLines(final List<String> vcfCardLines, final int startLineNumber) throws Exception {
		int currentLineNumber = startLineNumber;
		final VcfCard card = new VcfCard();
		String version = null;
		boolean namePropertyWasPresent = false;
		boolean formattedNamePropertyWasPresent = false;
		boolean versionWasFirstLine = false;

		for (final String line : vcfCardLines) {
			currentLineNumber++;

			// Syntax:
			// PROPERTY[;PARAMETER[; ...]]:VALUE[;VALUE[; ...]]

			if (!line.contains(":")) {
				throw new Exception("Missing prefix separator ':' in line: " + currentLineNumber);
			}

			final String[] prefixes = Utilities.split(line.substring(0, line.indexOf(":")), ';', '\\');
			final String[] values = Utilities.split(line.substring(line.indexOf(":") + 1), ';', '\\', -1);

			final String property = prefixes[0];

			if (VcfConstants.VERSION_PROPERTY.equals(property)) {
				if (values.length != 1) {
					throw new Exception("Invalid version (" + VcfConstants.VERSION_PROPERTY + ") data (must have 1 part, has " + values.length + ") in line " + currentLineNumber);
				}
				version = values[0];
				if (currentLineNumber - startLineNumber == 1) {
					versionWasFirstLine = true;
				}
			} else if (VcfConstants.NAME_PROPERTY.equals(property)) {
				namePropertyWasPresent = true;
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 5) {
					throw new Exception("Invalid name (" + VcfConstants.NAME_PROPERTY + ") data (must have 5 parts, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				card.setLastName(decodedValues.get(0));
				card.setFirstName(decodedValues.get(1));
				card.setAdditionalFirstName(decodedValues.get(2));
				card.setNamePrefix(decodedValues.get(3));
				card.setNameSuffix(decodedValues.get(4));
			} else if (VcfConstants.FORMATTED_NAME_PROPERTY.equals(property)) {
				formattedNamePropertyWasPresent = true;
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid formatted name (" + VcfConstants.FORMATTED_NAME_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				card.setFormattedName(decodedValues.get(0));
			} else if (VcfConstants.ORGANIZATION_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values).stream().filter(x -> Utilities.isNotBlank(x)).collect(Collectors.toList());
				final ArrayList<String> organization = new ArrayList<>();
				organization.add(Utilities.join(decodedValues, ", "));
				card.setOrganization(organization);
			} else if (VcfConstants.ROLE_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid role (" + VcfConstants.ROLE_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				card.setRole(decodedValues.get(0));
			} else if (VcfConstants.TITLE_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid title (" + VcfConstants.TITLE_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				card.setTitle(decodedValues.get(0));
			} else if (VcfConstants.PHOTO_PROPERTY.equals(property)) {
				boolean isBase64Encoded = false;
				for (final String prefix : prefixes) {
					if ("ENCODING=BASE64".equalsIgnoreCase(prefix) || "ENCODING=b".equalsIgnoreCase(prefix)) {
						isBase64Encoded = true;
						break;
					}
				}
				if (isBase64Encoded) {
					if (values.length != 1) {
						throw new Exception("Invalid photo (" + VcfConstants.PHOTO_PROPERTY + ") data (must have 1 part, has " + values.length + ") in line " + currentLineNumber);
					}
					final byte[] data = Utilities.decodeBase64(values[0]);
					card.setPhotoData(data);
				} else {
					final List<String> decodedValues = decodeValues(prefixes, values);
					if (decodedValues.size() != 1) {
						throw new Exception("Invalid photo (" + VcfConstants.PHOTO_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
					}
					card.setPhotoUrl(decodedValues.get(0));
				}
			} else if (VcfConstants.TELEPHONE_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid telephonenumber (" + VcfConstants.TELEPHONE_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				final List<String> reducedPrefixes = new ArrayList<>();
				for (final String prefix : prefixes) {
					reducedPrefixes.add(prefix);
				}
				reducedPrefixes.remove(0);
				card.addTelephoneNumber(new VcfAttributedValue(decodedValues.get(0), reducedPrefixes));
			} else if (VcfConstants.EMAIL_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid email (" + VcfConstants.EMAIL_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				final List<String> reducedPrefixes = new ArrayList<>();
				for (final String prefix : prefixes) {
					reducedPrefixes.add(prefix);
				}
				reducedPrefixes.remove(0);
				card.addEmail(new VcfAttributedValue(decodedValues.get(0), reducedPrefixes));
			} else if (VcfConstants.ADDRESS_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 7) {
					throw new Exception("Invalid address (" + VcfConstants.ADDRESS_PROPERTY + ") data (must have 7 parts, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				final List<String> reducedPrefixes = new ArrayList<>();
				for (final String prefix : prefixes) {
					reducedPrefixes.add(prefix);
				}
				reducedPrefixes.remove(0);
				card.addAddress(new VcfAttributedAddress(decodedValues, reducedPrefixes));
			} else if (VcfConstants.REVISION_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid title (" + VcfConstants.REVISION_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				if (decodedValues.get(0).contains("-")) {
					card.setLatestUpdate(DateUtilities.parseIso8601DateTimeString(decodedValues.get(0)));
				} else if (decodedValues.get(0).contains("T")) {
					card.setLatestUpdate(DateUtilities.parseZonedDateTime("yyyyMMdd'T'HHmmssX", decodedValues.get(0), ZoneId.systemDefault()));
				} else {
					card.setLatestUpdate(DateUtilities.parseZonedDateTime("yyyyMMdd", decodedValues.get(0), ZoneId.systemDefault()));
				}
			} else if (VcfConstants.URL_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid title (" + VcfConstants.URL_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				card.setUrl(decodedValues.get(0));
			} else if (VcfConstants.NOTE_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid title (" + VcfConstants.NOTE_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				card.setNote(decodedValues.get(0));
			} else if (VcfConstants.BIRTHDAY_PROPERTY.equals(property)) {
				final List<String> decodedValues = decodeValues(prefixes, values);
				if (decodedValues.size() != 1) {
					throw new Exception("Invalid title (" + VcfConstants.BIRTHDAY_PROPERTY + ") data (must have 1 part, has " + decodedValues.size() + ") in line " + currentLineNumber);
				}
				if (decodedValues.get(0).startsWith("--")) {
					// Date without year "--12-31"
					card.setBirthday(MonthDay.parse(decodedValues.get(0), DateTimeFormatter.ofPattern("--MM-dd")));
					card.setBirthyear(null);
				} else if (decodedValues.get(0).contains("-")) {
					final LocalDate birthDay = DateUtilities.parseIso8601DateTimeString(decodedValues.get(0)).toLocalDate();
					card.setBirthday(MonthDay.from(birthDay));
					card.setBirthyear(Year.from(birthDay));
				} else {
					final LocalDate birthDay = DateUtilities.parseLocalDate("yyyyMMdd", decodedValues.get(0));
					card.setBirthday(MonthDay.from(birthDay));
					card.setBirthyear(Year.from(birthDay));
				}
			} else if (VcfConstants.A_ANDROID_CUSTOM_PROPERTY.equals(property)) {
				// Ignore property
			} else {
				throw new Exception("Unknown property name '" + property + "' found in line " + currentLineNumber);
			}
		}

		if (strict) {
			if (version == null) {
				throw new Exception("Missing mandatory version (VERSION) line for vcf card beginning at line " + startLineNumber);
			} else if ("2.1".equals(version)) {
				if (!namePropertyWasPresent) {
					throw new Exception("Missing mandatory name (N) line for vcf card beginning at line " + startLineNumber);
				}
			} else if ("3.0".equals(version)) {
				if (!namePropertyWasPresent) {
					throw new Exception("Missing mandatory name (N) line for vcf card beginning at line " + startLineNumber);
				} else if (!formattedNamePropertyWasPresent) {
					throw new Exception("Missing mandatory formatted name (FN) line for vcf card beginning at line " + startLineNumber);
				}
				if (!versionWasFirstLine) {
					throw new Exception("Version data (" + VcfConstants.VERSION_PROPERTY + ") must be first line of data of each vcf card using version 3.0");
				}
			} else if ("4.0".equals(version)) {
				if (!formattedNamePropertyWasPresent) {
					throw new Exception("Missing mandatory formatted name (FN) line for vcf card beginning at line " + startLineNumber);
				}
				if (!versionWasFirstLine) {
					throw new Exception("Version data (" + VcfConstants.VERSION_PROPERTY + ") must be first line of data of each vcf card using version 4.0");
				}
			} else {
				throw new Exception("Unknown version (VERSION) '" + version + "' for vcf card beginning at line " + startLineNumber);
			}
		}

		return card;
	}

	private static List<String> decodeValues(final String[] prefixes, final String[] values) {
		final List<String> prefixList = Arrays.asList(prefixes);
		final List<String> valuesDecoded = new ArrayList<>();

		if (prefixList.contains("ENCODING=QUOTED-PRINTABLE")) {
			for (final String value : values) {
				valuesDecoded.add(QuotedPrintableCodec.decode(value, StandardCharsets.UTF_8));
			}
		} else {
			for (final String value : values) {
				valuesDecoded.add(value);
			}
		}

		return valuesDecoded;
	}

	/**
	 * Get cards read until now.
	 *
	 * @return the read cards
	 */
	public int getNumberOfCardsRead() {
		return readCards;
	}

	public List<VcfCard> readAll() throws Exception {
		if (singleReadStarted) {
			throw new IllegalStateException("Single readNextCard was called before readAll");
		}

		final List<VcfCard> cards = new ArrayList<>();
		VcfCard nextCard;
		while ((nextCard = readNextCard()) != null) {
			cards.add(nextCard);
		}
		return cards;
	}

	@Override
	public void close() {
		if (inputReader != null) {
			try {
				inputReader.close();
			} catch (@SuppressWarnings("unused") final IOException e) {
				// Do nothing
			}
		}

		inputReader = null;
	}
}