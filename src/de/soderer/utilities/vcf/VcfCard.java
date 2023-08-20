package de.soderer.utilities.vcf;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import de.soderer.utilities.DateUtilities;
import de.soderer.utilities.Utilities;

public class VcfCard {
	private String lastName = null;
	private String firstName = null;
	private String additionalFirstName = null;
	private String namePrefix = null;
	private String nameSuffix = null;
	private String formattedName = null;
	private List<String> organization = null;
	private String role = null;
	private String title = null;
	private String photoUrl = null;
	private byte[] photoData = null;
	private ZonedDateTime latestUpdate = null;
	private String url = null;
	private String note = null;

	private MonthDay birthday = null;
	private Year birthyear = null;

	private final List<VcfAttributedValue> telephoneNumbers = new ArrayList<>();
	private final List<VcfAttributedAddress> addresses = new ArrayList<>();
	private final List<VcfAttributedValue> emails = new ArrayList<>();

	public String getLastName() {
		return lastName;
	}

	public void setLastName(final String lastName) {
		this.lastName = lastName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(final String firstName) {
		this.firstName = firstName;
	}

	public String getAdditionalFirstName() {
		return additionalFirstName;
	}

	public void setAdditionalFirstName(final String additionalFirstName) {
		this.additionalFirstName = additionalFirstName;
	}

	public String getNamePrefix() {
		return namePrefix;
	}

	public void setNamePrefix(final String namePrefix) {
		this.namePrefix = namePrefix;
	}

	public String getNameSuffix() {
		return nameSuffix;
	}

	public void setNameSuffix(final String nameSuffix) {
		this.nameSuffix = nameSuffix;
	}

	public String getFormattedName() {
		return formattedName;
	}

	public void setFormattedName(final String formattedName) {
		this.formattedName = formattedName;
	}

	public List<String> getOrganization() {
		return organization;
	}

	public void setOrganization(final List<String> organization) {
		this.organization = organization;
	}

	public String getRole() {
		return role;
	}

	public void setRole(final String role) {
		this.role = role;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(final String title) {
		this.title = title;
	}

	public String getPhotoUrl() {
		return photoUrl;
	}

	public void setPhotoUrl(final String photoUrl) {
		this.photoUrl = photoUrl;
	}

	public byte[] getPhotoData() {
		return photoData;
	}

	public void setPhotoData(final byte[] photoData) {
		this.photoData = photoData;
	}

	public void addTelephoneNumber(final VcfAttributedValue telephoneNumber) {
		telephoneNumbers.add(telephoneNumber);
	}

	public List<VcfAttributedValue> getTelephoneNumbers() {
		return telephoneNumbers;
	}

	public void addAddress(final VcfAttributedAddress address) {
		addresses.add(address);
	}

	public List<VcfAttributedAddress> getAddresses() {
		return addresses;
	}

	public void addEmail(final VcfAttributedValue email) {
		emails.add(email);
	}

	public List<VcfAttributedValue> getEmails() {
		return emails;
	}

	public ZonedDateTime getLatestUpdate() {
		return latestUpdate;
	}

	public void setLatestUpdate(final ZonedDateTime latestUpdate) {
		this.latestUpdate = latestUpdate;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(final String url) {
		this.url = url;
	}

	public String getNote() {
		return note;
	}

	public void setNote(final String note) {
		this.note = note;
	}

	public MonthDay getBirthday() {
		return birthday;
	}

	public void setBirthday(final MonthDay birthday) {
		this.birthday = birthday;
	}

	public Year getBirthyear() {
		return birthyear;
	}

	public void setBirthyear(final Year birthyear) {
		this.birthyear = birthyear;
	}

	public static Map<String, Object> toMap(final VcfCard vcfCard) {
		final Map<String, Object> returnMap = new HashMap<>();

		if (Utilities.isNotBlank(vcfCard.getLastName())) {
			returnMap.put("lastname", vcfCard.getLastName());
		}

		if (Utilities.isNotBlank(vcfCard.getFirstName())) {
			returnMap.put("firstname", vcfCard.getFirstName());
		}

		if (Utilities.isNotBlank(vcfCard.getAdditionalFirstName())) {
			returnMap.put("additionalfirstname", vcfCard.getAdditionalFirstName());
		}

		if (Utilities.isNotBlank(vcfCard.getNamePrefix())) {
			returnMap.put("nameprefix", vcfCard.getNamePrefix());
		}

		if (Utilities.isNotBlank(vcfCard.getNameSuffix())) {
			returnMap.put("namesuffix", vcfCard.getNameSuffix());
		}

		if (Utilities.isNotBlank(vcfCard.getFormattedName())) {
			returnMap.put("formattedName", vcfCard.getFormattedName());
		}

		if (Utilities.isNotBlank(vcfCard.getRole())) {
			returnMap.put("forrolemattedName", vcfCard.getRole());
		}

		if (Utilities.isNotBlank(vcfCard.getTitle())) {
			returnMap.put("title", vcfCard.getTitle());
		}

		if (Utilities.isNotBlank(vcfCard.getPhotoUrl())) {
			returnMap.put("photourl", vcfCard.getPhotoUrl());
		}

		if (vcfCard.getPhotoData() != null && vcfCard.getPhotoData().length > 0) {
			returnMap.put("photodata", vcfCard.getPhotoData());
		}

		if (vcfCard.getLatestUpdate() != null) {
			returnMap.put("latestupdate", vcfCard.getLatestUpdate());
		}

		if (Utilities.isNotBlank(vcfCard.getUrl())) {
			returnMap.put("url", vcfCard.getUrl());
		}

		if (Utilities.isNotBlank(vcfCard.getNote())) {
			returnMap.put("note", vcfCard.getNote());
		}

		if (vcfCard.getBirthday() != null) {
			if (vcfCard.getBirthyear() != null) {
				returnMap.put("birthday", vcfCard.getBirthday().atYear(vcfCard.getBirthyear().getValue()));
			} else {
				returnMap.put("birthday", vcfCard.getBirthday());
			}
		}

		if (Utilities.isNotEmpty(vcfCard.getOrganization())) {
			returnMap.put("organization", Utilities.joinNotBlank(vcfCard.getOrganization(), ", "));
		}

		int telephoneNumberCount = 0;
		for (final VcfAttributedValue telephoneNumber : vcfCard.getTelephoneNumbers()) {
			if (Utilities.isNotBlank(telephoneNumber.getValue())) {
				telephoneNumberCount++;
				final String mapKey = "telephoneNumber";
				returnMap.put(mapKey + "_" + telephoneNumberCount, telephoneNumber.getValue());
				returnMap.put(mapKey + "_" + telephoneNumberCount + "_attr", Utilities.joinNotBlank(telephoneNumber.getAttributes(), ", "));
			}
		}

		int addressCount = 0;
		for (final VcfAttributedAddress address : vcfCard.getAddresses()) {
			if (Utilities.isNotEmpty(address.getValues())) {
				addressCount++;
				final String mapKey = "address";
				List<String> attributes = address.getAttributes();
				attributes = attributes.stream().filter(x -> !x.toLowerCase().startsWith("charset=") && !x.toLowerCase().startsWith("encoding=")).collect(Collectors.toList());
				returnMap.put(mapKey + "_" + addressCount, Utilities.joinNotBlank(address.getValues(), ", "));
				returnMap.put(mapKey + "_" + addressCount + "_attr", Utilities.joinNotBlank(attributes, ", "));
			}
		}

		int emailCount = 0;
		for (final VcfAttributedValue email : vcfCard.getEmails()) {
			if (Utilities.isNotBlank(email.getValue())) {
				emailCount++;
				final String mapKey = "email";
				returnMap.put(mapKey + "_" + emailCount, email.getValue());
				returnMap.put(mapKey + "_" + emailCount + "_attr", Utilities.joinNotBlank(email.getAttributes(), ", "));
			}
		}

		return returnMap;
	}

	public static VcfCard fromMap(final Map<String, Object> map) throws Exception {
		final VcfCard newVcfCard = new VcfCard();

		for (final Entry<String, Object> entry : map.entrySet()) {
			if ("lastname".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setLastName(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("firstname".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setFirstName(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("additionalfirstname".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setAdditionalFirstName(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("nameprefix".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setNamePrefix(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("namesuffix".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setNameSuffix(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("formattedName".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setFormattedName(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("role".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setRole(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("title".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setTitle(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("photourl".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setPhotoUrl(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("url".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setUrl(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("note".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setNote(entry.getValue() == null ? null : entry.getValue().toString());
			} else if ("latestupdate".equalsIgnoreCase(entry.getKey())) {
				if (entry.getValue() == null) {
					newVcfCard.setLatestUpdate(null);
				} else if (entry.getValue() instanceof ZonedDateTime) {
					newVcfCard.setLatestUpdate((ZonedDateTime) entry.getValue());
				} else if (entry.getValue() instanceof LocalDateTime) {
					newVcfCard.setLatestUpdate(((LocalDateTime) entry.getValue()).atZone(ZoneId.systemDefault()));
				} else if (entry.getValue() instanceof LocalDate) {
					newVcfCard.setLatestUpdate(((LocalDate) entry.getValue()).atStartOfDay().atZone(ZoneId.systemDefault()));
				} else {
					final String value = entry.getValue().toString();
					newVcfCard.setLatestUpdate(DateUtilities.parseUnknownDateFormat(value));
				}
			} else if ("birthday".equalsIgnoreCase(entry.getKey())) {
				if (entry.getValue() == null) {
					newVcfCard.setBirthday(null);
					newVcfCard.setBirthyear(null);
				} else if (entry.getValue() instanceof ZonedDateTime) {
					newVcfCard.setBirthday(MonthDay.of(((ZonedDateTime) entry.getValue()).getMonth(), ((ZonedDateTime) entry.getValue()).getDayOfMonth()));
					newVcfCard.setBirthyear(Year.of(((ZonedDateTime) entry.getValue()).getYear()));
				} else if (entry.getValue() instanceof LocalDateTime) {
					newVcfCard.setBirthday(MonthDay.of(((LocalDateTime) entry.getValue()).getMonth(), ((LocalDateTime) entry.getValue()).getDayOfMonth()));
					newVcfCard.setBirthyear(Year.of(((LocalDateTime) entry.getValue()).getYear()));
				} else if (entry.getValue() instanceof LocalDate) {
					newVcfCard.setBirthday(MonthDay.of(((LocalDate) entry.getValue()).getMonth(), ((LocalDate) entry.getValue()).getDayOfMonth()));
					newVcfCard.setBirthyear(Year.of(((LocalDate) entry.getValue()).getYear()));
				} else {
					final String value = entry.getValue().toString();
					if (value.startsWith("--")) {
						// Date without year "--12-31"
						newVcfCard.setBirthday(MonthDay.parse(value, DateTimeFormatter.ofPattern("--MM-dd")));
						newVcfCard.setBirthyear(null);
					} else if (value.contains("-")) {
						final LocalDate birthDay = DateUtilities.parseIso8601DateTimeString(value).toLocalDate();
						newVcfCard.setBirthday(MonthDay.from(birthDay));
						newVcfCard.setBirthyear(Year.from(birthDay));
					} else {
						final LocalDate birthDay = DateUtilities.parseLocalDate("yyyyMMdd", value);
						newVcfCard.setBirthday(MonthDay.from(birthDay));
						newVcfCard.setBirthyear(Year.from(birthDay));
					}
				}
			} else if ("photodata".equalsIgnoreCase(entry.getKey())) {
				newVcfCard.setPhotoData((byte[]) entry.getValue());
			} else if ("organization".equalsIgnoreCase(entry.getKey())) {
				final List<String> organization = new ArrayList<>();
				for (final String attributeString : ((String) entry.getValue()).split(",")) {
					organization.add(attributeString.trim());
				}
				newVcfCard.setOrganization(organization);
			} else if (Utilities.startsWithCaseinsensitive(entry.getKey(), "telephoneNumber_")) {
				final String keyDataPart = entry.getKey().substring("telephoneNumber_".length());
				if (!keyDataPart.endsWith("_attr")) {
					final String value = (String) entry.getValue();
					final String attributesString = (String) map.get("telephoneNumber_" + keyDataPart + "_attr");
					if (attributesString != null) {
						final List<String> attributes = new ArrayList<>();
						for (final String attributeString : attributesString.split(",")) {
							attributes.add(attributeString.trim());
						}
						newVcfCard.getTelephoneNumbers().add(new VcfAttributedValue(value, attributes));
					} else {
						newVcfCard.getTelephoneNumbers().add(new VcfAttributedValue(value, null));
					}
				}
			} else if (Utilities.startsWithCaseinsensitive(entry.getKey(), "address_")) {
				final String keyDataPart = entry.getKey().substring("address_".length());
				if (!keyDataPart.endsWith("_attr")) {
					final List<String> addressValues = new ArrayList<>();
					for (final String addressValueString : ((String) entry.getValue()).split(",")) {
						addressValues.add(addressValueString.trim());
					}
					final String attributesString = (String) map.get("address_" + keyDataPart + "_attr");
					if (attributesString != null) {
						final List<String> attributes = new ArrayList<>();
						for (final String attributeString : attributesString.split(",")) {
							attributes.add(attributeString.trim());
						}
						newVcfCard.getAddresses().add(new VcfAttributedAddress(addressValues, attributes));
					} else {
						newVcfCard.getAddresses().add(new VcfAttributedAddress(addressValues, null));
					}
				}
			} else if (Utilities.startsWithCaseinsensitive(entry.getKey(), "email_")) {
				final String keyDataPart = entry.getKey().substring("email_".length());
				if (!keyDataPart.endsWith("_attr")) {
					final String value = (String) entry.getValue();
					final String attributesString = (String) map.get("email_" + keyDataPart + "_attr");
					if (attributesString != null) {
						final List<String> attributes = new ArrayList<>();
						for (final String attributeString : attributesString.split(",")) {
							attributes.add(attributeString.trim());
						}
						newVcfCard.getEmails().add(new VcfAttributedValue(value, attributes));
					} else {
						newVcfCard.getEmails().add(new VcfAttributedValue(value, null));
					}
				}
			}
		}

		return newVcfCard;
	}
}
