package de.soderer.utilities.kdbx;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.soderer.utilities.kdbx.data.KdbxBinary;
import de.soderer.utilities.kdbx.data.KdbxConstants;
import de.soderer.utilities.kdbx.data.KdbxEntry;
import de.soderer.utilities.kdbx.data.KdbxEntryBinary;
import de.soderer.utilities.kdbx.data.KdbxGroup;
import de.soderer.utilities.kdbx.data.KdbxHeaderFormat;
import de.soderer.utilities.kdbx.data.KdbxMeta;
import de.soderer.utilities.kdbx.data.KdbxUUID;
import de.soderer.utilities.kdbx.util.Utilities;

public class KdbxDatabase {
	private KdbxHeaderFormat headerFormat;
	private List<KdbxBinary> binaryAttachments = null;
	private KdbxMeta meta = new KdbxMeta();
	private List<KdbxGroup> groups = new ArrayList<>();
	private List<KdbxEntry> entries = new ArrayList<>();
	private final Map<KdbxUUID, ZonedDateTime> deletedObjects = new LinkedHashMap<>();

	public KdbxDatabase setHeaderFormat(final KdbxHeaderFormat headerFormat) {
		this.headerFormat = headerFormat;
		return this;
	}

	public KdbxHeaderFormat getHeaderFormat() {
		return headerFormat;
	}

	/**
	 * Binary data of entry attachments.
	 * Dataversion <= 3.1 --> stored in KdbxMeta binaries
	 * Dataversion >= 4.0 --> stored in KdbxInnerHeaderType.BINARY_ATTACHMENT
	 */
	public KdbxDatabase setBinaryAttachments(final List<KdbxBinary> binaryAttachments) {
		this.binaryAttachments = binaryAttachments;
		return this;
	}

	/**
	 * Binary data of entry attachments.
	 * Dataversion <= 3.1 --> stored in KdbxMeta binaries
	 * Dataversion >= 4.0 --> stored in KdbxInnerHeaderType.BINARY_ATTACHMENT
	 */
	public List<KdbxBinary> getBinaryAttachments() {
		return binaryAttachments;
	}

	public KdbxDatabase setMeta(final KdbxMeta meta) {
		if (meta == null) {
			throw new IllegalArgumentException("Database's meta may not be null");
		} else {
			this.meta = meta;
			return this;
		}
	}

	public KdbxMeta getMeta() {
		return meta;
	}

	public KdbxDatabase setGroups(final List<KdbxGroup> groups) {
		this.groups = groups;
		return this;
	}

	public List<KdbxGroup> getGroups() {
		return groups;
	}

	public KdbxDatabase setEntries(final List<KdbxEntry> entries) {
		this.entries = entries;
		return this;
	}

	public List<KdbxEntry> getEntries() {
		return entries;
	}

	public Map<KdbxUUID, ZonedDateTime> getDeletedObjects() {
		return deletedObjects;
	}

	public KdbxGroup getGroupByUUID(final KdbxUUID groupUuid) {
		for (final KdbxGroup group : groups) {
			if (group.getUuid().equals(groupUuid)) {
				return group;
			}
		}
		return null;
	}

	public KdbxEntry getEntryByUUID(final KdbxUUID entryUuid) {
		for (final KdbxEntry entry : entries) {
			if (entry.getUuid().equals(entryUuid)) {
				return entry;
			}
		}
		for (final KdbxGroup group : groups) {
			final KdbxEntry entry = group.getEntryByUUID(entryUuid);
			if (entry != null) {
				return entry;
			}
		}
		return null;
	}

	public List<KdbxUUID> getUuidPath(final KdbxUUID uuid) {
		for (final KdbxEntry entry : entries) {
			if (entry.getUuid().equals(uuid)) {
				final List<KdbxUUID> pathUuids = new ArrayList<>();
				pathUuids.add(entry.getUuid());
				return pathUuids;
			}
		}
		for (final KdbxGroup group : groups) {
			if (group.getUuid().equals(uuid)) {
				final List<KdbxUUID> pathUuids = new ArrayList<>();
				pathUuids.add(group.getUuid());
				return pathUuids;
			} else {
				final List<KdbxUUID> subPathUuids = group.getUuidPath(uuid);
				if (subPathUuids != null) {
					return subPathUuids;
				}
			}
		}
		return null;
	}

	public List<KdbxGroup> getAllGroups() {
		final List<KdbxGroup> groupsList = new ArrayList<>();
		for (final KdbxGroup group : groups) {
			groupsList.add(group);
		}
		for (final KdbxGroup group : groups) {
			groupsList.addAll(group.getAllGroups());
		}
		return groupsList;
	}

	public List<KdbxEntry> getAllEntries() {
		final List<KdbxEntry> entriesList = new ArrayList<>();
		for (final KdbxEntry entry : entries) {
			entriesList.add(entry);
		}
		for (final KdbxGroup group : groups) {
			entriesList.addAll(group.getAllEntries());
		}
		return entriesList;
	}

	public void validate() throws Exception {
		final Set<KdbxUUID> usedUuids = new HashSet<>();
		for (final KdbxGroup group : getAllGroups()) {
			if (!usedUuids.add(group.getUuid())) {
				throw new Exception("Group with duplicate UUID found: " + group.getUuid().toHex());
			}
			if (group.getIconID() != null) {
				try {
					KdbxConstants.KdbxStandardIcon.getById(group.getIconID());
				} catch (final Exception e) {
					throw new Exception("Invalid standard icon id found in group '" + group.getName() + "': " + group.getIconID(), e);
				}
			}
		}
		for (final KdbxEntry entry : getAllEntries()) {
			if (!usedUuids.add(entry.getUuid())) {
				throw new Exception("Entry with duplicate UUID found: " + entry.getUuid().toHex());
			}
			if (entry.getIconID() != null) {
				try {
					KdbxConstants.KdbxStandardIcon.getById(entry.getIconID());
				} catch (final Exception e) {
					throw new Exception("Invalid standard icon id found in entry '" + entry.getTitle() + "': " + entry.getIconID(), e);
				}
			}
		}

		// Only referenced binaries will be stored
		// The same binary attachment should not be stored multiple times
		setBinaryAttachments(new ArrayList<>());
		for (final KdbxEntry entry : getAllEntries()) {
			for (final KdbxEntryBinary entryBinary : entry.getBinaries()) {
				final byte[] entryBinaryData = entryBinary.getData();
				if (entryBinaryData != null) {
					boolean foundInBinaryAttachments = false;
					for (final KdbxBinary binaryAttachment : getBinaryAttachments()) {
						byte[] binaryAttachmentData = binaryAttachment.getData();
						if (binaryAttachment.isCompressed()) {
							binaryAttachmentData = Utilities.gunzip(binaryAttachmentData);
						}
						if (Arrays.equals(binaryAttachmentData, entryBinaryData)) {
							entryBinary.setRefId(binaryAttachment.getId());
							foundInBinaryAttachments = true;
							break;
						}
					}
					if (!foundInBinaryAttachments) {
						final KdbxBinary newBinaryAttachment = new KdbxBinary();
						newBinaryAttachment.setId(getBinaryAttachments().size());
						newBinaryAttachment.setData(Utilities.gzip(entryBinaryData));
						newBinaryAttachment.setCompressed(true);
						getBinaryAttachments().add(newBinaryAttachment);
						entryBinary.setRefId(newBinaryAttachment.getId());
					}
				}
			}
		}
	}
}
