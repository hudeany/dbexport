package de.soderer.utilities.kdbx.data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KdbxEntry {
	private KdbxUUID uuid;
	private Integer iconID;
	public KdbxUUID customIconUuid;
	private String foregroundColor;
	private String backgroundColor;
	private String overrideURL;
	private String tags;
	private KdbxTimes times = new KdbxTimes();
	private Map<String, Object> items = new LinkedHashMap<>();
	private boolean autoTypeEnabled = false;
	private String autoTypeDataTransferObfuscation;
	private String autoTypeDefaultSequence;
	private String autoTypeAssociationWindow;
	private String autoTypeAssociationKeystrokeSequence;
	private List<KdbxCustomDataItem> customData;
	private final List<KdbxEntry> history = new ArrayList<>();
	private final List<KdbxEntryBinary> binaries = new ArrayList<>();

	public KdbxEntry setUuid(final KdbxUUID uuid) {
		this.uuid = uuid;
		return this;
	}

	public KdbxUUID getUuid() {
		if (uuid == null) {
			uuid = new KdbxUUID();
		}
		return uuid;
	}

	public KdbxEntry setTitle(final String title) {
		items.put("Title", title);
		return this;
	}

	public String getTitle() {
		return (String) items.get("Title");
	}

	public KdbxEntry setUsername(final String username) {
		items.put("Username", username);
		return this;
	}

	public String getUsername() {
		return (String) items.get("Username");
	}

	public KdbxEntry setPassword(final String password) {
		items.put("Password", password);
		return this;
	}

	public String getPassword() {
		return (String) items.get("Password");
	}

	public KdbxEntry setUrl(final String url) {
		items.put("Url", url);
		return this;
	}

	public String getUrl() {
		return (String) items.get("Url");
	}

	public KdbxEntry setNotes(final String notes) {
		items.put("Notes", notes);
		return this;
	}

	public String getNotes() {
		return (String) items.get("Notes");
	}

	public KdbxEntry setIconID(final Integer iconID) {
		this.iconID = iconID;
		return this;
	}

	public Integer getIconID() {
		return iconID;
	}

	public KdbxEntry setCustomIconUuid(final KdbxUUID customIconUuid) {
		this.customIconUuid = customIconUuid;
		return this;
	}

	public KdbxUUID getCustomIconUuid() {
		return customIconUuid;
	}

	public KdbxEntry setForegroundColor(final String foregroundColor) {
		this.foregroundColor = foregroundColor;
		return this;
	}

	public String getForegroundColor() {
		return foregroundColor;
	}

	public KdbxEntry setBackgroundColor(final String backgroundColor) {
		this.backgroundColor = backgroundColor;
		return this;
	}

	public String getBackgroundColor() {
		return backgroundColor;
	}

	public KdbxEntry setOverrideURL(final String overrideURL) {
		this.overrideURL = overrideURL;
		return this;
	}

	public String getOverrideURL() {
		return overrideURL;
	}

	public KdbxEntry setTags(final String tags) {
		this.tags = tags;
		return this;
	}

	public String getTags() {
		return tags;
	}

	public KdbxEntry setTimes(final KdbxTimes times) {
		if (times == null) {
			throw new IllegalArgumentException("Entry's times may not be null");
		} else {
			this.times = times;
			return this;
		}
	}

	public KdbxTimes getTimes() {
		return times;
	}

	public KdbxEntry setItem(final String itemKey, final Object itemValue) {
		items.put(itemKey, itemValue);
		return this;
	}

	public String getItem(final String itemKey) {
		return (String) items.get(itemKey);
	}

	public KdbxEntry setItems(final Map<String, Object> items) {
		this.items = items;
		return this;
	}

	public Map<String, Object> getItems() {
		return items;
	}

	public KdbxEntry setAutoType(final boolean enabled, final String dataTransferObfuscation, final String defaultSequence, final String associationWindow, final String associationKeystrokeSequence) {
		autoTypeEnabled = enabled;
		autoTypeDataTransferObfuscation = dataTransferObfuscation;
		autoTypeDefaultSequence = defaultSequence;
		autoTypeAssociationWindow = associationWindow;
		autoTypeAssociationKeystrokeSequence = associationKeystrokeSequence;
		return this;
	}

	public boolean isAutoTypeEnabled() {
		return autoTypeEnabled;
	}

	public String getAutoTypeDataTransferObfuscation() {
		return autoTypeDataTransferObfuscation;
	}

	public String getAutoTypeDefaultSequence() {
		return autoTypeDefaultSequence;
	}

	public String getAutoTypeAssociationWindow() {
		return autoTypeAssociationWindow;
	}

	public String getAutoTypeAssociationKeystrokeSequence() {
		return autoTypeAssociationKeystrokeSequence;
	}

	/**
	 * Data items of stored files for this entry.
	 */
	public KdbxEntry setCustomData(final List<KdbxCustomDataItem> customData) {
		this.customData = customData;
		return this;
	}

	/**
	 * Data items of stored files for this entry.
	 */
	public List<KdbxCustomDataItem> getCustomData() {
		return customData;
	}

	public List<KdbxEntry> getHistory() {
		return history;
	}

	public List<KdbxEntryBinary> getBinaries() {
		return binaries;
	}
}
