package de.soderer.utilities.kdbx.data;

public class KdbxMemoryProtection {
	private boolean protectTitle;
	private boolean protectUserName;
	private boolean protectPassword = true;
	private boolean protectURL;
	private boolean protectNotes;

	public KdbxMemoryProtection setProtectTitle(final boolean protectTitle) {
		this.protectTitle = protectTitle;
		return this;
	}

	public boolean isProtectTitle() {
		return protectTitle;
	}

	public KdbxMemoryProtection setProtectUserName(final boolean protectUserName) {
		this.protectUserName = protectUserName;
		return this;
	}

	public boolean isProtectUserName() {
		return protectUserName;
	}

	public KdbxMemoryProtection setProtectPassword(final boolean protectPassword) {
		this.protectPassword = protectPassword;
		return this;
	}

	public boolean isProtectPassword() {
		return protectPassword;
	}

	public KdbxMemoryProtection setProtectURL(final boolean protectURL) {
		this.protectURL = protectURL;
		return this;
	}

	public boolean isProtectURL() {
		return protectURL;
	}

	public KdbxMemoryProtection setProtectNotes(final boolean protectNotes) {
		this.protectNotes = protectNotes;
		return this;
	}

	public boolean isProtectNotes() {
		return protectNotes;
	}
}
