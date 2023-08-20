package de.soderer.utilities.vcf;

import java.util.List;

public class VcfAttributedValue {
	private final String value;
	private final List<String> attributes;

	public VcfAttributedValue(final String value, final List<String> attributes) {
		this.value = value;
		this.attributes = attributes;
	}

	public String getValue() {
		return value;
	}

	public List<String> getAttributes() {
		return attributes;
	}
}
