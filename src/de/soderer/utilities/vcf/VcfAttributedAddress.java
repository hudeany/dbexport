package de.soderer.utilities.vcf;

import java.util.List;

public class VcfAttributedAddress {
	private final List<String> values;
	private final List<String> attributes;

	// TODO split 7 values?
	public VcfAttributedAddress(final List<String> values, final List<String> attributes) {
		this.values = values;
		this.attributes = attributes;
	}

	public List<String> getValues() {
		return values;
	}

	public List<String> getAttributes() {
		return attributes;
	}
}
