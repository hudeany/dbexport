package de.soderer.utilities.sql.whereclause.token;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SubSelect extends Value {
	public static final Set<String> SIGNS = new HashSet<String>(Arrays.asList(new String[] { "select" }));

	public String value;

	public SubSelect(String subSelectString) {
		if (!subSelectString.trim().toLowerCase().startsWith("select ") || !subSelectString.toLowerCase().contains(" from ")) {
			throw new IllegalArgumentException("Illegal subselect");
		}
		value = subSelectString;
	}

	@Override
	public String toString() {
		return value.trim();
	}

	@Override
	public String toString(RulePart.StringType stringType) {
		return toString();
	}
}
