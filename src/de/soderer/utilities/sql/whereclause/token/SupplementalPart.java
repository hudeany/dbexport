package de.soderer.utilities.sql.whereclause.token;

public class SupplementalPart extends RulePart {
	public enum Type {
		OpeningBracket, ClosingBracket, Separator
	}

	public Type type;

	public SupplementalPart(Type type) {
		this.type = type;
	}

	@Override
	public String toString() {
		switch (type) {
			case OpeningBracket:
				return "(";
			case ClosingBracket:
				return ")";
			default:
				return ", ";
		}
	}

	@Override
	public String toString(RulePart.StringType stringType) {
		switch (type) {
			case OpeningBracket:
				return "(";
			case ClosingBracket:
				return ")";
			default:
				return ", ";
		}
	}
}
