package de.soderer.utilities.sql.whereclause.token;

public class Operator extends RulePart {
	public String sign;

	public Operator(String sign) {
		if (sign != null) {
			this.sign = sign.toLowerCase();
		}
	}

	@Override
	public String toString() {
		return sign;
	}

	@Override
	public String toString(RulePart.StringType stringType) {

		if (stringType == StringType.Oracle) {
			if (Expression.DATE_FUNCTION_SIGNS.contains(sign)) {
				return "TO_DATE";
			} else if (Expression.CHAR_FUNCTION_SIGNS.contains(sign)) {
				return "TO_CHAR";
			}
		} else if (stringType == StringType.MySQL) {
			if (Expression.DATE_FUNCTION_SIGNS.contains(sign)) {
				return "STR_TO_DATE";
			} else if (Expression.CHAR_FUNCTION_SIGNS.contains(sign)) {
				return "DATE_FORMAT";
			}
		}

		if (stringType == StringType.BeanShell) {
			if (Expression.STRING_ONLY_COMPARE_OPERATOR_SIGNS.contains(sign)) {
				throw new IllegalArgumentException("No beanshell representation available for function: " + sign);
			} else if ("<>".equals(sign)) {
				return "!=";
			} else if ("mod".equalsIgnoreCase(sign)) {
				return "%";
			}

			return sign;
		} else {
			return sign.toUpperCase();
		}
	}
}