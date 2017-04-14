package de.soderer.utilities.sql.whereclause.token;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ListExpression extends Value {
	public static final Set<String> SIGNS = new HashSet<String>(Arrays.asList(new String[] { "in", "not in" }));

	public Value value;
	public Operator operator;
	public List<Value> valueList = new ArrayList<Value>();

	public ListExpression(Value value, Operator operator, List<Value> values) {
		type = Type.Bool;
		this.value = value;
		this.operator = operator;
		for (Value valueItem : values) {
			if (value.type == valueItem.type || valueItem instanceof SubSelect) {
				valueList.add(valueItem);
			} else {
				throw new IllegalArgumentException("Illegal list value: " + valueItem);
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder returnValue = new StringBuilder();
		returnValue.append(value.toString());
		returnValue.append(" ");
		returnValue.append(operator.toString());
		returnValue.append(" (");
		boolean first = true;
		for (Value valueItem : valueList) {
			if (!first) {
				returnValue.append(", ");
			}
			returnValue.append(valueItem.toString());
			first = false;
		}
		returnValue.append(")");
		return returnValue.toString();
	}

	@Override
	public String toString(RulePart.StringType stringType) {
		StringBuilder returnValue = new StringBuilder();
		returnValue.append(value.toString(stringType));
		returnValue.append(" ");
		returnValue.append(operator.toString(stringType));
		returnValue.append(" (");
		boolean first = true;
		for (Value valueItem : valueList) {
			if (!first) {
				returnValue.append(", ");
			}
			returnValue.append(valueItem.toString(stringType));
			first = false;
		}
		returnValue.append(")");
		return returnValue.toString();
	}
}
