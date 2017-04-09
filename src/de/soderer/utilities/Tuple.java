package de.soderer.utilities;

public class Tuple<T1, T2> {
	private T1 value1;
	private T2 value2;

	public Tuple(T1 value1, T2 value2) {
		this.value1 = value1;
		this.value2 = value2;
	}

	public T1 getFirst() {
		return value1;
	}

	public T2 getSecond() {
		return value2;
	}

	@Override
	public String toString() {
		StringBuilder returnString = new StringBuilder("<");

		if (value1 != null) {
			returnString.append(value1.toString());
		} else {
			returnString.append("<null>");
		}

		returnString.append(", ");

		if (value2 != null) {
			returnString.append(value2.toString());
		} else {
			returnString.append("<null>");
		}

		returnString.append(">");
		return returnString.toString();
	}
}
