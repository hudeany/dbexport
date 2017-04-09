package de.soderer.utilities.sql.whereclause.token;

/**
 * Simple Oberklasse, die alle Arten von Rule-Tokens und Aggregationen davon zusammenfasst
 *
 * @author Andreas
 *
 */
public abstract class RulePart {
	public enum StringType {
		Oracle, MySQL, BeanShell
	}

	@Override
	public abstract String toString();

	public abstract String toString(StringType type);
}
