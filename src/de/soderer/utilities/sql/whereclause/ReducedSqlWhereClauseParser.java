package de.soderer.utilities.sql.whereclause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.soderer.utilities.Utilities;
import de.soderer.utilities.sql.whereclause.token.Expression;
import de.soderer.utilities.sql.whereclause.token.ListExpression;
import de.soderer.utilities.sql.whereclause.token.Operator;
import de.soderer.utilities.sql.whereclause.token.RulePart;
import de.soderer.utilities.sql.whereclause.token.SubSelect;
import de.soderer.utilities.sql.whereclause.token.SupplementalPart;
import de.soderer.utilities.sql.whereclause.token.Value;
import de.soderer.utilities.sql.whereclause.token.Value.Type;

/**
 * Parser for SQL Where-clauses. The parsed has NOT full SQL-Standard functionality. The parsed clause can be output formatted as Oracle, MySQL or BeanShell string.
 *
 * Supported functionality: String-values ( 'abc' ) Number-values ( 1, 2.5 ) Date-values ( date('01.02.2001', 'dd.mm.yyyy') ) Date-conversions ( char(datefield, 'dd.mm.yyyy') ) Systemtime-values (
 * sysdate ) Equations and Comparisons ( =, <, >, >=, <=, !=, <> ) Regular Expressions ( ... like '...' / ... not like '...' ) NullChecks ( ... is null / ... is not null ) ListChecks ( ... in (1, 2,
 * 3) ) Boolean operators ( and, or ) Brackets around expressions Modulo operator ( ... mod 3 = 0 )
 *
 * @author Andreas
 *
 */
public class ReducedSqlWhereClauseParser {
	public RulePart parse(String formula, Map<String, Value.Type> descriptors) {
		Map<String, Value.Type> fieldDescriptors = new HashMap<String, Value.Type>();
		for (String value : descriptors.keySet()) {
			fieldDescriptors.put(value.toLowerCase(), descriptors.get(value));
		}
		for (String value : Expression.SYSDATE_VALUES) {
			fieldDescriptors.put(value, Value.Type.Date);
		}

		int charIndex = 0;
		int length = formula.length();
		String lowerFormula = formula.toLowerCase();
		List<RulePart> parts = new ArrayList<RulePart>();
		boolean readingString = false;
		StringBuilder currentString = new StringBuilder();

		Set<String> operators = new HashSet<String>();
		operators.addAll(Expression.BOOL_UNARY_POSTFIX_OPERATOR_SIGNS);
		operators.addAll(Expression.COMPARE_OPERATOR_SIGNS);
		operators.addAll(Expression.CALCULATION_OPERATOR_SIGNS);
		operators.addAll(Expression.STRING_ONLY_COMPARE_OPERATOR_SIGNS);
		operators.addAll(Expression.BOOL_ONLY_OPERATOR_SIGNS);
		operators.add(Expression.MOD_FUNCTION_SIGN);
		operators.add(Expression.BEANSHELL_MOD_SIGN);
		operators.addAll(Expression.DATE_FUNCTION_SIGNS);
		operators.addAll(Expression.CHAR_FUNCTION_SIGNS);
		operators.addAll(Expression.SINGLE_PARAMETER_STRING_FUNCTION_SIGNS);
		operators.addAll(ListExpression.SIGNS);

		Set<String> texts = new HashSet<String>();
		texts.addAll(operators);
		texts.addAll(fieldDescriptors.keySet());

		int openBrackets = 0;

		while (charIndex < length) {
			char current = formula.charAt(charIndex);

			if (current == '\'') {
				if (charIndex + 1 < length && formula.charAt(charIndex + 1) == '\'') {
					if (readingString) {
						currentString.append(current);
						charIndex += 2;
					} else if (charIndex + 2 < length && formula.charAt(charIndex + 2) == '\'') {
						currentString.append(current);
						readingString = true;
						charIndex += 3;
					} else {
						parts.add(new Value(""));
						charIndex += 2;
					}
				} else {
					readingString = !readingString;
					if (!readingString) {
						parts.add(new Value(currentString.toString()));
						currentString = new StringBuilder();
					}
					charIndex++;
				}
			} else if (readingString) {
				currentString.append(current);
				charIndex++;
			} else if (current == ',') {
				parts.add(new SupplementalPart(SupplementalPart.Type.Separator));
				charIndex++;
			} else if (!Character.isWhitespace(current)) {
				if (current == '(') {
					parts.add(new SupplementalPart(SupplementalPart.Type.OpeningBracket));
					openBrackets++;
					charIndex++;
				} else if (current == ')') {
					parts.add(new SupplementalPart(SupplementalPart.Type.ClosingBracket));
					openBrackets--;
					if (openBrackets < 0) {
						throw new IllegalArgumentException("Too many closing brackets");
					}
					charIndex++;
				} else if (Character.isDigit(current) || current == '.') {
					int end = charIndex + 1;
					boolean pointSeen = current == '.';

					while (end < length) {
						char next = formula.charAt(end);
						if (Character.isDigit(next)) {
							end++;
						} else if (next == '.' && !pointSeen) {
							pointSeen = true;
							end++;
						} else {
							break;
						}
					}

					parts.add(new Value(Double.parseDouble(formula.substring(charIndex, end))));
					charIndex = end;
				} else {
					int bestLength = 0;

					for (String check : texts) {
						if (lowerFormula.startsWith(check, charIndex)) {
							if (check.length() > bestLength) {
								bestLength = check.length();
							}
						}
					}

					if (bestLength == 0) {
						if (parts.size() > 2 && parts.get(parts.size() - 2) instanceof Operator
								&& (((Operator) parts.get(parts.size() - 2)).sign.equalsIgnoreCase("in") || ((Operator) parts.get(parts.size() - 2)).sign.equalsIgnoreCase("not in"))
								&& parts.get(parts.size() - 1) instanceof SupplementalPart && ((SupplementalPart) parts.get(parts.size() - 1)).type == SupplementalPart.Type.OpeningBracket) {
							SubSelect subSelect = searchSubselect(formula, charIndex);
							if (subSelect != null) {
								parts.add(subSelect);
								charIndex += subSelect.value.length();
							} else {
								throw new IllegalArgumentException("Invalid data found at position " + charIndex + " starting with " + current);
							}
						} else {
							throw new IllegalArgumentException("Invalid data found at position " + charIndex + " starting with " + current);
						}
					} else {
						String value = formula.substring(charIndex, charIndex + bestLength);
						if (operators.contains(value.toLowerCase())) {
							parts.add(new Operator(value));
						} else if (fieldDescriptors.keySet().contains(value.toLowerCase())) {
							parts.add(new Value(value, fieldDescriptors.get(value.toLowerCase())));
						}
						charIndex += bestLength;
					}
				}
			} else {
				charIndex++;
			}
		}

		if (openBrackets > 0) {
			throw new IllegalArgumentException("Too many opening brackets");
		} else {
			boolean changed = true;
			while (parts.size() > 1 && changed) {
				changed = false;
				changed = parseLevel1Expressions(parts) || changed;
				changed = parseLevel2Expressions(parts) || changed;
				changed = parseConditions(parts) || changed;
				changed = removeUnusedBrackets(parts) || changed;
			}

			if (parts.size() > 1 || !(parts.get(0) instanceof Value) || ((Value) parts.get(0)).type != Value.Type.Bool) {
				throw new IllegalArgumentException("Rule cannot be evaluated");
			} else {
				return parts.get(0);
			}
		}
	}

	private static boolean parseLevel1Expressions(List<RulePart> parts) {
		boolean changed = false;
		for (int i = parts.size() - 1; i >= 0; i--) {
			if (parts.get(i) instanceof Operator) {
				Operator operator = (Operator) parts.get(i);
				if (Expression.BEANSHELL_MOD_SIGN.equals(operator.sign)) {
					if (i - 1 < 0 || i + 1 >= parts.size() || !(parts.get(i - 1) instanceof Value) || !(parts.get(i + 1) instanceof Value)) {
						throw new IllegalArgumentException("Invalid definition for operator: " + operator.sign);
					} else {
						Expression newValue = new Expression(new Operator(Expression.MOD_FUNCTION_SIGN), ((Value) (parts.get(i - 1))), ((Value) (parts.get(i + 1))));
						parts.remove(i + 1);
						parts.remove(i);
						parts.set(i - 1, newValue);
						changed = true;
					}
				} else if (Expression.CALCULATION_OPERATOR_SIGNS.contains(operator.sign)) {
					if (i - 1 < 0 || i + 1 >= parts.size() || !(parts.get(i - 1) instanceof Value) || !(parts.get(i + 1) instanceof Value)) {
						throw new IllegalArgumentException("Invalid definition for operator: " + operator.sign);
					} else {
						Expression newValue = new Expression(((Value) (parts.get(i - 1))), operator, ((Value) (parts.get(i + 1))));
						parts.remove(i + 1);
						parts.remove(i);
						parts.set(i - 1, newValue);
						changed = true;
					}
				} else if (Expression.DATE_FUNCTION_SIGNS.contains(operator.sign) || Expression.CHAR_FUNCTION_SIGNS.contains(operator.sign) || Expression.MOD_FUNCTION_SIGN.equals(operator.sign)) {
					if (i + 5 >= parts.size() || !(parts.get(i + 1) instanceof SupplementalPart) || ((SupplementalPart) parts.get(i + 1)).type != SupplementalPart.Type.OpeningBracket
							|| !(parts.get(i + 2) instanceof Value) || !(parts.get(i + 3) instanceof SupplementalPart) || ((SupplementalPart) parts.get(i + 3)).type != SupplementalPart.Type.Separator
							|| !(parts.get(i + 4) instanceof Value) || !(parts.get(i + 5) instanceof SupplementalPart)
							|| ((SupplementalPart) parts.get(i + 5)).type != SupplementalPart.Type.ClosingBracket) {
						throw new IllegalArgumentException("Invalid definition for function: " + operator.sign);
					} else {
						Expression newValue = new Expression(operator, (Value) (parts.get(i + 2)), (Value) (parts.get(i + 4)));
						parts.remove(i + 5);
						parts.remove(i + 4);
						parts.remove(i + 3);
						parts.remove(i + 2);
						parts.remove(i + 1);
						parts.set(i, newValue);
						changed = true;
					}
				} else if (Expression.SINGLE_PARAMETER_STRING_FUNCTION_SIGNS.contains(operator.sign)) {
					if (i + 3 >= parts.size() || !(parts.get(i + 1) instanceof SupplementalPart) || ((SupplementalPart) parts.get(i + 1)).type != SupplementalPart.Type.OpeningBracket
							|| !(parts.get(i + 2) instanceof Value) || !(parts.get(i + 3) instanceof SupplementalPart)
							|| ((SupplementalPart) parts.get(i + 3)).type != SupplementalPart.Type.ClosingBracket) {
						throw new IllegalArgumentException("Invalid definition for function: " + operator.sign);
					} else {
						Expression newValue = new Expression(operator, (Value) parts.get(i + 2));
						parts.remove(i + 3);
						parts.remove(i + 2);
						parts.remove(i + 1);
						parts.set(i, newValue);
						changed = true;
					}
				} else if (Expression.BOOL_UNARY_POSTFIX_OPERATOR_SIGNS.contains(operator.sign)) {
					if (i - 1 < 0 || !(parts.get(i - 1) instanceof Value)) {
						throw new IllegalArgumentException("Invalid definition for unary operator: " + operator.sign);
					} else {
						Expression newValue = new Expression(((Value) (parts.get(i - 1))), operator);
						parts.remove(i);
						parts.set(i - 1, newValue);
						changed = true;
					}
				}
			}
		}
		return changed;
	}

	private static boolean parseLevel2Expressions(List<RulePart> parts) {
		boolean changed = false;
		for (int i = parts.size() - 1; i >= 0; i--) {
			if (parts.get(i) instanceof Operator) {
				Operator operator = (Operator) parts.get(i);
				if (Expression.STRING_ONLY_COMPARE_OPERATOR_SIGNS.contains(operator.sign) || Expression.COMPARE_OPERATOR_SIGNS.contains(operator.sign)) {
					if (i - 1 < 0 || i + 1 >= parts.size() || !(parts.get(i - 1) instanceof Value) || !(parts.get(i + 1) instanceof Value)) {
						throw new IllegalArgumentException("Invalid definition for operator: " + operator.sign);
					} else {
						Expression newValue = new Expression(((Value) (parts.get(i - 1))), operator, ((Value) (parts.get(i + 1))));
						parts.remove(i + 1);
						parts.remove(i);
						parts.set(i - 1, newValue);
						changed = true;
					}
				} else if (ListExpression.SIGNS.contains(operator.sign)) {
					if (i - 1 < 0 || !(parts.get(i - 1) instanceof Value) || !(parts.get(i + 1) instanceof SupplementalPart)
							|| ((SupplementalPart) parts.get(i + 1)).type != SupplementalPart.Type.OpeningBracket) {
						throw new IllegalArgumentException("Invalid definition for list operator: " + operator.sign);
					} else {
						int closingPosition = -1;
						List<Value> values = new ArrayList<Value>();
						for (int j = i + 2; j < parts.size(); j++) {
							if (parts.get(j) instanceof SupplementalPart && ((SupplementalPart) parts.get(j)).type == SupplementalPart.Type.ClosingBracket) {
								closingPosition = j;
								break;
							} else if (parts.get(j) instanceof Value) {
								values.add((Value) parts.get(j));
							} else if (!(parts.get(j) instanceof SupplementalPart) || ((SupplementalPart) parts.get(j)).type != SupplementalPart.Type.Separator) {
								throw new IllegalArgumentException("Invalid list definition");
							}
						}

						if (closingPosition < 0) {
							throw new IllegalArgumentException("Invalid list definition");
						} else {
							ListExpression newValue = new ListExpression((Value) parts.get(i - 1), (Operator) parts.get(i), values);
							for (int j = closingPosition; j > i; j--) {
								parts.remove(j);
							}
							parts.remove(i);
							parts.set(i - 1, newValue);
						}
					}
				}
			}
		}
		return changed;
	}

	private static boolean removeUnusedBrackets(List<RulePart> parts) {
		boolean changed = false;
		for (int i = parts.size() - 1; i >= 0; i--) {
			if (parts.get(i) instanceof SupplementalPart && ((SupplementalPart) parts.get(i)).type == SupplementalPart.Type.OpeningBracket) {
				if (i + 1 < parts.size() && parts.get(i) instanceof SupplementalPart && ((SupplementalPart) parts.get(i)).type == SupplementalPart.Type.ClosingBracket) {
					parts.remove(i + 1);
					parts.remove(i);
					changed = true;
				} else if (i + 2 < parts.size() && parts.get(i + 2) instanceof SupplementalPart && ((SupplementalPart) parts.get(i + 2)).type == SupplementalPart.Type.ClosingBracket) {
					parts.remove(i + 2);
					parts.remove(i);
					changed = true;
				}
			}
		}
		return changed;
	}

	private static boolean parseConditions(List<RulePart> parts) {
		boolean changed = false;
		int bracketlevel = -1;
		int arithmeticLevel = -1;
		int currentBracketLevel = 0;
		int position = -1;
		for (int i = parts.size() - 1; i >= 0; i--) {
			if ((parts.get(i) instanceof SupplementalPart)) {
				if (((SupplementalPart) parts.get(i)).type == SupplementalPart.Type.OpeningBracket) {
					currentBracketLevel--;
				} else if (((SupplementalPart) parts.get(i)).type == SupplementalPart.Type.ClosingBracket) {
					currentBracketLevel++;
				}
			} else if (parts.get(i) instanceof Operator && Expression.BOOL_ONLY_OPERATOR_SIGNS.contains(((Operator) parts.get(i)).sign.toLowerCase())) {
				if (i - 1 >= 0 && i + 1 < parts.size() && parts.get(i - 1) instanceof Value && ((Value) parts.get(i - 1)).type == Type.Bool && parts.get(i + 1) instanceof Value
						&& ((Value) parts.get(i + 1)).type == Type.Bool) {
					int nextArithmeticLevel = Expression.BOOL_ONLY_OPERATOR_SIGNS.indexOf(((Operator) parts.get(i)).sign);
					if (bracketlevel < currentBracketLevel || (currentBracketLevel == bracketlevel && arithmeticLevel <= nextArithmeticLevel)) {
						arithmeticLevel = nextArithmeticLevel;
						bracketlevel = currentBracketLevel;
						position = i;
					}
				}
			}
		}

		if (position >= 0) {
			Expression newValue = new Expression((Value) parts.get(position - 1), (Operator) parts.get(position), (Value) parts.get(position + 1));
			parts.remove(position + 1);
			parts.remove(position);
			parts.set(position - 1, newValue);
			changed = true;
		}
		return changed;
	}

	private static SubSelect searchSubselect(String text, int startIndex) {
		int openBrackets = 0;
		int charIndex = startIndex;
		StringBuilder subSelectString = new StringBuilder();

		while (charIndex < text.length()) {
			char currentChar = text.charAt(charIndex);
			if (currentChar == '(') {
				openBrackets++;
			} else if (currentChar == ')') {
				openBrackets--;
				if (openBrackets == -1) {
					return new SubSelect(subSelectString.toString());
				}
			}

			subSelectString.append(currentChar);
			charIndex++;
		}

		return null;
	}

	/**
	 * Debug Helper Method
	 *
	 * @param tokens
	 */
	public static void printJoinedParts(List<RulePart> tokens) {
		StringBuilder countBuilder = new StringBuilder();
		StringBuilder builder = new StringBuilder();
		for (Object token : tokens) {
			if (builder.length() > 0) {
				countBuilder.append(" ");
				builder.append(" ");
			}
			builder.append(token);
			countBuilder.append((tokens.indexOf(token) % 10) + Utilities.repeat(" ", token.toString().length() - 1));
		}
		System.out.println(countBuilder);
		System.out.println(builder);
	}
}
