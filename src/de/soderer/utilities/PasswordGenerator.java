package de.soderer.utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PasswordGenerator {
	public static char[] generatePassword(int size) throws Exception {
		return PasswordGenerator.generatePassword(size, "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789", "!?\"'`#$%&*+-<>=.,:;/|\\@^_(){}[]~", "§€äöüÄÖÜß°²³µ");
	}

	public static char[] generatePassword(int size, String... characterGroups) throws Exception {
		List<PasswordGeneratorCharacterGroup> groups = new ArrayList<PasswordGeneratorCharacterGroup>();
		for (String characterGroup : characterGroups) {
			groups.add(new PasswordGeneratorCharacterGroup(1, 99, characterGroup.toCharArray()));
		}
		return PasswordGenerator.generatePassword(groups, size, size);
	}

	public static char[] generatePassword(List<PasswordGeneratorCharacterGroup> characterGroups, int minimumLength, int maximumLength) throws Exception {
		minimumLength = Math.min(minimumLength, minimumLength);
		maximumLength = Math.max(minimumLength, maximumLength);

		List<Character> passwordLetters = new ArrayList<Character>();
		Map<PasswordGeneratorCharacterGroup, Integer> groupsLeftToUse = new HashMap<PasswordGeneratorCharacterGroup, Integer>();
		for (PasswordGeneratorCharacterGroup group : characterGroups) {
			List<Character> nextPasswordLetters = new ArrayList<Character>();
			for (int i = 0; i < group.minimum; i++) {
				nextPasswordLetters.add(group.getRandomCharacter());
			}
			if (group.maximum > group.minimum) {
				groupsLeftToUse.put(group, group.minimum);
			}
			passwordLetters.addAll(nextPasswordLetters);
		}
		if (passwordLetters.size() > maximumLength) {
			throw new Exception("Too many fix set letters");
		}
		int passwordLength = minimumLength + Utilities.getRandomNumber(maximumLength - minimumLength + 1);
		for (int i = passwordLetters.size(); i < passwordLength; i++) {
			if (groupsLeftToUse.size() <= 0) {
				throw new Exception("Too few free letters to use");
			}
			PasswordGeneratorCharacterGroup[] keys = groupsLeftToUse.keySet().toArray(new PasswordGeneratorCharacterGroup[0]);
			PasswordGeneratorCharacterGroup nextGroupToUse = keys[Utilities.getRandomNumber(keys.length)];
			passwordLetters.add(nextGroupToUse.getRandomCharacter());
			int timesUsed = groupsLeftToUse.get(nextGroupToUse) + 1;
			if (timesUsed >= nextGroupToUse.maximum) {
				groupsLeftToUse.remove(nextGroupToUse);
			} else {
				groupsLeftToUse.put(nextGroupToUse, timesUsed);
			}
		}
		char[] password = new char[passwordLength];
		for (int i = 0; i < passwordLength; i++) {
			Character nextCharacter = passwordLetters.get(Utilities.getRandomNumber(passwordLetters.size()));
			password[i] = nextCharacter;
			passwordLetters.remove(nextCharacter);
		}
		return password;
	}
}
