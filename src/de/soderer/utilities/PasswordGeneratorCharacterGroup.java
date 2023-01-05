package de.soderer.utilities;

public class PasswordGeneratorCharacterGroup {
	public char[] characters;
	public int minimum;
	public int maximum;

	public PasswordGeneratorCharacterGroup(final char... characters) throws Exception {
		this(0, characters);
	}

	public PasswordGeneratorCharacterGroup(final int minimum, final char... characters) throws Exception {
		this(minimum, Integer.MAX_VALUE, characters);
	}

	public PasswordGeneratorCharacterGroup(final int minimum, final int maximum, final char... characters) throws Exception {
		if (maximum == -1) {
			this.minimum = minimum;
			this.maximum = maximum;
		} else {
			this.minimum = Math.min(minimum, maximum);
			this.maximum = Math.max(minimum, maximum);
		}
		this.characters = characters;

		if (this.minimum < 0 || this.maximum == 0 || this.maximum < -1 || characters == null || characters.length <= 0) {
			throw new Exception("Invalid parameters");
		}
	}

	public char getRandomCharacter() {
		return characters[Utilities.getRandomNumber(characters.length)];
	}
}
