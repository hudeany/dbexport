package de.soderer.utilities;

public class PasswordGeneratorCharacterGroup {
	public char[] characters;
	public int minimum;
	public int maximum;

	public PasswordGeneratorCharacterGroup(int minimum, int maximum, char... characters) throws Exception {
		if (minimum < 0 || maximum <= 0 || characters == null || characters.length <= 0) {
			throw new Exception("Invalid parameters");
		}

		minimum = Math.min(minimum, maximum);
		maximum = Math.max(minimum, maximum);

		this.minimum = minimum;
		this.maximum = maximum;
		this.characters = characters;
	}

	public char getRandomCharacter() {
		return characters[Utilities.getRandomNumber(characters.length)];
	}
}
