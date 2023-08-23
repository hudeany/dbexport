package de.soderer.utilities.json.schema.validator;

import java.util.List;

import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class ItemsValidator extends ExtendedBaseJsonSchemaValidator {
	public ItemsValidator(final JsonObject parentValidatorData, final JsonSchemaDependencyResolver jsonSchemaDependencyResolver, final String jsonSchemaPath, final Object validatorData, final JsonNode jsonNode, final String jsonPath) throws JsonSchemaDefinitionError {
		super(parentValidatorData, jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);

		if (!(validatorData instanceof JsonArray) && !(validatorData instanceof JsonObject)) {
			throw new JsonSchemaDefinitionError("Items data is not an 'object' or 'array'", jsonSchemaPath);
		}
	}

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		if (!(jsonNode.isJsonArray())) {
			if (!jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
				throw new JsonSchemaDataValidationError("Expected data type 'array' but was '" + jsonNode.getJsonDataType().getName() + "'", jsonPath);
			}
		} else {
			if (validatorData == null) {
				throw new JsonSchemaDefinitionError("Items data is 'null'", jsonSchemaPath);
			} else if (validatorData instanceof JsonObject) {
				for (int i = 0; i < ((JsonArray) jsonNode.getValue()).size(); i++) {
					JsonNode jsonNodeToCheck;
					try {
						jsonNodeToCheck = new JsonNode(((JsonArray) jsonNode.getValue()).get(i));
					} catch (final Exception e) {
						throw new JsonSchemaDataValidationError("Invalid data type '" + ((JsonArray) jsonNode.getValue()).get(i).getClass().getSimpleName() + "'", jsonPath + "[" + i + "]");
					}
					final List<BaseJsonSchemaValidator> validators = JsonSchema.createValidators((JsonObject) validatorData, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNodeToCheck, jsonPath + "[" + i + "]");
					for (final BaseJsonSchemaValidator validator : validators) {
						validator.validate();
					}
				}

				if (parentValidatorData.containsPropertyKey("additionalItems") && !jsonSchemaDependencyResolver.isUseDraftV4Mode()) {
					throw new JsonSchemaDefinitionError("'additionalItems' is only allowed for 'items' with 'array' data value", jsonSchemaPath);
				}
			} else if (validatorData instanceof JsonArray) {
				if (((JsonArray) jsonNode.getValue()).size() < ((JsonArray) validatorData).size()) {
					throw new JsonSchemaDataValidationError("Minimum amount of array items is " + ((JsonArray) validatorData).size() + " but was " + ((JsonArray) jsonNode.getValue()).size(), jsonPath);
				}
				for (int i = 0; i < ((JsonArray) validatorData).size(); i++) {
					final Object object = ((JsonArray) validatorData).get(i);
					if (!(object instanceof JsonObject)) {
						throw new JsonSchemaDefinitionError("Items data item is not an 'object'", jsonSchemaPath);
					}
					final JsonObject validatorObject = (JsonObject) object;

					JsonNode jsonNodeToCheck;
					try {
						jsonNodeToCheck = new JsonNode(((JsonArray) jsonNode.getValue()).get(i));
					} catch (final Exception e) {
						throw new JsonSchemaDataValidationError("Invalid data type '" + ((JsonArray) jsonNode.getValue()).get(i).getClass().getSimpleName() + "'", jsonPath + "[" + i + "]");
					}
					final List<BaseJsonSchemaValidator> validators = JsonSchema.createValidators(validatorObject, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNodeToCheck, jsonPath + "[" + i + "]");
					for (final BaseJsonSchemaValidator validator : validators) {
						validator.validate();
					}
				}

				if (parentValidatorData.containsPropertyKey("additionalItems")) {
					final Object additionalItemsRaw = parentValidatorData.get("additionalItems");
					if (additionalItemsRaw == null) {
						throw new JsonSchemaDefinitionError("Property 'additionalItems' is 'null'", jsonSchemaPath);
					} else if (additionalItemsRaw instanceof Boolean) {
						if (!(Boolean) additionalItemsRaw) {
							if (((JsonArray) jsonNode.getValue()).size() > ((JsonArray) validatorData).size()) {
								throw new JsonSchemaDataValidationError("Maximum amount of array items is " + ((JsonArray) validatorData).size() + " but was " + ((JsonArray) jsonNode.getValue()).size(), jsonPath);
							}
						}
					} else if (additionalItemsRaw instanceof JsonObject) {
						for (int i = ((JsonArray) validatorData).size(); i < ((JsonArray) jsonNode.getValue()).size(); i++) {
							JsonNode newJsonNode;
							try {
								newJsonNode = new JsonNode(((JsonArray) jsonNode.getValue()).get(i));
							} catch (final Exception e) {
								throw new JsonSchemaDataValidationError("Invalid data type '" + ((JsonArray) jsonNode.getValue()).get(i).getClass().getSimpleName() + "'", jsonPath + "[" + i + "]");
							}
							final List<BaseJsonSchemaValidator> subValidators = JsonSchema.createValidators(((JsonObject) additionalItemsRaw), jsonSchemaDependencyResolver, jsonSchemaPath, newJsonNode, jsonPath + "[" + i + "]");
							for (final BaseJsonSchemaValidator subValidator : subValidators) {
								subValidator.validate();
							}
						}
					} else {
						throw new JsonSchemaDefinitionError("AdditionalItems data is not a 'boolean' or 'object'", jsonSchemaPath);
					}
				}
			} else {
				throw new JsonSchemaDefinitionError("Items data not an 'object' or 'array'", jsonSchemaPath);
			}
		}
	}
}
