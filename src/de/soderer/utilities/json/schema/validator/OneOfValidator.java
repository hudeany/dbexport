package de.soderer.utilities.json.schema.validator;

import java.util.ArrayList;
import java.util.List;

import de.soderer.utilities.json.JsonArray;
import de.soderer.utilities.json.JsonNode;
import de.soderer.utilities.json.JsonObject;
import de.soderer.utilities.json.schema.JsonSchema;
import de.soderer.utilities.json.schema.JsonSchemaDataValidationError;
import de.soderer.utilities.json.schema.JsonSchemaDefinitionError;
import de.soderer.utilities.json.schema.JsonSchemaDependencyResolver;

public class OneOfValidator extends BaseJsonSchemaValidator {
	private List<List<BaseJsonSchemaValidator>> subValidatorPackages = null;

	public OneOfValidator(final JsonSchemaDependencyResolver jsonSchemaDependencyResolver, final String jsonSchemaPath, final Object validatorData, final JsonNode jsonNode, final String jsonPath) throws JsonSchemaDefinitionError {
		super(jsonSchemaDependencyResolver, jsonSchemaPath, validatorData, jsonNode, jsonPath);

		if (validatorData == null) {
			throw new JsonSchemaDefinitionError("OneOf array is 'null'", jsonSchemaPath);
		} else if (validatorData instanceof JsonArray) {
			subValidatorPackages = new ArrayList<>();
			for (final Object subValidationData : ((JsonArray) validatorData)) {
				if (subValidationData instanceof JsonObject) {
					subValidatorPackages.add(JsonSchema.createValidators((JsonObject) subValidationData, jsonSchemaDependencyResolver, jsonSchemaPath, jsonNode, jsonPath));
				} else {
					throw new JsonSchemaDefinitionError("OneOf array contains a non-JsonObject", jsonSchemaPath);
				}
			}
			if (subValidatorPackages == null || subValidatorPackages.size() == 0) {
				throw new JsonSchemaDefinitionError("OneOf array is empty", jsonSchemaPath);
			}
		} else {
			throw new JsonSchemaDefinitionError("OneOf property does not have an array value", jsonSchemaPath);
		}
	}

	@Override
	public void validate() throws JsonSchemaDefinitionError, JsonSchemaDataValidationError {
		int applyCount = 0;
		for (final List<BaseJsonSchemaValidator> subValidatorPackage : subValidatorPackages) {
			try {
				for (final BaseJsonSchemaValidator subValidator : subValidatorPackage) {
					subValidator.validate();
				}
				applyCount++;
			} catch (@SuppressWarnings("unused") final JsonSchemaDataValidationError e) {
				// Do nothing, exactly one subvalidator must have successfully validated
			}
		}

		if (applyCount < 1) {
			throw new JsonSchemaDataValidationError("No option of 'oneOf' property did apply to JsonNode", jsonPath);
		} else if (applyCount > 1) {
			throw new JsonSchemaDataValidationError("More than one option of 'oneOf' property did apply to JsonNode", jsonPath);
		}
	}
}
