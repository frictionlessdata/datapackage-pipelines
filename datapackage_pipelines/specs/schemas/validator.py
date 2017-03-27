import os
import json
import jsonschema

from ..errors import SpecError


schema_filename = 'pipeline-spec.schema.json'
schema_filename = os.path.join(os.path.dirname(__file__),
                               schema_filename)
schema = json.load(open(schema_filename))
validator = jsonschema.validators.validator_for(schema)
schema = validator(schema)


def validate_pipeline(pipeline_details, errors):
    try:
        schema.validate(pipeline_details)
    except jsonschema.ValidationError as e:
        errors.append(SpecError('Invalid Pipeline', str(e)))
        return False
    return True
