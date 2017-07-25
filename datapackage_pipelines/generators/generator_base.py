import jsonschema


class GeneratorBase(object):

    def __init__(self):
        self.schema = None

    def _get_schema(self):
        if self.schema is not None:
            return self.schema
        self.schema = self.get_schema()
        validator = jsonschema.validators.validator_for(self.schema)
        self.schema = validator(self.schema)
        return self.schema

    def internal_validate(self, source):
        schema = self._get_schema()
        try:
            schema.validate(source)
        except jsonschema.ValidationError as e:
            return False
        return True

    def internal_generate(self, source):
        if not self.internal_validate(source):
            return None
        return self.generate_pipeline(source)

    @classmethod
    def get_schema(cls):
        raise NotImplementedError()

    @classmethod
    def generate_pipeline(cls, source):
        raise NotImplementedError()
