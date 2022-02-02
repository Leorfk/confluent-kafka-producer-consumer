class AvroService:

    def __init__(self) -> None:
        pass

    def get_avro_schema(self, schema_registry_client, subject_name):
        subjects = schema_registry_client.get_subjects()
        print(subjects)
        return schema_registry_client.get_latest_version(subject_name).schema.schema_str

    def get_avro(self, schema_registry_client, subject_name):
        subjects = schema_registry_client.get_subjects()
        print(subjects)
        return schema_registry_client.get_latest_version(subject_name)
