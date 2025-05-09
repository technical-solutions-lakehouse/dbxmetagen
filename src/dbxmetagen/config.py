import yaml

class MetadataConfig:
    ACRO_CONTENT = {
        }
    SETUP_PARAMS = {
        "yaml_file_path": "../variables.yml",
        "yaml_variable_names": ['host', 'catalog_name', 'schema_name', 'catalog_tokenizable', 'disable_medical_information_value', 'format_catalog', 'model', 'registered_model_name', 'model_type', 'volume_name', 'table_names_source', 'source_file_path', 'control_table', 'apply_ddl', 'ddl_output_format', 'allow_data', 'dry_run', 'pi_classification_rules', 'allow_manual_override', 'override_csv_path', 'tag_none_fields', 'pi_column_field_names', 'max_prompt_length', 'columns_per_call', 'sample_size', 'max_tokens', 'word_limit_per_cell', 'limit_prompt_based_on_cell_len', 'temperature', 'add_metadata', 'acro_content']    
    }
    MODEL_PARAMS = {
    }

    def __init__(self, **kwargs):
        print(kwargs)
        self.setup_params = self.__class__.SETUP_PARAMS
        self.model_params = self.__class__.MODEL_PARAMS
        self.log_timestamp = None


        for key, value in self.setup_params.items():
            setattr(self, key, value)

        for key, value in self.model_params.items():
            setattr(self, key, value)

        for key, value in kwargs.items():
            setattr(self, key, value)

        yaml_variables = self.load_yaml()
        for key, value in yaml_variables.items():
            setattr(self, key, value)

        self.instantiate_environments()

        print(self.current_user)

    def load_yaml(self):
        with open(self.yaml_file_path, 'r') as file:
            variables = yaml.safe_load(file)
        selected_variables = {key: variables['variables'][key]['default'] for key in self.yaml_variable_names if key in variables['variables']}
        return selected_variables

    def instantiate_environments(self):
        try:
            self.base_url = self.host
        except:
            raise Exception(f"Environment {self.env} does not match any provided host in variables.yml.")
