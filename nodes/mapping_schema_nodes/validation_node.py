from states.src_schema_state import  State
from typing import Dict, List
from langgraph.types import Command
from typing import Literal

import json
from json_repair import repair_json
import jsonschema 

class ValidationNode:
    def __init__(self):
        pass

    def extract_json(self, json_text: str) -> dict:
        """
        Ripara e deserializza l'output JSON generato da LLM. Restituisce una lista oppure {} in caso di errore.
        """
        try:
            #print("json text: ", json_text)
            repaired_text = repair_json(json_text)
            parsed_json = json.loads(repaired_text)

            if not isinstance(parsed_json, dict):
                raise ValueError("Parsed JSON is not a list")

            return parsed_json

        except Exception as e:
            return e

    def _apply_mapping(self, sample: Dict, mapping: List[Dict], target_schema: Dict) -> Dict:
        """
        Applica il mapping a un singolo campione di dati e popola i campi rimanenti con null.
        Gestisce la navigazione di percorsi complessi e assegna valori fissi.
        """
        transformed_sample = {}
        
        # 1. Applicazione del mapping
        for map_item in mapping:
            src_field = map_item.get("src_field")
            target_field = map_item.get("target_field")
            transformation = map_item.get("transformation")
            
            if not target_field:
                continue

            # Naviga nel sample di destinazione per impostare il valore
            path = target_field.split('.')
            temp_dict = transformed_sample
            for i, key in enumerate(path):
                is_last = (i == len(path) - 1)
                
                if is_last:
                    # Assegna il valore finale
                    value_to_set = None
                    if src_field == "N/A" or src_field is None:
                        # Valore fisso o calcolo diretto
                        value_to_set = transformation
                    else:
                        # Recupera il valore dal sample di origine navigando il percorso
                        src_value = sample
                        for src_key in src_field.split('.'):
                            src_value = src_value.get(src_key)
                            if src_value is None:
                                break
                        value_to_set = src_value

                    # Gestisce l'assegnazione finale, anche per gli array
                    if isinstance(temp_dict, list) and key.isdigit():
                        index = int(key)
                        while len(temp_dict) <= index:
                            temp_dict.append({}) # Estende con oggetti vuoti
                        temp_dict[index] = value_to_set
                    else:
                        temp_dict[key] = value_to_set
                else:
                    # Naviga o crea un oggetto/array nidificato
                    if isinstance(temp_dict, dict):
                        if key not in temp_dict:
                            # Prepara per il prossimo livello. Se il prossimo 'key' è un numero, crea un array.
                            next_is_array = (path[i+1].isdigit())
                            temp_dict[key] = [] if next_is_array else {}
                        
                        if isinstance(temp_dict[key], list) and path[i+1].isdigit():
                            index = int(path[i+1])
                            while len(temp_dict[key]) <= index:
                                temp_dict[key].append({}) # Pre-popola gli elementi
                        
                        temp_dict = temp_dict[key]
                    elif isinstance(temp_dict, list) and key.isdigit():
                        temp_dict = temp_dict[int(key)]
        
        # 2. Popolare i campi rimanenti con null
        def _populate_nulls(obj, schema_props):
            if not isinstance(obj, dict):
                return
            for prop_name, prop_details in schema_props.items():
                if prop_name not in obj:
                    obj[prop_name] = None
                elif prop_details.get("type") == "object" and isinstance(obj.get(prop_name), dict):
                    _populate_nulls(obj[prop_name], prop_details.get("properties", {}))
                elif prop_details.get("type") == "array" and isinstance(obj.get(prop_name), list):
                    if prop_details.get("items") and prop_details["items"].get("type") == "object":
                        for item in obj[prop_name]:
                            _populate_nulls(item, prop_details["items"].get("properties", {}))

        target_properties = target_schema.get("properties", {})
        _populate_nulls(transformed_sample, target_properties)
        
        return transformed_sample


    def __call__(self, state: State) -> Command[Literal["llm_node", "writer_node"]]:
        print("Validating mapping with samples...")
        
        try:
            mapping_str = state.chat_history[-1].content
            # Il mapping deve essere un array, non un dizionario con una chiave "mappings"
            generated_mapping = self.extract_json(mapping_str)
            
            # Se l'LLM ha generato un dizionario con la chiave "mappings", estraiamo l'array
            if isinstance(generated_mapping, dict) and "mappings" in generated_mapping:
                generated_mapping = generated_mapping["mappings"]
                
            if not isinstance(generated_mapping, list):
                raise ValueError("Il mapping generato non è un array JSON valido.")
            
        except Exception as e:
            return Command(
                goto="llm_node",
                update={
                    "valid": False,
                    "feedback": f"Errore nellastruttura dell'output del mapping: {str(e)}",
                    "generated_mapping": generated_mapping
                }
            )

        try:
            # Per il testing, assumiamo che lo schema target sia presente nello stato
            # In un'implementazione reale, il percorso deve essere caricato
            target_schema = state.target_schema
        except AttributeError:
            raise RuntimeError("target_schema non è presente nello stato. Assicurati di caricarlo prima.")


        error_messages = []
        valid = True

        for i, sample in enumerate(state.samples):
            try:
                transformed_sample = self._apply_mapping(sample, generated_mapping, target_schema)
                
                validator = jsonschema.Draft7Validator(target_schema)
                validator.validate(transformed_sample)
                
            except jsonschema.exceptions.ValidationError as e:
                valid = False
                error_messages.append(f"Errore di validazione sul sample {i+1}: {e.message}")
            except Exception as e:
                valid = False
                error_messages.append(f"Errore nella conversione del sample {i+1}: {str(e)}")

        if valid:
            print("Mapping validato con successo!")
            return Command(
                goto="writer_node",
                update={
                    "mapping": generated_mapping,
                    "valid": True
                })
        else:
            print("Mapping non valido. Errori di validazione:", error_messages)
            feedback_msg = f"Il mapping non è valido. Errori riscontrati:\n" + "\n".join(error_messages)
            return Command(
                goto="llm_node",
                update={
                    "valid": False,
                    "feedback": feedback_msg,
                    "generated_mapping": generated_mapping
                })