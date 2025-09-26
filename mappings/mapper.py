import re
import jsonschema
from typing import Dict, List, Any, Optional, Callable, Tuple, Union

PathSegment = Union[str, int]
MappingEntry = Dict[str, Any]


class Mapper:
    ARRAY_INDEX_RE = re.compile(r"^([^\[\]]+)\[(\d+)\]$")
    ARRAY_PLACEHOLDER_RE = re.compile(r"\[\]")
    # Nuova regex per gestire annidazione multipla: messages[].attachments[].url
    NESTED_ARRAY_RE = re.compile(r"(.+?)(\[\][^\[\]]*)*(\[\])(.*)") 

    def __init__(self):
        self.dummy_mode = "null"
        self.transform_registry: Dict[str, Callable[[Any], Any]] = {}
        self._register_default_transforms()

    def set_dummy_mode(self, mode: str):
        assert mode in {"null", "dummy"}
        self.dummy_mode = mode

    def _register_default_transforms(self):
        self.transform_registry["null"] = lambda _: None
        self.transform_registry["N/A"] = lambda _: None
        self.transform_registry["src"] = lambda src: src
        self.transform_registry["USER"] = lambda _: "USER"
        self.transform_registry["ASSISTANT"] = lambda _: "ASSISTANT"
        self.transform_registry["chat_template"] = lambda src: src

    def register_transform(self, name: str, fn: Callable[[Any], Any]) -> None:
        self.transform_registry[name] = fn

    def _parse_path(self, path: Optional[str]) -> List[PathSegment]:
        if not path:
            return []
        
        segments: List[PathSegment] = []
        parts = re.split(r'(\.|\[.*?\])', path)
        parts = [p for p in parts if p and p != '.']
        
        for part in parts:
            if part.startswith('['):
                if part == '[]':
                    segments.append('[]')
                else:
                    m = re.match(r'^\[(\d+)\]$', part)
                    if m:
                        segments.append(int(m.group(1)))
            else:
                segments.append(part)
        return segments

    def _get_by_path(self, src: Any, path: Optional[str]) -> Any:
        if path is None or path == "":
            return None
        cur = src
        for seg in self._parse_path(path):
            if cur is None:
                return None
            if isinstance(seg, int):
                if isinstance(cur, list) and 0 <= seg < len(cur):
                    cur = cur[seg]
                else:
                    return None
            elif seg == '[]':
                return None
            else:
                if isinstance(cur, dict):
                    cur = cur.get(seg)
                else:
                    return None
        return cur

    def _get_target_field_schema(self, target_field: str, dst_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Ottiene lo schema del campo target per validazione tipo"""
        if not target_field or not dst_schema:
            return {}
        
        try:
            # Naviga lo schema seguendo il path del target_field
            segments = self._parse_path(target_field)
            current_schema = dst_schema
            
            for i, seg in enumerate(segments):
                if isinstance(seg, int) or seg == '[]':
                    # È un array, prendi lo schema degli items
                    if current_schema.get("type") == "array" and "items" in current_schema:
                        current_schema = current_schema["items"]
                elif isinstance(seg, str):
                    # È una proprietà di oggetto
                    if "properties" in current_schema and seg in current_schema["properties"]:
                        current_schema = current_schema["properties"][seg]
                    else:
                        return {}
            
            return current_schema
        except:
            return {}

    def _cast_to_schema_type(self, value: Any, target_schema: Dict[str, Any]) -> Any:
        """Converte il valore al tipo richiesto dallo schema target, senza serializzare oggetti complessi a stringa"""
        if value is None or not target_schema:
            return value
        target_type = target_schema.get("type")
        if isinstance(target_type, list):
            target_type = next((t for t in target_type if t != "null"), "string")
        try:
            if target_type == "integer" and not isinstance(value, int):
                if isinstance(value, str) and value.isdigit():
                    return int(value)
                elif isinstance(value, float):
                    return int(value)
            elif target_type == "number" and not isinstance(value, (int, float)):
                if isinstance(value, str):
                    return float(value)
            elif target_type == "boolean" and not isinstance(value, bool):
                if isinstance(value, str):
                    return value.lower() in ["true", "1", "yes", "on"]
                else:
                    return bool(value)
            elif target_type == "string" and not isinstance(value, str):
                # NON serializzare oggetti complessi a stringa
                if isinstance(value, (dict, list)):
                    return value  # <-- restituisci dict/list senza serializzare
                return str(value)
        except (ValueError, TypeError):
            pass
        return value

    def _resolve_transformation(self, transformation: Optional[Any], src_value: Any, src_field: Optional[str], sample_root: Any, target_schema: Dict[str, Any] = None) -> Any:
        # Se transformation è un dict, è un enum mapping
        if isinstance(transformation, dict):
            result = transformation.get(src_value, None)
            if result is not None and target_schema:
                result = self._cast_to_schema_type(result, target_schema)
            return result
        
        # Se src_field è N/A, transformation contiene il valore fisso
        if src_field in {"N/A", None, ""}:
            if target_schema:
                transformation = self._cast_to_schema_type(transformation, target_schema)
            return transformation
        
        # Se src_value è None, ritorna None
        if src_value is None:
            return None
        
        # Altrimenti, usa src_value e applica casting se necessario
        if target_schema:
            src_value = self._cast_to_schema_type(src_value, target_schema)
        
        return src_value
    
    def instantiate_from_schema_minimal(self, schema: Dict[str, Any]) -> Any:
        if not schema:
            return None
        
        typ = schema.get("type")
        if isinstance(typ, list):
            types = [t for t in typ if t != "null"]
            typ = types[0] if types else "null"
        
        if "default" in schema:
            d = schema["default"]
            if d == "null":
                return None
            return d
        if "enum" in schema and isinstance(schema["enum"], list) and len(schema["enum"]) > 0:
            return schema["enum"][0]
        
        if typ == "object" or ("properties" in schema and typ is None):
            return {}
        if typ == "array":
            items_schema = schema.get("items", {})
            min_items = schema.get("minItems", 1)
            min_items = max(1, min_items)
            return [self.instantiate_from_schema_minimal(items_schema) for _ in range(min_items)]
        if typ == "string":
            return "" if self.dummy_mode == "dummy" else None
        if typ == "integer":
            return 0 if self.dummy_mode == "dummy" else None
        if typ == "number":
            return 0.0 if self.dummy_mode == "dummy" else None
        if typ == "boolean":
            return False if self.dummy_mode == "dummy" else None
        if typ == "null":
            return None
        if "anyOf" in schema:
            return self.instantiate_from_schema_minimal(schema["anyOf"][0])
        if "oneOf" in schema:
            return self.instantiate_from_schema_minimal(schema["oneOf"][0])
        if "allOf" in schema:
            merged = {}
            for subs in schema["allOf"]:
                c = self.instantiate_from_schema_minimal(subs)
                if isinstance(c, dict):
                    merged.update(c)
            return merged
        return None

    def _create_containers_and_set(self, root: Any, path: str, value: Any):
        segments = self._parse_path(path)
        if not segments:
            return
        
        cur = root
        for i, seg in enumerate(segments):
            last = (i == len(segments) - 1)
            if isinstance(seg, int):
                if not isinstance(cur, list):
                    raise TypeError("Cannot create list at non-list parent")
                while len(cur) <= seg:
                    cur.append(None)
                if last:
                    cur[seg] = value
                    return
                if cur[seg] is None:
                    next_seg = segments[i + 1]
                    cur[seg] = [] if isinstance(next_seg, int) or next_seg == '[]' else {}
                cur = cur[seg]
            else:
                if not isinstance(cur, dict):
                    raise TypeError("Cannot create dict at non-dict parent")
                if last:
                    cur[seg] = value
                    return
                if seg not in cur or cur[seg] is None:
                    next_seg = segments[i + 1]
                    cur[seg] = [] if isinstance(next_seg, int) or next_seg == '[]' else {}
                cur = cur[seg]

    def _populate_required_and_defaults(self, root_obj: Dict[str, Any], schema: Dict[str, Any], mapping_targets: set):
        if not isinstance(schema, dict):
            return
        typ = schema.get("type")
        if isinstance(typ, list):
            types = [t for t in typ if t != "null"]
            typ_primary = types[0] if types else "null"
        else:
            typ_primary = typ

        if typ_primary == "object" or ("properties" in schema and typ_primary is None):
            props = schema.get("properties", {})
            required = schema.get("required", [])
            for pname, pschema in props.items():
                should_create = (pname in required) or any(t.startswith(pname + ".") or t == pname or t.startswith(pname + "[") for t in mapping_targets) or ("default" in pschema) or ("enum" in pschema)
                if not should_create:
                    if pname not in root_obj:
                        continue
                if pname in root_obj and root_obj[pname] is not None:
                    if isinstance(root_obj[pname], dict):
                        self._populate_required_and_defaults(root_obj[pname], pschema, {t[len(pname)+1:] for t in mapping_targets if t.startswith(pname + ".")})
                    elif isinstance(root_obj[pname], list):
                        items_schema = pschema.get("items", {})
                        for i, it in enumerate(root_obj[pname]):
                            if isinstance(it, dict):
                                self._populate_required_and_defaults(it, items_schema, set())
                    continue
                if "default" in pschema:
                    d = pschema["default"]
                    if d == "null":
                        root_obj[pname] = None
                    else:
                        root_obj[pname] = d
                    continue
                if "enum" in pschema and isinstance(pschema["enum"], list) and pschema["enum"]:
                    root_obj[pname] = pschema["enum"][0]
                    continue
                ptype = pschema.get("type")
                if isinstance(ptype, list) and "null" in ptype:
                    root_obj[pname] = None
                    continue
                root_obj[pname] = self.instantiate_from_schema_minimal(pschema)
            return

        if typ_primary == "array":
            return

    def _find_all_array_paths(self, mapping: List[MappingEntry]) -> Dict[str, List[str]]:
        """Trova tutti i path degli array per gestire annidazione multipla"""
        array_paths = {}
        
        for entry in mapping:
            src_field = entry.get('src_field', '')
            target_field = entry.get('target_field', '')
            
            if '[]' in src_field or '[]' in target_field:
                # Estrai il base path dell'array (tutto prima del primo [])
                src_base = src_field.split('[]')[0].rstrip('.') if '[]' in src_field else ""
                target_base = target_field.split('[]')[0].rstrip('.') if '[]' in target_field else ""
                
                key = f"{src_base}→{target_base}"
                if key not in array_paths:
                    array_paths[key] = []
                array_paths[key].append(entry)
        
        return array_paths

    def apply_mapping(self, sample: Dict[str, Any], mapping: List[MappingEntry], dst_schema: Dict[str, Any]) -> Dict[str, Any]:
        T: Dict[str, Any] = {}
        
        # Separo i mapping in categorie
        fixed_mappings = [e for e in mapping if not self.ARRAY_INDEX_RE.search(e.get('target_field', '')) and '[]' not in e.get('target_field', '')]
        indexed_mappings = [e for e in mapping if self.ARRAY_INDEX_RE.search(e.get('target_field', ''))]
        dynamic_mappings = [e for e in mapping if '[]' in e.get('target_field', '')]

        # Applica i mapping fissi e indicizzati (statici)
        for entry in fixed_mappings + indexed_mappings:
            src = entry.get("src_field")
            transf = entry.get("transformation")
            tgt = entry.get("target_field")
            
            # Ottieni schema del campo target per type casting
            target_schema = self._get_target_field_schema(tgt, dst_schema)
            
            src_val = None
            if src not in {"N/A", None, ""}:
                src_val = self._get_by_path(sample, src)
            
            val = self._resolve_transformation(transf, src_val, src, sample, target_schema)
            if tgt:
                self._create_containers_and_set(T, tgt, val)

        # Applica i mapping dinamici (con supporto annidazione multipla)
        if dynamic_mappings:
            array_paths = self._find_all_array_paths(dynamic_mappings)
            
            for path_key, entries in array_paths.items():
                src_base, target_base = path_key.split('→')
                
                # Ottieni l'array sorgente
                src_array = self._get_by_path(sample, src_base) if src_base else []
                if not isinstance(src_array, list):
                    src_array = []
                
                # Processa ogni elemento dell'array
                for i, src_item in enumerate(src_array):
                    for entry in entries:
                        src_field = entry.get('src_field', '').replace('[]', f'[{i}]')
                        target_field = entry.get('target_field', '').replace('[]', f'[{i}]')
                        transf = entry.get("transformation")
                        
                        # Ottieni schema del campo target
                        target_schema = self._get_target_field_schema(target_field, dst_schema)
                        
                        src_val = None
                        if src_field not in {"N/A", None, ""}:
                            src_val = self._get_by_path(sample, src_field)
                        
                        val = self._resolve_transformation(transf, src_val, src_field, sample, target_schema)
                        if target_field:
                            self._create_containers_and_set(T, target_field, val)
        
        mapping_targets = {e['target_field'] for e in mapping if e.get('target_field')}
        self._populate_required_and_defaults(T, dst_schema, mapping_targets)
        return T

    def validate(self, transformed: Dict[str, Any], dst_schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
        validator = jsonschema.Draft7Validator(dst_schema)
        errors: List[str] = []
        for err in validator.iter_errors(transformed):
            path = ".".join([str(p) for p in err.absolute_path])
            errors.append(f"{path}: {err.message}")
        return (len(errors) == 0, errors)

    def map_and_validate(self, sample: Dict[str, Any], mapping: List[MappingEntry], dst_schema: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, List[str]]:
        transformed = self.apply_mapping(sample, mapping, dst_schema)
        valid, errors = self.validate(transformed, dst_schema)
        return transformed, valid, errors



