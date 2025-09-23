
import re
from typing import Any, Dict, List, Optional, Tuple, Callable
import jsonschema

PathSegment = Any  # str | int
MappingEntry = Dict[str, Any]


'''class Mapper:

    ARRAY_INDEX_RE = re.compile(r"^([^\[\]]+)\[(\d+)\]$")
    ARRAY_PLACEHOLDER_RE = re.compile(r"\[\]")

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

    def _resolve_transformation(self, transformation: Optional[Any], src_value: Any, src_field: Optional[str], sample_root: Any) -> Any:
        # Caso enum mapping
        if isinstance(transformation, dict):
            return transformation.get(src_value, None)
        # Caso src_field = "N/A" o fixed value
        if src_field in {"N/A", None, ""}:
            return transformation
        # Se src_value è None (il campo non esiste)
        if src_value is None:
            return None
        # Altrimenti, copia il valore sorgente
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

    def apply_mapping(self, sample: Dict[str, Any], mapping: List[MappingEntry], dst_schema: Dict[str, Any]) -> Dict[str, Any]:
        T: Dict[str, Any] = {}
        
        static_mappings = [e for e in mapping if '[]' not in e.get('target_field', '')]
        dynamic_mappings = [e for e in mapping if '[]' in e.get('target_field', '')]

        for entry in static_mappings:
            src = entry.get("src_field")
            transf = entry.get("transformation")
            tgt = entry.get("target_field")
            
            src_val = None
            if src not in {"N/A", None, ""}:
                src_val = self._get_by_path(sample, src)
            
            val = self._resolve_transformation(transf, src_val, src, sample)
            if tgt:
                self._create_containers_and_set(T, tgt, val)

        if dynamic_mappings:
            src_array_path = ""
            for e in dynamic_mappings:
                if '[]' in e.get('src_field', ''):
                    src_array_path = e['src_field'].split('[]')[0].strip('.')
                    break
            
            src_array = self._get_by_path(sample, src_array_path) if src_array_path else []
            if not isinstance(src_array, list):
                src_array = []
                
            for i, src_item in enumerate(src_array):
                for entry in dynamic_mappings:
                    src_field = entry.get('src_field', '').replace('[]', f'[{i}]')
                    target_field = entry.get('target_field', '').replace('[]', f'[{i}]')
                    transf = entry.get("transformation")
                    
                    src_val = None
                    if src_field not in {"N/A", None, ""}:
                        src_val = self._get_by_path(sample, src_field)
                    
                    val = self._resolve_transformation(transf, src_val, src_field, sample)
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
'''

class Mapper:
    ARRAY_INDEX_RE = re.compile(r"^([^\[\]]+)\[(\d+)\]$")
    ARRAY_PLACEHOLDER_RE = re.compile(r"\[\]")

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

    def _resolve_transformation(self, transformation: Optional[Any], src_value: Any, src_field: Optional[str], sample_root: Any) -> Any:
        if isinstance(transformation, dict):
            return transformation.get(src_value, None)
        if src_field in {"N/A", None, ""}:
            return transformation
        if src_value is None:
            return None
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

    def apply_mapping(self, sample: Dict[str, Any], mapping: List[MappingEntry], dst_schema: Dict[str, Any]) -> Dict[str, Any]:
        T: Dict[str, Any] = {}
        
        # Separo i mapping in 3 categorie: fissi (metadata), con indici fissi (es. messages[0]), e dinamici (es. messages[])
        fixed_mappings = [e for e in mapping if not self.ARRAY_INDEX_RE.search(e.get('target_field', '')) and '[]' not in e.get('target_field', '')]
        indexed_mappings = [e for e in mapping if self.ARRAY_INDEX_RE.search(e.get('target_field', ''))]
        dynamic_mappings = [e for e in mapping if '[]' in e.get('target_field', '')]

        # Applica i mapping fissi e indicizzati (statici)
        for entry in fixed_mappings + indexed_mappings:
            src = entry.get("src_field")
            transf = entry.get("transformation")
            tgt = entry.get("target_field")
            
            src_val = None
            if src not in {"N/A", None, ""}:
                src_val = self._get_by_path(sample, src)
            
            val = self._resolve_transformation(transf, src_val, src, sample)
            if tgt:
                self._create_containers_and_set(T, tgt, val)

        # Applica i mapping dinamici
        if dynamic_mappings:
            src_array_path = ""
            for e in dynamic_mappings:
                if '[]' in e.get('src_field', ''):
                    src_array_path = e['src_field'].split('[]')[0].strip('.')
                    break
            
            src_array = self._get_by_path(sample, src_array_path) if src_array_path else []
            if not isinstance(src_array, list):
                src_array = []
                
            for i, src_item in enumerate(src_array):
                for entry in dynamic_mappings:
                    src_field = entry.get('src_field', '').replace('[]', f'[{i}]')
                    target_field = entry.get('target_field', '').replace('[]', f'[{i}]')
                    transf = entry.get("transformation")
                    
                    src_val = None
                    if src_field not in {"N/A", None, ""}:
                        src_val = self._get_by_path(sample, src_field)
                    
                    val = self._resolve_transformation(transf, src_val, src_field, sample)
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