# mapper.py
import re
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
import jsonschema

PathSegment = Union[str, int]
MappingEntry = Dict[str, Optional[str]]

class Mapper:
    ARRAY_INDEX_RE = re.compile(r"^([^\[\]]+)\[(\d+)\]$")  # matches 'arr[0]'

    def __init__(self):
        # intentionally empty (lazy init below)
        pass

    # ---------------- lazy init ----------------
    def _ensure_initialized(self):
        if not hasattr(self, "dummy_mode"):
            self.dummy_mode = "null"
        if not hasattr(self, "transform_registry"):
            self.transform_registry: Dict[str, Callable[[Any], Any]] = {}
            self._register_default_transforms()

    def set_dummy_mode(self, mode: str):
        assert mode in {"null", "dummy"}
        self._ensure_initialized()
        self.dummy_mode = mode

    # ---------------- transform registry ----------------
    def _register_default_transforms(self):
        # NOTE: keep token case exact (USER, ASSISTANT)
        self.transform_registry["null"] = lambda _: None
        self.transform_registry["N/A"] = lambda _: None
        self.transform_registry["src"] = lambda src: src
        self.transform_registry["USER"] = lambda _: "USER"
        self.transform_registry["ASSISTANT"] = lambda _: "ASSISTANT"
        self.transform_registry["chat_template"] = lambda src: src

    def register_transform(self, name: str, fn: Callable[[Any], Any]) -> None:
        self._ensure_initialized()
        self.transform_registry[name] = fn

    # ---------------- path parsing ----------------
    def _parse_path(self, path: Optional[str]) -> List[PathSegment]:
        if not path:
            return []
        segments: List[PathSegment] = []
        parts = path.split(".")
        for part in parts:
            # match simple pattern like "messages[0]"
            m = self.ARRAY_INDEX_RE.match(part)
            if m:
                segments.append(m.group(1))
                segments.append(int(m.group(2)))
            elif "[" in part:
                # generic handling: split by '[' and ']'
                cur = part
                while cur:
                    idx = cur.find("[")
                    if idx == -1:
                        segments.append(cur)
                        break
                    if idx > 0:
                        segments.append(cur[:idx])
                    rest = cur[idx:]
                    m2 = re.match(r"^\[(\d+)\](.*)$", rest)
                    if not m2:
                        # malformed bracket -> take raw remainder
                        segments.append(rest)
                        break
                    segments.append(int(m2.group(1)))
                    cur = m2.group(2)
            else:
                segments.append(part)
        return segments

    # ---------------- safe get ----------------
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
            else:
                if isinstance(cur, dict):
                    cur = cur.get(seg)
                else:
                    return None
        return cur

    # ---------------- transform resolution ----------------
    def _resolve_transformation(self, transformation: Optional[str], src_value: Any, sample_root: Any) -> Any:
        self._ensure_initialized()
        if transformation is None:
            return src_value
        t = transformation
        if isinstance(t, str) and t in {"N/A", "null"}:
            return None
        if isinstance(t, str) and t in self.transform_registry:
            return self.transform_registry[t](src_value)
        # template substitution using sample root
        if isinstance(t, str):
            placeholders = re.findall(r"\{\{([^}]+)\}\}", t)
            if placeholders:
                out = t
                for ph in placeholders:
                    ph_val = self._get_by_path(sample_root, ph)
                    out = out.replace("{{" + ph + "}}", str(ph_val) if ph_val is not None else "")
                return out
        # fallback: literal string (no inference)
        return t

    # ---------------- instantiate helpers ----------------
    def instantiate_from_schema_minimal(self, schema: Dict[str, Any]) -> Any:
        """
        Minimal instantiation used as template when schema forces default/enum or required.
        But we will not populate optional non-required fields by default.
        Use this to create nested structures when needed.
        """
        if not schema:
            return None
        typ = schema.get("type")
        if isinstance(typ, list):
            types = [t for t in typ if t != "null"]
            typ = types[0] if types else "null"

        # prefer default > enum > null (if allowed) > minimal object/array/dummy
        if "default" in schema:
            d = schema["default"]
            # interpret string "null" as None defensively
            if d == "null":
                return None
            return d
        if "enum" in schema and isinstance(schema["enum"], list) and len(schema["enum"]) > 0:
            return schema["enum"][0]

        if typ == "object" or ("properties" in schema and typ is None):
            # create empty dict; we will populate required fields later
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

    # ---------------- set_by_path with creation (best-effort) ----------------
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
                idx = seg
                while len(cur) <= idx:
                    cur.append(None)
                if last:
                    cur[idx] = value
                    return
                if cur[idx] is None:
                    next_seg = segments[i + 1]
                    cur[idx] = [] if isinstance(next_seg, int) else {}
                cur = cur[idx]
            else:
                if not isinstance(cur, dict):
                    raise TypeError("Cannot create dict at non-dict parent")
                if last:
                    cur[seg] = value
                    return
                if seg not in cur or cur[seg] is None:
                    next_seg = segments[i + 1]
                    cur[seg] = [] if isinstance(next_seg, int) else {}
                cur = cur[seg]

    # ---------------- populate required/defaults after mapping ----------------
    def _populate_required_and_defaults(self, root_obj: Dict[str, Any], schema: Dict[str, Any], mapping_targets: set):
        """
        Ensure:
          - all required properties exist and have plausible values (default/enum or minimal)
          - do not create non-required properties unless they are in mapping_targets
          - for union types including null, prefer None if not mapped
        """
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
                full_target = pname  # top-level name; mapping_targets contains dot-paths so we check prefixes elsewhere
                # decide whether to create:
                should_create = (pname in required) or any(t.startswith(pname + ".") or t == pname or t.startswith(pname + "[") for t in mapping_targets) or ("default" in pschema) or ("enum" in pschema)
                if not should_create:
                    # ensure absent (don't overwrite if mapping set it)
                    if pname not in root_obj:
                        continue
                # if mapping already set the property, recurse into it
                if pname in root_obj and root_obj[pname] is not None:
                    # recurse for objects/arrays
                    if isinstance(root_obj[pname], dict):
                        self._populate_required_and_defaults(root_obj[pname], pschema, {t[len(pname)+1:] for t in mapping_targets if t.startswith(pname + ".")})
                    elif isinstance(root_obj[pname], list):
                        items_schema = pschema.get("items", {})
                        for i, it in enumerate(root_obj[pname]):
                            if isinstance(it, dict):
                                self._populate_required_and_defaults(it, items_schema, set())  # deep mapping targets maybe complex
                    continue
                # not present or None -> create sensible default
                # prefer explicit default or enum
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
                # if schema allows null, set None
                ptype = pschema.get("type")
                if isinstance(ptype, list) and "null" in ptype:
                    root_obj[pname] = None
                    continue
                # else create minimal structure consistent with type
                root_obj[pname] = self.instantiate_from_schema_minimal(pschema)
            return

        if typ_primary == "array":
            # if root_obj is not a list, leave it (shouldn't happen normally)
            return

    # ---------------- apply mapping ----------------
    def apply_mapping(self, sample: Dict[str, Any], mapping: List[MappingEntry], dst_schema: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_initialized()
        # Start with empty root and only create what mapping requires (plus required/defaults)
        T: Dict[str, Any] = {}
        mapping_targets = set()
        for entry in mapping:
            tgt = entry.get("target_field")
            if not tgt:
                continue
            mapping_targets.add(tgt)
            src = entry.get("src_field")
            transf = entry.get("transformation")
            src_val = None
            if src and src not in {"N/A", None, ""}:
                src_val = self._get_by_path(sample, src)
            val = self._resolve_transformation(transf, src_val, sample)
            # attempt best-effort set: create containers if necessary
            # we must ensure intermediate containers: create top-level dicts/arrays if needed
            # If path begins with array literal (e.g. "messages[0]...") ensure T["messages"] is list
            try:
                # careful: attempt to create containers for top-level if missing
                # ensure first segment container
                segments = self._parse_path(tgt)
                if not segments:
                    continue
                first = segments[0]
                if isinstance(first, int):
                    raise TypeError("target path cannot start with integer")
                if first not in T:
                    # decide whether first should be list or dict by inspecting next segment
                    if len(segments) > 1 and isinstance(segments[1], int):
                        T[first] = []
                    else:
                        T[first] = {}
                # now create containers and set
                self._create_containers_and_set(T, tgt, val)
            except Exception:
                # as last resort, try to set with suppressed errors (entry skipped)
                try:
                    self._create_containers_and_set(T, tgt, val)
                except Exception:
                    # ignore bad entry; validation will catch missing requireds later
                    continue

        # After applying mapping entries, populate required/defaults according to dst_schema
        self._populate_required_and_defaults(T, dst_schema, mapping_targets)
        return T

    # ---------------- validation ----------------
    def validate(self, transformed: Dict[str, Any], dst_schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
        validator = jsonschema.Draft7Validator(dst_schema)
        errors: List[str] = []
        for err in validator.iter_errors(transformed):
            path = ".".join([str(p) for p in err.absolute_path])
            errors.append(f"{path}: {err.message}")
        return (len(errors) == 0, errors)

    # ---------------- combined api ----------------
    def map_and_validate(self, sample: Dict[str, Any], mapping: List[MappingEntry], dst_schema: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, List[str]]:
        transformed = self.apply_mapping(sample, mapping, dst_schema)
        valid, errors = self.validate(transformed, dst_schema)
        return transformed, valid, errors
