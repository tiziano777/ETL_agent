from mapper import Mapper
import json

mapper = Mapper()
mapper.set_dummy_mode("null")

# ✅ mapping = lista di regole di trasformazione
mapping = [
        {
            "src_field": "N/A",
            "target_field": "template",
            "transformation": "chat_template"
        },
        {
            "src_field": "N/A",
            "target_field": "system",
            "transformation": "N/A"
        },
        {
            "src_field": "prompt",
            "target_field": "messages[0].content",
            "transformation": "Map the prompt field to the content of the first message in the messages array."
        },
        {
            "src_field": "N/A",
            "target_field": "messages[0].role",
            "transformation": "USER"
        },
        {
            "src_field": "response",
            "target_field": "messages[1].content",
            "transformation": "Map the response field to the content of the second message in the messages array."
        },
        {
            "src_field": "N/A",
            "target_field": "messages[1].role",
            "transformation": "ASSISTANT"
        },
        {
            "src_field": "N/A",
            "target_field": "function_call",
            "transformation": "null"
        },
        {
            "src_field": "N/A",
            "target_field": "think",
            "transformation": "null"
        },
        {
            "src_field": "N/A",
            "target_field": "context",
            "transformation": "null"
        },
        {
            "src_field": "N/A",
            "target_field": "_lang",
            "transformation": "null"
        },
        {
            "src_field": "N/A",
            "target_field": "_dataset_name",
            "transformation": "null"
        },
        {
            "src_field": "N/A",
            "target_field": "_file_name",
            "transformation": "glaive"
        },
        {
            "src_field": "N/A",
            "target_field": "_subpath",
            "transformation": "data"
        },
        {
            "src_field": "N/A",
            "target_field": "_id_hash",
            "transformation": "null"
        }
    ]

# ✅ dst_schema = schema JSON (quello che hai già definito correttamente)
dst_schema = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Almawave Schema Template 2",
  "type": "object",
  "properties": {
    "template": {
      "type": "string",
      "description": "A JSON schema template defining the structure of the data.",
      "enum": ["fc_chat_st"],
      "default": "fc_chat_st"
    },
    "system": {
      "type": ["string", "null"],
      "description": "System prompt instructions for the LLM.",
      "default": "null"
    },
    "context": {
      "type": ["string", "null"],
      "description": "Global conversation context, separate from per-message context.",
      "default": "null"
    },
    "messages": {
      "type": "array",
      "description": "Conversation turns between USER and ASSISTANT.",
      "items": {
        "type": "object",
        "properties": {
          "role": {
            "type": "string",
            "enum": ["USER", "ASSISTANT"],
            "description": "The role of the message sender."
          },
          "content": {
            "type": ["string", "null"],
            "description": "The textual content of the message, if any."
          },
          "functioncall": {
            "type": ["object", "null"],
            "description": "Optional function call metadata for this message.",
            "default": "null",
            "properties": {
              "payload": {
                "type": "string",
                "description": "Serialized function call request."
              },
              "response": {
                "type": "string",
                "description": "Serialized function call response."
              }
            },
            "required": ["payload", "response"]
          },
          "think": {
            "type": ["string", "null"],
            "description": "Optional chain-of-thought or reasoning trace.",
            "default": "null"
          },
          "context": {
            "type": ["string", "null"],
            "description": "Optional context specific to this message.",
            "default": "null"
          }
        },
        "required": ["role"]
      },
      "minItems": 1
    },
    "_lang": {
      "type": ["string", "null"],
      "description": "Language of the dataset.",
      "default": "null"
    },
    "_dataset_name": {
      "type": ["string", "null"],
      "description": "The name of the dataset to be processed.",
      "default": "null"
    },
    "_filename": {
      "type": ["string", "null"],
      "description": "The file name of the dataset to be processed.",
      "default": "null"
    },
    "_subpath": {
      "type": ["string", "null"],
      "description": "The subpath of the dataset to be processed.",
      "default": "null"
    },
    "_id_hash": {
      "type": ["string", "null"],
      "description": "Unique identifier hash of the dataset.",
      "default": "null"
    }
  },
  "required": ["messages"],
  "additionalProperties": "false"
}
sample = {
    "prompt": "Se 'caldo' sta a 'freddo' come 'giorno' sta a 'notte', a cosa sta 'alto' in relazione a 'basso'? Analizza la relazione di opposti.",
    "response": "Alto sta a basso, perché alto e basso sono opposti, così come caldo e freddo o giorno e notte."
}

# ✅ mapping + schema
Y, valid, errors = mapper.map_and_validate(sample, mapping, dst_schema)

print("Mapped output Y:")
print(json.dumps(Y, indent=2, ensure_ascii=False))
print("Is valid:", valid)
print("Validation errors:", errors)

with open("output.json", "w") as f:
    json.dump(Y, f, indent=2, ensure_ascii=False)
