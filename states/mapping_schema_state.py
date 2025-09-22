from typing import List, Dict, Any, Literal
from typing_extensions import Annotated
from pydantic import Field, BaseModel
from langchain_core.messages import BaseMessage


class State(BaseModel):
    samples: List[Dict[str, Any]] = Field(default=[], description="Esempi di dati grezzi")
    mapped_samples: List[Dict[str, Any]] = Field(default=[], description="Esempi di dati trasformati")
    metadata: Dict[str, Any] | None = Field(default=None, description="Metadati associati ai dati")

    src_schema: Dict[str, Any] | None = Field(default=None, description="Schema dei dati")
    dst_schema: Dict[str, Any] | None = Field(default=None, description="Schema di destinazione per il mapping")
    
    chat_history: Annotated[List[BaseMessage], "Conversation"] = Field(default=[], description="Storico della conversazione con l'LLM")
    feedback: str | None = Field(default=None, description="Feedback dell'utente sulla generazione dello schema")
    
    mapping: List[Dict[str, Any]] | None = Field(default=None, description="Mapping generato tra src_schema e dst_schema")
    accept_mapping_generation: Literal["continue", "break", "restart", "manual"] | None = Field(default=None, description="Decisione dell'utente sulla generazione dello schema")
    
    valid: bool = Field(default=False, description="Indica se il mapping è valido rispetto allo schema di destinazione")
    
    output_path: str | None = Field(default='', description="Percorso di output per il salvataggio dello schema")
    error_messages: List[str] = Field(default=[], description="Lista di errori di validazione")