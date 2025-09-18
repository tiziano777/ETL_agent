from typing import TypedDict, List, Dict, Any, Literal
from typing_extensions import Annotated
from pydantic import Field, BaseModel
from langchain_core.messages import BaseMessage


class State(BaseModel):
    samples: List[Dict[str, Any]] = Field(default=[], description="Esempi di dati grezzi")

    chat_history: Annotated[List[BaseMessage], "Conversation"] = Field(default=[], description="Storico della conversazione con l'LLM")

    accept_schema_generation: Literal["continue", "break", "restart", "manual"] | None = Field(default=None, description="Decisione dell'utente sulla generazione dello schema")
    feedback: str | None = Field(default=None, description="Feedback dell'utente sulla generazione dello schema")

    valid: bool | None = Field(default=None, description="Flag di validità generale dello stato")
    
    generated_schema: Dict[str, Any] | None = Field(default=None, description="Schema dei dati")

    output_path: str | None = Field(default='', description="Percorso di output per il salvataggio dello schema")

    error_messagess: List[str] = Field(default=[], description="Lista di errori di validazione")