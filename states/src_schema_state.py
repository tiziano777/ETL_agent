from typing import TypedDict, List, Dict, Any, Literal
from typing_extensions import Annotated
from pydantic import Field, BaseModel
from langchain_core.messages import ChatMessage


class State(BaseModel):
    samples: Dict[str, Any] = Field(default={}, description="Esempi di dati grezzi")

    chat_history: Annotated[List[ChatMessage], "Conversation"] = Field(default=[], description="Storico della conversazione con l'LLM")

    accept_schema_generation: Literal["continue","break","restart"] | None = Field(default=None, description="Decisione dell'utente sulla generazione dello schema")
    feedback: str | None = Field(default=None, description="Feedback dell'utente sulla generazione dello schema")

    valid: bool | None = Field(default=None, description="Flag di validità generale dello stato")
    
    generated_schema: Dict[str, Any] | None = Field(default=None, description="Schema dei dati")
