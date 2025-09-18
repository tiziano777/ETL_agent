import streamlit as st
import uuid
import json
import os
import traceback
import yaml
import ast

from langgraph.types import Command
from langchain_google_genai import ChatGoogleGenerativeAI

from pipelines.source_schema_pipeline import create_pipeline

from states.src_schema_state import State

from nodes.src_schema_nodes.schema_node import SchemaNode
from nodes.src_schema_nodes.human_review_node import HumanReviewNode
from nodes.src_schema_nodes.validation_node import ValidationNode

from langfuse import Langfuse
from langfuse.langchain import CallbackHandler

import dotenv
dotenv.load_dotenv()
BASE_PATH = os.getenv("BASE_PATH", "")

langfuse = Langfuse( 
    public_key= os.environ.get('LANGFUSE_PUBLIC_KEY'),
    secret_key= os.environ.get('LANGFUSE_PRIVATE_KEY'), 
    host= os.environ.get('LANGFUSE_STRING_CONNECTION')
)
langfuse_handler = CallbackHandler()

MODEL_CONFIG = "./config/gemini2.0-flash.yml"
PROMPTS_PATH = "./config/prompts.yml"

with open(MODEL_CONFIG, "r", encoding="utf-8") as f:
    llmConfig = yaml.safe_load(f)
    api_key = os.environ.get("GEMINI_API_KEY")
    llmConfig["gemini_api_key"] = api_key
with open(PROMPTS_PATH, "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)

geminiLLM = ChatGoogleGenerativeAI(
    model = llmConfig["model_name"],
    google_api_key = llmConfig["gemini_api_key"],
    temperature = llmConfig["temperature"],
    max_output_tokens = llmConfig["max_output_tokens"],
    top_p = llmConfig["top_p"],
    top_k = llmConfig.get("top_k", None),
)

# Pipeline creata una sola volta
if "graph" not in st.session_state:
    st.session_state.graph = create_pipeline(
        llm_node=SchemaNode(llm=geminiLLM, prompt=prompts["source_schema_extraction_prompt"]),
        human_node=HumanReviewNode(),
        validation_node=ValidationNode()
    )

def main():
    if "thread_id" not in st.session_state:
        st.session_state.thread_id = str(uuid.uuid4())

    config = {
        "configurable": {"thread_id": st.session_state.thread_id},
        "callbacks": [langfuse_handler]
    }

    st.title("Step-by-step Dataset Schema Creation")

    st.sample_input = '''{"prompt": "Se 'caldo' sta a 'freddo' come 'giorno' sta a 'notte', a cosa sta 'alto' in relazione a 'basso'? Analizza la relazione di opposti.", "response": "Alto sta a basso, perché alto e basso sono opposti, così come caldo e freddo o giorno e notte."}'''

    if "pipeline_started" not in st.session_state:
        st.session_state.pipeline_started = False

    if not st.session_state.pipeline_started:
        if st.button("Avvia pipeline"):
            try:
                samples = json.loads(st.sample_input)
            except Exception as e:
                st.error(f"Errore parsing JSON: {e}")
                st.stop()

            init_state = State(
                samples=samples,
                accept_schema_generation=None,
                valid=None,
                generated_schema=None,
                feedback=None
            )

            result = st.session_state.graph.invoke(init_state, config=config)
            st.session_state.interrupt = result["__interrupt__"]
            st.session_state.state = result
            st.session_state.pipeline_started = True

    if "interrupt" in st.session_state:
        interrupt = st.session_state.interrupt

        # Normalizza: se è lista → prendi il primo elemento
        if isinstance(interrupt, list):
            interrupt = interrupt[0]

        st.subheader("Ultima risposta del modello")

        schema_str = interrupt.value.get("assistant_output", "{}")
        # Only try to parse if schema_str looks like JSON or dict
        if schema_str and (schema_str.strip().startswith('{') or schema_str.strip().startswith('[')):
            try:
                schema_dict = json.loads(schema_str)
                st.json(schema_dict)
            except Exception:
                try:
                    schema_dict = ast.literal_eval(schema_str)
                    st.json(schema_dict)
                except Exception:
                    print(traceback.format_exc())
                    st.write(schema_str)
        else:
            st.write(schema_str)

        st.subheader("📊 Samples forniti")
        for msg in interrupt.value.get("chat_history", []):
            if getattr(msg, "role", None) == "user":  # controlla che sia ChatMessage
                st.code(msg.content, language="json")

        st.subheader("📝 Istruzioni")
        st.write(interrupt.value.get("instructions", ""))

        st.subheader("Decisione umana")
        action = st.radio("Scegli azione:", ["break","continue","restart"])

        feedback_text = ""
        if action == "continue":
            feedback_text = st.text_area("Inserisci feedback testuale")

        if st.button("Invia decisione"):
            decision = {"action": action}
            if action == "continue":
                decision["feedback"] = feedback_text

            result2 = st.session_state.graph.invoke(Command(resume=decision), config=config)
            if "__interrupt__" in result2:
                st.session_state.interrupt = result2["__interrupt__"]
                st.session_state.state = result2
                st.rerun()
            else:
                st.success("Pipeline completata ✅")
                st.json(result2)

if __name__ == "__main__":
    main()
