from langgraph.types import interrupt, Command
from typing import Literal
from states.src_schema_state import State
import json


class HumanReviewNode:

    def __init__(self):
        pass

    def __call__(self, state: State) -> Command[Literal["llm_node","validation_node"]]:
        """
        Human interrupt for schema review and manual correction, explicit approval/rejection.
        """
        print("Human review of the generated schema...")

        decision = interrupt({
            "assistant_output": state.chat_history[-1].content,
            "chat_history": state.chat_history,
            "instructions": "Decidi se accettare lo schema (break), fornire feedback (continue), o resettare la chat (restart)."
        })
        
        # decision = {"action":"break"}
        # oppure {"action":"continue","feedback":"..."}
        #  oppure {"action":"restart"}
        action = decision["action"]


        if action == "break":
            print("User accept the generated schema.")
            return Command(
                goto="validation_node",
                update={
                    "accept_schema_generation":"break"
                }
            )
        
        elif action == "continue":
            print("User provides feedback for schema improvement.")
            feedback_msg = str(decision["feedback"])
            return Command(
                goto="llm_node",
                update={
                    "accept_schema_generation": "continue",
                    "feedback": feedback_msg
                }
            )

        elif action == "restart":
            print("User requests to restart the schema generation process.")
            return Command(
                goto="llm_node",
                update={
                    "accept_schema_generation": "restart",
                    "chat_history": [],
                    "generated_schema": None,
                    "feedback": None
                }
            )
        else:
            raise ValueError("Invalid human decision")
