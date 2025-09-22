from langgraph.types import interrupt, Command
from typing import Literal
from states.mapping_schema_state import State
import json

class HumanReviewNode:
    def __init__(self):
        pass

    def __call__(self,state: State) -> Command[Literal["llm_node","validation_node"]]:
        """
        Human interrupt for mapping review and manual correction, explicit approval/rejection.
        """
        print("Human review of the generated mapping...")
        

        decision = interrupt({
            "assistant_output": state.chat_history[-1].content,
            "chat_history": state.chat_history,
            "instructions": "Decidi se accettare il mapping (break), fornire feedback (continue), resettare la chat (restart) oppure modificarla manualmente."
        })
        
        # decision = {"action":"break"}
        # oppure {"action":"continue","feedback":"suggested corrections"}
        # oppure {"action":"restart"}
        # oppure {"action":"manual","feedback":"final JSON schema"}
        action = decision["action"]

        if action == "break":
            print("User accept the generated mapping.")
            return Command(
                goto="validation_node",
                update={
                    "accept_mapping_generation":"break",
                    "mapping": json.loads(state.chat_history[-1].content)
                }
            )
        
        elif action == "continue":
            print("User provides feedback for mapping improvement.")
            feedback_msg = str(decision["feedback"])
            return Command(
                goto="llm_node",
                update={
                    "accept_mapping_generation": "continue",
                    "feedback": feedback_msg
                }
            )

        elif action == "restart":
            print("User requests to restart the mapping generation process.")
            return Command(
                goto="llm_node",
                update={
                    "accept_mapping_generation": "restart",
                    "chat_history": [],
                    "mapping": None,
                    "feedback": None
                }
            )
        
        elif action == "manual":
            print("User provides a manual mapping.")
            manual_mapping = json.loads(decision["feedback"])
            try:
                
                return Command(
                    goto="validation_node",
                    update={
                        "accept_mapping_generation": "manual",
                        "mapping": manual_mapping
                    }
                )
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON mapping provided by the user.")
            
        else:
            raise ValueError("Invalid human decision")