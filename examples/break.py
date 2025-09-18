from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from IPython.display import Image, display

class State(TypedDict):
    user_input: str
    response: str
    user_approval: str

def step_1(state):
    print("---Step 1---")
    if "hello" in state["user_input"].lower():
        state["response"] = "Hello! How can I assist you today?"
    else:
        state["response"] = "I'm not sure how to respond to that."
    return state

def step_2(state):
    print("---Step 2---")
    print(state)
    pass

def step_3(state):
    print("---Step 3---")
    if state["user_approval"] == "yes":
        print("Since you approved, I will show the response.")
        return state
    else:
        return {"response":"No response to show."}
    
builder = StateGraph(State)

builder.add_node("step_1", step_1)
builder.add_node("step_2", step_2)
builder.add_node("step_3", step_3)

builder.add_edge(START, "step_1")
builder.add_edge("step_1", "step_2")
builder.add_edge("step_2", "step_3")
builder.add_edge("step_3", END)

# Set up memory
memory = MemorySaver()
# Add
graph = builder.compile(checkpointer=memory, interrupt_before=["step_3"])

# Input
initial_input = {"user_input": "hello world"}

# Thread
thread = {"configurable": {"thread_id": "1"}}

# Run the graph until the first interruption
for event in graph.stream(initial_input, thread, stream_mode="values"):
    print(event)

print("Breakpoint reached. Execution paused.")
print('----------------------------------------------------------------')
#print('\n')
user_approval = input("Do you want to execute step 3? (yes/no): ")

snapshot=graph.get_state(thread)
snapshot.values['user_approval']=user_approval
graph.update_state(thread, snapshot.values, as_node="step_2")

#print(snapshot.values)

for event in graph.stream(None, thread, stream_mode="values"):
    print(event)