from langgraph.constants import START, END
from langgraph.graph import StateGraph, State
from typing import TypedDict, Literal
from typing import Dict

builder = StateGraph(State)
builder.add_node(a)
builder.add_node(b)

def route(state: State) -> Literal["b", 'END']:
    if termination_condition(state):
        return END
    else:
        return "b"

builder.add_edge(START, "a")
builder.add_conditional_edges("a", route)
builder.add_edge("b", "a")
graph = builder.compile()