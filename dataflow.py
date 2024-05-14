from dataclasses import dataclass, field
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import run_main

import json

def deserialize(data):
    try:
        return [json.loads(data)]  # Ensure output is iterable
    except json.JSONDecodeError:
        # Log error and return an empty list to signify a failed parse
        print(f"Failed to decode JSON: {data}")
        return []
    except ValueError:
        # Catch non-JSON lines and ignore them
        return []

def fixed_deserialize(s):
    if s.startswith("FAIL"):  # Fix the bug.
        return []
    else:
        return [json.loads(s)]
    
def safe_key_off_user_id(event):
    if event and isinstance(event, dict) and "user_id" in event:
        return (event["user_id"], event)
    return None 


def build_state():
    return {"unpaid_order_ids": [], "paid_order_ids": []}

def joiner(state, event):
    if state is None:
        state = build_state()

    e_type = event.get("type")
    order_id = event.get("order_id")

    print(e_type, order_id)

    if e_type == "order":
        if order_id not in state["unpaid_order_ids"]:
            state["unpaid_order_ids"].append(order_id)
    elif e_type == "payment":
        if order_id in state["unpaid_order_ids"]:
            state["unpaid_order_ids"].remove(order_id)
            state["paid_order_ids"].append(order_id)
        else:
            print(f"No matching unpaid order found for payment ID: {order_id}")

    return state, event


def inspect_final_state(user_id, state_event):
    print(f"Final state for user {user_id}: {state_event}")


def format_output(user_id__joined_state):
    user_id, joined_state = user_id__joined_state
    paid_order_ids = joined_state.get("paid_order_ids", [])
    unpaid_order_ids = joined_state.get("unpaid_order_ids", [])
    return {
        "user_id": user_id,
        "paid_order_ids": paid_order_ids,
        "unpaid_order_ids": unpaid_order_ids,
    }


flow = Dataflow("shopping-cart-joiner")
input_data = op.input("input", flow, FileSource("data/cart-join.json"))
deserialize_data = op.flat_map("deserialize", input_data, deserialize)
get_user_id = op.map("get_user_id", deserialize_data, safe_key_off_user_id)
join_data = op.stateful_map("joiner", get_user_id, joiner)

formatted_data = op.map("format_output", join_data, format_output)

output = op.output("output", formatted_data, StdOutSink())






