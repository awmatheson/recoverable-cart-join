import json
from dataclasses import dataclass, field
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSource
from bytewax.testing import run_main

def safe_deserialize(data):
    try:
        event = json.loads(data)
        if "user_id" in event and "type" in event and "order_id" in event:
            return (event['user_id'], event)  # Return as (key, value) pair
    except json.JSONDecodeError:
        pass
    print(f"Skipping invalid data: {data}")
    return None

@dataclass
class ShoppingCartState:
    unpaid_order_ids: dict = field(default_factory=dict)
    paid_order_ids: list = field(default_factory=list)

    def update(self, event):
        order_id = event["order_id"]
        if event["type"] == "order":
            self.unpaid_order_ids[order_id] = event
        elif event["type"] == "payment":
            if order_id in self.unpaid_order_ids:
                self.paid_order_ids.append(self.unpaid_order_ids.pop(order_id))

    def summarize(self):
        return {
            "paid_order_ids": [order["order_id"] for order in self.paid_order_ids],
            "unpaid_order_ids": list(self.unpaid_order_ids.keys())
        }

def state_manager(state, value):
    if state is None:
        state = ShoppingCartState()  
    state.update(value)
    return state, state.summarize()  


flow = Dataflow("shopping-cart-joiner")
input_data = op.input("input", flow, FileSource("data/cart-join.json"))
deserialize_data = op.map("deserialize", input_data, safe_deserialize)
filter_valid = op.filter("filter_valid", deserialize_data, lambda x: x is not None)
joined_data = op.stateful_map("joiner", filter_valid, state_manager)

formatted_output = op.map("format_output", joined_data, lambda x: f"Final summary for user {x[0]}: {x[1]}")
op.output("output", formatted_output, StdOutSink())




