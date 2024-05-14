# Recoverable Shopping Cart Join Application

- Skill level
    
    **Intermediate, Some prior knowledge required**
    
- Time to complete
    
    **Approx. 15 min**
    
### Introduction

In this example, we're going to build a small online order fulfillment system. It will join two events within a stream: one event type containing customer orders and another containing successful payments. The dataflow will emit completed orders for each customer that have been paid. It will also handle a failure event without crashing.

**Sample Data**

Make a file named `data/cart-join.json` with the following data:

```json
{"user_id": "a", "type": "order", "order_id": 1}
{"user_id": "a", "type": "order", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 3}
{"user_id": "a", "type": "payment", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 4}
FAIL HERE
{"user_id": "a", "type": "payment", "order_id": 1}
{"user_id": "b", "type": "payment", "order_id": 4}
```

**Python modules**
bytewax==0.19.*

## Your Takeaway

*Your takeaway from this tutorial will be a streaming application that aggregates shoppers data into a completed shopping cart.*


## Resources

[GitHub Repo](https://github.com/bytewax/recoverable-cart-join)

## Imports

First, let's discuss the necessary imports and setup for our application:

```python
import json
from dataclasses import dataclass, field
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSource
```

We import necessary modules like json for parsing data, and Bytewax-specific components for building the dataflow.

## Deserialization Function

To ensure the data integrity and usability, we first define a function to safely deserialize incoming JSON data:

```python
def safe_deserialize(data):
    try:
        event = json.loads(data)
        if "user_id" in event and "type" in event and "order_id" in event:
            return (event['user_id'], event)
    except json.JSONDecodeError:
        pass
    print(f"Skipping invalid data: {data}")
    return None
```

This function attempts to parse JSON data and checks for the necessary keys before returning them as a tuple. If parsing fails, it returns None and prints a warning.

## State Management Class
We use a dataclass to manage the state of each user's shopping cart:

```python
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
```

This class keeps track of unpaid and paid orders. The update method modifies the state based on the type of event, and the summarize method generates a summary of the state.

In the context of the `ShoppingCartState` class, an event refers to a single transaction or action taken by a user regarding their shopping cart. In the Bytewax stream processing setup, events are typically represented as dictionary objects derived from JSON data. Each event contains several key-value pairs that detail the action:

* `order_id`: This is a unique identifier for an order. It is crucial for tracking the status of each order as it moves from unpaid to paid.
* `type`: This specifies the nature of the event. It can either be "order", indicating the creation of a new order, or "payment", indicating the completion of a payment for an existing order.
* `user_id` and other possible fields that help in further processing or analysis.

### Event Handling in update Method
The update method in ShoppingCartState handles these events:

* If the event type is "order", it adds the order to the `unpaid_order_ids` dictionary with the `order_id` as the key. This way, the order can be easily retrieved and updated upon receiving a payment.
* If the event type is "payment", it checks if the order_id exists in the `unpaid_order_ids`. If it does, it moves the order to the paid_order_ids list, marking it as paid.
* The method effectively transitions orders based on their payment status, keeping the state of each cart up-to-date.

## Stateful Data Processing

Next, we define a state_manager function to manage state transitions and output data summaries:

```python
def state_manager(state, value):
    if state is None:
        state = ShoppingCartState()
    state.update(value)
    return state, state.summarize()
```

This function initializes state if not already present, updates the state based on the input, and returns a summarized state.

The `state_manager` function is crucial for managing and maintaining the state throughout the lifecycle of the dataflow in a Bytewax application. It deals with state objects and the values processed in each step of the dataflow:

* State Initialization: If there is no existing state (state is None), it initializes a new ShoppingCartState. This step is crucial for new users or sessions where previous state data is not available.

* State Update: It calls state.update(value), where value is the event tuple (user_id, event) extracted and filtered from the incoming data. The tuple format is particularly useful here because it bundles related data (user and their event) together, ensuring that all necessary information for state updates is passed as a single unit.

* State and Summary Return: The function returns a tuple state, state.sumarize(). The first element is the updated state object, useful for further stateful operations within the dataflow if needed. The second element, state.summarize(), provides a snapshot of the current state, specifically listing all paid and unpaid order IDs. 

This summarization is useful for output or further processing to analyze shopping cart behaviors.

Returning a tuple with both the state and its summary allows Bytewax to manage continuity and data integrity effectively. It ensures that each step of the dataflow can access both the updated state for further processing and a human-readable summary for outputs or checkpoints, aligning with typical stream processing needs where both real-time processing and summarization are crucial.

## Setting Up the Dataflow
The dataflow processes input from a file and applies transformations and stateful operations:

```python
flow = Dataflow("shopping-cart-joiner")
input_data = op.input("input", flow, FileSource("data/cart-join.json"))
deserialize_data = op.map("deserialize", input_data, safe_deserialize)
filter_valid = op.filter("filter_valid", deserialize_data, lambda x: x is not None)
joined_data = op.stateful_map("joiner", filter_valid, state_manager)
```
Here, we define the steps of the dataflow:

* Input is read from a JSON file.
* Map applies safe_deserialize.
* Filter removes any None results.
* Stateful Map applies state_manager to manage and summarize state.

### Formatting and Output
Finally, we add a formatting step to prepare the output and define the final output operation:

```python
formatted_output = op.map("format_output", joined_data, lambda x: f"Final summary for user {x[0]}: {x[1]}")
op.output("output", formatted_output, StdOutSink())
```

##  Execution


At this point our dataflow is constructed, and we can run it. Here we're setting our current directory as the path for our SQLite recovery store, and setting our epoch interval to 0, so that we can create a checkpoint of our state for every line in the file:

``` bash
> python -m bytewax.run dataflow 

Skipping invalid data: FAIL HERE
Final summary for user a: {'paid_order_ids': [], 'unpaid_order_ids': [1]}
Final summary for user a: {'paid_order_ids': [], 'unpaid_order_ids': [1, 2]}
Final summary for user b: {'paid_order_ids': [], 'unpaid_order_ids': [3]}
Final summary for user a: {'paid_order_ids': [2], 'unpaid_order_ids': [1]}
Final summary for user b: {'paid_order_ids': [], 'unpaid_order_ids': [3, 4]}
Final summary for user a: {'paid_order_ids': [2, 1], 'unpaid_order_ids': []}
Final summary for user b: {'paid_order_ids': [4], 'unpaid_order_ids': [3]}
```

The output shows the final summary for each user, including paid and unpaid order IDs. It was also able to handle the invalid data line without crashing.

## Summary

Recoverable dataflows are key to any production system. This tutorial demonstrated how you can use `stateful_map` to join two event types together from a stream of incoming data.

## We want to hear from you!

If you have any trouble with the process or have ideas about how to improve this document, come talk to us in the #troubleshooting Slack channel!

## Where to next?

See our full gallery of tutorials → 

[Share your tutorial progress!](https://twitter.com/intent/tweet?text=I%27m%20mastering%20data%20streaming%20with%20%40bytewax!%20&url=https://bytewax.io/tutorials/&hashtags=Bytewax,Tutorials)
