from gpt_client import gpt_client as client
from typing_extensions import override
from openai import AssistantEventHandler
from filter_property import assistant_filter_properties
import json


ASSISTANT_CONFIG = {
    "instructions": (
        "You are a helpful real estate assistant. "
        "The user will describe their housing requirements in natural language. "
        "Your job is to parse the requirements, confirm them with the user, and then call the function. "
        "Call the function `assistant_filter_properties` ONLY AFTER the user confirms. "
        "Please print the parameters you passed to the function."
    ),
    "model": "gpt-4o",  # 假设使用 gpt-4o 或最新支持function call并行的模型
    "tools": [
        {
            "type": "function",
            "function": {
                "name": "assistant_filter_properties",
                "description": "Filter properties based on user requirements. Return top matches.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "user_requirements": {
                            "type": "object",
                            "description": "User's structured housing requirements, e.g. rent range, location, etc.",
                            "properties": {
                                "rent_range": {
                                    "type": "array",
                                    "items": {"type": "number"},
                                    "description": "Min and max rent, e.g. [800, 1100]."
                                },
                                "location": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Desired city or district names, e.g. ['Charlottenburg']"
                                },
                                "min_area": {
                                    "type": "number",
                                    "description": "Minimum area required (in sqm)."
                                },
                                "max_area": {
                                    "type": "number",
                                    "description": "Maximum area allowed (in sqm)."
                                },
                                "rooms": {
                                    "type": "number",
                                    "description": "Number of rooms required."
                                }
                            },
                            "required": [],
                            "additionalProperties": True
                        },
                        "file_path": {
                            "type": "string",
                            "description": "JSON file path containing property data.",
                            "default": "property_analysis_results3.json"
                        }
                    },
                    "required": ["user_requirements"],
                    "additionalProperties": False
                },
                "strict": False  # 使用Structured Outputs严格模式
            }
        }
    ]
}


def create_assistant_and_thread():
    """创建一个Assistant对象，并创建一个Thread。"""
    assistant_obj = client.beta.assistants.create(**ASSISTANT_CONFIG)
    thread_obj = client.beta.threads.create()
    return assistant_obj, thread_obj


assistant, thread = create_assistant_and_thread()


# 流式传输
class EventHandler(AssistantEventHandler):
    def __init__(self, client, thread_id, debug):
        super().__init__()
        self.client = client  # 明确注入client
        self.thread_id = thread_id
        self.debug = debug

    @override
    def on_text_created(self, text) -> None:
        print(f"\nassistant > ", end="", flush=True)

    @override
    def on_text_delta(self, delta, snapshot):
        print(delta.value, end="", flush=True)

    def on_tool_call_created(self, tool_call):
        print(f"\nassistant > {tool_call.type}\n", flush=True)

    def on_tool_call_delta(self, delta, snapshot):
        if delta.type == 'code_interpreter':
            if delta.code_interpreter.input:
                print(delta.code_interpreter.input, end="", flush=True)
            if delta.code_interpreter.outputs:
                print(f"\n\noutput >", flush=True)
                for output in delta.code_interpreter.outputs:
                    if output.type == "logs":
                        print(f"\n{output.logs}", flush=True)

    def on_event(self, event):
        # 只有在debug模式下才打印调试信息
        def dprint(*args, **kwargs):
            if self.debug:
                print(*args, **kwargs)

        # 调试信息
        dprint(f"on_event triggered with event: {event.event}")
        dprint("event.data type:", type(event.data))
        dprint("event.data dir:", dir(event.data))

        # 当Assistant请求调用函数(工具)时，会触发requires_action事件
        if event.event == 'thread.run.requires_action':
            # 尝试使用 to_dict() 将 Run 对象转换成字典
            run_data = event.data.to_dict()
            # run_data 现在应该是一个可通过 keys 访问的字典
            dprint("[Debug] run_data dict:", run_data)

            # 检查 required_action 信息
            if 'required_action' in run_data and 'submit_tool_outputs' in run_data['required_action']:
                tool_calls = run_data['required_action']['submit_tool_outputs'].get('tool_calls', [])
                tool_outputs = []

                for tc in tool_calls:
                    fn_name = tc['function']['name']
                    fn_args_str = tc['function']['arguments']
                    dprint(f"[Debug] Function to call: {fn_name}, arguments: {fn_args_str}")

                    try:
                        fn_args = json.loads(fn_args_str)
                    except json.JSONDecodeError as e:
                        dprint("JSON解析参数失败:", e)
                        continue

                    tool_call_id = tc['id']

                    if fn_name == 'assistant_filter_properties':
                        try:
                            result = assistant_filter_properties(**fn_args)
                            output_str = json.dumps(result, ensure_ascii=False)
                            tool_outputs.append({
                                'tool_call_id': tool_call_id,
                                'output': output_str
                            })
                            dprint("Successfully got result from assistant_filter_properties")
                        except Exception as e:
                            dprint("Error executing assistant_filter_properties:", e)
                            # 这里可以决定是否提交一个错误信息给assistant

                if tool_outputs:
                    try:
                        run_id = run_data.get('id', None)
                        if run_id is None:
                            run_id = event.data.id  # 也可从对象直接拿id属性
                        dprint(f"Submitting tool_outputs for run_id: {run_id}")
                        self.client.beta.threads.runs.submit_tool_outputs_stream(
                            thread_id=self.thread_id,
                            run_id=run_id,
                            tool_outputs=tool_outputs,
                            event_handler=self
                        )
                    except Exception as e:
                        dprint("Error submitting tool outputs:", e)
            else:
                dprint("[Debug] No submit_tool_outputs found in required_action or required_action not in run_data.")


# Then, we use the `stream` SDK helper
# with the `EventHandler` class to create the Run
# and stream the response.

def send_message(client, thread_id, assistant_obj, user_message):
    # 1) 创建用户消息
    client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=user_message
    )

    # 2) 创建并流式Run
    handler = EventHandler(client, thread_id, False)  # 传入client和thread_id
    with client.beta.threads.runs.stream(
            thread_id=thread_id,
            assistant_id=assistant_obj.id,
            event_handler=handler  # <-- 使用我们自定义的EventHandler
    ) as stream:
        stream.until_done()


while True:
    user_input = input("\nuser > ")
    if user_input.lower() == "exit":
        break
    send_message(client, thread.id, assistant, user_input)
