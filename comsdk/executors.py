from comsdk.remote_cpp.executor import execute_remote_cpp


def build_executor_function(executor, operation, input_key=None, output_key=None):
    executor_name = str(executor or "").strip()
    operation_name = str(operation or "").strip()

    if not operation_name:
        raise ValueError(f"Executor '{executor_name}' requires an operation")

    if executor_name != "remote_cpp":
        raise ValueError(f"Unsupported executor: {executor_name}")

    resolved_input_key = str(input_key or "x").strip()
    resolved_output_key = str(output_key or f"{operation_name}_result").strip()

    def execute(data):
        return execute_remote_cpp(
            data,
            operation=operation_name,
            input_key=resolved_input_key,
            output_key=resolved_output_key,
        )

    execute.__name__ = f"{executor_name}_{operation_name}"
    execute._comsdk_executor_spec = {
        "executor_type": executor_name,
        "operation": operation_name,
        "input_key": resolved_input_key,
        "output_key": resolved_output_key,
    }
    return execute
