import { startExecution } from "../services/executionApi";
import { connectExecution } from "../services/executionSSE";
import { useExecutionStore } from "../utils/executionReducer";

export default function ExecutionController({ graphId, onStateEvent }) {
  const run = async () => {
    const { session_id } = await startExecution(graphId);

    connectExecution(session_id, (event) => {
      onStateEvent(event);
    });
  };

  return <button onClick={run}>Run</button>;
}