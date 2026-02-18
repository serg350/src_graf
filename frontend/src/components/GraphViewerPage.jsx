import { useParams } from "react-router-dom";
import { useState } from "react";
import GraphVisLabLayout from "../components/GraphVisLabLayout";
import GraphView from "../components/GraphView";
import ExecutionController from "./ExecutionController";

export default function GraphViewerPage() {
  const { id } = useParams();

  const [executionEvent, setExecutionEvent] = useState(null);
  const [history, setHistory] = useState([]);

  const handleStateEvent = (event) => {
    setExecutionEvent(event);
    setHistory((prev) => [...prev, event]);
  };

  return (
    <GraphVisLabLayout>
      <ExecutionController
        graphId={id}
        onStateEvent={handleStateEvent}
      />

      <GraphView
        graphId={id}
        executionEvent={executionEvent}
        onHistoryAdd={handleStateEvent}
      />
    </GraphVisLabLayout>
  );
}
