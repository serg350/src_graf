import { useParams } from "react-router-dom";
import { useState } from "react";

import GraphVisLabLayout from "../components/GraphVisLabLayout";
import GraphView from "../components/GraphView";
import ExecutionController from "./ExecutionController";

export default function GraphViewerPage() {
  const { id } = useParams();
  const [executionEvent, setExecutionEvent] = useState(null);
  const [graphMeta, setGraphMeta] = useState({
    isLoaded: false,
    name: "",
    executionInputSchema: { fields: [], prefilled_count: 0 },
    executionInputError: "",
  });

  return (
    <GraphVisLabLayout>
      <GraphView
        graphId={id}
        executionEvent={executionEvent}
        onGraphMeta={setGraphMeta}
        executionControls={
          <ExecutionController
            graphId={id}
            isGraphReady={graphMeta.isLoaded}
            graphName={graphMeta.name}
            executionInputSchema={graphMeta.executionInputSchema}
            executionInputError={graphMeta.executionInputError}
            onStateEvent={setExecutionEvent}
          />
        }
      />
    </GraphVisLabLayout>
  );
}
