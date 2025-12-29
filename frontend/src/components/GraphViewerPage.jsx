import { useParams } from "react-router-dom";
import GraphVisLabLayout from "../components/GraphVisLabLayout";
import GraphView from "../components/GraphView";

export default function GraphViewerPage() {
  const { id } = useParams();   // ← КЛЮЧЕВО

  return (
    <GraphVisLabLayout>
      <GraphView graphId={id} />
    </GraphVisLabLayout>
  );
}
