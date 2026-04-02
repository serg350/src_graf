import { useNavigate } from "react-router-dom";

import GraphListView from "../components/GraphListView";
import GraphVisLabLayout from "../components/GraphVisLabLayout";

export default function GraphListPage() {
  const navigate = useNavigate();

  return (
    <GraphVisLabLayout>
      <GraphListView onSelect={(id) => navigate(`/graphs/${id}`)} />
    </GraphVisLabLayout>
  );
}
