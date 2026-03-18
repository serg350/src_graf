import { Navigate, Route, Routes } from "react-router-dom";

import GraphImportPage from "./components/ImportGraphView";
import GraphListPage from "./components/GraphListPage";
import GraphViewerPage from "./components/GraphViewerPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/graphs" replace />} />
      <Route path="/graphs" element={<GraphListPage />} />
      <Route path="/graphs/import" element={<GraphImportPage />} />
      <Route path="/graphs/:id" element={<GraphViewerPage />} />
    </Routes>
  );
}
