import React, { useEffect, useState } from "react";
import { loadAllGraphs } from "../services/api";

export default function GraphListView({ onSelect }) {
  const [graphs, setGraphs] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadAllGraphs().then((data) => {
      setGraphs(data);
      setLoading(false);
    });
  }, []);

  if (loading) {
    return <div style={{ padding: 20 }}>Загрузка графов...</div>;
  }

  return (
    <div
      style={{
        padding: 20,
        height: "100%",        // ← важно
        boxSizing: "border-box",
      }}
    >
      <h3 style={{ marginBottom: 12 }}>Все графы</h3>

      <div style={{ display: "grid", gap: 10 }}>
        {graphs.map((g) => (
          <div
            key={g.id}
            onClick={() => onSelect(g.id)}
            style={{
              padding: "12px 14px",
              borderRadius: 10,
              border: "1px solid #e5e7eb",
              cursor: "pointer",
              background: "#fff",
              transition: "all 0.15s",
            }}
            onMouseEnter={(e) =>
              (e.currentTarget.style.background = "#f8fafc")
            }
            onMouseLeave={(e) =>
              (e.currentTarget.style.background = "#fff")
            }
          >
            <div style={{ fontWeight: 600 }}>{g.name}</div>
            <div style={{ fontSize: 12, color: "#64748b" }}>
              nodes: {g.nodes_count ?? "—"} | edges: {g.edges_count ?? "—"}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
