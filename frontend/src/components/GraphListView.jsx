import { useEffect, useState } from "react";

import { loadAllGraphs } from "../services/api";

function formatDateTime(value) {
  if (!value) {
    return "—";
  }

  return new Date(value).toLocaleString("ru-RU");
}

export default function GraphListView({ onSelect }) {
  const [graphs, setGraphs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let isActive = true;

    loadAllGraphs()
      .then((data) => {
        if (!isActive) {
          return;
        }

        setGraphs(data);
      })
      .catch((nextError) => {
        if (!isActive) {
          return;
        }

        setError(nextError.message || "Не удалось загрузить графы");
      })
      .finally(() => {
        if (isActive) {
          setLoading(false);
        }
      });

    return () => {
      isActive = false;
    };
  }, []);

  return (
    <div style={{ padding: 20, display: "grid", gap: 16 }}>
      <div
        style={{
          display: "grid",
          gap: 6,
          padding: 20,
          borderRadius: 18,
          background: "linear-gradient(180deg, #fbfdff 0%, #f3f8fc 100%)",
          border: "1px solid #d7e1ec",
        }}
      >
        <div style={{ fontSize: 24, fontWeight: 800, color: "#274c6a" }}>Все графы</div>
        <div style={{ fontSize: 14, color: "#5f7184" }}>
          Каталог импортированных графов и накопленная история запусков.
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 6 }}>
          <span className="gv-chip">Всего: {loading ? "…" : graphs.length}</span>
        </div>
      </div>

      {loading ? (
        <div className="gv-empty-card">Загрузка графов...</div>
      ) : null}

      {error ? <div className="gv-error-card">{error}</div> : null}

      {!loading && !error ? (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
            gap: 14,
          }}
        >
          {graphs.map((graph) => (
            <button
              key={graph.id}
              type="button"
              onClick={() => onSelect(graph.id)}
              style={{
                border: "1px solid #d7e1ec",
                borderRadius: 18,
                background: "#ffffff",
                padding: 18,
                textAlign: "left",
                display: "grid",
                gap: 12,
                boxShadow: "0 10px 24px rgba(15, 23, 42, 0.05)",
              }}
            >
              <div style={{ display: "grid", gap: 6 }}>
                <div style={{ fontSize: 17, fontWeight: 700, color: "#1f2937" }}>
                  {graph.name}
                </div>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                  <span className="gv-chip">
                    {graph.is_subgraph ? "Подграф" : "Основной граф"}
                  </span>
                  <span className="gv-chip">Узлов: {graph.nodes_count ?? "—"}</span>
                  <span className="gv-chip">Переходов: {graph.edges_count ?? "—"}</span>
                </div>
              </div>

              <div style={{ display: "grid", gap: 4, fontSize: 13, color: "#5f7184" }}>
                <div>Запусков: {graph.execution_count ?? 0}</div>
                <div>Последний запуск: {formatDateTime(graph.last_execution_at)}</div>
              </div>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
