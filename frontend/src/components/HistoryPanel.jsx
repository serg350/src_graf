import React from "react";

export default function HistoryPanel({ history }) {
  return (
    <div style={{ padding: 14 }}>
      <h3 style={{ margin: "0 0 10px 0" }}>История выполнения</h3>

      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {history.map((h, i) => (
          <div
            key={i}
            style={{
              padding: "8px 10px",
              background: "#f1f5f9",
              borderLeft: "4px solid #6366f1",
              borderRadius: 6,
              fontSize: 13,
            }}
          >
            <strong>{h.type}</strong>
            <br />
            {h.description}
          </div>
        ))}
      </div>
    </div>
  );
}
