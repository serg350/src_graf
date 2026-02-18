import React from "react";

export default function HistoryPanel({ history }) {
  return (
    <div style={{ padding: 14 }}>
      <h3 style={{ margin: "0 0 10px 0" }}>История выполнения</h3>

      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {history.map((h) => (
          <div
            key={h.id}
            style={{
              padding: "8px 10px",
              background: "#f1f5f9",
              borderLeft: "4px solid #6366f1",
              borderRadius: 6,
              fontSize: 13,
            }}
          >
            {/* 🔹 Название состояния */}
            <div style={{ fontWeight: 600, marginBottom: 4 }}>
              {h.title}
            </div>

            {/* 🔸 Тип события + время (по желанию) */}
            {h.event && (
              <div style={{ fontSize: 11, color: "#64748b" }}>
                {h.event}
                {h.timestamp && (
                  <> · {new Date(h.timestamp * 1000).toLocaleTimeString()}</>
                )}
              </div>
            )}

            {/* 🔸 Данные состояния */}
            {h.payload && Object.keys(h.payload).length > 0 && (
              <pre
                style={{
                  marginTop: 6,
                  fontSize: 12,
                  background: "#e5e7eb",
                  padding: 6,
                  borderRadius: 4,
                  overflowX: "auto",
                }}
              >
                {JSON.stringify(h.payload, null, 2)}
              </pre>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
