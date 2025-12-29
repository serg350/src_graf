import React from "react";
import GraphListView from "../components/GraphListView";

export default function GraphListPage() {
  return (
    <div
      style={{
        height: "100vh",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div style={{ padding: 24 }}>
        <h2 style={{ margin: 0 }}>Все графы</h2>
      </div>

      {/* SCROLL AREA */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
        }}
      >
        <GraphListView
          onSelect={(id) => {
            window.location.href = `/graphs/${id}`;
          }}
        />
      </div>
    </div>
  );
}
