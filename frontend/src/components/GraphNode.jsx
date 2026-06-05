import React from "react";
import { Handle, Position } from "reactflow";

export default function GraphNode({ data }) {
  const hasComment = Boolean(data.comment);

  return (
    <div
      title={data.comment || data.label}
      style={{
        minWidth: 150,
        maxWidth: 220,
        padding: hasComment ? "8px 10px" : "9px 12px",
        background: "#ffffff",
        border: "1px solid #94a3b8",
        borderRadius: 8,
        color: "#0f172a",
        fontSize: 12,
        lineHeight: "15px",
        textAlign: "center",
        boxShadow: "0 1px 3px rgba(15, 23, 42, 0.1)",
      }}
    >
      <div style={{ fontWeight: 600 }}>{data.label}</div>
      {hasComment ? (
        <div
          style={{
            marginTop: 3,
            color: "#64748b",
            fontSize: 10,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {data.comment}
        </div>
      ) : null}

      <Handle type="target" position={Position.Left} id="in" />
      <Handle type="source" position={Position.Right} id="out" />
    </div>
  );
}
