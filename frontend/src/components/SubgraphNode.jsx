import React from "react";
import { Handle, Position } from "reactflow";
import { FolderTree } from "lucide-react";

export default function SubgraphNode({
  data,
  targetPosition = Position.Left,
  sourcePosition = Position.Right,
}) {
  const phase = data.execution?.phase;
  const isSelected = Boolean(data.selected);
  const isRelatedSelected = Boolean(data.relatedSelected);
  const border =
    isSelected
      ? "#2563eb"
      : phase === "failed"
      ? "#dc2626"
      : phase === "active"
        ? "#f97316"
        : phase === "waiting"
          ? "#3b82f6"
          : phase === "completed"
            ? "#22c55e"
            : "#d97706";
  const background =
    phase === "failed"
      ? "#fff1f1"
      : phase === "active"
        ? "#fff7ed"
        : phase === "waiting"
          ? "#eff6ff"
          : phase === "completed"
            ? "#ecfdf3"
            : "#fff4e5";

  return (
    <div
      title={data.comment || data.label}
      style={{
        width: "100%",
        height: "100%",
        minWidth: 166,
        minHeight: 52,
        padding: data.comment ? "8px 10px" : "12px",
        background,
        border: `2px solid ${border}`,
        borderRadius: 8,
        fontSize: 12,
        fontWeight: 600,
        display: "flex",
        alignItems: "center",
        gap: 8,
        cursor: "pointer",
        position: "relative",
        boxShadow: isSelected
          ? "0 0 0 4px rgba(37, 99, 235, 0.18), 0 8px 18px rgba(15, 23, 42, 0.14)"
          : isRelatedSelected
            ? "0 0 0 3px rgba(37, 99, 235, 0.12), 0 1px 4px rgba(15, 23, 42, 0.12)"
            : phase
              ? `0 0 0 3px ${border}24`
              : "0 1px 3px rgba(15, 23, 42, 0.1)",
        transition: "background 160ms ease, border-color 160ms ease, box-shadow 160ms ease",
      }}
      onClick={(e) => {
        e.stopPropagation();
        data.onSelectNode?.();
        data.onOpenSubgraph?.(data.subgraphId);
      }}
    >
      <FolderTree size={16} color="#b45309" />
      <span>
        {data.label}
        {data.comment ? (
          <span
            style={{
              display: "block",
              color: "#92400e",
              fontSize: 10,
              fontWeight: 500,
              marginTop: 2,
              maxWidth: 180,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {data.comment}
          </span>
        ) : null}
      </span>

      {data.execution?.visits > 1 ? (
        <span className="gv-node-visit-count">×{data.execution.visits}</span>
      ) : null}

      <Handle type="target" position={targetPosition} id="in" style={{ background: border }} />
      <Handle type="source" position={sourcePosition} id="out" style={{ background: border }} />
    </div>
  );
}
