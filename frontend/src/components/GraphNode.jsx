import React from "react";
import { Handle, Position } from "reactflow";

const PHASE_STYLES = {
  active: {
    background: "#fff7ed",
    border: "#f97316",
    shadow: "0 0 0 3px rgba(249, 115, 22, 0.18)",
    label: "Выполняется",
  },
  waiting: {
    background: "#eff6ff",
    border: "#3b82f6",
    shadow: "0 0 0 3px rgba(59, 130, 246, 0.16)",
    label: "Ожидает",
  },
  completed: {
    background: "#ecfdf3",
    border: "#22c55e",
    shadow: "0 1px 4px rgba(22, 101, 52, 0.16)",
    label: "Завершено",
  },
  failed: {
    background: "#fff1f1",
    border: "#dc2626",
    shadow: "0 0 0 3px rgba(220, 38, 38, 0.16)",
    label: "Ошибка",
  },
};

export default function GraphNode({
  data,
  targetPosition = Position.Left,
  sourcePosition = Position.Right,
}) {
  const hasComment = Boolean(data.comment);
  const execution = data.execution || null;
  const phaseStyle = PHASE_STYLES[execution?.phase] || {
    background: "#ffffff",
    border: "#94a3b8",
    shadow: "0 1px 3px rgba(15, 23, 42, 0.1)",
    label: "",
  };

  return (
    <div
      title={data.comment || data.label}
      style={{
        width: "100%",
        height: "100%",
        minWidth: 196,
        minHeight: 72,
        padding: hasComment ? "9px 12px" : "14px 12px",
        background: phaseStyle.background,
        border: `2px solid ${phaseStyle.border}`,
        borderRadius: 8,
        color: "#0f172a",
        fontSize: 12,
        lineHeight: "15px",
        textAlign: "center",
        boxShadow: phaseStyle.shadow,
        position: "relative",
        transition: "background 160ms ease, border-color 160ms ease, box-shadow 160ms ease",
      }}
    >
      {execution?.phase ? (
        <span
          className="gv-node-status-dot"
          title={phaseStyle.label}
          style={{ background: phaseStyle.border }}
        />
      ) : null}
      {execution?.visits > 1 ? (
        <span className="gv-node-visit-count">×{execution.visits}</span>
      ) : null}
      <div style={{ fontWeight: 600 }}>{data.label}</div>
      {hasComment ? (
        <div
          style={{
            marginTop: 3,
            color: "#64748b",
            fontSize: 10,
            overflow: "hidden",
            display: "-webkit-box",
            WebkitBoxOrient: "vertical",
            WebkitLineClamp: 2,
          }}
        >
          {data.comment}
        </div>
      ) : null}
      {execution?.phase === "waiting" && execution.requiredInputs ? (
        <div className="gv-node-wait-count">
          {execution.activeInputs}/{execution.requiredInputs} входов
        </div>
      ) : null}

      <Handle type="target" position={targetPosition} id="in" />
      <Handle type="source" position={sourcePosition} id="out" />
    </div>
  );
}
