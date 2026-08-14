import React from "react";
import { Handle, Position } from "reactflow";
import { Clock3, Cpu, HardDrive, Server } from "lucide-react";

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

function formatMs(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) {
    return "";
  }

  if (number >= 1000) {
    return `${(number / 1000).toFixed(1)} s`;
  }

  return `${number.toFixed(number >= 10 ? 0 : 1)} ms`;
}

function formatBytes(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) {
    return "";
  }

  const mb = number / 1024 / 1024;
  if (mb >= 1024) {
    return `${(mb / 1024).toFixed(1)} GB`;
  }

  return `${mb.toFixed(mb >= 10 ? 0 : 1)} MB`;
}

function buildRuntimeBadges(execution) {
  if (!execution) {
    return [];
  }

  const badges = [];
  const runtimeMs =
    execution.edgeDurationMs ||
    execution.durationMs ||
    execution.totalDurationMs;
  const runtimeLabel = formatMs(runtimeMs);

  if (runtimeLabel) {
    badges.push({ label: runtimeLabel, tone: execution.runningEdges ? "running" : "neutral", icon: Clock3 });
  }

  if (execution.loadPercent !== null && execution.loadPercent !== undefined) {
    badges.push({
      label: `${Number(execution.loadPercent).toFixed(0)}%`,
      tone: "cpp",
      icon: Cpu,
    });
  }

  const memoryLabel = formatBytes(execution.memoryBytes);
  if (memoryLabel) {
    badges.push({
      label: memoryLabel,
      tone: "cpp",
      icon: HardDrive,
    });
  }

  if (execution.hasCppExecution) {
    const worker = execution.workerIds?.[0];
    badges.push({
      label: worker || execution.cppOperation || "C++",
      tone: "cpp",
      icon: Server,
    });
  }

  return badges.slice(0, 3);
}

function getBadgeStyle(tone) {
  if (tone === "running") {
    return {
      background: "#fff7ed",
      borderColor: "#fed7aa",
      color: "#9a3412",
    };
  }

  if (tone === "cpp") {
    return {
      background: "#eff6ff",
      borderColor: "#bfdbfe",
      color: "#1d4ed8",
    };
  }

  return {
    background: "#f8fafc",
    borderColor: "#d7e1ec",
    color: "#475569",
  };
}

export default function GraphNode({
  data,
  targetPosition = Position.Left,
  sourcePosition = Position.Right,
}) {
  const hasComment = Boolean(data.comment);
  const execution = data.execution || null;
  const isSelected = Boolean(data.selected);
  const isRelatedSelected = Boolean(data.relatedSelected);
  const phaseStyle = PHASE_STYLES[execution?.phase] || {
    background: "#ffffff",
    border: "#94a3b8",
    shadow: "0 1px 3px rgba(15, 23, 42, 0.1)",
    label: "",
  };
  const borderColor = isSelected ? "#2563eb" : phaseStyle.border;
  const boxShadow = isSelected
    ? "0 0 0 4px rgba(37, 99, 235, 0.18), 0 8px 18px rgba(15, 23, 42, 0.14)"
    : isRelatedSelected
      ? "0 0 0 3px rgba(37, 99, 235, 0.12), 0 1px 4px rgba(15, 23, 42, 0.12)"
      : phaseStyle.shadow;
  const runtimeBadges = buildRuntimeBadges(execution);

  return (
    <div
      title={data.comment || data.label}
      style={{
        width: "100%",
        height: "100%",
        minWidth: 148,
        minHeight: runtimeBadges.length > 0 ? 64 : 46,
        padding: hasComment ? "7px 10px" : "11px 10px",
        background: phaseStyle.background,
        border: `2px solid ${borderColor}`,
        borderRadius: 8,
        color: "#0f172a",
        fontSize: 11,
        lineHeight: "13px",
        textAlign: "center",
        boxShadow,
        position: "relative",
        transition: "background 160ms ease, border-color 160ms ease, box-shadow 160ms ease",
      }}
    >
      {execution?.phase ? (
        <span
          className="gv-node-status-dot"
          title={phaseStyle.label}
          style={{ background: borderColor }}
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
            fontSize: 9,
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
      {runtimeBadges.length > 0 ? (
        <div className="gv-node-runtime">
          {runtimeBadges.map((badge) => {
            const badgeStyle = getBadgeStyle(badge.tone);
            const Icon = badge.icon;
            return (
              <span
                key={badge.label}
                className="gv-node-runtime-badge"
                title={badge.label}
                style={badgeStyle}
              >
                {Icon ? <Icon size={9} strokeWidth={2.2} /> : null}
                {badge.label}
              </span>
            );
          })}
        </div>
      ) : null}

      <Handle type="target" position={targetPosition} id="in" />
      <Handle type="source" position={sourcePosition} id="out" />
    </div>
  );
}
