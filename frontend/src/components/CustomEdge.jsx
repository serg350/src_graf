import { EdgeLabelRenderer, getSmoothStepPath } from "reactflow";

const PHASE_COLORS = {
  running: "#f97316",
  completed: "#22c55e",
  failed: "#dc2626",
};

function buildBackEdgePath({
  sourceX,
  sourceY,
  targetX,
  targetY,
  orientation,
  lane,
}) {
  const laneOffset = (lane || 0) * 34;

  if (orientation === "TB") {
    const loopX = Math.max(sourceX, targetX) + 98 + laneOffset;
    return {
      path: `M ${sourceX} ${sourceY} L ${loopX} ${sourceY} L ${loopX} ${targetY} L ${targetX} ${targetY}`,
      labelX: loopX,
      labelY: (sourceY + targetY) / 2,
    };
  }

  const loopY = Math.max(sourceY, targetY) + 112 + laneOffset;
  return {
    path: `M ${sourceX} ${sourceY} L ${sourceX + 32} ${sourceY} L ${sourceX + 32} ${loopY} L ${targetX - 32} ${loopY} L ${targetX - 32} ${targetY} L ${targetX} ${targetY}`,
    labelX: (sourceX + targetX) / 2,
    labelY: loopY,
  };
}

export default function CustomEdge(props) {
  const {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    data,
    label,
  } = props;

  const regularPath = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 12,
    offset: 32,
  });
  const route = data?.isBackEdge
    ? buildBackEdgePath({
        sourceX,
        sourceY,
        targetX,
        targetY,
        orientation: data.orientation,
        lane: data.routingLane,
      })
    : {
        path: regularPath[0],
        labelX: regularPath[1],
        labelY: regularPath[2],
      };
  const edgeComment = data?.comment || label || "";
  const execution = data?.execution;
  const phase = execution?.phase;
  const isSelected = Boolean(data?.selected);
  const isRelatedSelected = Boolean(data?.relatedSelected);
  const stroke = isSelected
    ? "#2563eb"
    : isRelatedSelected
      ? "#3b82f6"
      : PHASE_COLORS[phase] || "#475569";
  const isRunning = phase === "running";
  const strokeWidth = isSelected
    ? 3.4
    : isRunning || phase === "failed"
      ? 3
      : phase === "completed" || isRelatedSelected
        ? 2
        : 1.4;
  const operation =
    execution?.executor_operation ||
    data?.executorOperation ||
    execution?.morph_func ||
    "";
  const runtimeDetails = [
    operation,
    execution?.worker_id,
    execution?.duration_ms !== undefined
      ? `${Number(execution.duration_ms).toFixed(1)} ms`
      : null,
  ].filter(Boolean);

  return (
    <>
      <g>
        <path
          id={id}
          className="react-flow__edge-path"
          d={route.path}
          fill="none"
          stroke={stroke}
          strokeWidth={strokeWidth}
          markerEnd={`url(#arrow-${id})`}
          onClick={() => data?.onSelectEdge?.()}
          style={{
            cursor: "pointer",
            filter:
              isSelected
                ? "drop-shadow(0 0 5px rgba(37, 99, 235, 0.38))"
                : isRunning
                  ? "drop-shadow(0 0 4px rgba(249, 115, 22, 0.48))"
                  : "none",
            pointerEvents: "stroke",
            transition: "stroke 160ms ease, stroke-width 160ms ease",
          }}
        />
        <path
          d={route.path}
          fill="none"
          stroke="transparent"
          strokeWidth={18}
          onClick={() => data?.onSelectEdge?.()}
          style={{ cursor: "pointer", pointerEvents: "stroke" }}
        />

        <defs>
          <marker
            id={`arrow-${id}`}
            viewBox="0 0 10 10"
            refX="10"
            refY="5"
            markerWidth="6"
            markerHeight="6"
            orient="auto-start-reverse"
          >
            <path
              d="M 0 0 L 10 5 L 0 10 z"
              fill={stroke}
            />
          </marker>
        </defs>
      </g>

      {edgeComment || runtimeDetails.length > 0 ? (
        <EdgeLabelRenderer>
          <div
            title={edgeComment}
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${route.labelX}px, ${route.labelY}px)`,
              background:
                isSelected
                  ? "#eff6ff"
                  : isRunning
                    ? "#fff7ed"
                    : phase === "failed"
                      ? "#fff1f1"
                      : "#ffffff",
              border: `1px solid ${
                isSelected || isRunning || phase === "failed" ? stroke : "#cbd5e1"
              }`,
              borderRadius: 6,
              boxShadow: "0 1px 4px rgba(15, 23, 42, 0.12)",
              color: "#334155",
              fontSize: 10,
              lineHeight: "14px",
              maxWidth: 170,
              padding: "3px 7px",
              pointerEvents: "auto",
              cursor: "pointer",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              zIndex: isSelected || isRunning ? 8 : 2,
            }}
            onClick={(event) => {
              event.stopPropagation();
              data?.onSelectEdge?.();
            }}
          >
            {edgeComment ? (
              <div style={{ overflow: "hidden", textOverflow: "ellipsis" }}>{edgeComment}</div>
            ) : null}
            {runtimeDetails.length > 0 && (isRunning || phase === "failed") ? (
              <div style={{ color: stroke, fontWeight: 700 }}>
                {runtimeDetails.join(" · ")}
              </div>
            ) : null}
          </div>
        </EdgeLabelRenderer>
      ) : null}
    </>
  );
}
