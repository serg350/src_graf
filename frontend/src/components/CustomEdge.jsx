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
    const loopX = Math.max(sourceX, targetX) + 130 + laneOffset;
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
  const stroke = PHASE_COLORS[phase] || "#475569";
  const isRunning = phase === "running";
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
          d={route.path}
          fill="none"
          stroke={stroke}
          strokeWidth={isRunning || phase === "failed" ? 3 : phase === "completed" ? 2 : 1.5}
          markerEnd={`url(#arrow-${id})`}
          style={{
            filter: isRunning ? "drop-shadow(0 0 4px rgba(249, 115, 22, 0.48))" : "none",
            transition: "stroke 160ms ease, stroke-width 160ms ease",
          }}
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
              background: isRunning ? "#fff7ed" : phase === "failed" ? "#fff1f1" : "#ffffff",
              border: `1px solid ${isRunning || phase === "failed" ? stroke : "#cbd5e1"}`,
              borderRadius: 6,
              boxShadow: "0 1px 4px rgba(15, 23, 42, 0.12)",
              color: "#334155",
              fontSize: 10,
              lineHeight: "14px",
              maxWidth: 170,
              padding: "3px 7px",
              pointerEvents: "auto",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              zIndex: isRunning ? 8 : 2,
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
