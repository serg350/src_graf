import { EdgeLabelRenderer, getBezierPath } from "reactflow";

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

  const [path, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  });
  const edgeComment = data?.comment || label || "";

  return (
    <>
      <g>
        <path
          id={id}
          d={path}
          fill="none"
          stroke={data?.active ? "#22c55e" : "#334155"}
          strokeWidth={data?.active ? 3 : 1.5}
          markerEnd={`url(#arrow-${id})`}
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
              fill={data?.active ? "#22c55e" : "#334155"}
            />
          </marker>
        </defs>
      </g>

      {edgeComment ? (
        <EdgeLabelRenderer>
          <div
            title={edgeComment}
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
              background: data?.active ? "#dcfce7" : "#ffffff",
              border: `1px solid ${data?.active ? "#22c55e" : "#cbd5e1"}`,
              borderRadius: 6,
              boxShadow: "0 1px 4px rgba(15, 23, 42, 0.12)",
              color: "#334155",
              fontSize: 11,
              lineHeight: "14px",
              maxWidth: 180,
              padding: "3px 6px",
              pointerEvents: "none",
              whiteSpace: "normal",
            }}
          >
            {edgeComment}
          </div>
        </EdgeLabelRenderer>
      ) : null}
    </>
  );
}
