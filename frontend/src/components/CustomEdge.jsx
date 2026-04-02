import { getBezierPath, MarkerType } from "reactflow";

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
  } = props;

  const [path] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  });

  return (
    <g>
      <path
        id={id}
        d={path}
        fill="none"
        stroke={data?.active ? "#22c55e" : "#334155"}
        strokeWidth={data?.active ? 3 : 1.5}
        markerEnd={`url(#arrow-${id})`}
      />

      {/* 🔻 СТРЕЛКА */}
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
  );
}
