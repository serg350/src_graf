import { getBezierPath, MarkerType } from "reactflow";

export default function CustomEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}) {
  const offset = data?.offset ?? 0;

  const [path] = getBezierPath({
    sourceX,
    sourceY: sourceY + offset,
    targetX,
    targetY: targetY + offset,
    sourcePosition,
    targetPosition,
  });

  return (
    <path
      d={path}
      fill="none"
      stroke="#334155"
      strokeWidth={1.5}
      markerEnd="url(#arrow)"
    />
  );
}