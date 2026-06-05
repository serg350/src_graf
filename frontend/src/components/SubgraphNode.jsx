import React from "react";
import { Handle, Position } from "reactflow";
import { FolderTree } from "lucide-react";

export default function SubgraphNode({ data }) {
  return (
    <div
      title={data.comment || data.label}
      style={{
        padding: data.comment ? "8px 12px" : "10px 14px",
        background: "#fff4e5",
        border: "2px solid #d97706",
        borderRadius: 8,
        fontSize: 13,
        fontWeight: 600,
        display: "flex",
        alignItems: "center",
        gap: 8,
        cursor: "pointer",
        position: "relative",
      }}
      onClick={(e) => {
        e.stopPropagation();
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

      <Handle type="target" position={Position.Left} id="in" style={{ background: "#d97706" }} />
      <Handle type="source" position={Position.Right} id="out" style={{ background: "#d97706" }} />
    </div>
  );
}
