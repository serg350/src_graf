import React from "react";
import { Handle, Position } from "reactflow";
import { FolderTree } from "lucide-react";

export default function SubgraphNode({ data }) {
  return (
    <div
      style={{
        padding: "10px 14px",
        background: "#fff4e5",
        border: "2px solid #d97706",
        borderRadius: 10,
        fontSize: 13,
        fontWeight: 600,
        display: "flex",
        alignItems: "center",
        gap: 8,
        cursor: "pointer",
        position: "relative"
      }}
      onClick={(e) => {
        e.stopPropagation();
        data.onOpenSubgraph?.(data.subgraphId);
      }}
    >
      <FolderTree size={16} color="#b45309" />
      <span>{data.label}</span>

      {/* обязательные handle'ы */}
      <Handle
        type="target"
        position={Position.Left}
        id="in"
        style={{ background: "#d97706" }}
      />

      <Handle
        type="source"
        position={Position.Right}
        id="out"
        style={{ background: "#d97706" }}
      />
    </div>
  );
}
