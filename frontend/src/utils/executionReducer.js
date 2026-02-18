import { create } from "zustand";

export const useExecutionStore = create((set) => ({
  activeNode: null,
  activeEdge: null,
  history: [],

  handleEvent(event) {
    set((state) => {
      const next = { ...state };

      if (event.type === "NODE_ENTER") {
        next.activeNode = event.node;
      }

      if (event.type === "EDGE_TRAVERSE") {
        next.activeEdge = `${event.source}-${event.target}`;
      }

      if (event.type === "NODE_EXIT") {
        next.activeNode = null;
        next.activeEdge = null;
      }

      next.history.push(event);
      return next;
    });
  },
}));
