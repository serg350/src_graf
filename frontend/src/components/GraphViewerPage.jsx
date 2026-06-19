import { useEffect, useReducer, useState } from "react";
import { GitBranch, Settings } from "lucide-react";
import { useParams } from "react-router-dom";

import GraphVisLabLayout from "../components/GraphVisLabLayout";
import GraphView from "../components/GraphView";
import HistoryPanel from "../components/HistoryPanel";
import { loadGraphExecutionHistory } from "../services/executionHistoryApi";
import {
  createPendingExecutionSession,
  mergeExecutionEvent,
  mergeSessionStart,
  normalizeExecutionSession,
} from "../utils/executionHistory";
import {
  createExecutionState,
  executionReducer,
} from "../utils/executionState";
import ExecutionController from "./ExecutionController";

function ViewerToolbar({
  orientation,
  showSubgraphs,
  onOrientationChange,
  onToggleSubgraphs,
}) {
  const [showMenu, setShowMenu] = useState(false);

  return (
    <div style={{ position: "relative" }}>
      <button
        type="button"
        className="btn"
        title="Настройки отображения"
        style={{
          padding: "8px 10px",
          background: showMenu ? "rgba(255,255,255,0.22)" : "rgba(255,255,255,0.12)",
        }}
        onClick={() => setShowMenu((current) => !current)}
      >
        <Settings color="white" size={18} />
      </button>

      <div className={`gv-dropdown ${showMenu ? "show" : ""}`}>
        <h4 style={{ margin: 0, marginBottom: 8 }}>Настройки графа</h4>

        <div style={{ display: "grid", gap: 8 }}>
          <div style={{ fontSize: 13, color: "#334155" }}>Ориентация графа</div>

          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              className="btn"
              onClick={() => {
                onOrientationChange("LR");
                setShowMenu(false);
              }}
              style={{
                padding: "8px 10px",
                borderRadius: 8,
                background: orientation === "LR" ? "#eef4ff" : "transparent",
              }}
            >
              Слева направо
            </button>

            <button
              type="button"
              className="btn"
              onClick={() => {
                onOrientationChange("TB");
                setShowMenu(false);
              }}
              style={{
                padding: "8px 10px",
                borderRadius: 8,
                background: orientation === "TB" ? "#eef4ff" : "transparent",
              }}
            >
              Сверху вниз
            </button>
          </div>

          <hr style={{ width: "100%", border: 0, borderTop: "1px solid #e2e8f0" }} />

          <button
            type="button"
            className="btn"
            onClick={() => {
              onToggleSubgraphs();
              setShowMenu(false);
            }}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              padding: "8px 10px",
              borderRadius: 8,
              background: showSubgraphs ? "#eef4ff" : "transparent",
            }}
          >
            <GitBranch size={16} />
            <span style={{ fontSize: 13 }}>
              {showSubgraphs ? "Подграфы выделены" : "Подграфы как обычные узлы"}
            </span>
          </button>
        </div>
      </div>
    </div>
  );
}

export default function GraphViewerPage() {
  const { id } = useParams();
  const [orientation, setOrientation] = useState("LR");
  const [showSubgraphs, setShowSubgraphs] = useState(true);
  const [executionState, dispatchExecution] = useReducer(
    executionReducer,
    undefined,
    createExecutionState
  );
  const [historySessions, setHistorySessions] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(true);
  const [historyError, setHistoryError] = useState("");
  const [graphMeta, setGraphMeta] = useState({
    isLoaded: false,
    name: "",
    executionInputSchema: { fields: [], prefilled_count: 0 },
    executionInputError: "",
  });

  useEffect(() => {
    let isActive = true;

    setHistoryLoading(true);
    setHistoryError("");
    setHistorySessions([]);
    dispatchExecution({ type: "reset", sessionId: null });

    loadGraphExecutionHistory(id)
      .then((sessions) => {
        if (!isActive) {
          return;
        }

        setHistorySessions(sessions.map(normalizeExecutionSession));
      })
      .catch((error) => {
        if (!isActive) {
          return;
        }

        setHistoryError(error.message || "Не удалось загрузить историю обходов");
      })
      .finally(() => {
        if (isActive) {
          setHistoryLoading(false);
        }
      });

    return () => {
      isActive = false;
    };
  }, [id]);

  const handleSessionStarted = ({ sessionId, initialData }) => {
    dispatchExecution({ type: "reset", sessionId });
    setHistorySessions((current) =>
      mergeSessionStart(
        current,
        createPendingExecutionSession({ sessionId, initialData })
      )
    );
  };

  const handleStateEvent = (event) => {
    dispatchExecution({ type: "event", event });
    setHistorySessions((current) => mergeExecutionEvent(current, event));
  };

  return (
    <GraphVisLabLayout
      rightPanel={
        <HistoryPanel
          sessions={historySessions}
          isLoading={historyLoading}
          error={historyError}
        />
      }
      headerActions={
        <ViewerToolbar
          orientation={orientation}
          showSubgraphs={showSubgraphs}
          onOrientationChange={setOrientation}
          onToggleSubgraphs={() => setShowSubgraphs((current) => !current)}
        />
      }
    >
      <GraphView
        graphId={id}
        orientation={orientation}
        showSubgraphs={showSubgraphs}
        executionState={executionState}
        onGraphMeta={setGraphMeta}
        executionControls={
          <ExecutionController
            graphId={id}
            isGraphReady={graphMeta.isLoaded}
            graphName={graphMeta.name}
            executionInputSchema={graphMeta.executionInputSchema}
            executionInputError={graphMeta.executionInputError}
            onSessionStarted={handleSessionStarted}
            onStateEvent={handleStateEvent}
          />
        }
      />
    </GraphVisLabLayout>
  );
}
