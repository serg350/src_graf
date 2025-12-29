import React, { useState, useRef, useEffect } from "react";
import { Settings, GitBranch } from "lucide-react";
import HistoryPanel from "./HistoryPanel";

/**
 * GraphVisLabLayout
 * - header + main (2 колонки)
 * - children (GraphView или GraphListView) рендерятся слева
 * - scroll ТОЛЬКО внутри панелей
 */

export default function GraphVisLabLayout({ children }) {
  const [showMenu, setShowMenu] = useState(false);
  const [orientation, setOrientation] = useState("LR");
  const [showSubgraphs, setShowSubgraphs] = useState(true);
  const [history, setHistory] = useState([]);

  // ширина левой панели (%)
  const [leftPct, setLeftPct] = useState(72);

  const containerRef = useRef(null);
  const draggingRef = useRef(false);

  // добавление записи в историю
  const onHistoryAdd = (entry) => {
    setHistory((h) => [...h, { ...entry, ts: Date.now() }]);
  };

  // drag resize
  useEffect(() => {
    const onMove = (e) => {
      if (!draggingRef.current || !containerRef.current) return;

      const rect = containerRef.current.getBoundingClientRect();
      const x = e.clientX - rect.left;
      let pct = (x / rect.width) * 100;

      pct = Math.max(20, Math.min(85, pct));
      setLeftPct(pct);
    };

    const onUp = () => {
      draggingRef.current = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };

    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);

    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
  }, []);

  const startDrag = () => {
    draggingRef.current = true;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  };

  // прокидываем props в GraphView / GraphListView
  const childWithProps = React.isValidElement(children)
    ? React.cloneElement(children, {
        orientation,
        showSubgraphs,
        onHistoryAdd,
      })
    : children;

  return (
    <div
      style={{
        height: "100vh",
        width: "100vw",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* ================= HEADER ================= */}
      <div className="gv-header">
        <div className="gv-title">
          <div className="gv-logo">G</div>
          <div>
            <div style={{ fontSize: 14 }}>GraphVisLab</div>
            <div style={{ fontSize: 11, color: "rgba(255,255,255,0.7)" }}>
              Interactive graph visualization
            </div>
          </div>
        </div>

        <button
          className="btn"
          style={{
            marginLeft: 12,
            padding: "6px 10px",
            fontSize: 12,
            background: "rgba(255,255,255,0.12)",
          }}
          onClick={() => {
            window.location.href = "/graphs";
          }}
        >
          Все графы
        </button>

        <button
          className="btn"
          style={{
            marginLeft: 8,
            padding: "6px 10px",
            fontSize: 12,
            background: "rgba(255,255,255,0.12)",
          }}
          onClick={() => {
            window.location.href = "/graphs/import";
          }}
        >
          Импорт графа
        </button>

        <div style={{ position: "relative" }}>
          <button
            className="btn"
            title="Settings"
            onClick={() => setShowMenu((s) => !s)}
          >
            <Settings color="white" />
          </button>

          <div className={`gv-dropdown ${showMenu ? "show" : ""}`}>
            <h4 style={{ margin: 0, marginBottom: 6 }}>Настройки</h4>

            <div style={{ display: "grid", gap: 8 }}>
              {/* ORIENTATION */}
              <div style={{ fontSize: 13, color: "#334155" }}>
                Ориентация графа
              </div>

              <div style={{ display: "flex", gap: 8 }}>
                <button
                  className="btn"
                  onClick={() => {
                    setOrientation("LR");
                    setShowMenu(false);
                  }}
                  style={{
                    padding: "8px 10px",
                    borderRadius: 8,
                    background:
                      orientation === "LR" ? "#eef2ff" : "transparent",
                  }}
                >
                  Слева → Направо
                </button>

                <button
                  className="btn"
                  onClick={() => {
                    setOrientation("TB");
                    setShowMenu(false);
                  }}
                  style={{
                    padding: "8px 10px",
                    borderRadius: 8,
                    background:
                      orientation === "TB" ? "#eef2ff" : "transparent",
                  }}
                >
                  Сверху → Вниз
                </button>
              </div>

              <hr />

              {/* SUBGRAPHS */}
              <button
                className="btn"
                onClick={() => setShowSubgraphs((s) => !s)}
                style={{
                  display: "flex",
                  gap: 8,
                  alignItems: "center",
                  padding: "8px 10px",
                  borderRadius: 8,
                  background: showSubgraphs ? "#eef2ff" : "transparent",
                }}
              >
                <GitBranch />
                <span style={{ fontSize: 13 }}>Показать подграфы</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* ================= MAIN ================= */}
      <div
        ref={containerRef}
        className="gv-main"
        style={{
          flex: 1,
          display: "flex",
          minHeight: 0, // КРИТИЧНО для scroll
        }}
      >
        {/* LEFT */}
        <div
          className="gv-graph"
          style={{
            width: `${leftPct}%`,
            height: "100%",
            overflow: "auto",
            transition: draggingRef.current
              ? "none"
              : "width 120ms ease",
          }}
        >
          {childWithProps}
        </div>

        {/* SPLITTER */}
        <div
          className="gv-splitter"
          onMouseDown={startDrag}
          title="Перетащите для изменения ширины"
        />

        {/* RIGHT */}
        <div
          className="gv-history"
          style={{
            width: `${100 - leftPct}%`,
            overflow: "auto",
          }}
        >
          <HistoryPanel history={history} />
        </div>
      </div>
    </div>
  );
}
