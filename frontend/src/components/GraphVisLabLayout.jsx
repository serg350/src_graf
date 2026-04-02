import { useEffect, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

const NAV_ITEMS = [
  { label: "Все графы", href: "/graphs" },
  { label: "Импорт графа", href: "/graphs/import" },
];

function isActivePath(currentPath, href) {
  if (href === "/graphs") {
    return currentPath === "/graphs";
  }

  return currentPath.startsWith(href);
}

export default function GraphVisLabLayout({
  children,
  rightPanel = null,
  headerActions = null,
  title = "GraphVisLab",
  subtitle = "Interactive graph visualization",
  defaultLeftPct = 68,
}) {
  const navigate = useNavigate();
  const location = useLocation();
  const hasRightPanel = Boolean(rightPanel);

  const [leftPct, setLeftPct] = useState(defaultLeftPct);
  const containerRef = useRef(null);
  const draggingRef = useRef(false);

  useEffect(() => {
    if (!hasRightPanel) {
      setLeftPct(100);
      return undefined;
    }

    const onMove = (event) => {
      if (!draggingRef.current || !containerRef.current) {
        return;
      }

      const rect = containerRef.current.getBoundingClientRect();
      const x = event.clientX - rect.left;
      let pct = (x / rect.width) * 100;
      pct = Math.max(42, Math.min(82, pct));
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
  }, [hasRightPanel]);

  const startDrag = () => {
    draggingRef.current = true;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  };

  return (
    <div
      style={{
        height: "100vh",
        width: "100vw",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div className="gv-header">
        <div className="gv-title">
          <div className="gv-logo">G</div>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700 }}>{title}</div>
            <div style={{ fontSize: 11, color: "rgba(255,255,255,0.72)" }}>{subtitle}</div>
          </div>
        </div>

        {NAV_ITEMS.map((item) => {
          const active = isActivePath(location.pathname, item.href);
          return (
            <button
              key={item.href}
              type="button"
              className="btn"
              style={{
                padding: "8px 12px",
                fontSize: 12,
                fontWeight: 600,
                background: active ? "rgba(255,255,255,0.24)" : "rgba(255,255,255,0.12)",
                border: active ? "1px solid rgba(255,255,255,0.24)" : "1px solid transparent",
              }}
              onClick={() => navigate(item.href)}
            >
              {item.label}
            </button>
          );
        })}

        {headerActions ? <div style={{ marginLeft: 6 }}>{headerActions}</div> : null}
      </div>

      <div
        ref={containerRef}
        className="gv-main"
        style={{
          flex: 1,
          display: "flex",
          minHeight: 0,
        }}
      >
        <div
          className="gv-graph"
          style={{
            width: hasRightPanel ? `${leftPct}%` : "100%",
            overflow: "auto",
            transition: hasRightPanel && !draggingRef.current ? "width 120ms ease" : "none",
          }}
        >
          {children}
        </div>

        {hasRightPanel ? (
          <>
            <div
              className="gv-splitter"
              onMouseDown={startDrag}
              title="Перетащите для изменения ширины панели"
            />
            <div
              className="gv-history"
              style={{
                width: `${100 - leftPct}%`,
                overflow: "auto",
              }}
            >
              {rightPanel}
            </div>
          </>
        ) : null}
      </div>
    </div>
  );
}
