import {
  filterSessionsBySelectedElement,
  getSelectedElementLabel,
} from "../utils/executionHistoryFilter";

const STATUS_META = {
  pending: {
    label: "Ожидает",
    background: "#eef4ff",
    color: "#375f84",
  },
  running: {
    label: "Выполняется",
    background: "#edf7ed",
    color: "#2f7d32",
  },
  completed: {
    label: "Завершён",
    background: "#e8f1fb",
    color: "#1f4f78",
  },
  failed: {
    label: "Ошибка",
    background: "#fff1f1",
    color: "#b42318",
  },
};

const EVENT_LABELS = {
  state_enter: "Вход в состояние",
  state_wait: "Ожидание входных веток",
  state_ready: "Состояние готово",
  state_exit: "Выход из состояния",
  edge_enter: "Запуск перехода",
  edge_exit: "Переход завершён",
  edge_error: "Ошибка перехода",
  complete: "Завершение",
  error: "Ошибка",
};

function formatDateTime(value) {
  if (!value) {
    return "—";
  }

  return new Date(value).toLocaleString("ru-RU");
}

function formatEventTime(timestamp) {
  if (!timestamp) {
    return "—";
  }

  return new Date(timestamp * 1000).toLocaleTimeString("ru-RU");
}

function renderJsonBlock(value) {
  if (!value || Object.keys(value).length === 0) {
    return null;
  }

  return (
    <pre
      style={{
        margin: 0,
        padding: "10px 12px",
        borderRadius: 12,
        background: "#f8fbfd",
        border: "1px solid #d7e1ec",
        fontSize: 12,
        overflowX: "auto",
        color: "#1f2937",
      }}
    >
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

export default function HistoryPanel({
  sessions,
  isLoading,
  error,
  selectedElement,
  onClearSelection,
}) {
  const visibleSessions = filterSessionsBySelectedElement(sessions, selectedElement);
  const selectedLabel = getSelectedElementLabel(selectedElement);
  const totalMatchedEvents = selectedElement
    ? visibleSessions.reduce(
        (total, session) => total + (session.filtered_event_count ?? session.events.length),
        0
      )
    : 0;

  return (
    <div style={{ padding: 16, display: "grid", gap: 12 }}>
      <div
        style={{
          display: "grid",
          gap: 4,
          paddingBottom: 10,
          borderBottom: "1px solid #d7e1ec",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{ fontSize: 18, fontWeight: 700, color: "#274c6a" }}>
            {selectedElement ? "История элемента" : "История обходов"}
          </div>
          {selectedElement ? (
            <button
              type="button"
              className="btn"
              title="Показать всю историю"
              style={{
                marginLeft: "auto",
                padding: "5px 9px",
                borderRadius: 8,
                background: "#eef4ff",
                color: "#375f84",
                fontSize: 12,
                fontWeight: 700,
              }}
              onClick={onClearSelection}
            >
              Сбросить
            </button>
          ) : null}
        </div>
        <div style={{ fontSize: 13, color: "#5f7184" }}>
          {selectedElement
            ? `${selectedElement.type === "edge" ? "Переход" : "Узел"}: ${selectedLabel}. Событий: ${totalMatchedEvents}.`
            : "Сохранённые сессии выполнения и входные параметры запуска."}
        </div>
      </div>

      {isLoading ? (
        <div
          style={{
            padding: 14,
            borderRadius: 14,
            background: "#f8fbfd",
            border: "1px solid #d7e1ec",
            color: "#5f7184",
          }}
        >
          Загрузка истории...
        </div>
      ) : null}

      {error ? (
        <div
          style={{
            padding: 14,
            borderRadius: 14,
            background: "#fff1f1",
            border: "1px solid #ef9a9a",
            color: "#b42318",
          }}
        >
          {error}
        </div>
      ) : null}

      {!isLoading && !error && sessions.length === 0 ? (
        <div
          style={{
            padding: 14,
            borderRadius: 14,
            background: "#f8fbfd",
            border: "1px solid #d7e1ec",
            color: "#5f7184",
          }}
        >
          Для этого графа пока нет сохранённых запусков.
        </div>
      ) : null}

      {!isLoading && !error && selectedElement && sessions.length > 0 && visibleSessions.length === 0 ? (
        <div
          style={{
            padding: 14,
            borderRadius: 14,
            background: "#f8fbfd",
            border: "1px solid #d7e1ec",
            color: "#5f7184",
          }}
        >
          Для выбранного элемента пока нет событий в истории.
        </div>
      ) : null}

      {visibleSessions.map((session) => {
        const status = STATUS_META[session.status] || STATUS_META.pending;
        return (
          <div
            key={session.session_id}
            style={{
              display: "grid",
              gap: 12,
              padding: 14,
              borderRadius: 16,
              background: "#ffffff",
              border: "1px solid #d7e1ec",
            }}
          >
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                justifyContent: "space-between",
                gap: 10,
                alignItems: "center",
              }}
            >
              <div style={{ display: "grid", gap: 4 }}>
                <div style={{ fontSize: 14, fontWeight: 700, color: "#274c6a" }}>
                  Сессия {session.session_id.slice(0, 8)}
                </div>
                <div style={{ fontSize: 12, color: "#5f7184" }}>
                  Запуск: {formatDateTime(session.created_at)}
                </div>
              </div>

              <span
                style={{
                  padding: "5px 10px",
                  borderRadius: 999,
                  background: status.background,
                  color: status.color,
                  fontSize: 12,
                  fontWeight: 700,
                }}
              >
                {status.label}
              </span>
            </div>

            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              <span className="gv-chip">
                {selectedElement
                  ? `Найдено: ${session.filtered_event_count ?? session.events.length} из ${session.total_event_count ?? session.event_count}`
                  : `Событий: ${session.event_count}`}
              </span>
              <span className="gv-chip">Последнее состояние: {session.last_state || "—"}</span>
              <span className="gv-chip">Завершение: {formatDateTime(session.finished_at)}</span>
            </div>

            {Object.keys(session.initial_data || {}).length > 0 ? (
              <details>
                <summary
                  style={{
                    cursor: "pointer",
                    fontSize: 12,
                    fontWeight: 700,
                    color: "#5f7184",
                  }}
                >
                  Входные параметры
                </summary>
                <div style={{ marginTop: 8 }}>{renderJsonBlock(session.initial_data)}</div>
              </details>
            ) : null}

            {session.error_message ? (
              <div
                style={{
                  padding: "10px 12px",
                  borderRadius: 12,
                  background: "#fff1f1",
                  border: "1px solid #ef9a9a",
                  color: "#b42318",
                  fontSize: 13,
                }}
              >
                {session.error_message}
              </div>
            ) : null}

            <div style={{ display: "grid", gap: 8 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: "#5f7184" }}>
                События
              </div>

              {session.events.length === 0 ? (
                <div
                  style={{
                    padding: 12,
                    borderRadius: 12,
                    background: "#f8fbfd",
                    border: "1px solid #d7e1ec",
                    color: "#5f7184",
                    fontSize: 13,
                  }}
                >
                  События ещё не поступили.
                </div>
              ) : (
                session.events.map((event, index) => (
                  <div
                    key={`${session.session_id}-${event.sequence}-${index}`}
                    style={{
                      display: "grid",
                      gap: 8,
                      padding: "12px 12px 10px",
                      borderRadius: 12,
                      background: "#f8fbfd",
                      border: "1px solid #d7e1ec",
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        flexWrap: "wrap",
                        justifyContent: "space-between",
                        gap: 8,
                      }}
                    >
                      <div style={{ fontSize: 13, fontWeight: 700, color: "#1f2937" }}>
                        {EVENT_LABELS[event.event] || event.event || "Событие"}
                      </div>
                      <div style={{ fontSize: 12, color: "#5f7184" }}>
                        {formatEventTime(event.timestamp)}
                      </div>
                    </div>

                    <div style={{ fontSize: 12, color: "#5f7184" }}>
                      {event.from_state || event.to_state
                        ? `Переход: ${event.from_state || "—"} → ${event.to_state || "—"}`
                        : `Состояние: ${event.state || "—"}`}
                    </div>

                    {event.executor_operation ||
                    event.worker_id ||
                    event.duration_ms !== undefined ? (
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                        {event.executor_operation ? (
                          <span className="gv-chip">{event.executor_operation}</span>
                        ) : null}
                        {event.worker_id ? (
                          <span className="gv-chip">{event.worker_id}</span>
                        ) : null}
                        {event.duration_ms !== undefined ? (
                          <span className="gv-chip">
                            {Number(event.duration_ms).toFixed(1)} ms
                          </span>
                        ) : null}
                      </div>
                    ) : null}

                    {event.message ? (
                      <div style={{ fontSize: 12, color: "#b42318" }}>{event.message}</div>
                    ) : null}

                    {event.data && Object.keys(event.data).length > 0 ? (
                      <details>
                        <summary
                          style={{
                            cursor: "pointer",
                            color: "#5f7184",
                            fontSize: 11,
                            fontWeight: 700,
                          }}
                        >
                          Снимок данных
                        </summary>
                        <div style={{ marginTop: 8 }}>{renderJsonBlock(event.data)}</div>
                      </details>
                    ) : null}
                  </div>
                ))
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
