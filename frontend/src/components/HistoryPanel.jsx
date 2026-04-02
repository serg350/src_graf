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
  state_exit: "Выход из состояния",
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

export default function HistoryPanel({ sessions, isLoading, error }) {
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
        <div style={{ fontSize: 18, fontWeight: 700, color: "#274c6a" }}>
          История обходов
        </div>
        <div style={{ fontSize: 13, color: "#5f7184" }}>
          Сохранённые сессии выполнения и входные параметры запуска.
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

      {sessions.map((session) => {
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
              <span className="gv-chip">Событий: {session.event_count}</span>
              <span className="gv-chip">Последнее состояние: {session.last_state || "—"}</span>
              <span className="gv-chip">Завершение: {formatDateTime(session.finished_at)}</span>
            </div>

            {Object.keys(session.initial_data || {}).length > 0 ? (
              <div style={{ display: "grid", gap: 8 }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: "#5f7184" }}>
                  Входные параметры
                </div>
                {renderJsonBlock(session.initial_data)}
              </div>
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
                session.events.map((event) => (
                  <div
                    key={`${session.session_id}-${event.sequence}`}
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
                      Состояние: {event.state || "—"}
                    </div>

                    {event.message ? (
                      <div style={{ fontSize: 12, color: "#b42318" }}>{event.message}</div>
                    ) : null}

                    {renderJsonBlock(event.data)}
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
