function toComparableTimestamp(value) {
  if (!value) {
    return 0;
  }

  if (typeof value === "number") {
    return value * 1000;
  }

  return new Date(value).getTime();
}

function normalizeEvent(event, index = 0) {
  return {
    sequence: event.sequence ?? index + 1,
    event: event.event ?? "",
    state: event.state ?? "",
    message: event.message ?? "",
    timestamp:
      typeof event.timestamp === "number"
        ? event.timestamp
        : Math.floor(Date.now() / 1000),
    data: event.data && typeof event.data === "object" ? event.data : {},
  };
}

export function normalizeExecutionSession(session) {
  return {
    session_id: session.session_id,
    status: session.status ?? "pending",
    created_at: session.created_at ?? new Date().toISOString(),
    finished_at: session.finished_at ?? null,
    initial_data:
      session.initial_data && typeof session.initial_data === "object"
        ? session.initial_data
        : {},
    last_state: session.last_state ?? "",
    event_count: session.event_count ?? 0,
    error_message: session.error_message ?? "",
    events: Array.isArray(session.events)
      ? session.events.map((event, index) => normalizeEvent(event, index))
      : [],
  };
}

function sortSessions(sessions) {
  return [...sessions].sort(
    (left, right) =>
      toComparableTimestamp(right.created_at) - toComparableTimestamp(left.created_at)
  );
}

export function createPendingExecutionSession({ sessionId, initialData }) {
  return normalizeExecutionSession({
    session_id: sessionId,
    status: "pending",
    created_at: new Date().toISOString(),
    initial_data: initialData ?? {},
    events: [],
  });
}

export function mergeSessionStart(sessions, pendingSession) {
  const normalized = normalizeExecutionSession(pendingSession);
  const next = sessions.filter((session) => session.session_id !== normalized.session_id);
  next.unshift(normalized);
  return sortSessions(next);
}

export function mergeExecutionEvent(sessions, rawEvent) {
  if (!rawEvent?.session_id) {
    return sessions;
  }

  const event = normalizeEvent(rawEvent);
  let found = false;

  const updated = sessions.map((session) => {
    if (session.session_id !== rawEvent.session_id) {
      return session;
    }

    found = true;
    const hasSameEvent = session.events.some(
      (item) =>
        item.sequence === event.sequence &&
        item.event === event.event &&
        item.state === event.state &&
        item.timestamp === event.timestamp
    );

    const events = hasSameEvent ? session.events : [...session.events, event];
    let status = session.status;

    if (event.event === "error") {
      status = "failed";
    } else if (event.event === "complete") {
      status = "completed";
    } else if (event.event) {
      status = "running";
    }

    return normalizeExecutionSession({
      ...session,
      status,
      events,
      event_count: events.length,
      last_state: event.state || session.last_state,
      finished_at:
        event.event === "error" || event.event === "complete"
          ? new Date(event.timestamp * 1000).toISOString()
          : session.finished_at,
      error_message: event.message || session.error_message,
    });
  });

  if (!found) {
    return mergeExecutionEvent(
      [createPendingExecutionSession({ sessionId: rawEvent.session_id }), ...sessions],
      rawEvent
    );
  }

  return sortSessions(updated);
}
