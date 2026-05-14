import { useEffect, useRef, useState } from "react";

import ExecutionStartModal from "./ExecutionStartModal";
import { startExecution } from "../services/executionApi";
import { connectExecution } from "../services/executionEvents";

function buildInitialFormValues(schema) {
  const values = {};

  for (const field of schema?.fields ?? []) {
    if (field.input_type === "checkbox") {
      values[field.name] = Boolean(field.initial_value);
      continue;
    }

    if (
      field.initial_value !== null &&
      field.initial_value !== undefined &&
      field.initial_value !== ""
    ) {
      values[field.name] = String(field.initial_value);
      continue;
    }

    values[field.name] = "";
  }

  return values;
}

function buildExecutionPayload(schema, values) {
  const data = {};
  const missingRequired = [];

  for (const field of schema?.fields ?? []) {
    const rawValue = values[field.name];

    if (field.input_type === "checkbox") {
      data[field.name] = Boolean(rawValue);
      continue;
    }

    const textValue = String(rawValue ?? "").trim();
    if (textValue === "") {
      if (field.required) {
        missingRequired.push(field.label || field.name);
      }
      continue;
    }

    if (field.input_type === "number") {
      const numericValue = Number(textValue);
      if (Number.isNaN(numericValue)) {
        return {
          error: `Поле "${field.label || field.name}" должно быть числом`,
        };
      }
      data[field.name] = numericValue;
      continue;
    }

    data[field.name] = textValue;
  }

  if (missingRequired.length > 0) {
    return {
      error: `Не заполнены обязательные поля: ${missingRequired.join(", ")}`,
    };
  }

  return { data };
}

export default function ExecutionController({
  graphId,
  isGraphReady,
  graphName,
  executionInputSchema,
  executionInputError,
  onSessionStarted,
  onStateEvent,
}) {
  const [isStarting, setIsStarting] = useState(false);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [launchError, setLaunchError] = useState("");
  const [formError, setFormError] = useState("");
  const [formValues, setFormValues] = useState(() =>
    buildInitialFormValues(executionInputSchema)
  );
  const disconnectRef = useRef(null);

  useEffect(() => {
    setFormValues(buildInitialFormValues(executionInputSchema));
    setFormError("");
  }, [executionInputSchema]);

  useEffect(
    () => () => {
      disconnectRef.current?.();
    },
    []
  );

  const fields = executionInputSchema?.fields ?? [];
  const hasInputs = fields.length > 0;

  const runExecution = async (payload) => {
    setIsStarting(true);
    setLaunchError("");

    disconnectRef.current?.();

    try {
      const { session_id } = await startExecution(graphId, payload);
      onSessionStarted?.({
        sessionId: session_id,
        initialData: payload,
      });
      disconnectRef.current = connectExecution(session_id, (event) => {
        onStateEvent(event);
      });
      setIsDialogOpen(false);
      setFormError("");
    } catch (error) {
      const message = error.message || "Не удалось запустить обход";
      setLaunchError(message);

      if (hasInputs) {
        setFormError(message);
        setIsDialogOpen(true);
      }
    } finally {
      setIsStarting(false);
    }
  };

  const handlePrimaryAction = () => {
    if (!isGraphReady) {
      return;
    }

    if (hasInputs) {
      setFormError("");
      setIsDialogOpen(true);
      return;
    }

    void runExecution({});
  };

  const handleSubmit = () => {
    const payload = buildExecutionPayload(executionInputSchema, formValues);
    if (payload.error) {
      setFormError(payload.error);
      return;
    }

    void runExecution(payload.data);
  };

  const handleFieldChange = (field, nextValue) => {
    setFormError("");
    setFormValues((current) => ({
      ...current,
      [field.name]: nextValue,
    }));
  };

  return (
    <>
      <div
        style={{
          margin: "16px 16px 12px",
          padding: 16,
          borderRadius: 16,
          border: "1px solid #d7e1ec",
          background: "linear-gradient(180deg, #fbfdff 0%, #f3f8fc 100%)",
          boxShadow: "0 10px 26px rgba(15, 23, 42, 0.08)",
          display: "flex",
          flexWrap: "wrap",
          justifyContent: "space-between",
          gap: 14,
          alignItems: "center",
        }}
      >
        <div style={{ display: "grid", gap: 6 }}>
          <div style={{ fontSize: 18, fontWeight: 700, color: "#274c6a" }}>
            Запуск обхода
          </div>
          <div style={{ fontSize: 13, color: "#5f7184" }}>
            {hasInputs
              ? `Параметры из aINI будут показаны в отдельном окне перед запуском.`
              : "Для этого графа входные параметры из aINI не загружены. Если они нужны, прикрепите aINI к графу."}
          </div>
          {hasInputs ? (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              <span
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  background: "#eef4ff",
                  color: "#375f84",
                  fontSize: 12,
                  fontWeight: 600,
                }}
              >
                Полей: {fields.length}
              </span>
              {executionInputSchema?.prefilled_count ? (
                <span
                  style={{
                    padding: "4px 10px",
                    borderRadius: 999,
                    background: "#edf7ed",
                    color: "#2f7d32",
                    fontSize: 12,
                    fontWeight: 600,
                  }}
                >
                  С начальными данными: {executionInputSchema.prefilled_count}
                </span>
              ) : null}
            </div>
          ) : null}
        </div>

        <button
          type="button"
          onClick={handlePrimaryAction}
          disabled={isStarting || !isGraphReady}
          style={{
            border: "none",
            borderRadius: 12,
            padding: "12px 18px",
            background: isStarting || !isGraphReady ? "#9db4c7" : "#2e5878",
            color: "#ffffff",
            cursor: isStarting || !isGraphReady ? "wait" : "pointer",
            fontWeight: 700,
            minWidth: 180,
          }}
        >
          {!isGraphReady
            ? "Загрузка схемы..."
            : isStarting
            ? "Запуск..."
            : hasInputs
              ? "Подготовить запуск"
              : "Запустить обход"}
        </button>
      </div>

      {executionInputError ? (
        <div
          style={{
            margin: "12px 16px 0",
            padding: "10px 12px",
            borderRadius: 12,
            border: "1px solid #ef9a9a",
            background: "#fff1f1",
            color: "#b42318",
            fontSize: 13,
          }}
        >
          Ошибка разбора aINI: {executionInputError}
        </div>
      ) : null}

      {launchError && !isDialogOpen ? (
        <div
          style={{
            margin: "12px 16px 0",
            padding: "10px 12px",
            borderRadius: 12,
            border: "1px solid #ef9a9a",
            background: "#fff1f1",
            color: "#b42318",
            fontSize: 13,
          }}
        >
          {launchError}
        </div>
      ) : null}

      <ExecutionStartModal
        open={isDialogOpen}
        graphName={graphName}
        schema={executionInputSchema}
        values={formValues}
        error={formError}
        isSubmitting={isStarting}
        onClose={() => {
          if (!isStarting) {
            setIsDialogOpen(false);
          }
        }}
        onSubmit={handleSubmit}
        onChange={handleFieldChange}
      />
    </>
  );
}
