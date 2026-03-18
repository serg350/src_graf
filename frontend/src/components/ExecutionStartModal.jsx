const overlayStyle = {
  position: "fixed",
  inset: 0,
  background: "rgba(15, 23, 42, 0.5)",
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
  padding: 16,
  zIndex: 3000,
};

const modalStyle = {
  width: "min(860px, 100%)",
  maxHeight: "calc(100vh - 32px)",
  overflow: "auto",
  background: "#ffffff",
  borderRadius: 16,
  border: "1px solid #d7e1ec",
  boxShadow: "0 28px 60px rgba(15, 23, 42, 0.24)",
  padding: 20,
};

const badgeStyle = {
  display: "inline-flex",
  alignItems: "center",
  padding: "5px 10px",
  borderRadius: 999,
  background: "#eef4ff",
  color: "#375f84",
  fontSize: 12,
  fontWeight: 600,
};

function groupFieldsBySection(fields) {
  return fields.reduce((acc, field) => {
    const section = field.section || "Input";
    if (!acc[section]) {
      acc[section] = [];
    }
    acc[section].push(field);
    return acc;
  }, {});
}

export default function ExecutionStartModal({
  open,
  graphName,
  schema,
  values,
  error,
  isSubmitting,
  onClose,
  onSubmit,
  onChange,
}) {
  if (!open) {
    return null;
  }

  const fields = schema?.fields ?? [];
  const sections = groupFieldsBySection(fields);

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={(event) => event.stopPropagation()}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 16,
            alignItems: "flex-start",
            marginBottom: 16,
          }}
        >
          <div>
            <h2 style={{ margin: 0, color: "#274c6a", fontSize: 22 }}>
              Параметры запуска
            </h2>
            <p style={{ margin: "8px 0 0", color: "#5f7184", fontSize: 14 }}>
              {graphName
                ? `Проверьте начальные данные для графа "${graphName}" перед обходом.`
                : "Проверьте начальные данные перед обходом."}
            </p>
          </div>

          <button
            type="button"
            onClick={onClose}
            style={{
              border: "none",
              background: "transparent",
              color: "#5f7184",
              fontSize: 24,
              lineHeight: 1,
              cursor: "pointer",
            }}
            aria-label="Закрыть"
          >
            x
          </button>
        </div>

        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 16 }}>
          <span style={badgeStyle}>Полей: {fields.length}</span>
          {schema?.prefilled_count ? (
            <span style={{ ...badgeStyle, background: "#edf7ed", color: "#2f7d32" }}>
              С начальными данными: {schema.prefilled_count}
            </span>
          ) : null}
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(250px, 1fr))",
            gap: 14,
          }}
        >
          {Object.entries(sections).map(([sectionName, sectionFields]) => (
            <div
              key={sectionName}
              style={{
                border: "1px solid #d7e1ec",
                borderRadius: 12,
                padding: 14,
                background: "#f8fbfd",
              }}
            >
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 8,
                  alignItems: "center",
                  marginBottom: 12,
                }}
              >
                <div
                  style={{
                    fontSize: 12,
                    fontWeight: 700,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    color: "#5f7184",
                  }}
                >
                  {sectionName}
                </div>

                {sectionFields.some((field) => field.has_initial_value) ? (
                  <span style={{ ...badgeStyle, fontSize: 11 }}>aINI</span>
                ) : null}
              </div>

              {sectionFields.map((field) => {
                const currentValue = values[field.name];

                return (
                  <div
                    key={field.name}
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      gap: 6,
                      marginBottom: 12,
                      color: "#1f2937",
                      fontSize: 13,
                    }}
                  >
                    <span style={{ fontWeight: 600 }}>
                      {field.label}
                      {field.required ? " *" : ""}
                    </span>

                    {field.input_type === "select" ? (
                      <select
                        value={currentValue ?? ""}
                        onChange={(event) => onChange(field, event.target.value)}
                        style={{
                          border: "1px solid #bfd1df",
                          borderRadius: 10,
                          padding: "10px 12px",
                          fontSize: 14,
                          background: "#ffffff",
                        }}
                      >
                        <option value="">--</option>
                        {(field.options || []).map((optionValue) => (
                          <option key={optionValue} value={String(optionValue)}>
                            {String(optionValue)}
                          </option>
                        ))}
                      </select>
                    ) : field.input_type === "checkbox" ? (
                      <label
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: 10,
                          padding: "10px 12px",
                          border: "1px solid #bfd1df",
                          borderRadius: 10,
                          background: "#ffffff",
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={Boolean(currentValue)}
                          onChange={(event) => onChange(field, event.target.checked)}
                        />
                        <span>{field.comment || "Логическое значение"}</span>
                      </label>
                    ) : (
                      <input
                        type={field.input_type === "number" ? "number" : "text"}
                        value={currentValue ?? ""}
                        placeholder={
                          field.sample !== null &&
                          field.sample !== undefined &&
                          field.sample !== ""
                            ? String(field.sample)
                            : ""
                        }
                        min={field.min ?? undefined}
                        max={field.max ?? undefined}
                        step={field.input_type === "number" ? field.step ?? "any" : undefined}
                        onChange={(event) => onChange(field, event.target.value)}
                        style={{
                          border: "1px solid #bfd1df",
                          borderRadius: 10,
                          padding: "10px 12px",
                          fontSize: 14,
                          background: "#ffffff",
                        }}
                      />
                    )}

                    {field.has_initial_value ? (
                      <div
                        style={{
                          padding: "8px 10px",
                          borderRadius: 10,
                          background: "#eef4ff",
                          color: "#375f84",
                          fontSize: 12,
                        }}
                      >
                        Начальное значение из aINI: {field.initial_value_label}
                      </div>
                    ) : null}

                    {field.comment && field.input_type !== "checkbox" ? (
                      <div style={{ color: "#5f7184", fontSize: 12 }}>
                        {field.comment}
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          ))}
        </div>

        {error ? (
          <div
            style={{
              marginTop: 16,
              padding: "10px 12px",
              borderRadius: 10,
              border: "1px solid #ef9a9a",
              background: "#fff1f1",
              color: "#b42318",
              fontSize: 13,
            }}
          >
            {error}
          </div>
        ) : null}

        <div
          style={{
            display: "flex",
            justifyContent: "flex-end",
            gap: 10,
            marginTop: 18,
          }}
        >
          <button
            type="button"
            onClick={onClose}
            style={{
              border: "1px solid #cbd5e1",
              background: "#ffffff",
              color: "#334155",
              borderRadius: 10,
              padding: "10px 14px",
              cursor: "pointer",
            }}
          >
            Отмена
          </button>
          <button
            type="button"
            onClick={onSubmit}
            disabled={isSubmitting}
            style={{
              border: "none",
              background: isSubmitting ? "#9db4c7" : "#2e5878",
              color: "#ffffff",
              borderRadius: 10,
              padding: "10px 16px",
              cursor: isSubmitting ? "wait" : "pointer",
              fontWeight: 600,
            }}
          >
            {isSubmitting ? "Запуск..." : "Запустить обход"}
          </button>
        </div>
      </div>
    </div>
  );
}
