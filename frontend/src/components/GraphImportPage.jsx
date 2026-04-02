import { useRef, useState } from "react";
import { FileText, Loader2, UploadCloud } from "lucide-react";
import { useNavigate } from "react-router-dom";

import GraphVisLabLayout from "./GraphVisLabLayout";
import { buildApiUrl } from "../services/apiClient";

export default function GraphImportPage() {
  const navigate = useNavigate();
  const inputRef = useRef(null);
  const ainiInputRef = useRef(null);

  const [file, setFile] = useState(null);
  const [ainiFile, setAiniFile] = useState(null);
  const [dragOver, setDragOver] = useState(false);
  const [loading, setLoading] = useState(false);
  const [log, setLog] = useState([]);

  const logLine = (text) =>
    setLog((current) => [...current, `[${new Date().toLocaleTimeString()}] ${text}`]);

  const onFileSelect = (selectedFile) => {
    if (!selectedFile) {
      return;
    }

    setFile(selectedFile);
    logLine(`Выбран файл графа: ${selectedFile.name}`);
  };

  const onAiniSelect = (selectedFile) => {
    if (!selectedFile) {
      return;
    }

    if (!selectedFile.name.toLowerCase().endsWith(".aini")) {
      logLine("Ошибка: файл aINI должен иметь расширение .aini");
      return;
    }

    setAiniFile(selectedFile);
    logLine(`Выбран файл aINI: ${selectedFile.name}`);
  };

  const onDrop = (event) => {
    event.preventDefault();
    setDragOver(false);
    onFileSelect(event.dataTransfer.files[0]);
  };

  const startImport = async () => {
    if (!file || loading) {
      return;
    }

    setLoading(true);
    setLog([]);
    logLine("Запуск импорта...");

    const formData = new FormData();
    formData.append("dot_file", file);
    if (ainiFile) {
      formData.append("aini_file", ainiFile);
    }

    try {
      const response = await fetch(buildApiUrl("/api/comwpc/graph/import-dot/"), {
        method: "POST",
        credentials: "include",
        body: formData,
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();

      if (!data.success) {
        logLine(`Ошибка импорта: ${data.error || "неизвестная ошибка"}`);
        return;
      }

      logLine("Импорт завершён успешно");
      logLine(`Создан граф с ID ${data.graph_id}`);

      setTimeout(() => {
        navigate(`/graphs/${data.graph_id}`);
      }, 600);
    } catch (error) {
      logLine(`Ошибка: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <GraphVisLabLayout>
      <div style={{ padding: 20 }}>
        <div
          style={{
            maxWidth: 900,
            margin: "0 auto",
            background: "#ffffff",
            borderRadius: 20,
            padding: 24,
            border: "1px solid #d7e1ec",
            boxShadow: "0 18px 40px rgba(15, 23, 42, 0.08)",
            display: "grid",
            gap: 16,
          }}
        >
          <div style={{ display: "grid", gap: 6 }}>
            <div style={{ fontSize: 24, fontWeight: 800, color: "#274c6a" }}>
              Импорт графа
            </div>
            <div style={{ fontSize: 14, color: "#5f7184" }}>
              Загрузите `.dot` или `.adot` файл и при необходимости приложите `.aini`.
            </div>
          </div>

          <div
            onClick={() => inputRef.current?.click()}
            onDragOver={(event) => {
              event.preventDefault();
              setDragOver(true);
            }}
            onDragLeave={() => setDragOver(false)}
            onDrop={onDrop}
            style={{
              border: "2px dashed #bfd1df",
              borderRadius: 18,
              padding: 36,
              textAlign: "center",
              cursor: "pointer",
              background: dragOver ? "#eef4ff" : "#f8fbfd",
              display: "grid",
              gap: 10,
            }}
          >
            <UploadCloud size={36} color="#2e5878" style={{ margin: "0 auto" }} />
            <div style={{ fontSize: 18, fontWeight: 700, color: "#274c6a" }}>
              Перетащите файл графа сюда
            </div>
            <div style={{ fontSize: 13, color: "#5f7184" }}>
              или нажмите для выбора файла `.dot` / `.adot`
            </div>

            {file ? (
              <div
                style={{
                  marginTop: 6,
                  display: "inline-flex",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: 8,
                  color: "#1f2937",
                  fontSize: 14,
                }}
              >
                <FileText size={16} />
                <span>{file.name}</span>
              </div>
            ) : null}

            <input
              ref={inputRef}
              type="file"
              accept=".dot,.adot"
              style={{ display: "none" }}
              onChange={(event) => onFileSelect(event.target.files[0])}
            />
          </div>

          <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }}>
            <button
              type="button"
              onClick={() => ainiInputRef.current?.click()}
              style={{
                padding: "10px 14px",
                borderRadius: 12,
                border: "1px solid #bfd1df",
                background: "#ffffff",
                color: "#274c6a",
                fontWeight: 600,
              }}
            >
              Добавить aINI
            </button>

            <span style={{ fontSize: 13, color: "#5f7184" }}>
              {ainiFile ? ainiFile.name : "Файл не выбран"}
            </span>

            <input
              ref={ainiInputRef}
              type="file"
              accept=".aini"
              style={{ display: "none" }}
              onChange={(event) => onAiniSelect(event.target.files[0])}
            />
          </div>

          <div>
            <button
              type="button"
              disabled={!file || loading}
              onClick={startImport}
              style={{
                padding: "12px 18px",
                borderRadius: 12,
                border: "none",
                background: !file || loading ? "#9db4c7" : "#2e5878",
                color: "#ffffff",
                fontWeight: 700,
                display: "inline-flex",
                gap: 8,
                alignItems: "center",
              }}
            >
              {loading ? <Loader2 size={16} className="spin" /> : null}
              {loading ? "Импорт..." : "Импортировать"}
            </button>
          </div>

          <div
            style={{
              background: "#0f172a",
              color: "#e2e8f0",
              padding: 14,
              borderRadius: 16,
              fontFamily: "Consolas, 'Courier New', monospace",
              fontSize: 12,
              minHeight: 220,
              maxHeight: 260,
              overflowY: "auto",
            }}
          >
            {log.length === 0 ? <div style={{ opacity: 0.6 }}>Лог импорта...</div> : null}
            {log.map((line, index) => (
              <div key={`${line}-${index}`}>{line}</div>
            ))}
          </div>
        </div>
      </div>
    </GraphVisLabLayout>
  );
}
