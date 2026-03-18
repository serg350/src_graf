import React, { useRef, useState } from "react";
import { UploadCloud, FileText, Loader2 } from "lucide-react";

import { buildApiUrl } from "../services/apiClient";

export default function GraphImportPage() {
  const inputRef = useRef(null);
  const ainiInputRef = useRef(null);

  const [file, setFile] = useState(null);
  const [ainiFile, setAiniFile] = useState(null);
  const [dragOver, setDragOver] = useState(false);
  const [loading, setLoading] = useState(false);
  const [log, setLog] = useState([]);

  const logLine = (text) =>
    setLog((l) => [...l, `[${new Date().toLocaleTimeString()}] ${text}`]);

  const onFileSelect = (f) => {
    if (!f) return;
    setFile(f);
    logLine(`Graph file selected: ${f.name}`);
  };

  const onAiniSelect = (f) => {
    if (!f) return;
    if (!f.name.toLowerCase().endsWith(".aini")) {
      logLine("Error: aINI file must use .aini extension");
      return;
    }
    setAiniFile(f);
    logLine(`aINI file selected: ${f.name}`);
  };

  const onDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    onFileSelect(e.dataTransfer.files[0]);
  };

  const startImport = async () => {
    if (!file || loading) return;

    setLoading(true);
    setLog([]);
    logLine("Starting import...");

    const formData = new FormData();
    formData.append("dot_file", file);
    if (ainiFile) {
      formData.append("aini_file", ainiFile);
    }

    try {
      const res = await fetch(buildApiUrl("/api/comwpc/graph/import-dot/"), {
        method: "POST",
        credentials: "include",
        body: formData,
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      const data = await res.json();

      if (data.success) {
        logLine("Import completed successfully");
        logLine(`Graph ID: ${data.graph_id}`);

        setTimeout(() => {
          window.location.href = `/graphs/${data.graph_id}`;
        }, 800);
      } else {
        logLine(`Import error: ${data.error || "Unknown error"}`);
      }
    } catch (err) {
      logLine(`Error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        height: "100vh",
        padding: 24,
        background: "#f8fafc",
        boxSizing: "border-box",
      }}
    >
      <div
        style={{
          maxWidth: 900,
          margin: "0 auto",
          background: "#fff",
          borderRadius: 16,
          padding: 24,
          boxShadow: "0 10px 30px rgba(0,0,0,0.06)",
        }}
      >
        <h2 style={{ marginBottom: 16 }}>Graph Import</h2>

        <div
          onClick={() => inputRef.current.click()}
          onDragOver={(e) => {
            e.preventDefault();
            setDragOver(true);
          }}
          onDragLeave={() => setDragOver(false)}
          onDrop={onDrop}
          style={{
            border: "2px dashed #c7d2fe",
            borderRadius: 14,
            padding: 32,
            textAlign: "center",
            cursor: "pointer",
            background: dragOver ? "#eef2ff" : "#fafafa",
            transition: "all 0.15s",
          }}
        >
          <UploadCloud size={36} color="#4f46e5" />
          <div style={{ marginTop: 12, fontWeight: 600 }}>
            Drag and drop graph file here
          </div>
          <div style={{ fontSize: 13, color: "#64748b" }}>
            or click to choose (.dot / .adot)
          </div>

          {file && (
            <div
              style={{
                marginTop: 14,
                display: "flex",
                justifyContent: "center",
                gap: 8,
                alignItems: "center",
                fontSize: 13,
              }}
            >
              <FileText size={16} />
              {file.name}
            </div>
          )}

          <input
            ref={inputRef}
            type="file"
            accept=".dot,.adot"
            style={{ display: "none" }}
            onChange={(e) => onFileSelect(e.target.files[0])}
          />
        </div>

        <div style={{ marginTop: 12 }}>
          <button
            type="button"
            onClick={() => ainiInputRef.current.click()}
            style={{
              padding: "8px 12px",
              borderRadius: 10,
              background: "#334155",
              color: "#fff",
            }}
          >
            Choose aINI (optional)
          </button>
          {ainiFile && (
            <span style={{ marginLeft: 10, fontSize: 13 }}>{ainiFile.name}</span>
          )}
          <input
            ref={ainiInputRef}
            type="file"
            accept=".aini"
            style={{ display: "none" }}
            onChange={(e) => onAiniSelect(e.target.files[0])}
          />
        </div>

        <div style={{ marginTop: 16 }}>
          <button
            className="btn"
            disabled={!file || loading}
            onClick={startImport}
            style={{
              padding: "10px 16px",
              borderRadius: 10,
              background: "#4f46e5",
              color: "#fff",
              display: "flex",
              alignItems: "center",
              gap: 8,
              opacity: !file || loading ? 0.6 : 1,
            }}
          >
            {loading && <Loader2 size={16} className="spin" />}
            {loading ? "Import..." : "Import"}
          </button>
        </div>

        <div
          style={{
            marginTop: 24,
            background: "#0f172a",
            color: "#e5e7eb",
            padding: 14,
            borderRadius: 12,
            fontFamily: "monospace",
            fontSize: 12,
            height: 200,
            overflowY: "auto",
          }}
        >
          {log.length === 0 && (
            <div style={{ opacity: 0.5 }}>Import log...</div>
          )}
          {log.map((l, i) => (
            <div key={i}>{l}</div>
          ))}
        </div>
      </div>
    </div>
  );
}
