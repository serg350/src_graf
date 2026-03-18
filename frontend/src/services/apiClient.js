const rawApiBaseUrl = (import.meta.env.VITE_API_BASE_URL || "").trim();

function detectDefaultApiBaseUrl() {
  if (typeof window === "undefined") {
    return "";
  }

  const { protocol, hostname, port } = window.location;
  const isLocalHost = hostname === "localhost" || hostname === "127.0.0.1";

  if (isLocalHost && port === "5173") {
    return `${protocol}//${hostname}:8000`;
  }

  return "";
}

export const API_BASE_URL = (rawApiBaseUrl || detectDefaultApiBaseUrl()).replace(/\/$/, "");

export function buildApiUrl(path) {
  if (!path.startsWith("/")) {
    throw new Error(`API path must start with "/": ${path}`);
  }

  return `${API_BASE_URL}${path}`;
}
