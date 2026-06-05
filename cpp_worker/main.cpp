#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

std::string get_env_or_default(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || std::strlen(value) == 0) {
        return fallback;
    }
    return std::string(value);
}

std::string read_request(int client_fd) {
    std::string request;
    char buffer[4096];

    while (true) {
        ssize_t bytes = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            break;
        }

        request.append(buffer, bytes);

        std::size_t header_end = request.find("\r\n\r\n");
        if (header_end == std::string::npos) {
            continue;
        }

        std::size_t content_length = 0;
        std::string headers = request.substr(0, header_end);
        std::string marker = "Content-Length:";
        std::size_t pos = headers.find(marker);

        if (pos != std::string::npos) {
            pos += marker.size();
            while (pos < headers.size() && headers[pos] == ' ') {
                ++pos;
            }

            std::size_t end = headers.find("\r\n", pos);
            content_length = static_cast<std::size_t>(
                std::stoul(headers.substr(pos, end - pos))
            );
        }

        std::size_t body_start = header_end + 4;
        if (request.size() >= body_start + content_length) {
            break;
        }
    }

    return request;
}

std::string extract_body(const std::string& request) {
    std::size_t pos = request.find("\r\n\r\n");
    if (pos == std::string::npos) {
        return "";
    }
    return request.substr(pos + 4);
}

std::string extract_request_path(const std::string& request) {
    std::size_t method_end = request.find(' ');
    if (method_end == std::string::npos) {
        throw std::runtime_error("Invalid request line");
    }

    std::size_t path_end = request.find(' ', method_end + 1);
    if (path_end == std::string::npos) {
        throw std::runtime_error("Invalid request path");
    }

    return request.substr(method_end + 1, path_end - method_end - 1);
}

std::string extract_binary_operation(const std::string& path) {
    const std::string prefix = "/compute-binary/";
    if (path.compare(0, prefix.size(), prefix) != 0) {
        throw std::runtime_error("Invalid binary compute path");
    }

    std::string operation = path.substr(prefix.size());
    std::size_t query_pos = operation.find('?');
    if (query_pos != std::string::npos) {
        operation = operation.substr(0, query_pos);
    }

    if (operation.empty()) {
        throw std::runtime_error("Missing operation");
    }

    return operation;
}

std::string extract_operation(const std::string& body) {
    std::string key = "\"operation\"";
    std::size_t key_pos = body.find(key);
    if (key_pos == std::string::npos) {
        throw std::runtime_error("Missing operation");
    }

    std::size_t colon = body.find(':', key_pos);
    std::size_t first_quote = body.find('"', colon + 1);
    std::size_t second_quote = body.find('"', first_quote + 1);

    if (colon == std::string::npos || first_quote == std::string::npos || second_quote == std::string::npos) {
        throw std::runtime_error("Invalid operation");
    }

    return body.substr(first_quote + 1, second_quote - first_quote - 1);
}

std::vector<double> extract_x_values(const std::string& body) {
    std::string key = "\"x\"";
    std::size_t key_pos = body.find(key);
    if (key_pos == std::string::npos) {
        throw std::runtime_error("Missing x");
    }

    std::size_t open = body.find('[', key_pos);
    std::size_t close = body.find(']', open + 1);

    if (open == std::string::npos || close == std::string::npos) {
        throw std::runtime_error("Invalid x array");
    }

    std::string array_text = body.substr(open + 1, close - open - 1);
    std::replace(array_text.begin(), array_text.end(), ',', ' ');

    std::istringstream stream(array_text);
    std::vector<double> values;
    double value;

    while (stream >> value) {
        values.push_back(value);
    }

    return values;
}

std::vector<double> extract_binary_x_values(const std::string& body) {
    if (body.size() % sizeof(double) != 0) {
        throw std::runtime_error("Invalid binary x payload size");
    }

    std::vector<double> values(body.size() / sizeof(double));
    if (!values.empty()) {
        std::memcpy(values.data(), body.data(), body.size());
    }

    return values;
}

std::vector<double> compute_values(const std::string& operation, const std::vector<double>& x) {
    std::vector<double> result;
    result.reserve(x.size());

    for (double value : x) {
        if (operation == "sin") {
            result.push_back(std::sin(value));
        } else if (operation == "cos") {
            result.push_back(std::cos(value));
        } else if (operation == "exp") {
            result.push_back(std::exp(value));
        } else {
            throw std::runtime_error("Unsupported operation: " + operation);
        }
    }

    return result;
}

std::string json_escape(const std::string& value) {
    std::ostringstream out;
    for (char ch : value) {
        if (ch == '"') {
            out << "\\\"";
        } else if (ch == '\\') {
            out << "\\\\";
        } else {
            out << ch;
        }
    }
    return out.str();
}

std::string build_compute_response(
    const std::string& operation,
    const std::string& worker_id,
    double elapsed_ms,
    const std::vector<double>& result
) {
    std::ostringstream out;
    out << std::setprecision(17);
    out << "{";
    out << "\"operation\":\"" << json_escape(operation) << "\",";
    out << "\"worker_id\":\"" << json_escape(worker_id) << "\",";
    out << "\"elapsed_ms\":" << elapsed_ms << ",";
    out << "\"result\":[";

    for (std::size_t i = 0; i < result.size(); ++i) {
        if (i > 0) {
            out << ",";
        }
        out << result[i];
    }

    out << "]";
    out << "}";
    return out.str();
}

std::string build_error_response(const std::string& message) {
    std::ostringstream out;
    out << "{";
    out << "\"error\":\"" << json_escape(message) << "\"";
    out << "}";
    return out.str();
}

void send_all(int client_fd, const char* data, std::size_t size) {
    std::size_t sent = 0;
    while (sent < size) {
        ssize_t bytes = send(client_fd, data + sent, size - sent, 0);
        if (bytes <= 0) {
            throw std::runtime_error("Failed to send response");
        }
        sent += static_cast<std::size_t>(bytes);
    }
}

void send_response(int client_fd, int status, const std::string& body) {
    std::string status_text = status == 200 ? "OK" : "Bad Request";

    std::ostringstream response;
    response << "HTTP/1.1 " << status << " " << status_text << "\r\n";
    response << "Content-Type: application/json\r\n";
    response << "Content-Length: " << body.size() << "\r\n";
    response << "Connection: close\r\n";
    response << "\r\n";
    response << body;

    std::string text = response.str();
    send_all(client_fd, text.data(), text.size());
}

void send_binary_response(
    int client_fd,
    const std::string& operation,
    const std::string& worker_id,
    double elapsed_ms,
    const std::vector<double>& result
) {
    const char* body = reinterpret_cast<const char*>(result.data());
    std::size_t body_size = result.size() * sizeof(double);

    std::ostringstream response;
    response << "HTTP/1.1 200 OK\r\n";
    response << "Content-Type: application/octet-stream\r\n";
    response << "Content-Length: " << body_size << "\r\n";
    response << "X-Operation: " << operation << "\r\n";
    response << "X-Worker-Id: " << worker_id << "\r\n";
    response << "X-Elapsed-Ms: " << elapsed_ms << "\r\n";
    response << "Connection: close\r\n";
    response << "\r\n";

    std::string header = response.str();
    send_all(client_fd, header.data(), header.size());
    if (body_size > 0) {
        send_all(client_fd, body, body_size);
    }
}

void handle_client(int client_fd, const std::string& worker_id) {
    try {
        std::string request = read_request(client_fd);
        std::string path = extract_request_path(request);

        if (path == "/health") {
            send_response(client_fd, 200, "{\"status\":\"ok\"}");
            return;
        }

        if (path.compare(0, std::string("/compute-binary/").size(), "/compute-binary/") == 0) {
            std::string operation = extract_binary_operation(path);
            std::string body = extract_body(request);
            std::vector<double> x = extract_binary_x_values(body);

            auto started_at = std::chrono::steady_clock::now();
            std::vector<double> result = compute_values(operation, x);
            auto finished_at = std::chrono::steady_clock::now();

            double elapsed_ms = std::chrono::duration<double, std::milli>(
                finished_at - started_at
            ).count();

            send_binary_response(client_fd, operation, worker_id, elapsed_ms, result);
            return;
        }

        if (path != "/compute") {
            send_response(client_fd, 400, build_error_response("Only POST /compute is supported"));
            return;
        }

        std::string body = extract_body(request);
        std::string operation = extract_operation(body);
        std::vector<double> x = extract_x_values(body);

        auto started_at = std::chrono::steady_clock::now();
        std::vector<double> result = compute_values(operation, x);
        auto finished_at = std::chrono::steady_clock::now();

        double elapsed_ms = std::chrono::duration<double, std::milli>(
            finished_at - started_at
        ).count();

        send_response(
            client_fd,
            200,
            build_compute_response(operation, worker_id, elapsed_ms, result)
        );
    } catch (const std::exception& exc) {
        send_response(client_fd, 400, build_error_response(exc.what()));
    }
}

int main() {
    std::string worker_id = get_env_or_default("WORKER_ID", "cpp-worker");
    int port = std::stoi(get_env_or_default("PORT", "9000"));

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        std::cerr << "Failed to bind port " << port << "\n";
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 64) < 0) {
        std::cerr << "Failed to listen\n";
        close(server_fd);
        return 1;
    }

    std::cout << worker_id << " listening on port " << port << std::endl;

    while (true) {
        sockaddr_in client_address{};
        socklen_t client_length = sizeof(client_address);

        int client_fd = accept(
            server_fd,
            reinterpret_cast<sockaddr*>(&client_address),
            &client_length
        );

        if (client_fd < 0) {
            continue;
        }

        handle_client(client_fd, worker_id);
        close(client_fd);
    }

    close(server_fd);
    return 0;
}
