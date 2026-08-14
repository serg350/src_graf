#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

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

std::uint64_t current_memory_bytes() {
    std::ifstream statm("/proc/self/statm");
    long pages = 0;
    long resident_pages = 0;

    if (statm >> pages >> resident_pages) {
        long page_size = sysconf(_SC_PAGESIZE);
        if (page_size > 0 && resident_pages > 0) {
            return static_cast<std::uint64_t>(resident_pages) *
                   static_cast<std::uint64_t>(page_size);
        }
    }

    struct rusage usage {};
    if (getrusage(RUSAGE_SELF, &usage) == 0 && usage.ru_maxrss > 0) {
        return static_cast<std::uint64_t>(usage.ru_maxrss) * 1024ULL;
    }

    return 0;
}

struct WorkerMetrics {
    double cpu_percent;
    std::uint64_t memory_bytes;
};

WorkerMetrics collect_worker_metrics(std::clock_t started_cpu, double elapsed_ms) {
    std::clock_t finished_cpu = std::clock();
    double cpu_ms = 1000.0 * static_cast<double>(finished_cpu - started_cpu) /
                    static_cast<double>(CLOCKS_PER_SEC);
    double cpu_percent = elapsed_ms > 0.0 ? (cpu_ms / elapsed_ms) * 100.0 : 0.0;

    return WorkerMetrics{
        cpu_percent,
        current_memory_bytes(),
    };
}

std::string build_compute_response(
    const std::string& operation,
    const std::string& worker_id,
    double elapsed_ms,
    const WorkerMetrics& metrics,
    const std::vector<double>& result
) {
    std::ostringstream out;
    out << std::setprecision(17);
    out << "{";
    out << "\"operation\":\"" << json_escape(operation) << "\",";
    out << "\"worker_id\":\"" << json_escape(worker_id) << "\",";
    out << "\"elapsed_ms\":" << elapsed_ms << ",";
    out << "\"cpu_percent\":" << metrics.cpu_percent << ",";
    out << "\"memory_bytes\":" << metrics.memory_bytes << ",";
    out << "\"memory_mb\":" << (static_cast<double>(metrics.memory_bytes) / 1048576.0) << ",";
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
    const WorkerMetrics& metrics,
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
    response << "X-Cpu-Percent: " << metrics.cpu_percent << "\r\n";
    response << "X-Memory-Bytes: " << metrics.memory_bytes << "\r\n";
    response << "X-Memory-Mb: " << (static_cast<double>(metrics.memory_bytes) / 1048576.0) << "\r\n";
    response << "Connection: close\r\n";
    response << "\r\n";

    std::string header = response.str();
    send_all(client_fd, header.data(), header.size());
    if (body_size > 0) {
        send_all(client_fd, body, body_size);
    }
}

struct MatrixBenchmarkResult {
    int size;
    int repeats;
    std::size_t dense_entries;
    std::size_t matrix_bytes;
    double response;
    double elapsed_ms;
};

int bounded_int_param(
    const json& params,
    const std::string& name,
    int fallback,
    int lower,
    int upper
) {
    int value = fallback;
    if (params.contains(name) && !params[name].is_null()) {
        value = params[name].get<int>();
    }
    return std::max(lower, std::min(value, upper));
}

MatrixBenchmarkResult run_matrix_benchmark(
    double vf,
    double theta_deg,
    double axial_modulus,
    double transverse_modulus,
    const json& params
) {
    int matrix_size = bounded_int_param(params, "matrix_size", 0, 0, 4096);
    int work_repeats = bounded_int_param(params, "matrix_work_repeats", 0, 0, 100);

    std::size_t dense_entries =
        static_cast<std::size_t>(matrix_size) *
        static_cast<std::size_t>(matrix_size);
    std::size_t matrix_bytes = dense_entries * sizeof(double);

    if (matrix_size == 0 || work_repeats == 0) {
        return {matrix_size, work_repeats, dense_entries, matrix_bytes, 0.0, 0.0};
    }

    auto started_at = std::chrono::steady_clock::now();

    std::vector<double> dense_matrix(dense_entries);
    std::vector<double> x(static_cast<std::size_t>(matrix_size));
    std::vector<double> y(static_cast<std::size_t>(matrix_size));

    double theta_rad = theta_deg * 3.14159265358979323846 / 180.0;
    double phase = 0.011 + 0.003 * vf + 0.0001 * theta_deg;
    double stiffness_scale = 0.5 * (axial_modulus + transverse_modulus);

    for (int i = 0; i < matrix_size; ++i) {
        double normalized_i = static_cast<double>(i + 1) / matrix_size;
        x[static_cast<std::size_t>(i)] =
            std::sin((i + 1) * phase) +
            0.25 * std::cos(theta_rad + normalized_i);
    }

    for (int i = 0; i < matrix_size; ++i) {
        double normalized_i = static_cast<double>(i + 1) / matrix_size;
        double diagonal =
            1.0 +
            0.01 * stiffness_scale +
            0.2 * vf +
            0.05 * std::sin(theta_rad + normalized_i);

        for (int j = 0; j < matrix_size; ++j) {
            double normalized_j = static_cast<double>(j + 1) / matrix_size;
            double distance = static_cast<double>(std::abs(i - j) + 1);
            double orientation_kernel =
                std::cos(theta_rad + normalized_i * normalized_j);
            double coupling =
                0.00005 * (1.0 + vf) * orientation_kernel / distance;

            dense_matrix[
                static_cast<std::size_t>(i) * matrix_size +
                static_cast<std::size_t>(j)
            ] = (i == j ? diagonal : coupling);
        }
    }

    double accumulated_response = 0.0;

    for (int repeat = 0; repeat < work_repeats; ++repeat) {
        double rayleigh_numerator = 0.0;
        double rayleigh_denominator = 0.0;

        for (int i = 0; i < matrix_size; ++i) {
            const double* row =
                dense_matrix.data() +
                static_cast<std::size_t>(i) * matrix_size;
            double value = 0.0;

            for (int j = 0; j < matrix_size; ++j) {
                value += row[j] * x[static_cast<std::size_t>(j)];
            }

            y[static_cast<std::size_t>(i)] = value;
            rayleigh_numerator += x[static_cast<std::size_t>(i)] * value;
            rayleigh_denominator +=
                x[static_cast<std::size_t>(i)] *
                x[static_cast<std::size_t>(i)];
        }

        double rayleigh =
            rayleigh_numerator / (rayleigh_denominator + 1.0e-12);
        double norm = 0.0;

        for (int i = 0; i < matrix_size; ++i) {
            norm += y[static_cast<std::size_t>(i)] * y[static_cast<std::size_t>(i)];
        }
        norm = std::sqrt(norm / matrix_size) + 1.0e-12;

        accumulated_response += std::abs(rayleigh);

        for (int i = 0; i < matrix_size; ++i) {
            x[static_cast<std::size_t>(i)] = y[static_cast<std::size_t>(i)] / norm;
        }
    }

    auto finished_at = std::chrono::steady_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(
        finished_at - started_at
    ).count();

    return {
        matrix_size,
        work_repeats,
        dense_entries,
        matrix_bytes,
        accumulated_response / work_repeats,
        elapsed_ms
    };
}

json evaluate_particle(const json& particle, const json& params) {
    int particle_index = particle.at("particle_index").get<int>();
    auto position = particle.at("position");

    double vf = position.at(0).get<double>();
    double theta_deg = position.at(1).get<double>();

    double e_fiber = params.value("E_fiber", 230.0);
    double e_matrix = params.value("E_matrix", 3.5);
    double rho_fiber = params.value("rho_fiber", 1.8);
    double rho_matrix = params.value("rho_matrix", 1.2);
    double e_target = params.value("E_target", 120.0);
    double density_weight = params.value("density_weight", 0.0);

    double axial_modulus = vf * e_fiber + (1.0 - vf) * e_matrix;
    double transverse_modulus = 1.0 / (vf / e_fiber + (1.0 - vf) / e_matrix);
    double theta_rad = theta_deg * 3.14159265358979323846 / 180.0;
    double orientation_factor = std::pow(std::cos(theta_rad), 4.0);

    double e_effective =
        orientation_factor * axial_modulus +
        (1.0 - orientation_factor) * transverse_modulus;

    MatrixBenchmarkResult matrix_benchmark = run_matrix_benchmark(
        vf,
        theta_deg,
        axial_modulus,
        transverse_modulus,
        params
    );
    double matrix_correction_weight = params.value("matrix_correction_weight", 0.0);
    double matrix_correction =
        matrix_correction_weight * matrix_benchmark.response;
    e_effective += matrix_correction;

    double density = vf * rho_fiber + (1.0 - vf) * rho_matrix;
    double score = std::abs(e_effective - e_target);

    if (params.contains("density_target") && !params["density_target"].is_null()) {
        double density_target = params["density_target"].get<double>();
        score += density_weight * std::abs(density - density_target);
    }

    return {
        {"particle_index", particle_index},
        {"score", score},
        {"properties", {
            {"Vf", vf},
            {"theta_deg", theta_deg},
            {"E_effective", e_effective},
            {"density", density},
            {"axial_modulus", axial_modulus},
            {"transverse_modulus", transverse_modulus},
            {"orientation_factor", orientation_factor},
            {"matrix_benchmark", {
                {"storage", "full_dense"},
                {"size", matrix_benchmark.size},
                {"work_repeats", matrix_benchmark.repeats},
                {"dense_entries", matrix_benchmark.dense_entries},
                {"matrix_bytes", matrix_benchmark.matrix_bytes},
                {"response", matrix_benchmark.response},
                {"elapsed_ms", matrix_benchmark.elapsed_ms},
                {"correction", matrix_correction}
            }}
        }}
    };
}


json handle_pso_evaluate_shard(const json& payload) {
    json params = payload.value("params", json::object());
    json results = json::array();

    for (const auto& particle : payload.at("particles")) {
        results.push_back(evaluate_particle(particle, params));
    }

    return {
        {"shard_index", payload.value("shard_index", -1)},
        {"shard_count", payload.value("shard_count", 1)},
        {"results", results}
    };
}


json handle_task_operation(const std::string& operation, const json& payload) {
    if (operation == "pso_evaluate_shard") {
        return handle_pso_evaluate_shard(payload);
    }

    throw std::runtime_error("Unsupported task operation: " + operation);
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
            std::clock_t started_cpu = std::clock();
            std::vector<double> result = compute_values(operation, x);
            auto finished_at = std::chrono::steady_clock::now();

            double elapsed_ms = std::chrono::duration<double, std::milli>(
                finished_at - started_at
            ).count();
            WorkerMetrics metrics = collect_worker_metrics(started_cpu, elapsed_ms);

            send_binary_response(client_fd, operation, worker_id, elapsed_ms, metrics, result);
            return;
        }

        if (path == "/task") {
            std::string body = extract_body(request);
            json request_json = json::parse(body);

            std::string operation = request_json.at("operation").get<std::string>();
            json payload = request_json.value("payload", json::object());

            auto started_at = std::chrono::steady_clock::now();
            std::clock_t started_cpu = std::clock();
            json task_result = handle_task_operation(operation, payload);
            auto finished_at = std::chrono::steady_clock::now();

            double elapsed_ms = std::chrono::duration<double, std::milli>(
                finished_at - started_at
            ).count();
            WorkerMetrics metrics = collect_worker_metrics(started_cpu, elapsed_ms);

            task_result["operation"] = operation;
            task_result["worker_id"] = worker_id;
            task_result["elapsed_ms"] = elapsed_ms;
            task_result["cpu_percent"] = metrics.cpu_percent;
            task_result["memory_bytes"] = metrics.memory_bytes;
            task_result["memory_mb"] = static_cast<double>(metrics.memory_bytes) / 1048576.0;

            send_response(client_fd, 200, task_result.dump());
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
        std::clock_t started_cpu = std::clock();
        std::vector<double> result = compute_values(operation, x);
        auto finished_at = std::chrono::steady_clock::now();

        double elapsed_ms = std::chrono::duration<double, std::milli>(
            finished_at - started_at
        ).count();
        WorkerMetrics metrics = collect_worker_metrics(started_cpu, elapsed_ms);

        send_response(
            client_fd,
            200,
            build_compute_response(operation, worker_id, elapsed_ms, metrics, result)
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
