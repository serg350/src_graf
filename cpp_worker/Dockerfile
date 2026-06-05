FROM gcc:13-bookworm

WORKDIR /app

COPY main.cpp .
RUN g++ -std=c++17 -O2 -pthread main.cpp -o cpp_worker

ENV PORT=9000
ENV WORKER_ID=cpp-worker

EXPOSE 9000

CMD ["./cpp_worker"]