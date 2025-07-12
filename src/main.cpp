#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

#include "KafkaResponse.h"
class Socket {
private:
    int fd_ = -1;

public:
    Socket(int domain, int type, int protocol = 0) {
        fd_ = socket(domain, type, protocol);
        if (fd_ < 0) {
            throw std::system_error(errno, std::system_category(), "socket creation failed");
        }
    }

    // TODO
    // check unsuccesfull completion (if close return -1)
    ~Socket() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    void bind(sockaddr_in &address) {
        if (::bind(fd_, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) != 0) {
            throw std::system_error(errno, std::system_category(), "bind failed");
        }
    }

    void listen(int backlog) {
        if (::listen(fd_, backlog) != 0) {
            throw std::system_error(errno, std::system_category(), "listen failed");
        }
    }

    int accept(sockaddr_in &client_addr) {
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = ::accept(fd_, reinterpret_cast<struct sockaddr *>(&client_addr), &addr_len);
        if (client_fd < 0) {
            throw std::system_error(errno, std::system_category(), "accept failed");
        }
        return client_fd;
    }

    operator int() const {
        return fd_;
    }
};

class ClientConnection {
private:
    int fd_;
    std::atomic<bool> is_active_ = true;

public:
    explicit ClientConnection(int fd) : fd_(fd) {
    }
    ~ClientConnection() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    std::vector<char> read_full() {
        std::vector<char> buffer(1024);
        ssize_t n = recv(fd_, buffer.data(), buffer.size(), 0);
        if (n == 0) {
            throw std::runtime_error("Connection closed by client");
            is_active_ = false;
        }
        if (n < 0) {
            is_active_ = false;
            throw std::system_error(errno, std::system_category(), "recv failed");
        }
        buffer.resize(n);
        return buffer;
    }
    void write_full(const char *data, size_t size) {
        write(fd_, data, size);
    }

    operator int() const {
        return fd_;
    }
    bool IsActive() const {
        return is_active_;
    }
};

// https://www.youtube.com/watch?v=xGDLkt-jBJ4&t=2659s
// NB: smart_ptr pass by value
void HandleClient(std::unique_ptr<ClientConnection> client) {
    while (client->IsActive()) {
        try {
            std::cout << "Client connected" << std::endl;
            auto buffer = client->read_full();
            ResponseHandler response_handler(buffer.data());
            const char *response_buffer = response_handler.GetResponseBuffer();
            size_t response_size = response_handler.GetResponseSize();
            client->write_full(response_buffer, response_size);
            std::cout << "Client handled" << std::endl;
        } catch (std::runtime_error &e) {
            std::cout << "Client disconnected" << std::endl;
        }
    }
}
int main(int argc, char *argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    try {
        Socket server_fd(AF_INET, SOCK_STREAM, 0);
        // Since the tester restarts your program quite often, setting SO_REUSEADDR
        // ensures that we don't run into 'Address already in use' errors
        int reuse = 1;
        if (setsockopt(static_cast<int>(server_fd), SOL_SOCKET, SO_REUSEADDR, &reuse,
                       sizeof(reuse)) < 0) {
            close(server_fd);
            std::cout << "setsockopt failed: " << std::endl;
            return 1;
        }
        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(9092);

        server_fd.bind(server_addr);
        int connection_backlog = 5;
        server_fd.listen(connection_backlog);

        std::cout << "Listening on port 9092..." << std::endl;

        std::vector<std::thread> workers;
        while (true) {
            try {
                struct sockaddr_in client_addr{};

                socklen_t client_addr_len = sizeof(client_addr);
                int client_fd = server_fd.accept(client_addr);
                std::unique_ptr<ClientConnection> client =
                    std::make_unique<ClientConnection>(client_fd);
                workers.emplace_back(HandleClient, std::move(client));
            } catch (std::exception &e) {
                std::cout << "Error occured" << e.what() << std::endl;
            }
        }

        for (auto &t : workers) {
            if (t.joinable())
                t.join();
        }

    } catch (const std::exception &e) {
        std::cout << "Server error: " << e.what() << std::endl;
    }
    return 0;
}