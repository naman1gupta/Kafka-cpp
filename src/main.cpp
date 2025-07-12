#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);




    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";
    
    
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);


    if (client_fd == -1) {
        throw std::runtime_error("Failed to accept connection");
    }
    // Read the request
    size_t RESPONSE_MAX_SIZE = 1024;
    void* buff = malloc(RESPONSE_MAX_SIZE);
    
    ssize_t bytes_read = read(client_fd, buff, RESPONSE_MAX_SIZE);


    printf("Read %ld bytes.\n", bytes_read);

    uint32_t message_size_nw =  *((uint32_t*) buff);
    int32_t  message_size = ntohl(message_size_nw);

    uint16_t* cursor =  (uint16_t*) buff;

    cursor += 2;
    uint16_t api_key_nw =  *((uint16_t*) cursor);
    int16_t api_key = ntohs(api_key_nw);

    cursor += 1;
    uint16_t api_version_nw =  *((uint16_t*) cursor);
    int16_t api_version = ntohs(api_version_nw);

    cursor += 1;

    uint32_t correlation_id_nw = *((uint32_t*) cursor);
    int32_t correlation_id = ntohl(correlation_id_nw);


    // printf("message_size: %d at addr %ld\n", message_size, buff);
    // printf("api_key: %d at addr %ld\n", api_key, cursor);
    // printf("api_version: %d at addr %ld\n", api_version, cursor);
    // printf("correlation_id: %d at addr %ld\n", correlation_id, cursor);

    // char* tmp = (char*) buff;

    // for (size_t i  = 0; i < 10; i++) {
    //     printf("%ld: byte %ld, %d\n",tmp, i,*tmp);
    //     tmp++;
    // }

    free(buff);

    // Tag buffer

    uint8_t tag_buffer = 0x00;

    // Error code

    uint16_t error_code = 0;

    // Check API-version
    if (api_version < 0) error_code = 35;
    if (api_version > 4) error_code = 35;

    uint16_t error_code_nw = htons(error_code);

    // Throttle time

    uint32_t throttle_time_ms = htonl(10);

    // API-keys

    uint16_t api_versions[] = {
        htons(18),
        htons(0),
        htons(4)
    };

    uint8_t num_of_api_keys = 1 + 1;

    // Message size

    uint32_t response_size = htonl(
        sizeof(correlation_id) +
        sizeof(error_code) + 
        sizeof(num_of_api_keys) +
        sizeof(api_versions) +
        sizeof(tag_buffer) +
        sizeof(throttle_time_ms) +
        sizeof(tag_buffer)
    );

    // Send response
    ssize_t bytes_sent = 0;
    bytes_sent += send(client_fd, &response_size, sizeof(response_size), 0);
    bytes_sent += send(client_fd, &correlation_id_nw, sizeof(correlation_id_nw), 0);
    bytes_sent += send(client_fd, &error_code_nw , sizeof(error_code), 0);
    bytes_sent += send(client_fd, &num_of_api_keys, sizeof(num_of_api_keys), 0);
    bytes_sent += send(client_fd, &api_versions, sizeof(api_versions), 0);
    bytes_sent += send(client_fd, &tag_buffer, sizeof(tag_buffer), 0);
    bytes_sent += send(client_fd, &throttle_time_ms, sizeof(throttle_time_ms), 0);
    bytes_sent += send(client_fd, &tag_buffer, sizeof(tag_buffer), 0);

    


    if (bytes_sent == -1) {
        throw std::runtime_error("Failed to send response");
    }
    printf("Sent %ld bytes.\n", bytes_sent);


    // Wrap-up
    close(client_fd);
    close(server_fd);
    return 0;
}