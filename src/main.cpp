#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string_view>
#include <cassert>

inline void write_int32_be(uint8_t **dest, int32_t value)
{
    (*dest)[0] = (value >> 24) & 0xFF;
    (*dest)[1] = (value >> 16) & 0xFF;
    (*dest)[2] = (value >> 8) & 0xFF;
    (*dest)[3] = value & 0xFF;
    (*dest) += 4;
}
inline void write_int64_be(uint8_t **dest, int64_t value)
{
    (*dest)[0] = (value >> 56) & 0xFF;
    (*dest)[1] = (value >> 48) & 0xFF;
    (*dest)[2] = (value >> 40) & 0xFF;
    (*dest)[3] = (value >> 32) & 0xFF;
    (*dest)[4] = (value >> 24) & 0xFF;
    (*dest)[5] = (value >> 16) & 0xFF;
    (*dest)[6] = (value >> 8) & 0xFF;
    (*dest)[7] = value & 0xFF;
    (*dest) += 8;
}
size_t varint_encode(uint64_t value, uint8_t *out)
{
    uint8_t tmp[10];
    int i = 0;
    // Extract 7-bit groups from the value
    do
    {
        tmp[i++] = value & 0x7F;
        value >>= 7;
    } while (value > 0);

    // Write from most significant to least
    size_t out_len = i;
    for (int j = i - 1; j >= 0; --j)
    {
        uint8_t byte = tmp[j];
        if (j != 0)
            byte |= 0x80;
        *out++ = byte;
    }
    return out_len;
}

inline void write_int16_be(uint8_t **dest, int16_t value)
{
    (*dest)[0] = (value >> 8) & 0xFF;
    (*dest)[1] = value & 0xFF;
    (*dest) += 2;
}

inline void copy_bytes(uint8_t **dest, char *src, int cnt)
{
    for (int i = 0; i < cnt; ++i)
    {
        *(*dest)++ = src[i];
    }
}

void hexdump(const void *data, size_t size)
{
    const unsigned char *byte = (const unsigned char *)data;
    char buffer[4096];
    size_t buf_used = 0;
    size_t i, j;

    for (i = 0; i < size; i += 16)
    {
        char line[80]; // A line won't exceed 80 chars
        int len = snprintf(line, sizeof(line), "%08zx  ", i);

        // Hex part
        for (j = 0; j < 16; j++)
        {
            if (i + j < size)
                len += snprintf(line + len, sizeof(line) - len, "%02x ", byte[i + j]);
            else
                len += snprintf(line + len, sizeof(line) - len, "   ");
            if (j == 7)
                len += snprintf(line + len, sizeof(line) - len, " ");
        }

        // ASCII part
        len += snprintf(line + len, sizeof(line) - len, " |");
        for (j = 0; j < 16 && i + j < size; j++)
        {
            unsigned char ch = byte[i + j];
            len += snprintf(line + len, sizeof(line) - len, "%c", isprint(ch) ? ch : '.');
        }
        len += snprintf(line + len, sizeof(line) - len, "|\n");

        // Append line to buffer
        if (buf_used + len < sizeof(buffer))
        {
            memcpy(buffer + buf_used, line, len);
            buf_used += len;
        }
        else
        {
            // Prevent buffer overflow
            break;
        }
    }

    // Null-terminate and print once
    buffer[buf_used] = '\0';
    printf("Idx       | Hex                                             | ASCII\n"
           "----------+-------------------------------------------------+-----------------\n"
           "%s",
           buffer);
}

int main(int argc, char *argv[])
{

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
    {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0)
    {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    std::cerr << "Logs from your program will appear here!\n";

    while (1)
    {
        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
        if (fork() != 0)
            continue;
        std::cout << "Client connected\n";

        char req_buf[1024];
        uint8_t resp_buf[1024];
        while (size_t bytes_read = read(client_fd, req_buf, 1024))
        {

            req_buf[bytes_read] = 0;
            memset(resp_buf, 0, 1024);
            uint8_t *ptr = resp_buf + 4;
            constexpr int cor_id_offset = 8;
            copy_bytes(&ptr, &req_buf[cor_id_offset], 4); // correlation ID

            constexpr int req_api_offset = 4;
            int16_t request_api_key = ((uint8_t)req_buf[req_api_offset + 0] >> 8 |
                                       (uint8_t)req_buf[req_api_offset + 1]);

            int16_t request_api_version = ((uint8_t)req_buf[req_api_offset + 2] >> 8 |
                                           (uint8_t)req_buf[req_api_offset + 3]);

            constexpr int8_t TAG_BUFFER = 0;

            if (request_api_key == 0x004b) // DescribeTopicPartitions
            {
                constexpr int client_id_offset = cor_id_offset + 4;
                int client_id_len = ((uint8_t)req_buf[client_id_offset] | (uint8_t)req_buf[client_id_offset + 1]) + /* TAG_BUFFER BYTE */ 1 + /* LENGTH BYTES */ 2;
                int topic_offset = client_id_offset + client_id_len;
                *ptr++ = TAG_BUFFER;
                write_int32_be(&ptr, 0); // throttle_time_ms
                int8_t topic_length = req_buf[topic_offset++];
                *ptr++ = topic_length;

                int log_fd = open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", O_RDONLY, S_IRUSR);
                assert(log_fd != -1);
                uint8_t metadata[1024];
                size_t log_bytes = read(log_fd, metadata, 1024);

                // hexdump(metadata, log_bytes);
                constexpr int log_topic_offset = 162;

                for (int8_t i = 1; i < topic_length; ++i)
                {
                    std::string_view topic_name(&req_buf[topic_offset + 1]);
                    // std::cout << i << ": topic_name: " << topic_name << '\n';

                    int curr_idx = 0;
                    bool found_topic = false;
                    int found_topic_id_offset = 0;
                    int8_t partitions_length = 1;
                    while (curr_idx < log_bytes)
                    {
                        int batch_length_idx = curr_idx + 8; //  Batch Length (4 bytes)
                        int32_t batch_len = ((uint8_t)metadata[batch_length_idx + 0] >> 24 |
                                             (uint8_t)metadata[batch_length_idx + 1] >> 16 |
                                             (uint8_t)metadata[batch_length_idx + 2] >> 8 |
                                             (uint8_t)metadata[batch_length_idx + 3]);

                        // std::cout << "batch_len: " << batch_len << '\n';
                        if (batch_len <= 0)
                            break;
                        int next_part = curr_idx + 12 + batch_len;

                        int records_len_offset = curr_idx + 57;
                        int32_t records_len = ((uint8_t)metadata[records_len_offset + 0] >> 24 |
                                               (uint8_t)metadata[records_len_offset + 1] >> 16 |
                                               (uint8_t)metadata[records_len_offset + 2] >> 8 |
                                               (uint8_t)metadata[records_len_offset + 3]);

                        int this_topic_idx = curr_idx + 71;
                        std::string_view this_topic_name((char *)metadata + this_topic_idx);
                        if (this_topic_name == topic_name)
                        {
                            found_topic = true;
                            found_topic_id_offset = this_topic_idx;
                            partitions_length = records_len;
                            break;
                        }
                        curr_idx = next_part;
                    }
                    if (found_topic)
                    {
                        write_int16_be(&ptr, 0); // (NO_ERROR)
                        copy_bytes(&ptr, &req_buf[topic_offset], topic_name.length() + 1);
                        copy_bytes(&ptr, (char *)metadata + found_topic_id_offset + topic_name.length(), 16);
                        *ptr++ = 0;                 // topic.is_internal
                        *ptr++ = partitions_length; // # of partitions  == 1
                        for (int i = 0; i < (partitions_length - 1); ++i)
                        {
                            write_int16_be(&ptr, 0); // # Partition 0 - Error Code (INT16, 0)
                            write_int32_be(&ptr, i); // # Partition 0 - Partition Index (INT32, 0)
                            write_int32_be(&ptr, 1); // # Partition 0 - Leader ID (INT32, 1)
                            write_int32_be(&ptr, 0); // # Partition 0 - Leader Epoch (INT32, 0)
                            *ptr++ = 2;              // # Partition 0 - Replica nodes length + 1 (1 replica node)
                            write_int32_be(&ptr, 1); // #   - Replica node 1 (INT32, 1)
                            *ptr++ = 2;              // # Partition 0 - ISR Nodes length + 1 (INT32, 2)
                            write_int32_be(&ptr, 1); // #   - ISR Node 1 (INT32, 1)
                            *ptr++ = 1;              // # Partition 0 - Eligible Leader Replicas count + 1 (INT32, 1) => 0 leader replicas
                            *ptr++ = 1;              // # Partition 0 - Last Known ELR count + 1 (INT32, 1) => 0 last known leader replica
                            *ptr++ = 1;              // # Partition 0 - Offline replicas count + 1 (INT32, 1) => 0 offline replicas
                            *ptr++ = 0;              // # Empty tag buffer
                        }

                        write_int32_be(&ptr, 0x00000df8); // Topic Authorized Operations
                        *ptr++ = TAG_BUFFER;
                    }
                    else
                    {
                        write_int16_be(&ptr, 3); // (UNKNOWN_TOPIC)
                        copy_bytes(&ptr, &req_buf[topic_offset], topic_name.length() + 1);
                        for (int i = 0; i < 16; ++i) // topic_id
                            *ptr++ = 0;

                        *ptr++ = 0;                       // topic.is_internal
                        *ptr++ = 1;                       // topic.partition
                        write_int32_be(&ptr, 0x00000df8); // Topic Authorized Operations
                        *ptr++ = TAG_BUFFER;
                    }
                    topic_offset += topic_name.length() + 2;
                }

                *ptr++ = 0xFF; // Next Cursor (0xff, indicating a null value.)
                *ptr++ = TAG_BUFFER;
            }

            if (request_api_key == 0x0001) // Fetch
            {
                *ptr++ = TAG_BUFFER;
                write_int32_be(&ptr, 0); // (throttle_time_ms)

                constexpr int client_id_offset = cor_id_offset + 4;
                int client_id_len = ((uint8_t)req_buf[client_id_offset] | (uint8_t)req_buf[client_id_offset + 1]) + /* TAG_BUFFER BYTE */ 1 + /* LENGTH BYTES */ 2;
                int topic_offset = client_id_offset + client_id_len + 21;
                int8_t topic_length = req_buf[topic_offset++];

                write_int16_be(&ptr, 0); // (NO_ERROR)
                write_int32_be(&ptr, 0); // (session_id)
                *ptr++ = topic_length;   // (.num_responses) = 0

                int log_fd = open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", O_RDONLY, S_IRUSR);
                assert(log_fd != -1);
                uint8_t metadata[1024];
                size_t total_bytes_in_log = read(log_fd, metadata, 1024);
                constexpr int log_topic_offset = 162;

                for (int8_t i = 1; i < topic_length; ++i)
                {
                    copy_bytes(&ptr, req_buf + topic_offset, 16);

                    int curr_log_idx = 0;
                    bool found_topic = false;
                    int found_topic_id_offset = 0;
                    int8_t partitions_length = req_buf[topic_offset + 16];
                    int16_t error_code = 100; // (UNKNOWN_TOPIC_ID)
                    size_t compact_records_length = 0;
                    uint8_t record_data[1024];

                    while (curr_log_idx < total_bytes_in_log)
                    {
                        int batch_length_idx = curr_log_idx + 8; //  Batch Length (4 bytes)
                        int32_t batch_len = ((uint8_t)metadata[batch_length_idx + 0] >> 24 |
                                             (uint8_t)metadata[batch_length_idx + 1] >> 16 |
                                             (uint8_t)metadata[batch_length_idx + 2] >> 8 |
                                             (uint8_t)metadata[batch_length_idx + 3]);

                        if (batch_len <= 0)
                            break;
                        int next_part = curr_log_idx + 12 + batch_len;

                        int records_len_offset = curr_log_idx + 57;
                        int32_t records_len = ((uint8_t)metadata[records_len_offset + 0] >> 24 |
                                               (uint8_t)metadata[records_len_offset + 1] >> 16 |
                                               (uint8_t)metadata[records_len_offset + 2] >> 8 |
                                               (uint8_t)metadata[records_len_offset + 3]);

                        int log_topic_idx = curr_log_idx + 70;
                        uint8_t log_topic_name_length = metadata[log_topic_idx];
                        uint8_t *A = (uint8_t *)metadata + log_topic_idx + log_topic_name_length;
                        uint8_t *B = (uint8_t *)req_buf + topic_offset;

                        if (std::memcmp(A, B, 16) == 0)
                        {
                            std::string topic_name((char *)metadata + log_topic_idx + 1);
                            std::string record_file = "/tmp/kraft-combined-logs/" + topic_name + "-0/00000000000000000000.log";
                            int record_fd = open(record_file.c_str(), O_RDONLY, S_IRUSR);
                            assert(record_fd != -1);

                            compact_records_length = read(record_fd, record_data, 1024);
                            hexdump(record_data, compact_records_length);

                            found_topic = true;
                            found_topic_id_offset = log_topic_idx;
                            error_code = 0;
                            break;
                        }
                        curr_log_idx = next_part;
                    }

                    *ptr++ = partitions_length; // # of partitions  == 1
                    for (int8_t i = 0; i < (partitions_length - 1); ++i)
                    {
                        write_int32_be(&ptr, i); // # Partition Index (INT32, 0)
                        write_int16_be(&ptr, error_code);

                        write_int64_be(&ptr, 0xffffffffffffffff); // high_watermark
                        write_int64_be(&ptr, 0xffffffffffffffff); // last_stable_offset
                        write_int64_be(&ptr, 0xffffffffffffffff); // log_start_offset
                        *ptr++ = 0;                               // num_aborted_transactions
                        write_int32_be(&ptr, 0xffffffff);         // preferred_read_replica
                        uint8_t compact_records_length_buf[9];
                        size_t varint_len = varint_encode(compact_records_length, compact_records_length_buf);
                        copy_bytes(&ptr, (char *)compact_records_length_buf, varint_len);
                        copy_bytes(&ptr, (char *)record_data, compact_records_length);
                        *ptr++ = TAG_BUFFER;
                        *ptr++ = TAG_BUFFER;
                    }
                }
                *ptr++ = TAG_BUFFER;
            }
            if (request_api_key == 0x0012) // API Versions
            {
                int error_code = 35; // (UNSUPPORTED_VERSION)
                if (request_api_version <= 4)
                    error_code = 0; // (NO_ERROR)

                write_int16_be(&ptr, error_code);
                int8_t num_api_keys = 1 + 3; // 1 + # of elements because 0 is null array and 1 is empty array
                *ptr++ = num_api_keys;
                copy_bytes(&ptr, &req_buf[req_api_offset], 2); // api_key
                write_int16_be(&ptr, 0);                       // min_ver
                write_int16_be(&ptr, request_api_version);     // max_ver
                *ptr++ = TAG_BUFFER;                           // array_end

                write_int16_be(&ptr, 75); // api_key ( DescribeTopicPartitions )
                write_int16_be(&ptr, 0);  // min_ver
                write_int16_be(&ptr, 0);  // max_ver
                *ptr++ = TAG_BUFFER;      // array_end

                write_int16_be(&ptr, 1);  // api_key ( FETCH )
                write_int16_be(&ptr, 0);  // min_ver
                write_int16_be(&ptr, 16); // max_ver
                *ptr++ = TAG_BUFFER;

                write_int32_be(&ptr, 0); // throttle_time_ms
                *ptr++ = TAG_BUFFER;
            }

            int message_size = ptr - resp_buf;
            ptr = resp_buf;
            write_int32_be(&ptr, message_size - 4);

            write(client_fd, resp_buf, message_size);
        }
        fflush(stdout);
        std::cout.flush();
        close(client_fd);
    }
    close(server_fd);
    return 0;
}