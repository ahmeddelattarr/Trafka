import socket, struct

HOST = "localhost"
PORT = 9092
BUFFER_SIZE = 1024

# Define what APIs you support (more readable than inline packing)
SUPPORTED_APIS = {
    18: {"min_version": 0, "max_version": 4},  # ApiVersions
}

def setup_server():
    return socket.create_server((HOST, PORT), reuse_port=True)

def build_api_versions_response(correlation_id, error_code=0):
    response_dict = {
        "correlation_id": correlation_id,
        "error_code": error_code,
        "apis": [],
        "throttle_time_ms": 0,
    }

    body = b""
    body += struct.pack(">i", response_dict["correlation_id"])
    body += struct.pack(">h", response_dict["error_code"])
    body += struct.pack(">i", len(response_dict["apis"]))  # num_api_keys
    body += struct.pack(">i", response_dict["throttle_time_ms"])
    body += b"\x00"  # tagged_fields_count = 0

    message_size = len(body)
    return struct.pack(">i", message_size) + body, response_dict


def handle_client(conn):
    data = conn.recv(BUFFER_SIZE)
    if not data:
        return False

    message_size = struct.unpack(">i", data[0:4])[0]
    request_api_key = struct.unpack(">h", data[4:6])[0]
    request_api_version = struct.unpack(">h", data[6:8])[0]
    correlation_id = struct.unpack(">i", data[8:12])[0]

    print(f"Received request: api_key={request_api_key}, version={request_api_version}, correlation_id={correlation_id}")

    if request_api_key == 18:  # ApiVersions
        response, response_dict = build_api_versions_response(correlation_id)
        print("Sending response:", response_dict)
    else:
        response, response_dict = build_api_versions_response(correlation_id, error_code=35)
        print("Sending error response:", response_dict)

    conn.sendall(response)
    print("Response sent.")

def main():
    print("Logs from your program will appear here!")
    server = setup_server()

    while True:
        conn, addr = server.accept()
        print(f"Client connected from {addr}")
        handle_client(conn)

if __name__ == "__main__":
    main()
