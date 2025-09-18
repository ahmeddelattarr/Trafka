import socket, struct

HOST = "localhost"
PORT = 9092
BUFFER_SIZE = 1024

# Define what APIs you support (clear & readable)
SUPPORTED_APIS = {
    18: {"min_version": 0, "max_version": 4},  # ApiVersions
}


def setup_server():
    return socket.create_server((HOST, PORT), reuse_port=True)


def build_api_versions_response(correlation_id, error_code=0):
    """
    Build ApiVersionsResponse v3 (correct order + tagged fields).
    """
    response_dict = {
        "correlation_id": correlation_id,
        "error_code": error_code,
        "apis": [
            {
                "api_key": api_key,
                "min_version": api["min_version"],
                "max_version": api["max_version"],
            }
            for api_key, api in SUPPORTED_APIS.items()
        ],
        "throttle_time_ms": 0,
    }

    # ---- Serialize ----
    body = b""
    body += struct.pack(">i", response_dict["correlation_id"])  # correlation_id
    body += struct.pack(">h", response_dict["error_code"])      # error_code

    # api_keys array
    body += struct.pack(">i", len(response_dict["apis"]))       # num_api_keys
    for api in response_dict["apis"]:
        body += struct.pack(">h", api["api_key"])
        body += struct.pack(">h", api["min_version"])
        body += struct.pack(">h", api["max_version"])
        body += b"\x00"  # per-API TAG_BUFFER (empty)

    # throttle_time_ms
    body += struct.pack(">i", response_dict["throttle_time_ms"])

    # Final TAG_BUFFER
    body += b"\x00"

    # Prepend message size
    message_size = len(body)
    response = struct.pack(">i", message_size) + body

    return response, response_dict


def handle_client(conn):
    data = conn.recv(BUFFER_SIZE)
    if not data:
        return False

    # ---- Parse Request Header ----
    message_size = struct.unpack(">i", data[0:4])[0]
    request_api_key = struct.unpack(">h", data[4:6])[0]
    request_api_version = struct.unpack(">h", data[6:8])[0]
    correlation_id = struct.unpack(">i", data[8:12])[0]

    print(
        f"Received request: api_key={request_api_key}, "
        f"version={request_api_version}, "
        f"correlation_id={correlation_id}"
    )

    # ---- Build Response ----
    if request_api_key == 18:  # ApiVersions
        response, response_dict = build_api_versions_response(correlation_id)
        print("Sending response:", response_dict)
    else:
        response, response_dict = build_api_versions_response(
            correlation_id, error_code=35
        )
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
