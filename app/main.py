import socket , struct   # noqa: F401

HOST = "localhost"
PORT = 9092
BUFFER_SIZE = 1024

SUPPORTED_API_VERSIONS = range(0, 5)  # supports 0–4
UNSUPPORTED_VERSION_ERROR = 35

def setup_server():
    server = socket.create_server((HOST, PORT), reuse_port=True)
    return server

def handle_client(conn):
    data = conn.recv(BUFFER_SIZE)
    if not data:
        return False
    print(f"Received data: {data}")

    #message_size = struct.unpack(">i", data[0:4])[0]
    message_size = 6
    request_api_key = struct.unpack(">h", data[4:6])[0]
    request_api_version = struct.unpack(">h", data[6:8])[0]
    correlation_id = struct.unpack(">i", data[8:12])[0]

    print(f"Received request: api_key={request_api_key}, version={request_api_version}, correlation_id={correlation_id}")

    if request_api_version not in SUPPORTED_API_VERSIONS:
        error_code = UNSUPPORTED_VERSION_ERROR
    else:
        error_code = 0



    # kafka response
    response = struct.pack(">i", message_size) + struct.pack(">i", correlation_id) + struct.pack(">h", 0)

    conn.sendall(response)



    print("Response sent.")

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")


    server = setup_server()

    while True:
        conn, addr = server.accept()
        print(f"Client connected from {addr}")
        handle_client(conn)



if __name__ == "__main__":
    main()
