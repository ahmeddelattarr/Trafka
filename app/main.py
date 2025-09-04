import socket , struct   # noqa: F401

HOST = "localhost"
PORT = 9092
BUFFER_SIZE = 1024

def setup_server():
    server = socket.create_server((HOST, PORT), reuse_port=True)

def handle_client(conn):
    data = conn.recv(BUFFER_SIZE)
    if not data:
        return False
    print(f"Received data: {data}")

    message = 4

    correlation_id = struct.unpack(">i", data[8:12])[0]  # extract correlation id from request

    # kafka response
    response = struct.pack(">i", message) + struct.pack(">i", correlation_id)

    conn.sendall(response)



    print("Response sent.")

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    server = setup_server()

    while True:
        conn, addr = server.accept()
        print(f"Client connected from {addr}")
        handle_client(conn)



if __name__ == "__main__":
    main()
