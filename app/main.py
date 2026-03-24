import socket
import struct
from dataclasses import dataclass

supp_broker_api_versions = [0,1,2,3,4]


@dataclass
class KafkaResponse:
    message_size: int
    correlation_id: int
    error_code: int

    def to_bytes(self):
        return struct.pack(">iih", self.message_size, self.correlation_id, self.error_code)

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    connection, addr = server.accept() # wait for client
    data = connection.recv(1024)

    print(data)

    correlation_id = struct.unpack(">i", data[8:12])[0]

    print(correlation_id)

    request_api_version = struct.unpack(">h", data[6:8])[0]
    error_code = 0 if request_api_version not in supp_broker_api_versions else 35
    
    response = KafkaResponse(message_size=0, correlation_id=correlation_id, error_code=error_code)
    connection.sendall(response.to_bytes())

if __name__ == "__main__":
    main()