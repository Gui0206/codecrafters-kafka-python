import socket
import struct
from dataclasses import dataclass

supp_broker_api_versions = [0,1,2,3,4]


@dataclass
class KafkaResponse:
    #message_size: int
    correlation_id: int
    error_code: int
    request_api_key: int
    request_api_version: int

    def to_bytes(self):
        self.message_size = len(struct.pack(">hhih", self.request_api_key, self.request_api_version, self.correlation_id, self.error_code))
        return struct.pack(">ihhih", self.message_size, self.request_api_key, self.request_api_version, self.correlation_id, self.error_code)

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    connection, addr = server.accept() # wait for client
    data = connection.recv(1024)

    print(data)

    correlation_id = struct.unpack(">i", data[8:12])[0]

    print(correlation_id)

    request_api_version = struct.unpack(">h", data[6:8])[0]

    if request_api_version not in supp_broker_api_versions:
        error_code = 35
    else:
        error_code = 0
        
    response = KafkaResponse(request_api_key = 0, request_api_version = 0, correlation_id=correlation_id, error_code=error_code)
    connection.sendall(response.to_bytes())

if __name__ == "__main__":
    main()

# first test: message_size
# message_size = numero de bytes depois do message_size