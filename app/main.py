import socket
import struct
from dataclasses import dataclass

supp_broker_api_versions = [0,1,2,3,4]


@dataclass
class KafkaResponse:
    correlation_id: int
    error_code: int
    request_api_version: int

    def to_bytes(self):
        message = b''
        message += struct.pack(">i", self.correlation_id)
        message += struct.pack(">h", self.error_code)
        message += struct.pack(">h", self.request_api_version)

        self.message_size = len(message)
        return struct.pack(">i", self.message_size) + message
    
def construct_api_arr(api_keys):
    api_arr = b''
    arr_len = len(api_keys)
    api_arr += arr_len + 1

    for api_key, min_version, max_version in api_keys:
        api_arr += (api_key, min_version, max_version)

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    connection, addr = server.accept() # wait for client
    data = connection.recv(1024)

    print(data)

    correlation_id = struct.unpack(">i", data[8:12])[0]

    request_api_version = struct.unpack(">h", data[6:8])[0]

    print(request_api_version)

    if request_api_version not in supp_broker_api_versions:
        error_code = 35
    else:
        error_code = 0

    api_key = struct.unpack(">h", data[4:6])[0]
    min_support_version = 0
    max_support_version = 4
    tag_buffer = 0

    response = KafkaResponse(request_api_version = 0, correlation_id=correlation_id, error_code=error_code)
    connection.sendall(response.to_bytes())

if __name__ == "__main__":
    main()