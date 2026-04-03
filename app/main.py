import socket
import struct
from dataclasses import dataclass

HOST = "localhost"
PORT = 9092
SUPPORTED_API_VERSIONS = [0,1,2,3,4]
ERROR_UNSUPPORTED_VERSION = 35
ERROR_NONE = 0

@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int

    @classmethod
    def from_bytes(cls, data: bytes):
        api_key = struct.unpack(">h", data[4:6])[0]
        api_version = struct.unpack(">h", data[6:8])[0]
        correlation_id = struct.unpack(">i", data[8:12])[0]
        return cls(api_key, api_version, correlation_id)


@dataclass
class KafkaResponse:
    correlation_id: int
    error_code: int
    api_arr: bytes
    throttle: int

    def to_bytes(self):
        message = b''
        message += struct.pack(">i", self.correlation_id)
        message += struct.pack(">h", self.error_code)
        message += self.api_arr
        message += struct.pack(">i", self.throttle)
        message += b"\x00"
        self.message_size = len(message)
        return struct.pack(">i", self.message_size) + message
    
class ApiVersionHandler:
    def handle(self, request: KafkaRequest) -> KafkaResponse:
        if request.api_version not in SUPPORTED_API_VERSIONS:
            error_code = ERROR_UNSUPPORTED_VERSION
        else:
            error_code = ERROR_NONE

        min_support_version = 0
        max_support_version = 4

        throttle = 0

        api_keys=[(request.api_key, min_support_version, max_support_version)]
        api_arr = self._construct_api_arr(api_keys)

        return KafkaResponse(
            correlation_id=request.correlation_id, 
            error_code=error_code, 
            api_arr=api_arr, 
            throttle=throttle
        )

    def _construct_api_arr(self, api_keys):
        api_arr = b''
        arr_len = len(api_keys)
        api_arr += (arr_len + 1).to_bytes(1, byteorder="big")

        for api_key, min_version, max_version in api_keys:
            api_arr += (
                api_key.to_bytes(2, byteorder="big") + min_version.to_bytes(2, byteorder="big") + max_version.to_bytes(2, byteorder="big") + b"\x00"
                )
        return api_arr

def main():
    server = socket.create_server((HOST, PORT), reuse_port=True)
    connection, addr = server.accept() # wait for client
    data = connection.recv(1024)

    request = KafkaRequest.from_bytes(data)

    handler = ApiVersionHandler()
    response = handler.handle(request)

    connection.sendall(response.to_bytes())

if __name__ == "__main__":
    main()