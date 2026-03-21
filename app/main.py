import socket
import struct
from dataclasses import dataclass

@dataclass
class KafkaResponse:
    message_size: int
    correlation_id: int

    def to_bytes(self):
        return struct.pack(">ii", self.message_size, self.correlation_id)

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    connection, addr = server.accept() # wait for client

    req_parse = connection.recv(1024)
    header, client, body  = req_parse.split()

    #message_size = header[:5]
    request_api_key = header[5:7]
    request_api_version = header[6:9]
    correlation_id = header[9:11]

    int_id = int.from_bytes(correlation_id, byteorder='big')

    print('header')
    print(header)
    print('-----')
    print('client')
    print(client)
    print('-----')
    print('body')
    print(body)

    
    response = KafkaResponse(message_size=0, correlation_id=int_id)
        
    connection.sendall(response.to_bytes())

if __name__ == "__main__":
    main()