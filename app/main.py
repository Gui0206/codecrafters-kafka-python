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
    
    response = KafkaResponse(message_size=0,correlation_id=7)
        
    connection.sendall(response.to_bytes())

if __name__ == "__main__":
    main()