'''
is this correct 
mini.py should be  running on 127.0.0.1:9092
demo.html should be running on localhost.8000 via http.server
mini-proxy.py should be running on localhost:3000
'''
import http.server
import socket
import json

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_POST(self):
        print("Received POST request")
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        print("Request body:", body)

        # Send request to MiniKafka server
        try:
            mini_kafka_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            mini_kafka_socket.connect(('localhost', 9092))
            print("Connected to MiniKafka server")
            mini_kafka_socket.sendall(body)
            print("Sent request to MiniKafka server")

            # Receive response from MiniKafka server
            response = b''
            while True:
                chunk = mini_kafka_socket.recv(4096)
                if not chunk:
                    break
                response += chunk
            mini_kafka_socket.close()
            print("Received response from MiniKafka server:", response)

        # Send response back to client
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(response)
            print("Sent response back to client")
        except Exception as e:
            print("Error:", e)
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(b"Error occurred")

def run_server():
    server_address = ('', 3000)
    httpd = http.server.HTTPServer(server_address, RequestHandler)
    print('Proxy server listening on port 3000')
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()