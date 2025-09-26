from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import time
import json
import random

class SSEHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    if self.path == '/films':
      self.send_response(200)
      self.send_header('Content-Type', 'text/event-stream')
      self.send_header('Cache-Control', 'no-cache')
      self.send_header('Connection', 'keep-alive')
      self.end_headers()

      for i in range(1, 10):
        film = {"name": f"film_{i}", "year": random.randint(1950, 2025)}
        event = f"""event: new_item
id: {i}
data: {json.dumps(film)}\n\n"""
        self.wfile.write(event.encode('utf-8'))
        print(i)
        self.wfile.flush()
        time.sleep(2)
    else:
        self.send_response(404)
        self.end_headers()

if __name__ == '__main__':
  server = ThreadingHTTPServer(('0.0.0.0', 8000), SSEHandler)
  print("SSE server running on http://localhost:8000")
  server.serve_forever()
