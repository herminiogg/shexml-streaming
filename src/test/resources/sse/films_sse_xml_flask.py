from flask import Flask, Response
import time
import json

app = Flask(__name__)

@app.route('/films')
def sse_handler():
  def event_stream():
    for i in range(1, 10):
      print(f"Sending film_{i}")
      film = f"<films><film><name>film_{i}</name><year>{1950 + i}</year></film></films>"
      event = f"""event: new_item
id: {i}
data: {film}\n\n"""
      yield event
      time.sleep(2)
  return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)