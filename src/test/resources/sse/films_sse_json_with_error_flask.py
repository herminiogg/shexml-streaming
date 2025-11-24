from flask import Flask, Response
import time
import json

app = Flask(__name__)

@app.route('/films')
def sse_handler():
  def event_stream():
    for i in range(1, 10):
      if i == 5:
        print(f"Sending error instead of film_{i}")
        event = f"""This is to simulate a format error"""
      else:
        print(f"Sending film_{i}")
        film = {"name": f"film_{i}", "year": 1950 + i}
        event = f"""event: new_item
id: {i}
data: {json.dumps(film)}\n\n"""
      yield event
      time.sleep(2)
  return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)