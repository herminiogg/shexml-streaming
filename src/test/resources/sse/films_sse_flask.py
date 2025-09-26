from flask import Flask, Response
import time
import json
import random

app = Flask(__name__)

@app.route('/films')
def sse_handler():
  def event_stream():
    for i in range(1, 10):
      print(f"Sending film_{i}")
      event = f"""event: new_item
id: {i}
data: film_{i}\n\n"""
      yield event
      time.sleep(2)
  return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)