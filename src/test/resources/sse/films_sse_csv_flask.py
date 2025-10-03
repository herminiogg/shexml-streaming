from flask import Flask, Response
import time
import json

app = Flask(__name__)

@app.route('/films')
def sse_handler():
  def event_stream():
    for i in range(1, 10):
      print(f"Sending film_{i}")
      data_1 = "id;name;year"
      data_2 = f"{i};film_{i};{1950 + i}"
      data_3 = f"{i + 9};film_{i + 9};{1950 + i}"
      event = f"""event: new_item
id: {i}
data: {data_1}\ndata: {data_2}\ndata: {data_3}\n\n"""
      yield event
      time.sleep(2)
  return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)