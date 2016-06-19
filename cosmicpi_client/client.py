import ast
import json
import logging
import threading
import uuid
import pika
from pika.exceptions import ConnectionClosed

log = logging.getLogger(__name__)


class Client(object):

    def __init__(self, url):
        self.event_handler = EventHandler(url)
        self.event_thread = threading.Thread(target=self.event_handler.run, args=(), kwargs={})
        self.event_thread.daemon = True
        self.event_thread.start()

        self.rpc_client = RpcClient(url)
        self.rpc_thread = threading.Thread(target=self.rpc_client.run, args=(), kwargs={})
        self.rpc_thread.daemon = True
        self.rpc_thread.start()

    def get_detectors(self):
        return self.rpc_client.get_detectors()

    def get_detector(self, id):
        return self.rpc_client.get_detector(id)

    def subscribe(self, detector_id, callback):
        self.event_handler.subscribe(detector_id, callback)

        # Fetch the latest state of the detector
        detector = self.get_detector(detector_id)
        callback(detector_id, detector)
        return detector

    def get_history(self, detector_id):
        return self.rpc_client.get_history(detector_id)


class EventHandler(object):

    def __init__(self, url):
        self.url = url
        self.listeners = {}

        self.connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='events', type='fanout')
        self.queue_name = self.channel.queue_declare(exclusive=True).method.queue
        self.channel.queue_bind(exchange='events', queue=self.queue_name)

    def run(self):
        self.channel.basic_consume(self.on_event, queue=self.queue_name, no_ack=True)
        self.channel.start_consuming()

    def on_event(self, channel, method, properties, body):
        event = json.loads(ast.literal_eval(body))
        detector_id = event['detector_id']

        if self.listeners.has_key(detector_id):
            for callback in self.listeners[detector_id]:
                callback(detector_id, event)

    def subscribe(self, detector_id, callback):
        if detector_id in self.listeners:
            if callback not in self.listeners[detector_id]:
                self.listeners[detector_id].append(callback)
        else:
            self.listeners[detector_id] = [callback]


class RpcClient(object):

    def __init__(self, url):
        self.url = url

    def run(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def get_detectors(self):
        return self.call('{"action": "get_detectors"}')

    def get_detector(self, id):
        return self.call('{"action": "get_detector", "id": "%s"}' % id)

    def get_history(self, id):
        return self.call('{"action": "get_history", "id": "%s"}' % id)

    def call(self, request):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        try:
            self.send_request(request)
        except ConnectionClosed:
            print ('Connection closed - trying to reconnect')
            self.run()
            self.send_request(request)

        while self.response is None:
            self.connection.process_data_events()
        return json.loads(self.response)

    def send_request(self, request):
        properties = pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id)
        self.channel.basic_publish(exchange='', routing_key='cosmicpi.client.request',
                                   properties=properties, body=request)
