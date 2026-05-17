import paho.mqtt.client as mqtt

from bytewax.outputs import DynamicSink, StatelessSinkPartition


class MQTTSinkPartition(StatelessSinkPartition):
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
    ):  
        #username_pw_set
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if username:
            self._client.username_pw_set(username, password)
        self._client.connect(host, int(port))
    def write_batch(self, chunks: list):
        if not chunks:
            return
        try:
            for topic, message in chunks:
                self._client.publish(topic, message, retain=True)
        except Exception as e:
            print(f'MQTT insert error: {type(e).__name__}: {e}')

    def close(self):
        if hasattr(self, '_client'):
            self._client.disconnect()


class MQTTSink(DynamicSink):
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 1883,
        username: str = None,
        password: str = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def build(self, *args) -> MQTTSinkPartition:
        return MQTTSinkPartition(
            self.host,
            self.port,
            self.username,
            self.password,
        )
