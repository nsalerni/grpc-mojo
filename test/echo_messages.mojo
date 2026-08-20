# Hand-written echo message fixtures for the umbrella gRPC tests.
# (The full codegen reference lives in packages/protomojo/test.)

from proto import ProtoMessage, WireReader, WireWriter


struct EchoRequest(Copyable, Defaultable, Movable, ProtoMessage):
    var message: String  # 1
    var _unknown: List[Byte]

    def __init__(out self):
        self.message = String()
        self._unknown = List[Byte]()

    def __init__(out self, var message: String):
        self.message = message^
        self._unknown = List[Byte]()

    def encode_to(self, mut writer: WireWriter):
        if self.message.byte_length() != 0:
            writer.string_field(1, self.message)
        writer.buf.extend(Span(self._unknown))

    def merge_from(mut self, mut reader: WireReader) raises:
        while not reader.done():
            var tag = reader.read_tag()
            if tag[0] == 1:
                self.message = reader.string_value()
            else:
                reader.capture_field(tag[0], tag[1], self._unknown)


struct EchoResponse(Copyable, Defaultable, Movable, ProtoMessage):
    var message: String  # 1
    var _unknown: List[Byte]

    def __init__(out self):
        self.message = String()
        self._unknown = List[Byte]()

    def __init__(out self, var message: String):
        self.message = message^
        self._unknown = List[Byte]()

    def encode_to(self, mut writer: WireWriter):
        if self.message.byte_length() != 0:
            writer.string_field(1, self.message)
        writer.buf.extend(Span(self._unknown))

    def merge_from(mut self, mut reader: WireReader) raises:
        while not reader.done():
            var tag = reader.read_tag()
            if tag[0] == 1:
                self.message = reader.string_value()
            else:
                reader.capture_field(tag[0], tag[1], self._unknown)
