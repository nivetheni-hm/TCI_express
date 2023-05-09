import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject
import numpy as np

GObject.threads_init()
Gst.init(None)


class PipelineWatcher:
    def __init__(self, pipelines):
        self.bus = None
        self.pipelines = pipelines
        self.loop = GObject.MainLoop()
        self.watch_buses()

    def watch_buses(self):
        for pipeline in self.pipelines:
            self.bus = pipeline.get_bus()
            self.bus.add_signal_watch()
            self.bus.connect("message", self.on_message, pipeline)

    def on_message(self, bus, message, pipeline):
        t = message.type
        if t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            print(f"Error: {err}", debug)
            self.loop.quit()
        # elif t == Gst.MessageType.WARNING:
        #     err, debug = message.parse_warning()
        #     print(f"Warning: {err}", debug)
        elif t == Gst.MessageType.EOS:
            print("End of stream")
            self.loop.quit()
        elif t == Gst.MessageType.STATE_CHANGED:
            old_state, new_state, pending_state = message.parse_state_changed()
            print(f"State changed: {old_state} -> {new_state}")
        # elif t == Gst.MessageType.STREAM_START:
        #     print("Stream started")
        # elif t == Gst.MessageType.STREAM_STATUS:
        #     status_type, owner = message.parse_stream_status()
        #     print(f"Stream status: {status_type} - {owner}")
        # elif t == Gst.MessageType.TAG:
        #     taglist = message.parse_tag()
        #     print(f"Tag: {taglist}")
        # elif t == Gst.MessageType.ASYNC_DONE:
        #     print("Async done")
        
        elif t == Gst.MessageType.TAG:
            taglist = message.parse_tag()
            print(f"Tag: {taglist}")
        elif t == Gst.MessageType.ELEMENT:
            if message.has_name('appsink'):
                sample = message.get_structure().get_value('sample')
                buffer = sample.get_buffer()
                caps = sample.get_caps()
                _, width = caps.get_structure(0).get_value('width')
                _, height = caps.get_structure(0).get_value('height')
                stride = buffer.get_stride()[0]
                arr = np.ndarray(
                    (height, width, 3),
                    buffer=buffer.extract_dup(0, buffer.get_size()),
                    dtype=np.uint8,
                )
                print(f"Got frame of shape {arr.shape}")
        else:
            print(f"Unknown message: {t}")


pipelines = []

for i in range(2):
    pipeline_str = f"rtspsrc location=rtsp://216.48.184.201:8554//stream1 latency=0 ! decodebin ! videoconvert ! appsink name=appsink{i}"
    pipeline = Gst.parse_launch(pipeline_str)
    pipeline.set_state(Gst.State.PLAYING)
    pipelines.append(pipeline)

watcher = PipelineWatcher(pipelines)
watcher.loop.run()

