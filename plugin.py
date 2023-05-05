# import gi
# gi.require_version('Gst', '1.0')
# from gi.repository import GObject, Gst
# import torch
# import numpy as np

# # Initialize GStreamer
# Gst.init(None)

# # Define the plugin class
# class YoloPlugin(Gst.Element):
#     # Define the input and output pads
#     __gstmetadata__ = ('yolov8',
#                        'Filter/Analyzer',
#                        'Detect objects in video frames using YOLOv8 PyTorch model',
#                        'Nivetheni')
#     __gsttemplates__ = (Gst.PadTemplate.new('src', Gst.PadDirection.SRC, Gst.PadPresence.ALWAYS, Gst.Caps.from_string('video/x-raw,format=RGB')),
#                         Gst.PadTemplate.new('sink', Gst.PadDirection.SINK, Gst.PadPresence.ALWAYS, Gst.Caps.from_string('video/x-raw,format=RGB')))

#     # Initialize the plugin
#     def __init__(self):
#         super(YoloPlugin, self).__init__()

#         # Initialize the YOLOv8 PyTorch model
#         self.model = torch.hub.load('ultralytics/yolov5', 'yolov5x', pretrained=True)

#     # Handle events on the input and output pads
#     def do_event(self, event):
#         return Gst.Element.do_event(self, event)

#     # Handle buffers on the input pad
#     def do_sink_event(self, event):
#         return Gst.Element.do_sink_event(self, event)

#     # Process buffers on the input pad
#     def do_sink_chain(self, element, buffer):
#         # Convert the input buffer to a numpy array
#         data = buffer.extract_dup(0, buffer.get_size())
#         img = np.frombuffer(data, dtype=np.uint8).reshape((buffer.get_height(), buffer.get_width(), 3))

#         # Preprocess the input frame
#         img = self.model.preprocess(img)

#         # Run inference on the input frame
#         detections = self.model(img)

#         # Postprocess the output
#         boxes = detections.xyxy[0].cpu().numpy()
#         confs = detections.conf[0].cpu().numpy()

#         # Convert the output to a GStreamer buffer and send it to the output pad
#         outbuf = Gst.Buffer.new_allocate(None, buffer.get_size(), None)
#         outbuf.fill(0, data)
#         outbuf.set_caps(buffer.get_caps())
#         outbuf.pts = buffer.pts
#         outbuf.dts = buffer.dts
#         outbuf.duration = buffer.duration
#         self.get_static_pad('src').push(outbuf)

#         return Gst.FlowReturn.OK

# # Register the plugin with GStreamer
# GObject.type_register(YoloPlugin)
# __gstelementfactory__ = (YoloPlugin.__name__, Gst.Rank.NONE, YoloPlugin)

# # # Define the pipeline
# # pipeline = Gst.Pipeline.new('test-pipeline')

# # # Define the input RTSP sources
# # src1 = Gst.ElementFactory.make('rtspsrc', 'src1')
# # src1.set_property('location', 'rtsp://test:test123456789@streams.ckdr.co.in:2554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif')
# # src2 = Gst.ElementFactory.make('rtspsrc', 'src2')
# # src2.set_property('location', 'rtsp://happymonk:admin123@streams.ckdr.co.in:3554/cam/realmonitor?channel=1&subtype=0&unicast=true&proto=Onvif')

# # # Define the YOLOv8 PyTorch plugin
# # yolo = Gst.ElementFactory.make('yoloplugin', 'yolo')

# # print(yolo)

# # # Define the output sink
# # sink = Gst.ElementFactory.make('fakesink', 'sink')

# # # Add all elements to the pipeline
# # pipeline.add(src1)
# # pipeline.add(src2)
# # pipeline.add(yolo)
# # pipeline.add(sink)

# # # Link the elements together
# # src1.link(yolo)
# # src2.link(yolo)
# # yolo.link(sink)

# # # Start the pipeline
# # pipeline.set_state(Gst.State.PLAYING)

# # # Create a GStreamer bus to receive messages from the pipeline
# # bus = pipeline.get_bus()
# # msg = bus.timed_pop_filtered(Gst.CLOCK_TIME_NONE, Gst.MessageType.ANY)

# # # Process messages from the pipeline
# # while msg is not None:
# #     if msg.type == Gst.MessageType.ERROR:
# #         err, debug = msg.parse_error()
# #         print('Error received from element {}: {}'.format(msg.src.get_name(), err))
# #         print('Debugging information: {}'.format(debug))
# #     elif msg.type == Gst.MessageType.EOS:
# #         print('End-Of-Stream reached')
# #         break
# #     elif msg.type == Gst.MessageType.STATE_CHANGED:
# #         if isinstance(msg.src, Gst.Pipeline):
# #             old_state, new_state, pending_state = msg.parse_state_changed()
# #             print('Pipeline state changed from {} to {}'.format(old_state.value_nick, new_state.value_nick))
# #     elif msg.type == Gst.MessageType.WARNING:
# #         err, debug = msg.parse_warning()
# #         print('Warning received from element {}: {}'.format(msg.src.get_name(), err))
# #         print('Debugging information: {}'.format(debug))
# #     elif msg.type == Gst.MessageType.TAG:
# #         taglist = msg.parse_tag()
# #         print('Tag received on element {}: {}'.format(msg.src.get_name(), taglist))
# #     elif msg.type == Gst.MessageType.ELEMENT:
# #         struct = msg.get_structure()
# #         if struct.get_name() == 'object-detection':
# #             # Get the inference results from the message
# #             boxes = np.array(struct.get_value('boxes'))
# #             confs = np.array(struct.get_value('confs'))
# #             classes = np.array(struct.get_value('classes'))

# #             # Print the inference results
# #             for i in range(len(boxes)):
# #                 print('Object detected: class={}, confidence={}, box={}'.format(classes[i], confs[i], boxes[i]))

# #     # Pop the next message from the bus
# #     msg = bus.timed_pop_filtered(Gst.CLOCK_TIME_NONE, Gst.MessageType.ANY)

# # # Stop the pipeline
# # pipeline.set_state(Gst.State.NULL)



