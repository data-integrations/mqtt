MQTT Streaming Source
=====================

The MQTT Streaming Source allows you to subscribe to an MQTT broker in a streaming context. You
specify the topic to subscribe to as an MQTT client.

Usage Notes
-----------

The MQTT Streaming Source will listen to an MQTT broker and subscribe to a given topic. To do this,
you need to supply the host and, optionally, the port of the MQTT broker. If the MQTT protocol is
TCP, the default port will be 1883. If the MQTT protocol is SSL, the default port is 8889.

When specifying the MQTT Topic, the wildcards '+' and '#' are permitted. The '+' wildcard indicates
we are selecting all topics on that level of hierarchy. The '#' wildcard indicates that we are
selecting all topics from that level of hierarchy and children of those topics. This also means that
if we choose to use it, '#' must be the last character of a topic subscription.

For example, if we have the topic "a/b/c/d", these will match:

* a/b/c/d
* a/+/c/d
* +/+/+/+
* a/b/c/+
* \#
* a/#
* a/b/c/#
* +/b/#

While these will _not_ match:

* a/b/c
* +/+/+
* /#

Additionally, the zero-length topic, "", is permitted:

* "a//b" will be matched by "a/+/b"
* "/a/b" will be matched by "+/a/b" and "/#"

Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Broker URL** | **Y** | N/A | Specifies the MQTT broker to listen to. Must be specified in the form of 'tcp://<host>[:<port>]' or 'ssl://<host>[:<port>]'. If the port is not specified, it will default to 1883 for TCP and 8889 for SSL. |
| **MQTT Topic** | **Y** | N/A | Specifies the MQTT topic to subscribe to. Wildcards '+' and '#' are acceptable and used as described above. |
