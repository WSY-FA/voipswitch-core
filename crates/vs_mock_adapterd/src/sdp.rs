//! Deterministic SDP bodies used by the loopback adapter.
//!
//! No RTP is sent or received. Ports are arbitrary but stable, so core media
//! allocation and SDP rewriting run through the same code path as a real call.

pub const OFFER: &str = "\
v=0\r\n\
o=- 1 1 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 40000 RTP/AVP 8 0 101\r\n\
a=rtpmap:8 PCMA/8000\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=rtpmap:101 telephone-event/8000\r\n\
a=fmtp:101 0-16\r\n\
a=sendrecv\r\n";

pub const ANSWER: &str = "\
v=0\r\n\
o=- 2 2 IN IP4 127.0.0.1\r\n\
s=-\r\n\
c=IN IP4 127.0.0.1\r\n\
t=0 0\r\n\
m=audio 40002 RTP/AVP 8 0\r\n\
a=rtpmap:8 PCMA/8000\r\n\
a=rtpmap:0 PCMU/8000\r\n\
a=sendrecv\r\n";

pub fn body(text: &str) -> serde_json::Value {
    serde_json::json!({
        "content_type": "application/sdp",
        "text": text,
    })
}
