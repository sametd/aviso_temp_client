import json
import time
from enum import Enum

import requests
import sseclient


class EventType(Enum):
    """SSE event types from Aviso service."""
    LIVE_NOTIFICATION = "live-notification"
    REPLAY = "replay"
    REPLAY_CONTROL = "replay-control"
    HEARTBEAT = "heartbeat"
    UNKNOWN = "unknown"

    @classmethod
    def from_string(cls, value: str) -> 'EventType':
        for event_type in cls:
            if event_type.value == value:
                return event_type
        return cls.UNKNOWN

def handle_live_notification(data):
    """Handle live notification events."""
    print(f"🔴 Live Notification: {data.get('id', 'N/A')}")
    print(json.dumps(data, indent=2))

def handle_replay(data):
    """Handle replay events."""
    print(f"⏯️  Replay: {data.get('id', 'N/A')}")

def handle_replay_control(data):
    """Handle replay control events."""
    control_type = data.get("type", "unknown")
    print(f"🎛️  Replay Control: {control_type}")

def handle_heartbeat(data):
    """Handle heartbeat events."""
    print(f"💓 Heartbeat: {data.get('timestamp', 'N/A')}")

def handle_unknown(data):
    """Handle unknown events."""
    print(f"❓ Unknown event: {json.dumps(data, indent=2)}")

class AvisoSSEClient:
    """Simple SSE client for Aviso notifications."""
    
    def __init__(self, base_url="http://localhost:8000", retry_interval=5):
        self.base_url = base_url
        self.retry_interval = retry_interval
        self.running = False
    
    def connect(self, payload):
        """Connect to the SSE stream and process events."""
        url = f"{self.base_url}/api/v1/watch"
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream"
        }
        
        self.running = True
        
        while self.running:
            try:
                print(f"Connecting to {url}...")
                
                response = requests.post(url, json=payload, headers=headers, stream=True, timeout=30)
                response.raise_for_status()
                
                client = sseclient.SSEClient(response)
                print("Connected. Listening for events...")
                
                for sse_event in client.events():
                    if not self.running:
                        break
                        
                    try:
                        if sse_event.data:
                            data = json.loads(sse_event.data)
                            event_type = EventType.from_string(sse_event.event or "unknown")
                            
                            # Handle events with match/case
                            match event_type:
                                case EventType.LIVE_NOTIFICATION:
                                    handle_live_notification(data)
                                case EventType.REPLAY:
                                    handle_replay(data)
                                case EventType.REPLAY_CONTROL:
                                    handle_replay_control(data)
                                case EventType.HEARTBEAT:
                                    handle_heartbeat(data)
                                case EventType.UNKNOWN:
                                    handle_unknown(data)
                            
                    except json.JSONDecodeError as e:
                        print(f"JSON parse error: {e}")
                    except Exception as e:
                        print(f"Event processing error: {e}")
                        
            except requests.exceptions.RequestException as e:
                print(f"Connection error: {e}")
                if self.running:
                    print(f"Retrying in {self.retry_interval} seconds...")
                    time.sleep(self.retry_interval)
            except KeyboardInterrupt:
                print("\nShutting down...")
                self.running = False
            except Exception as e:
                print(f"Unexpected error: {e}")
                if self.running:
                    print(f"Retrying in {self.retry_interval} seconds...")
                    time.sleep(self.retry_interval)
    
    def stop(self):
        """Stop the SSE client."""
        self.running = False

def main():
    payload = {
        "event_type": "test_polygon",
        "identifier": {
            "time": "1200",
            "polygon": "(52.5,13.4,52.6,13.5,52.5,13.6,52.4,13.5,52.5,13.4)"
        },
        "from_id": "0"
    }
    
    client = AvisoSSEClient("https://aviso-server.ecmwf.int")
    
    try:
        client.connect(payload)
    except KeyboardInterrupt:
        client.stop()

if __name__ == "__main__":
    main()
