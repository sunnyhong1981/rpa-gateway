from typing import Literal
from pydantic import BaseModel, ConfigDict, Field

ID = r'^[A-Za-z0-9_-]{1,96}$'

class Model(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)

class Binding(Model):
    inbox_id: str = Field(min_length=1, max_length=96)
    room_id: str = Field(min_length=1, max_length=128)

class DeviceCreate(Model):
    device_id: str = Field(pattern=ID)
    bindings: list[Binding] = Field(min_length=1, max_length=100)

class Hello(Model):
    protocol: int = Field(ge=1, le=1)
    installation_id: str = Field(pattern=ID)
    runtime_version: str = Field(min_length=1, max_length=64)
    worker_instance_id: str = Field(default='', pattern=r'^[A-Za-z0-9_-]{0,96}$')
    capabilities: list[Literal['text_v1', 'caption_images_v1', 'official_identity_v1']] = Field(max_length=3)

class Heartbeat(Model):
    session_id: str = Field(pattern=ID)
    worker_alive: bool
    desktop_unlocked: bool
    wecom_logged_in: bool
    auto_send: bool

class TaskCreate(Model):
    device_id: str = Field(pattern=ID)
    inbox_id: str = Field(min_length=1, max_length=96)
    room_id: str = Field(min_length=1, max_length=128)
    source_message_id: str = Field(min_length=1, max_length=96)
    waybill: str = Field(min_length=1, max_length=96)
    recipient_id: str = Field(min_length=1, max_length=128)
    robot_id: str = Field(min_length=1, max_length=128)
    caption: str = Field(min_length=1, max_length=8000)
    media_ids: list[str] = Field(default_factory=list, max_length=9)
    ttl_seconds: int = Field(default=180, ge=15, le=3600)
    context: dict = Field(default_factory=dict)

class Claim(Model):
    session_id: str = Field(pattern=ID)

class Receipt(Model):
    session_id: str = Field(pattern=ID)
    lease_token: str = Field(min_length=20, max_length=128)
    event_id: str = Field(pattern=ID)
    state: Literal['renew', 'submitting', 'submitted', 'desktop_released', 'quarantine_released', 'unknown', 'failed_pre_submit']
    reason: str = Field(default='', max_length=256)
    native_submit_complete: bool = False
    desktop_idle_verified: bool = False
    input_cleared: bool = False
    group_verified: bool = False

class Evidence(Model):
    event_id: str = Field(min_length=1, max_length=128)
    room_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=128)
    occurred_at: float = Field(allow_inf_nan=False)
    kind: Literal['text', 'image']
    text: str = Field(default='', max_length=8000)
    md5: str = Field(default='', max_length=32)
    size: int = Field(default=0, ge=0)

class Confirmation(Model):
    events: list[Evidence] = Field(min_length=1, max_length=10)

class Review(Model):
    action: Literal['cancel_before_submit', 'close_unknown_without_retry']
    reason: str = Field(min_length=10, max_length=1024)

class UploadCreate(Model):
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    md5: str = Field(pattern=r'^[0-9a-f]{32}$')
    size: int = Field(ge=1,le=12*1024*1024)
