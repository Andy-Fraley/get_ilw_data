from dataclasses import dataclass, field
from typing import Optional, IO
import io

@dataclass
class Config:
    """
    Holds runtime configuration and state for the ILW data processing pipeline.
    """
    datetime_start: Optional[str] = None
    datetime_start_string: Optional[str] = None
    string_stream: Optional[IO[str]] = field(default_factory=io.StringIO)
    gmail_user: Optional[str] = None
    gmail_password: Optional[str] = None
    notification_target_email: Optional[str] = None
    ccb_app_username: Optional[str] = None
    ccb_app_password: Optional[str] = None
    ccb_subdomain: Optional[str] = None
    prog_name: Optional[str] = None
    prog_dir: Optional[str] = None
    curr_year: Optional[int] = None 