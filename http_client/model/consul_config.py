from typing import Optional

from pydantic import BaseModel


class RetryPolicyItem(BaseModel):
    retry_non_idempotent: bool = False


RetryPolicies = dict[int, RetryPolicyItem]


class Profile(BaseModel):
    request_timeout_sec: Optional[float] = None
    connect_timeout_sec: Optional[float] = None
    max_timeout_tries: Optional[int] = None
    max_tries: Optional[int] = None
    retry_policy: Optional[RetryPolicies] = None
    slow_start_interval_sec: Optional[int] = None
    session_required: Optional[bool] = None
    speculative_timeout_pct: Optional[float] = None


class Host(BaseModel):
    profiles: dict[str, Profile]


class ConsulConfig(BaseModel):
    hosts: dict[str, Host]
    balancing_strategy: Optional[str] = None
