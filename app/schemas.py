from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class ValidationIssue(BaseModel):
    row: int
    field: Optional[str] = None
    message: str


class CSVValidationResponse(BaseModel):
    valid: bool
    total_rows: int
    issues: List[ValidationIssue] = Field(default_factory=list)


class HospitalRowResult(BaseModel):
    row: int
    hospital_id: Optional[int] = None
    name: str
    status: str


class BulkProcessingResponse(BaseModel):
    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    duplicate_hospitals: int = 0
    processing_time_seconds: float
    batch_activated: bool
    status: str
    hospitals: List[HospitalRowResult] = Field(default_factory=list)
    progress_url: Optional[str] = None


class BulkJobStatusResponse(BaseModel):
    batch_id: str
    status: str
    total_rows: int
    processed_rows: int
    valid_rows: int
    duplicate_rows: int
    failed_rows: int
    created_rows: int
    batch_activated: bool
    progress_percent: float
    source_filename: str
    error_message: Optional[str] = None


class ResumeResponse(BaseModel):
    batch_id: str
    status: str
    resumed: bool
