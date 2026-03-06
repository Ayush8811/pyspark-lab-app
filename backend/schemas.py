from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Any, List
import json

class UserCreate(BaseModel):
    username: str
    email: Optional[str] = None
    password: str

class SearchQuery(BaseModel):
    query: str

class UserResponse(BaseModel):
    id: int
    username: str
    email: Optional[str] = None
    name: Optional[str] = None
    age: Optional[int] = None
    bio: Optional[str] = None
    
    class Config:
        from_attributes = True

class UserSettingsUpdate(BaseModel):
    name: Optional[str] = None
    age: Optional[int] = None
    bio: Optional[str] = None

class Token(BaseModel):
    access_token: str
    token_type: str
    
class SavedProblemCreate(BaseModel):
    title: str
    description: str
    difficulty: str
    tags: str
    datasets: Dict[str, Any]
    expected_output: List[Dict[str, Any]]
    initial_code: str
    language: str = "pyspark"
    
class SavedProblemResponse(SavedProblemCreate):
    id: int
    
    # SQLite stores JSON columns as plain strings; parse them back for Pydantic
    @field_validator('datasets', mode='before')
    @classmethod
    def parse_datasets(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator('expected_output', mode='before')
    @classmethod
    def parse_expected_output(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v
    
    class Config:
        from_attributes = True

class UserStats(BaseModel):
    total_solved: int
    xp: int
    current_streak: int
    by_complexity: Dict[str, int]

class ActivityLogResponse(BaseModel):
    id: int
    date: str
    difficulty: str
    problem_title: Optional[str] = None
    
    class Config:
        from_attributes = True

class UserProfileResponse(BaseModel):
    username: str
    stats: UserStats
    activity_heatmap: Dict[str, int]
    recent_submissions: List[ActivityLogResponse] = []

class ForgotPasswordRequest(BaseModel):
    email: str
    
class ResetPasswordRequest(BaseModel):
    email: str
    code: str
    new_password: str
