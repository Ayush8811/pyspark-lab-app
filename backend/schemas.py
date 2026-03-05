from pydantic import BaseModel
from typing import Optional, Dict, Any, List

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
    
class SavedProblemResponse(SavedProblemCreate):
    id: int
    
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
