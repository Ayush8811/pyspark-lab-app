from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from spark_runner import execute_pyspark_code
from ai_generator import generate_problem, generate_search_response, generate_subtopics

# DB and Auth imports
import models
from database import engine, get_db
import auth
import schemas
from email_utils import send_recovery_email

import os
import random
import string
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

# Create tables if they don't exist
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="PySpark Platform API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")
optional_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

def get_optional_user(token: Optional[str] = Depends(optional_oauth2_scheme), db: Session = Depends(get_db)):
    if not token:
        return None
    try:
        payload = auth.decode_access_token(token)
        if not payload:
            return None
        return db.query(models.User).filter(models.User.username == payload.get("sub")).first()
    except:
        return None

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    payload = auth.decode_access_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    username: str = payload.get("sub")
    if username is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = db.query(models.User).filter(models.User.username == username).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user

# ------ AUTH ROUTES ------

@app.post("/api/auth/register", response_model=schemas.Token)
def register_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(models.User).filter(models.User.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
        
    if user.email:
        email_check = db.query(models.User).filter(models.User.email == user.email).first()
        if email_check:
            raise HTTPException(status_code=400, detail="Email already registered")
            
    hashed_password = auth.get_password_hash(user.password)
    new_user = models.User(username=user.username, email=user.email, hashed_password=hashed_password)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    access_token = auth.create_access_token(data={"sub": new_user.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/api/auth/login", response_model=schemas.Token)
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.username == form_data.username).first()
    if not user or not auth.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = auth.create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/api/auth/me", response_model=schemas.UserResponse)
def get_current_user_info(current_user: models.User = Depends(get_current_user)):
    return current_user

class GoogleTokenRequest(BaseModel):
    token: str

@app.post("/api/auth/google")
def google_login(req: GoogleTokenRequest, db: Session = Depends(get_db)):
    """Verify a Google ID token and create/login the user."""
    try:
        idinfo = id_token.verify_oauth2_token(
            req.token, google_requests.Request(), GOOGLE_CLIENT_ID
        )
        
        google_email = idinfo.get("email")
        google_name = idinfo.get("name", "")
        
        if not google_email:
            raise HTTPException(status_code=400, detail="Google account has no email.")
            
        # Check if user already exists by email
        user = db.query(models.User).filter(models.User.email == google_email).first()
        
        if not user:
            # Create a new user with email as username (sanitized)
            base_username = google_email.split("@")[0]
            username = base_username
            counter = 1
            while db.query(models.User).filter(models.User.username == username).first():
                username = f"{base_username}{counter}"
                counter += 1
            
            # Create with a random password (user will use Google to log in)
            random_pw = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
            hashed_pw = auth.get_password_hash(random_pw)
            
            user = models.User(
                username=username,
                email=google_email,
                name=google_name,
                hashed_password=hashed_pw
            )
            db.add(user)
            db.commit()
            db.refresh(user)
        
        # Issue JWT
        access_token = auth.create_access_token(data={"sub": user.username})
        return {"access_token": access_token, "token_type": "bearer"}
        
    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"Invalid Google token: {str(e)}")

@app.put("/api/user/settings", response_model=schemas.UserResponse)
def update_user_settings(settings: schemas.UserSettingsUpdate, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Phase 11: Update Profile metadata"""
    # Only update fields that were provided
    if settings.name is not None:
        current_user.name = settings.name
    if settings.age is not None:
        current_user.age = settings.age
    if settings.bio is not None:
        current_user.bio = settings.bio
        
    db.commit()
    db.refresh(current_user)
    return current_user

@app.post("/api/auth/forgot-password")
def forgot_password(req: schemas.ForgotPasswordRequest, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.email == req.email).first()
    if not user:
        # Don't leak if email exists, just return success
        return {"success": True, "message": "If that email exists, a reset code was sent."}
        
    # Generate 6 digit code
    code = ''.join(random.choices(string.digits, k=6))
    
    # Set expiration to 15 mins from now
    expires = (datetime.utcnow() + timedelta(minutes=15)).isoformat()
    
    user.reset_code = code
    user.reset_code_expires = expires
    db.commit()
    
    send_recovery_email(req.email, code)
    return {"success": True, "message": "If that email exists, a reset code was sent."}

@app.post("/api/auth/reset-password")
def reset_password(req: schemas.ResetPasswordRequest, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.email == req.email, models.User.reset_code == req.code).first()
    
    if not user:
        raise HTTPException(status_code=400, detail="Invalid recovery code or email.")
        
    # Check expiration
    if not user.reset_code_expires:
        raise HTTPException(status_code=400, detail="No active recovery requested.")
        
    expires_dt = datetime.fromisoformat(user.reset_code_expires)
    if datetime.utcnow() > expires_dt:
        raise HTTPException(status_code=400, detail="Recovery code has expired. Please request a new one.")
        
    # Hash new password
    user.hashed_password = auth.get_password_hash(req.new_password)
    
    # Clear recovery fields
    user.reset_code = None
    user.reset_code_expires = None
    db.commit()
    
    return {"success": True, "message": "Password successfully reset. You can now log in."}

# ------ APP ROUTES ------

class ExecuteRequest(BaseModel):
    code: str
    datasets: Dict[str, List[Dict[str, Any]]]

@app.get("/api/topics/subtopics")
def get_subtopics(topic: str, difficulty: str = "Medium", exclude: str = None):
    subtopics = generate_subtopics(topic, difficulty, exclude)
    return {"subtopics": subtopics}

@app.get("/api/problem/generate")
def get_problem(topic: str = "general", difficulty: str = "Medium"):
    return generate_problem(topic, difficulty)

class SubmitRequest(BaseModel):
    code: str
    datasets: Dict[str, List[Dict[str, Any]]]
    expected_output: List[Dict[str, Any]]
    difficulty: str = "Medium"
    title: Optional[str] = "Unknown Problem"

@app.post("/api/problem/execute")
def execute_code(req: ExecuteRequest):
    return execute_pyspark_code(req.code, req.datasets)

@app.post("/api/problem/submit")
def submit_code(req: SubmitRequest, user: Optional[models.User] = Depends(get_optional_user), db: Session = Depends(get_db)):
    result = execute_pyspark_code(req.code, req.datasets)
    
    if not result["success"]:
        return {
            "success": False,
            "passed": False,
            "user_output": None,
            "message": "Execution failed. Check your syntax."
        }
        
    if result["final_df_rows"] is None:
        return {
            "success": True,
            "passed": False,
            "user_output": None,
            "message": "Grading failed: Could not find a 'final_df' variable. Did you assign your final result to 'final_df'?"
        }

@app.post("/api/search")
def search_pyspark(req: schemas.SearchQuery):
    """Phase 10: Semantic PySpark Search via LLM."""
    if not req.query or len(req.query.strip()) == 0:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
        
    response_markdown = generate_search_response(req.query.strip())
    return {"markdown": response_markdown}
        
    # Grading Logic
    user_rows = result["final_df_rows"]
    expected_rows = req.expected_output
    
    if len(user_rows) != len(expected_rows):
        return {
            "success": True,
            "passed": False,
            "user_output": user_rows,
            "message": f"Row count mismatch. Expected {len(expected_rows)}, got {len(user_rows)}."
        }
        
    # Phase 5 Enhancement: Order-agnostic grading 
    # (PySpark hash joins destroy order, so we sort both lists of dictionaries by their canonical normalized contents)
    def normalize_val(v):
        if v is None:
            return "None"
        try:
            # If numeric, strip trailing .0 if present to handle int vs float equality robustly
            fv = float(v)
            if fv.is_integer():
                return str(int(fv))
            return str(fv)
        except (ValueError, TypeError):
            return str(v)

    def sort_rows(rows):
        return sorted(rows, key=lambda r: str(sorted([(k, normalize_val(v)) for k, v in r.items()])))
        
    user_rows_sorted = sort_rows(user_rows)
    expected_rows_sorted = sort_rows(expected_rows)
    
    # Compare row by row (now order-agnostic + type-forgiving)
    for i, (u_row, e_row) in enumerate(zip(user_rows_sorted, expected_rows_sorted)):
        for key, expected_val in e_row.items():
            if key not in u_row:
                return {
                    "success": True, "passed": False, "user_output": user_rows,
                    "message": f"Missing column '{key}' in your output."
                }
            # Handle PySpark int vs float vs string conversion slop generically
            if normalize_val(u_row[key]) != normalize_val(expected_val):
                return {
                    "success": True, "passed": False, "user_output": user_rows,
                    "message": f"Data mismatch. Expected '{expected_val}' for column '{key}', got '{u_row[key]}'."
                }
                
    # Phase 7: Log activity on success if user is logged in
    if user:
        today_str = datetime.now().strftime("%Y-%m-%d")
        new_log = models.ActivityLog(user_id=user.id, date=today_str, difficulty=req.difficulty, problem_title=req.title)
        db.add(new_log)
        db.commit()

    return {
        "success": True,
        "passed": True,
        "user_output": user_rows,
        "message": "All test cases passed!"
    }

# ------ PERSISTENCE ROUTES ------

@app.post("/api/problem/save", response_model=schemas.SavedProblemResponse)
def save_problem(problem: schemas.SavedProblemCreate, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    # Phase 7: Deduplication check
    existing = db.query(models.SavedProblem).filter(
        models.SavedProblem.user_id == current_user.id,
        models.SavedProblem.title == problem.title
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Problem already saved")

    db_problem = models.SavedProblem(**problem.model_dump(), user_id=current_user.id)
    db.add(db_problem)
    db.commit()
    db.refresh(db_problem)
    return db_problem

@app.get("/api/problem/saved", response_model=List[schemas.SavedProblemResponse])
def get_saved_problems(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    problems = db.query(models.SavedProblem).filter(models.SavedProblem.user_id == current_user.id).order_by(models.SavedProblem.id.desc()).all()
    return problems

@app.delete("/api/problem/saved/{problem_id}")
def delete_saved_problem(problem_id: int, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    problem = db.query(models.SavedProblem).filter(models.SavedProblem.id == problem_id, models.SavedProblem.user_id == current_user.id).first()
    if not problem:
        raise HTTPException(status_code=404, detail="Problem not found")
    db.delete(problem)
    db.commit()
    return {"success": True, "message": "Problem removed successfully"}

@app.get("/api/user/profile", response_model=schemas.UserProfileResponse)
def get_user_profile(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    logs = db.query(models.ActivityLog).filter(models.ActivityLog.user_id == current_user.id).all()
    
    total_solved = len(logs)
    xp = 0
    by_complexity = {"Easy": 0, "Medium": 0, "Hard": 0}
    activity_heatmap = {}
    
    for log in logs:
        if log.difficulty == "Easy": xp += 10
        elif log.difficulty == "Medium": xp += 20
        elif log.difficulty == "Hard": xp += 30
        
        if log.difficulty in by_complexity:
            by_complexity[log.difficulty] += 1
        else:
            by_complexity[log.difficulty] = 1
            
        if log.date in activity_heatmap:
            activity_heatmap[log.date] += 1
        else:
            activity_heatmap[log.date] = 1
            
    # Calculate streak roughly looking backward
    today = datetime.now().date()
    current_streak = 0
    
    today_str = today.strftime("%Y-%m-%d")
    yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    
    check_date = today
    if today_str not in activity_heatmap and yesterday_str in activity_heatmap:
        check_date = today - timedelta(days=1)
        
    while check_date.strftime("%Y-%m-%d") in activity_heatmap:
        current_streak += 1
        check_date -= timedelta(days=1)
        
    recent_submissions = db.query(models.ActivityLog).filter(
        models.ActivityLog.user_id == current_user.id
    ).order_by(models.ActivityLog.id.desc()).all()
        
    return {
        "username": current_user.username,
        "stats": {
            "total_solved": total_solved,
            "xp": xp,
            "current_streak": current_streak,
            "by_complexity": by_complexity
        },
        "activity_heatmap": activity_heatmap,
        "recent_submissions": recent_submissions
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
