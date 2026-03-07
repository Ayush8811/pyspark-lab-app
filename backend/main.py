from fastapi import FastAPI, Depends, HTTPException, status, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import json

from spark_runner import execute_pyspark_code, execute_sql_code
from ai_generator import generate_problem, generate_search_response, generate_subtopics, generate_sql_problem, generate_sql_subtopics
from room_manager import room_manager

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

@app.post("/api/search")
def search_pyspark(req: schemas.SearchQuery):
    """Phase 10: Semantic PySpark Search via LLM."""
    if not req.query or len(req.query.strip()) == 0:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
        
    response_markdown = generate_search_response(req.query.strip())
    return {"markdown": response_markdown}

# ------ SQL ROUTES ------

@app.get("/api/sql/topics/subtopics")
def get_sql_subtopics(topic: str, difficulty: str = "Medium", exclude: str = None):
    subtopics = generate_sql_subtopics(topic, difficulty, exclude)
    return {"subtopics": subtopics}

@app.get("/api/sql/problem/generate")
def get_sql_problem(topic: str = "general", difficulty: str = "Medium"):
    return generate_sql_problem(topic, difficulty)

class SqlExecuteRequest(BaseModel):
    code: str
    datasets: Dict[str, List[Dict[str, Any]]]

@app.post("/api/sql/problem/execute")
def execute_sql(req: SqlExecuteRequest):
    return execute_sql_code(req.code, req.datasets)

class SqlSubmitRequest(BaseModel):
    code: str
    datasets: Dict[str, List[Dict[str, Any]]]
    expected_output: List[Dict[str, Any]]
    difficulty: str = "Medium"
    title: Optional[str] = "Unknown Problem"

@app.post("/api/sql/problem/submit")
def submit_sql(req: SqlSubmitRequest, user: Optional[models.User] = Depends(get_optional_user), db: Session = Depends(get_db)):
    result = execute_sql_code(req.code, req.datasets)

    if not result["success"]:
        return {
            "success": False,
            "passed": False,
            "user_output": None,
            "message": "Query execution failed. Check your SQL syntax."
        }

    if result["final_df_rows"] is None:
        return {
            "success": True,
            "passed": False,
            "user_output": None,
            "message": "Grading failed: No result returned from your query."
        }

    user_rows = result["final_df_rows"]
    expected_rows = req.expected_output

    if len(user_rows) != len(expected_rows):
        return {
            "success": True,
            "passed": False,
            "user_output": user_rows,
            "message": f"Row count mismatch. Expected {len(expected_rows)}, got {len(user_rows)}."
        }

    def normalize_val(v):
        if v is None:
            return "None"
        try:
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

    for u_row, e_row in zip(user_rows_sorted, expected_rows_sorted):
        for key, expected_val in e_row.items():
            if key not in u_row:
                return {
                    "success": True, "passed": False, "user_output": user_rows,
                    "message": f"Missing column '{key}' in your output."
                }
            if normalize_val(u_row[key]) != normalize_val(expected_val):
                return {
                    "success": True, "passed": False, "user_output": user_rows,
                    "message": f"Data mismatch. Expected '{expected_val}' for column '{key}', got '{u_row[key]}'."
                }

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

# ------ TWIN CHALLENGE ROUTES ------

class RoomCreateRequest(BaseModel):
    pass  # No body needed, auth provides user

class RoomJoinRequest(BaseModel):
    room_code: str

@app.post("/api/room/create")
def create_room(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Create a new challenge room. Returns the room code."""
    room_code = room_manager.generate_room_code()
    # Ensure uniqueness
    while db.query(models.ChallengeRoom).filter(models.ChallengeRoom.room_code == room_code).first():
        room_code = room_manager.generate_room_code()

    new_room = models.ChallengeRoom(
        room_code=room_code,
        creator_id=current_user.id,
        status="waiting",
        created_at=datetime.utcnow().isoformat()
    )
    db.add(new_room)
    db.commit()
    db.refresh(new_room)

    return {
        "room_code": room_code,
        "status": "waiting",
        "creator": current_user.username
    }

@app.post("/api/room/join")
def join_room(req: RoomJoinRequest, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Join an existing challenge room by code."""
    room = db.query(models.ChallengeRoom).filter(
        models.ChallengeRoom.room_code == req.room_code.upper()
    ).first()

    if not room:
        raise HTTPException(status_code=404, detail="Room not found. Check the code and try again.")

    if room.status == "finished":
        raise HTTPException(status_code=400, detail="This room has already ended.")

    if room.creator_id == current_user.id:
        # Creator is re-joining their own room
        return {
            "room_code": room.room_code,
            "status": room.status,
            "creator": room.creator.username,
            "joiner": room.joiner.username if room.joiner else None
        }

    if room.joiner_id and room.joiner_id != current_user.id:
        raise HTTPException(status_code=400, detail="Room is already full.")

    # Set joiner and activate room
    room.joiner_id = current_user.id
    room.status = "active"
    db.commit()
    db.refresh(room)

    return {
        "room_code": room.room_code,
        "status": room.status,
        "creator": room.creator.username,
        "joiner": current_user.username
    }

@app.get("/api/room/{room_code}")
def get_room_info(room_code: str, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get current room state."""
    room = db.query(models.ChallengeRoom).filter(
        models.ChallengeRoom.room_code == room_code.upper()
    ).first()
    if not room:
        raise HTTPException(status_code=404, detail="Room not found.")

    return {
        "room_code": room.room_code,
        "status": room.status,
        "creator": room.creator.username,
        "joiner": room.joiner.username if room.joiner else None,
        "problem": room.problem_data,
        "language": room.language,
        "connected_users": room_manager.get_connected_usernames(room.room_code)
    }

@app.websocket("/ws/room/{room_code}")
async def websocket_room(websocket: WebSocket, room_code: str, token: str = Query(...)):
    """
    WebSocket endpoint for real-time Twin Challenge communication.
    Auth via JWT token in query params.
    """
    # Authenticate via token
    payload = auth.decode_access_token(token)
    if not payload:
        await websocket.close(code=4001)
        return
    username = payload.get("sub")
    if not username:
        await websocket.close(code=4001)
        return

    # Validate room exists in DB
    db = next(get_db())
    room = db.query(models.ChallengeRoom).filter(
        models.ChallengeRoom.room_code == room_code.upper()
    ).first()
    if not room:
        await websocket.close(code=4004)
        return

    await websocket.accept()
    room_manager.register_connection(room_code.upper(), username, websocket)

    # Notify others that this user connected
    await room_manager.broadcast(room_code.upper(), {
        "type": "player_connected",
        "username": username,
        "connected_users": room_manager.get_connected_usernames(room_code.upper())
    }, exclude=username)

    # Send current room state to the connecting user
    await room_manager.send_to_user(room_code.upper(), username, {
        "type": "room_state",
        "connected_users": room_manager.get_connected_usernames(room_code.upper()),
        "problem": room.problem_data,
        "language": room.language,
        "opponent_code": room_manager.get_opponent_code(room_code.upper(), username)
    })

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "code_update":
                # User's code changed — store and broadcast to opponent
                code = data.get("code", "")
                room_manager.update_code(room_code.upper(), username, code)
                await room_manager.broadcast(room_code.upper(), {
                    "type": "opponent_code",
                    "username": username,
                    "code": code
                }, exclude=username)
            elif msg_type == "generating_problem":
                # Notify opponent that the other player is generating
                await room_manager.broadcast(room_code.upper(), {
                    "type": "opponent_generating",
                    "username": username
                }, exclude=username)

            elif msg_type == "set_problem":
                # A player selected a problem — save to DB and broadcast
                problem = data.get("problem")
                language = data.get("language", "pyspark")
                room = db.query(models.ChallengeRoom).filter(
                    models.ChallengeRoom.room_code == room_code.upper()
                ).first()
                if room:
                    room.problem_data = problem
                    room.language = language
                    db.commit()
                await room_manager.broadcast(room_code.upper(), {
                    "type": "problem_set",
                    "problem": problem,
                    "language": language,
                    "set_by": username
                })

            elif msg_type == "run_result":
                # Share run results with opponent
                await room_manager.broadcast(room_code.upper(), {
                    "type": "opponent_run_result",
                    "username": username,
                    "result": data.get("result")
                }, exclude=username)

            elif msg_type == "submit_result":
                # Share submit results with opponent
                await room_manager.broadcast(room_code.upper(), {
                    "type": "opponent_submit_result",
                    "username": username,
                    "result": data.get("result")
                }, exclude=username)

            elif msg_type == "chat_message":
                # Relay chat message to opponent
                await room_manager.broadcast(room_code.upper(), {
                    "type": "chat_message",
                    "username": username,
                    "message": data.get("message", ""),
                    "timestamp": datetime.utcnow().isoformat()
                }, exclude=username)

            # --- WebRTC Signaling (voice chat) ---
            elif msg_type in ("webrtc_offer", "webrtc_answer", "webrtc_ice_candidate"):
                # Forward signaling messages directly to the other player
                await room_manager.broadcast(room_code.upper(), {
                    "type": msg_type,
                    "username": username,
                    "data": data.get("data")
                }, exclude=username)
    except WebSocketDisconnect:
        room_manager.remove_connection(room_code.upper(), username)
        # Notify opponent and delete room from DB
        await room_manager.broadcast(room_code.upper(), {
            "type": "room_closed",
            "username": username,
            "message": f"{username} left the room. Room closed."
        })
        # Delete room from DB
        room = db.query(models.ChallengeRoom).filter(
            models.ChallengeRoom.room_code == room_code.upper()
        ).first()
        if room:
            db.delete(room)
            db.commit()
    except Exception:
        room_manager.remove_connection(room_code.upper(), username)
        await room_manager.broadcast(room_code.upper(), {
            "type": "room_closed",
            "username": username,
            "message": f"{username} disconnected. Room closed."
        })
        room = db.query(models.ChallengeRoom).filter(
            models.ChallengeRoom.room_code == room_code.upper()
        ).first()
        if room:
            db.delete(room)
            db.commit()
    finally:
        db.close()


# ------ PRACTICE LIST ROUTES ------

TRACK_METADATA = {
    "window_functions": {
        "track_name": "Window Functions",
        "description": "Master ROW_NUMBER, RANK, LAG, LEAD, running totals, and more with 18 curated real-world problems."
    }
}

def _grade_submission(user_rows, expected_rows):
    """Shared grading logic: returns (passed: bool, message: str, user_output)."""
    if len(user_rows) != len(expected_rows):
        return False, f"Row count mismatch. Expected {len(expected_rows)}, got {len(user_rows)}.", user_rows

    def normalize_val(v):
        if v is None:
            return "None"
        try:
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

    for u_row, e_row in zip(user_rows_sorted, expected_rows_sorted):
        for key, expected_val in e_row.items():
            if key not in u_row:
                return False, f"Missing column '{key}' in your output.", user_rows
            if normalize_val(u_row[key]) != normalize_val(expected_val):
                return False, f"Data mismatch. Expected '{expected_val}' for column '{key}', got '{u_row[key]}'.", user_rows

    return True, "All test cases passed!", user_rows


@app.get("/api/practice-list/tracks")
def get_practice_tracks(user: Optional[models.User] = Depends(get_optional_user), db: Session = Depends(get_db)):
    """List available practice tracks with progress counts."""
    from sqlalchemy import func
    track_counts = db.query(
        models.PracticeListProblem.track, func.count(models.PracticeListProblem.id)
    ).group_by(models.PracticeListProblem.track).all()

    result = []
    for track_id, total in track_counts:
        meta = TRACK_METADATA.get(track_id, {"track_name": track_id, "description": ""})
        solved_count = 0
        if user:
            solved_count = db.query(func.count(func.distinct(models.PracticeListProgress.problem_id))).join(
                models.PracticeListProblem
            ).filter(
                models.PracticeListProgress.user_id == user.id,
                models.PracticeListProblem.track == track_id
            ).scalar() or 0
        result.append({
            "track_id": track_id,
            "track_name": meta["track_name"],
            "description": meta["description"],
            "total_problems": total,
            "solved_count": solved_count,
        })
    return result


@app.get("/api/practice-list/problems")
def get_practice_problems(
    track: str,
    window_function_type: Optional[str] = None,
    difficulty: Optional[str] = None,
    user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """List problems for a track, with optional filters and solved status."""
    query = db.query(models.PracticeListProblem).filter(models.PracticeListProblem.track == track)
    if window_function_type:
        query = query.filter(models.PracticeListProblem.window_function_type == window_function_type)
    if difficulty:
        query = query.filter(models.PracticeListProblem.difficulty == difficulty)
    problems = query.order_by(models.PracticeListProblem.order_index).all()

    # Get user's progress for this track
    solved_map = {}
    if user:
        progress = db.query(models.PracticeListProgress).filter(
            models.PracticeListProgress.user_id == user.id
        ).all()
        for p in progress:
            key = p.problem_id
            if key not in solved_map:
                solved_map[key] = {"pyspark": False, "sql": False}
            solved_map[key][p.language] = True

    result = []
    for prob in problems:
        s = solved_map.get(prob.id, {"pyspark": False, "sql": False})
        result.append({
            "id": prob.id,
            "track": prob.track,
            "title": prob.title,
            "difficulty": prob.difficulty,
            "window_function_type": prob.window_function_type,
            "use_case_category": prob.use_case_category,
            "order_index": prob.order_index,
            "solved_pyspark": s["pyspark"],
            "solved_sql": s["sql"],
        })
    return result


@app.get("/api/practice-list/problems/{problem_id}")
def get_practice_problem_detail(
    problem_id: int,
    user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """Get full problem data for the IDE view."""
    prob = db.query(models.PracticeListProblem).filter(models.PracticeListProblem.id == problem_id).first()
    if not prob:
        raise HTTPException(status_code=404, detail="Problem not found")

    solved_pyspark = False
    solved_sql = False
    if user:
        for lang in ["pyspark", "sql"]:
            exists = db.query(models.PracticeListProgress).filter(
                models.PracticeListProgress.user_id == user.id,
                models.PracticeListProgress.problem_id == problem_id,
                models.PracticeListProgress.language == lang
            ).first()
            if exists:
                if lang == "pyspark":
                    solved_pyspark = True
                else:
                    solved_sql = True

    datasets = prob.datasets
    expected_output = prob.expected_output
    if isinstance(datasets, str):
        datasets = json.loads(datasets)
    if isinstance(expected_output, str):
        expected_output = json.loads(expected_output)

    return {
        "id": prob.id,
        "track": prob.track,
        "title": prob.title,
        "description": prob.description,
        "difficulty": prob.difficulty,
        "window_function_type": prob.window_function_type,
        "use_case_category": prob.use_case_category,
        "order_index": prob.order_index,
        "datasets": datasets,
        "expected_output": expected_output,
        "initial_code_pyspark": prob.initial_code_pyspark,
        "initial_code_sql": prob.initial_code_sql,
        "solved_pyspark": solved_pyspark,
        "solved_sql": solved_sql,
    }


@app.get("/api/practice-list/problems/{problem_id}/solution")
def get_practice_problem_solution(problem_id: int, db: Session = Depends(get_db)):
    """Get solution code and explanation."""
    prob = db.query(models.PracticeListProblem).filter(models.PracticeListProblem.id == problem_id).first()
    if not prob:
        raise HTTPException(status_code=404, detail="Problem not found")
    return {
        "solution_code_pyspark": prob.solution_code_pyspark,
        "solution_code_sql": prob.solution_code_sql,
        "explanation": prob.explanation,
    }


@app.post("/api/practice-list/problems/{problem_id}/submit")
def submit_practice_problem(
    problem_id: int,
    req: schemas.PracticeListSubmitRequest,
    user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """Submit code for a practice list problem, grade it, and track progress."""
    prob = db.query(models.PracticeListProblem).filter(models.PracticeListProblem.id == problem_id).first()
    if not prob:
        raise HTTPException(status_code=404, detail="Problem not found")

    # Execute code
    if req.language == "sql":
        result = execute_sql_code(req.code, req.datasets)
    else:
        result = execute_pyspark_code(req.code, req.datasets)

    if not result["success"]:
        return {
            "success": False,
            "passed": False,
            "user_output": None,
            "message": "Execution failed. Check your syntax.",
            "problem_id": problem_id,
        }

    if result["final_df_rows"] is None:
        msg = ("Grading failed: No result returned from your query."
               if req.language == "sql"
               else "Grading failed: Could not find a 'final_df' variable. Did you assign your final result to 'final_df'?")
        return {
            "success": True,
            "passed": False,
            "user_output": None,
            "message": msg,
            "problem_id": problem_id,
        }

    passed, message, user_output = _grade_submission(result["final_df_rows"], req.expected_output)

    if passed and user:
        # Record progress (upsert — skip if already solved)
        existing = db.query(models.PracticeListProgress).filter(
            models.PracticeListProgress.user_id == user.id,
            models.PracticeListProgress.problem_id == problem_id,
            models.PracticeListProgress.language == req.language,
        ).first()
        if not existing:
            progress = models.PracticeListProgress(
                user_id=user.id,
                problem_id=problem_id,
                language=req.language,
                solved_at=datetime.utcnow().isoformat(),
            )
            db.add(progress)

        # Also log to ActivityLog for streak/XP tracking
        today_str = datetime.now().strftime("%Y-%m-%d")
        new_log = models.ActivityLog(
            user_id=user.id, date=today_str,
            difficulty=req.difficulty, problem_title=req.title
        )
        db.add(new_log)
        db.commit()

    return {
        "success": True,
        "passed": passed,
        "user_output": user_output,
        "message": message,
        "problem_id": problem_id,
    }


@app.get("/api/practice-list/progress")
def get_practice_progress(
    track: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user's progress for a specific track."""
    progress = db.query(models.PracticeListProgress).join(
        models.PracticeListProblem
    ).filter(
        models.PracticeListProgress.user_id == current_user.id,
        models.PracticeListProblem.track == track
    ).all()
    return [
        {"problem_id": p.problem_id, "language": p.language, "solved_at": p.solved_at}
        for p in progress
    ]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
